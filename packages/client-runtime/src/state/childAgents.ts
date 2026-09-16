/**
 * Child-agent roster and transcript state shared by the web and desktop
 * clients (and available to mobile once it grows a surface).
 *
 * The wire contracts for `childAgent.*` are owned by `@t3tools/contracts`.
 * This module mirrors the agreed protocol on the client side so components
 * stay dumb and the behavior is testable without rendering markup:
 *
 * - full child identity `{threadId, instanceId, runId, childId}` on every
 *   transcript read and cancel;
 * - a typed flat chunk `item` union for text / thinking / user / tool calls /
 *   tool results, where `finalize` replaces a streamed text item;
 * - `childAgent.subscribeChanges({threadId})` notices carrying either
 *   `{type: "reset"}` or `{type: "changed", ...identity}`.
 *
 * Three small controllers own the concurrency the panels otherwise get wrong:
 * the roster list never fans out or drops a notice, transcripts never overlap
 * reads or let a stale loop mutate new state, pages dedupe by sequence, and
 * cancellation reports an explicit outcome.
 */
import type {
  ChildAgentChangeEvent,
  ChildAgentIdentity,
  ChildAgentState,
  ChildTranscriptChunk,
  ChildTranscriptItem,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import type { AgentPanelModel, RuntimeSubagent } from "./subagentRuntime.ts";

export type {
  ChildAgentChangeEvent,
  ChildAgentIdentity,
  ChildAgentState,
  ChildTranscriptChunk,
  ChildTranscriptItem,
};

// --- Identity -----------------------------------------------------------------

/** Stable, collision-free key for a full child identity. */
export function childAgentIdentityKey(identity: ChildAgentIdentity): string {
  return [identity.threadId, identity.instanceId, identity.runId, identity.childId].join("\u0000");
}

/** The identity carried by a durable child record. */
export function childAgentIdentityOf(child: ChildAgentState): ChildAgentIdentity {
  return {
    threadId: child.threadId,
    instanceId: child.instanceId,
    runId: child.runId,
    childId: child.childId,
  };
}

// --- Transcript chunks --------------------------------------------------------

export const CHILD_TRANSCRIPT_PAGE_SIZE = 200;
const MAX_SEQUENTIAL_CATCHUP_PAGES = 25;

/** A rendered transcript row: one visual unit in the viewer. */
export interface TranscriptRow {
  readonly key: string;
  readonly kind: "user" | "assistant" | "thinking" | "tool";
  readonly label: string | null;
  readonly text: string;
  readonly isError?: boolean;
}

/**
 * Folds an ordered chunk list into renderable rows.
 *
 * Every item kind is keyed by a stable row key — `{kind}:{itemId}` for text /
 * thinking / user and `{kind}:{itemId}:{toolId}` for tool calls and results —
 * and later writes for the SAME key replace the earlier row instead of
 * appending a duplicate. This is what makes a live (provisional) tool result
 * upgrade to its finalized form in place, and what stops an `append` that
 * arrives after a `finalize` from re-opening a closed item.
 */
export function deriveTranscriptRows(
  chunks: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>,
): ReadonlyArray<TranscriptRow> {
  type ItemKind = "text" | "thinking" | "user" | "toolCall" | "toolResult";
  const itemRowKey = (itemId: string, kind: ItemKind, toolId?: string): string =>
    kind === "toolCall" || kind === "toolResult"
      ? `${kind}:${itemId}:${toolId ?? ""}`
      : `${kind}:${itemId}`;

  const rows: TranscriptRow[] = [];
  const indexByKey = new Map<string, number>();
  // Item ids currently open as a streamed text/thinking row. A `finalize`/`add`
  // closes them; a later `append` for a closed id is ignored rather than
  // creating a second row for the same item.
  const openKeys = new Set<string>();

  const upsert = (key: string, row: TranscriptRow): void => {
    const index = indexByKey.get(key);
    if (index !== undefined) {
      rows[index] = row;
    } else {
      indexByKey.set(key, rows.length);
      rows.push(row);
    }
  };

  for (const { chunk } of chunks) {
    if (chunk.op === "append") {
      const key = itemRowKey(chunk.itemId, chunk.kind);
      const existing = indexByKey.get(key);
      if (existing === undefined) {
        // Brand-new streamed item: open it.
        openKeys.add(key);
        upsert(key, {
          key,
          kind: chunk.kind === "thinking" ? "thinking" : "assistant",
          label: null,
          text: chunk.text,
        });
      } else if (openKeys.has(key)) {
        // Extend the still-open item.
        const row = rows[existing];
        if (row !== undefined) {
          rows[existing] = { ...row, text: row.text + chunk.text };
        }
      }
      // Otherwise the item was already finalized: ignore the late append so it
      // can never re-open a closed fact or duplicate the row.
      continue;
    }

    const item = chunk.item;
    switch (item.kind) {
      case "text":
      case "thinking": {
        const key = itemRowKey(chunk.itemId, item.kind);
        openKeys.delete(key);
        upsert(key, {
          key,
          kind: item.kind === "thinking" ? "thinking" : "assistant",
          label: null,
          text: item.text,
        });
        break;
      }
      case "user": {
        const key = itemRowKey(chunk.itemId, "user");
        upsert(key, { key, kind: "user", label: null, text: item.text });
        break;
      }
      case "toolCall": {
        const key = itemRowKey(chunk.itemId, "toolCall", item.toolId);
        upsert(key, { key, kind: "tool", label: item.name, text: item.argsPreview ?? "" });
        break;
      }
      case "toolResult": {
        const key = itemRowKey(chunk.itemId, "toolResult", item.toolId);
        upsert(key, {
          key,
          kind: "tool",
          label: item.name,
          text: item.outputPreview ?? "",
          isError: item.isError,
        });
        break;
      }
    }
  }

  return rows;
}

/**
 * Appends an incoming page onto the existing chunk list, deduplicating by
 * sequence number so duplicate deliveries and overlapping pages never render
 * the same chunk twice. Output stays sorted by sequence.
 */
export function appendTranscriptChunks(
  existing: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>,
  incoming: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>,
): ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }> {
  if (incoming.length === 0) {
    return existing;
  }
  const merged = new Map<number, { readonly seq: number; readonly chunk: ChildTranscriptChunk }>();
  for (const entry of existing) {
    merged.set(entry.seq, entry);
  }
  for (const entry of incoming) {
    if (!merged.has(entry.seq)) {
      merged.set(entry.seq, entry);
    }
  }
  return Array.from(merged.values()).sort((a, b) => a.seq - b.seq);
}

// --- Roster merge -------------------------------------------------------------

/** The panel model extended with the durable provider-child roster. */
export interface ChildAgentPanelModel extends AgentPanelModel {
  readonly providerChildren: ReadonlyArray<ChildAgentState>;
}

/**
 * Exact activity-reconciliation key for a roster row. Rows without a canonical
 * child run id (legacy rows, non-bridged providers) return null and NEVER match
 * a durable child — a bare child-id comparison is deliberately not a match.
 */
function exactChildActivityKey(agent: RuntimeSubagent): string | null {
  if (agent.childRunId === null) {
    return null;
  }
  return `${agent.id}\u0000${agent.childRunId}`;
}

function exactDurableChildKey(child: ChildAgentState): string {
  return `${child.childId}\u0000${child.runId}`;
}

export function childAgentStatusIsLive(status: ChildAgentState["status"]): boolean {
  return status === "running";
}

export function childAgentStatusIsTerminal(status: ChildAgentState["status"]): boolean {
  return status !== "running";
}

/**
 * Merges durable provider children into the activity-derived panel model.
 *
 * - The provider list is deduplicated by full identity first (duplicate
 *   deliveries must not double-render).
 * - A provider child REPLACES an ordinary activity row only when both agree on
 *   the exact (childId, runId) tuple. A reused `sa-1` across generations stays
 *   two distinct rows, and the durable row — the one that owns transcript and
 *   cancel — survives even when the bridge also emitted the canonical task
 *   row. Rows without a run id are preserved unchanged.
 * - The removed activity twin is subtracted and the durable child contributes
 *   its own live/settled/token count, so the footer and the ChatView indicator
 *   agree without double-counting.
 *
 * Existing providers are never touched: workflows pass through unchanged and
 * non-matching direct agents are preserved. (Pi children are direct spawns;
 * no durable child maps to a workflow coordinator or member today.)
 */
export function mergeProviderChildren(
  model: AgentPanelModel,
  children: ReadonlyArray<ChildAgentState>,
): ChildAgentPanelModel {
  const unique: ChildAgentState[] = [];
  const seen = new Set<string>();
  for (const child of children) {
    const key = childAgentIdentityKey(childAgentIdentityOf(child));
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(child);
  }

  const durableKeys = new Set(unique.map(exactDurableChildKey));

  const keptDirectAgents: RuntimeSubagent[] = [];
  let runningCount = model.runningCount;
  let waitingCount = model.waitingCount;
  let idleCount = model.idleCount;
  let settledCount = model.settledCount;
  let totalTokens = model.totalTokens;

  for (const agent of model.directAgents) {
    const key = exactChildActivityKey(agent);
    if (key !== null && durableKeys.has(key)) {
      if (agent.status === "running" || agent.status === "pending") runningCount -= 1;
      else if (agent.status === "waiting") waitingCount -= 1;
      else if (agent.status === "idle") idleCount -= 1;
      else settledCount -= 1;
      totalTokens -= agent.usage?.totalTokens ?? 0;
      continue;
    }
    keptDirectAgents.push(agent);
  }

  for (const child of unique) {
    if (childAgentStatusIsLive(child.status)) {
      runningCount += 1;
    } else {
      settledCount += 1;
    }
    totalTokens += child.tokens ?? 0;
  }

  const liveCount = runningCount + waitingCount;
  return {
    ...model,
    directAgents: keptDirectAgents,
    providerChildren: unique,
    runningCount,
    waitingCount,
    idleCount,
    settledCount,
    totalTokens,
    liveCount,
    hasAgents: keptDirectAgents.length > 0 || model.workflows.length > 0 || unique.length > 0,
  };
}

export function childAgentErrorMessage(cause: unknown): string {
  const error = cause instanceof Error ? cause : null;
  if (error !== null && error.message.trim().length > 0) {
    return error.message;
  }
  return "The request failed.";
}

// --- Transcript controller ----------------------------------------------------

export interface ChildTranscriptPage {
  readonly chunks: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>;
  readonly hasMore: boolean;
  readonly lastSeq: number | null;
}

export interface ChildTranscriptReadRequest {
  readonly environmentId: EnvironmentId;
  readonly identity: ChildAgentIdentity;
  readonly afterSeq: number | null;
  readonly limit: number;
}

export interface ChildTranscriptReader {
  readonly readPage: (request: ChildTranscriptReadRequest) => Promise<ChildTranscriptPage>;
}

export interface ChildTranscriptControllerState {
  readonly chunks: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>;
  readonly hasMore: boolean;
  readonly loading: boolean;
  readonly error: string | null;
  readonly lastSeq: number | null;
}

export interface ChildTranscriptController {
  readonly getState: () => ChildTranscriptControllerState;
  readonly subscribe: (listener: () => void) => () => void;
  /** Identity/environment change: clear synchronously, drop in-flight reads. */
  readonly reset: (target: {
    readonly environmentId: EnvironmentId;
    readonly identity: ChildAgentIdentity;
  }) => void;
  /** Reconnect reset: catch up from the existing cursor, never re-fetch history. */
  readonly reload: () => Promise<void>;
  /** First page after a reset. */
  readonly loadInitial: () => Promise<void>;
  /** One more page after the current cursor (manual load-more). */
  readonly loadMore: () => Promise<void>;
  /** Sequential catch-up until the transcript is current or the bound is hit. */
  readonly catchUp: () => Promise<void>;
}

export interface ChildTranscriptTarget {
  readonly environmentId: EnvironmentId;
  readonly identity: ChildAgentIdentity;
}

type TranscriptReadMode = "bounded" | "catchUp";

/** Coalesce a requested mode into the queue, keeping the stronger mode. */
const upgradeReadMode = (
  current: TranscriptReadMode | null,
  next: TranscriptReadMode,
): TranscriptReadMode => (current === "catchUp" || next === "catchUp" ? "catchUp" : "bounded");

/**
 * One serialized read run per generation. Bumping `generation` hands the read
 * line to a fresh run, so a loop still awaiting an old run's read can never
 * observe the new state or drain the new run's queue.
 */
interface TranscriptReadRun {
  readonly generation: number;
  active: boolean;
  queued: TranscriptReadMode | null;
}

/**
 * Owns cursor pagination, sequence dedupe, and the read-race guard. All reads
 * (first page, manual load-more, reconnect/catch-up) run through one
 * generation-bound serialized loop:
 *
 * - `bounded` pages (initial/load-more) read exactly one page plus one
 *   trailing page per invalidation queued while a read was in flight, so a
 *   `changed` notice is never dropped; they never poll unprompted.
 * - `catchUp` pages the transcript from the current cursor (no history
 *   re-fetch) until it reports no further data or the page bound is hit.
 * - `reset` bumps the generation and replaces the run, discarding any
 *   in-flight read so a stale loop cannot mutate the new state.
 */
export function createChildTranscriptController(
  reader: ChildTranscriptReader,
): ChildTranscriptController {
  let state: ChildTranscriptControllerState = {
    chunks: [],
    hasMore: false,
    loading: false,
    error: null,
    lastSeq: null,
  };
  let target: ChildTranscriptTarget | null = null;
  let generation = 0;
  let run: TranscriptReadRun = { generation, active: false, queued: null };
  const listeners = new Set<() => void>();

  const emit = (): void => {
    for (const listener of listeners) {
      listener();
    }
  };
  const setState = (next: ChildTranscriptControllerState): void => {
    state = next;
    emit();
  };
  const clear = (next: ChildTranscriptTarget): void => {
    generation += 1;
    target = next;
    run = { generation, active: false, queued: null };
    setState({ chunks: [], hasMore: false, loading: false, error: null, lastSeq: null });
  };

  // One bounded page read. Only the serialized loop calls this, so reads never
  // overlap. Returns null when the owning run was superseded mid-flight
  // (callers must stop immediately), otherwise whether a non-empty page still
  // reports more data to fetch.
  const readOne = async (active: TranscriptReadRun): Promise<{ readonly more: boolean } | null> => {
    if (target === null || active !== run || active.generation !== generation) {
      return null;
    }
    const current = target;
    const afterSeq = state.lastSeq;
    setState({ ...state, loading: state.chunks.length === 0, error: null });
    try {
      const page = await reader.readPage({
        environmentId: current.environmentId,
        identity: current.identity,
        afterSeq,
        limit: CHILD_TRANSCRIPT_PAGE_SIZE,
      });
      if (active !== run || active.generation !== generation) {
        return null;
      }
      const chunks = appendTranscriptChunks(state.chunks, page.chunks);
      const lastSeq = page.lastSeq ?? page.chunks.at(-1)?.seq ?? state.lastSeq;
      setState({ chunks, hasMore: page.hasMore, loading: false, error: null, lastSeq });
      return { more: page.hasMore && page.chunks.length > 0 };
    } catch (cause) {
      if (active !== run || active.generation !== generation) {
        return null;
      }
      setState({ ...state, loading: false, error: childAgentErrorMessage(cause) });
      return { more: false };
    }
  };

  // Serialized drain for one run. `bounded` stops after one page plus any
  // queued invalidation; `catchUp` also continues while `hasMore` reports a
  // non-empty page, up to the sequential bound.
  const drain = async (
    initialMode: TranscriptReadMode,
    active: TranscriptReadRun,
  ): Promise<void> => {
    let mode = initialMode;
    let pages = 0;
    for (;;) {
      if (active !== run || active.generation !== generation) {
        return;
      }
      if (mode === "catchUp") {
        pages += 1;
        if (pages > MAX_SEQUENTIAL_CATCHUP_PAGES) {
          return;
        }
      }
      const outcome = await readOne(active);
      if (active !== run || active.generation !== generation) {
        return;
      }
      if (outcome === null) {
        return;
      }
      if (active.queued !== null) {
        // A notice/request landed while this read was in flight: drain it as
        // one more read rather than dropping it. A queued catch-up upgrades
        // the drain so a coalesced reconnect is not truncated to one page.
        mode = upgradeReadMode(mode, active.queued);
        active.queued = null;
        continue;
      }
      if (mode === "catchUp" && outcome.more) {
        continue;
      }
      return;
    }
  };

  const request = async (mode: TranscriptReadMode): Promise<void> => {
    if (target === null) {
      return;
    }
    const active = run;
    if (active.generation !== generation) {
      return;
    }
    if (active.active) {
      // Another read already owns this generation's line: coalesce instead of
      // overlapping.
      active.queued = upgradeReadMode(active.queued, mode);
      return;
    }
    active.active = true;
    try {
      await drain(mode, active);
    } finally {
      if (active === run && active.generation === generation) {
        const queued = active.queued;
        active.active = false;
        active.queued = null;
        // A request can arrive in the microtask gap after the drain resolved;
        // start one trailing pass rather than dropping the latest change.
        if (queued !== null) {
          void request(queued);
        }
      }
    }
  };

  return {
    getState: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    reset: clear,
    reload: () => request("catchUp"),
    loadInitial: () => request("bounded"),
    loadMore: () => request("bounded"),
    catchUp: () => request("catchUp"),
  };
}

// --- Cancel controller --------------------------------------------------------

export type ChildCancelStatus = "idle" | "cancelling" | "cancelled" | "not-cancelled" | "failed";

export interface ChildCancelControllerState {
  readonly status: ChildCancelStatus;
  readonly message: string | null;
}

export interface ChildCancelRequest {
  readonly environmentId: EnvironmentId;
  readonly identity: ChildAgentIdentity;
}

export interface ChildCancelDeps {
  readonly cancel: (request: ChildCancelRequest) => Promise<{ readonly cancelled: boolean }>;
}

export interface ChildCancelController {
  readonly getState: () => ChildCancelControllerState;
  readonly subscribe: (listener: () => void) => () => void;
  readonly reset: (target: ChildCancelRequest) => void;
  readonly cancel: () => Promise<void>;
}

/**
 * Cancellation is single-flight and generation-safe. A reset (thread, child,
 * or selection change) bumps the generation; a cancel that resolves afterwards
 * is discarded instead of overwriting the new selection's state. Concurrent
 * `cancel` calls are ignored while one is already in flight, and the outcome
 * is explicit: `cancelled`, `not-cancelled`, or `failed` (retryable).
 */
export function createChildCancelController(deps: ChildCancelDeps): ChildCancelController {
  let state: ChildCancelControllerState = { status: "idle", message: null };
  let target: ChildCancelRequest | null = null;
  let generation = 0;
  let inFlight = false;
  const listeners = new Set<() => void>();

  const emit = (): void => {
    for (const listener of listeners) {
      listener();
    }
  };
  const setState = (next: ChildCancelControllerState): void => {
    state = next;
    emit();
  };

  return {
    getState: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    reset: (next) => {
      generation += 1;
      inFlight = false;
      target = next;
      setState({ status: "idle", message: null });
    },
    cancel: async () => {
      if (target === null || inFlight) {
        return;
      }
      const gen = generation;
      const request = target;
      inFlight = true;
      setState({ status: "cancelling", message: null });
      try {
        const result = await deps.cancel(request);
        if (gen !== generation) {
          return;
        }
        setState(
          result.cancelled
            ? { status: "cancelled", message: null }
            : { status: "not-cancelled", message: "The child could not be cancelled." },
        );
      } catch (cause) {
        if (gen !== generation) {
          return;
        }
        setState({ status: "failed", message: childAgentErrorMessage(cause) });
      } finally {
        if (gen === generation) {
          inFlight = false;
        }
      }
    },
  };
}

// --- Roster controller --------------------------------------------------------

export type ChildAgentRosterListResult =
  | { readonly _tag: "Success"; readonly children: ReadonlyArray<ChildAgentState> }
  | { readonly _tag: "Interrupted" }
  | { readonly _tag: "Failed"; readonly message: string };

export interface ChildAgentRosterScope {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}

export function childAgentRosterScopeKey(scope: ChildAgentRosterScope): string {
  return `${scope.environmentId}:${scope.threadId}`;
}

export interface ChildAgentRosterState {
  readonly scope: string | null;
  readonly children: ReadonlyArray<ChildAgentState>;
  readonly error: string | null;
  readonly loading: boolean;
}

export interface ChildAgentRosterController {
  readonly getState: () => ChildAgentRosterState;
  readonly subscribe: (listener: () => void) => () => void;
  readonly reset: (scope: ChildAgentRosterScope | null) => void;
  /** Serialized coalesced refresh; resolves after any queued trailing read. */
  readonly refresh: () => Promise<void>;
}

/**
 * Owns the roster list read for one scope, serialized and coalesced so a
 * burst of change notices collapses to at most one follow-up request instead
 * of fanning out or dropping the latest data. `reset` (scope change or
 * unmount) bumps the generation and drops any in-flight response, so a stale
 * response can never regress the rows. Retryable failures keep the rows that
 * were last loaded, and an intentional interruption is not surfaced as a user
 * error.
 */
export function createChildAgentRosterController(deps: {
  readonly list: (scope: ChildAgentRosterScope) => Promise<ChildAgentRosterListResult>;
}): ChildAgentRosterController {
  let state: ChildAgentRosterState = { scope: null, children: [], error: null, loading: false };
  let target: ChildAgentRosterScope | null = null;
  let generation = 0;
  let run: { readonly generation: number; active: boolean; queued: boolean } = {
    generation,
    active: false,
    queued: false,
  };
  const listeners = new Set<() => void>();
  const emit = (): void => {
    for (const listener of listeners) {
      listener();
    }
  };
  const setState = (next: ChildAgentRosterState): void => {
    state = next;
    emit();
  };

  const refresh = async (): Promise<void> => {
    if (target === null) {
      return;
    }
    const currentRun = run;
    const gen = generation;
    const scope = target;
    if (currentRun.active) {
      currentRun.queued = true;
      return;
    }
    currentRun.active = true;
    try {
      for (;;) {
        if (currentRun !== run || gen !== generation) {
          return;
        }
        const result = await deps.list(scope);
        if (currentRun !== run || gen !== generation) {
          return;
        }
        if (result._tag === "Success") {
          setState({
            scope: childAgentRosterScopeKey(scope),
            children: result.children,
            error: null,
            loading: false,
          });
        } else if (result._tag === "Interrupted") {
          // Superseded, not a user-facing failure: keep the current rows.
          setState({ ...state, loading: false });
        } else {
          // Retryable failure: keep the previously-loaded rows visible; the
          // next notice or a manual refresh retries.
          setState({ ...state, loading: false, error: result.message });
        }
        // Drain a trailing read queued while this read was in flight.
        if (!currentRun.queued) {
          return;
        }
        currentRun.queued = false;
      }
    } finally {
      if (currentRun === run && gen === generation) {
        currentRun.active = false;
        currentRun.queued = false;
      }
    }
  };

  return {
    getState: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    reset: (scope) => {
      generation += 1;
      target = scope;
      run = { generation, active: false, queued: false };
      setState({
        scope: scope === null ? null : childAgentRosterScopeKey(scope),
        children: [],
        error: null,
        loading: scope !== null,
      });
    },
    refresh,
  };
}
