/**
 * PiAdapter — sessions over one owned `pi --mode rpc` process per thread.
 *
 * Pi owns execution of its own children (the subagents extension runs inside
 * the same process); this adapter normalizes the parent RPC stream into
 * T3's canonical runtime events. Terminal turn settlement follows
 * `agent_settled`, the only Pi event that guarantees no retry, compaction
 * retry, or queued continuation remains.
 *
 * Conversation rollback is reported unsupported: Pi's `fork`/`get_entries`
 * machinery has no tested mapping onto T3's turn-based revert yet.
 *
 * @module provider/Layers/PiAdapter
 */
import type {
  PiSettings,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import {
  Cause,
  Data,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  PubSub,
  Queue,
  Scope,
  Stream,
} from "effect";
import * as Crypto from "effect/Crypto";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as FileSystem from "effect/FileSystem";
import * as DateTime from "effect/DateTime";

import { ServerConfig } from "../../config.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import {
  makePiRpcConnectionIn,
  closePiRpcConnection,
  type PiRpcConnectionHandle,
  type PiRpcEvent,
} from "../pi/PiRpcConnection.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import {
  decodePiBridgeEntryFrame,
  makePiBridgeListener,
  type PiBridgeChildFrame,
  type PiBridgeListener,
} from "../pi/PiBridge.ts";

const PROVIDER = ProviderDriverKind.make("pi");

const PI_RESUME_VERSION = 1;

/**
 * Bounded wait for the extension to quiesce children before teardown. Must
 * exceed the manager's cancellation bound (abortEntry force-closes after 5s)
 * so an ordinary slow child can settle and acknowledge before the process is
 * retired; the bridge's own command timeout (10s) is the outer bound.
 */
const QUIESCE_GRACE_MS = 10_000;

export interface PiAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId | undefined;
  /** Directory new Pi session files are created in (T3 state dir). */
  readonly sessionDir?: string | undefined;
  /** Instance-shared model catalog updated from live sessions. */
  readonly modelCatalog?:
    | {
        readonly set: (
          models: ReadonlyArray<import("@t3tools/contracts").ServerProviderModel>,
        ) => Effect.Effect<void>;
      }
    | undefined;
}

export class PiInterruptedError extends Data.TaggedError("PiInterruptedError")<{}> {}

interface PiResumeCursor {
  readonly schemaVersion: number;
  readonly sessionFile: string;
  readonly sessionId: string | undefined;
}

const parsePiResume = (cursor: unknown): PiResumeCursor | undefined => {
  if (typeof cursor !== "object" || cursor === null) return undefined;
  const record = cursor as Record<string, unknown>;
  if (record.schemaVersion !== PI_RESUME_VERSION) return undefined;
  if (typeof record.sessionFile !== "string" || record.sessionFile.length === 0) return undefined;
  return {
    schemaVersion: PI_RESUME_VERSION,
    sessionFile: record.sessionFile,
    sessionId:
      typeof record.sessionId === "string" && record.sessionId.length > 0
        ? record.sessionId
        : undefined,
  };
};

interface PiTurnRecord {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface PendingExtensionUi {
  readonly nativeId: string;
  readonly method: string;
  readonly deferred: Deferred.Deferred<Record<string, unknown>, never>;
}

interface PiSessionContext {
  readonly threadId: string;
  readonly scope: Scope.Closeable;
  connection: PiRpcConnectionHandle;
  session: ProviderSession;
  turns: Array<PiTurnRecord>;
  activeTurnId: TurnId | undefined;
  promptsInFlight: number;
  interruptedTurnIds: Set<TurnId>;
  /** Latest authoritative assistant outcome for the active turn, settled once at `agent_settled`. */
  turnOutcome: { readonly state: "completed" | "failed"; readonly error?: string } | undefined;
  pumpFiber: Fiber.Fiber<void, never> | undefined;
  pendingExtensionUi: Map<string, PendingExtensionUi>;
  /** Latest cumulative usage from message_update events. */
  lastUsage: Record<string, unknown> | undefined;
  currentModel: string | undefined;
  currentThinkingLevel: string | undefined;
  bridge: PiBridgeListener | undefined;
  stopped: boolean;
  /** Set once the RPC stream reported process exit; ends the event pump loop. */
  exited: boolean;
  /**
   * Resolved when `openChildIds` empties, so teardown can drain accepted
   * terminal child frames before its fallback reconcile.
   */
  childSettlement: Deferred.Deferred<void> | undefined;
  /** Children seen started but not yet terminal; session teardown reconciles them to interruption. */
  openChildIds: Set<string>;
  /**
   * Result identities already receipted for the whole session, keyed by the
   * full generation identity (runId + childId + settleSeq). Stable across
   * turns so a replayed delivery never produces a second receipt.
   */
  consumedResultIdentities: Set<string>;
}

const canonicalItemTypeForTool = (toolName: string): string => {
  const name = toolName.toLowerCase();
  if (name === "bash" || name === "powershell") return "command_execution";
  if (name === "edit" || name === "write") return "file_change";
  return "dynamic_tool_call";
};

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  piSettings: PiSettings,
  options: PiAdapterOptions = {},
): Effect.fn.Return<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  | FileSystem.FileSystem
  | ServerConfig
  | Crypto.Crypto
  | ChildProcessSpawner.ChildProcessSpawner
  | Scope.Scope
> {
  const fileSystem = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* ServerConfig;
  const environment = options.environment;
  const boundInstanceId = options.instanceId;

  const sessions = new Map<string, PiSessionContext>();
  const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomUUIDv4 = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause): ProviderAdapterError =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate Pi runtime identifier.",
          cause,
        }),
    ),
  );
  const nextEventId = Effect.map(randomUUIDv4, (id) => id);
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
    PubSub.publish(runtimeEventPubSub, event);

  interface EmitInput {
    readonly type: ProviderRuntimeEvent["type"];
    readonly turnId?: TurnId | undefined;
    readonly itemId?: string | undefined;
    readonly requestId?: string | undefined;
    readonly payload: unknown;
  }

  const emitEvent = (threadId: string, event: EmitInput) =>
    Effect.gen(function* () {
      const stamp = yield* makeEventStamp();
      const full = {
        eventId: stamp.eventId,
        provider: PROVIDER,
        ...(boundInstanceId !== undefined ? { providerInstanceId: boundInstanceId } : {}),
        threadId,
        createdAt: stamp.createdAt,
        ...event,
      } as ProviderRuntimeEvent;
      yield* offerRuntimeEvent(full);
    });

  const profileEnv = (): NodeJS.ProcessEnv => {
    const base = environment ?? {};
    const resolvedProfile = piSettings.profileDir
      ? expandHomePath(piSettings.profileDir)
      : undefined;
    return resolvedProfile ? { ...base, PI_CODING_AGENT_DIR: resolvedProfile } : base;
  };

  const sessionDir = options.sessionDir;

  const buildSpawnArgs = (input: {
    resume: PiResumeCursor | undefined;
    sessionName: string | undefined;
  }): Array<string> => {
    const args: Array<string> = ["--mode", "rpc"];
    if (input.resume !== undefined) {
      args.push("--session", input.resume.sessionFile);
    } else if (sessionDir !== undefined) {
      args.push("--session-dir", sessionDir);
    }
    if (input.sessionName !== undefined) {
      args.push("--name", input.sessionName.slice(0, 120));
    }
    return args;
  };

  const updateSession = (ctx: PiSessionContext, patch: Partial<ProviderSession>) => {
    ctx.session = { ...ctx.session, ...patch } as ProviderSession;
  };

  // --- Child bridge ---------------------------------------------------------

  /**
   * Bridge frame status → RuntimeTaskStatus. The wire carries the extension's
   * `done`/`error`/`cancelled` vocabulary; canonical task events speak the
   * shared RuntimeTaskStatus vocabulary (completed/failed/cancelled/running).
   */
  const normalizeChildStatus = (
    status: "running" | "done" | "error" | "cancelled",
  ): "running" | "completed" | "failed" | "cancelled" => {
    switch (status) {
      case "done":
        return "completed";
      case "error":
        return "failed";
      case "cancelled":
      case "running":
        return status;
    }
  };

  /**
   * Project one decoded child frame into canonical task events. Every event
   * carries the process-generation `runId` so a reused `sa-1` after a restart
   * cannot collide with history; `task.transcript` is explicitly routed with
   * `taskType: "subagent"` (not merely an absent field) so ingestion never
   * falls back to a constant run id and never folds bulk chunks into the
   * parent activity stream.
   */
  /**
   * Mark one child terminal in the open-child ledger. A terminal `child.status`
   * (done/error/cancelled) settles the child exactly like `child.result`, so a
   * child that already reported `cancelled` is never later overwritten with
   * `interrupted` by the fallback reconcile.
   */
  const markChildTerminal = (ctx: PiSessionContext, childId: string) =>
    Effect.gen(function* () {
      ctx.openChildIds.delete(childId);
      if (ctx.openChildIds.size === 0 && ctx.childSettlement !== undefined) {
        const settlement = ctx.childSettlement;
        ctx.childSettlement = undefined;
        yield* Deferred.succeed(settlement, undefined);
      }
    });

  const handleBridgeFrame = (ctx: PiSessionContext, frame: PiBridgeChildFrame) =>
    Effect.gen(function* () {
      const runId = ctx.bridge?.runId;
      if (runId === undefined) return;
      switch (frame.type) {
        case "child.started":
          ctx.openChildIds.add(frame.childId);
          return yield* emitEvent(ctx.threadId, {
            type: "task.started",
            turnId: ctx.activeTurnId,
            payload: {
              taskId: frame.childId,
              description: frame.title,
              taskType: "subagent",
              agentKind: "agent",
              title: frame.title,
              runId,
              ...(frame.model !== undefined ? { model: frame.model } : {}),
              ...(frame.effort !== undefined ? { effort: frame.effort } : {}),
            },
          });
        case "child.status": {
          const terminal = frame.status !== "running";
          // Publish the terminal state before releasing the drain signal: the
          // drain must not let teardown interrupt this pump fiber mid-emit and
          // drop the child's real final state.
          yield* emitEvent(ctx.threadId, {
            type: "task.updated",
            turnId: ctx.activeTurnId,
            payload: {
              taskId: frame.childId,
              status: normalizeChildStatus(frame.status),
              ...(frame.errorText !== undefined ? { error: frame.errorText } : {}),
              taskType: "subagent",
              agentKind: "agent",
              runId,
            },
          });
          if (terminal) {
            yield* markChildTerminal(ctx, frame.childId);
          }
          return;
        }
        case "child.transcript":
          return yield* emitEvent(ctx.threadId, {
            type: "task.transcript",
            turnId: ctx.activeTurnId,
            payload: {
              taskId: frame.childId,
              runId,
              taskType: "subagent",
              seq: frame.seq,
              chunk: frame.chunk,
            },
          });
        case "child.usage":
          return yield* emitEvent(ctx.threadId, {
            type: "task.progress",
            turnId: ctx.activeTurnId,
            payload: {
              taskId: frame.childId,
              description: "child activity",
              typedUsage: frame.tokens === undefined ? undefined : { totalTokens: frame.tokens },
              taskType: "subagent",
              agentKind: "agent",
              runId,
            },
          });
        case "child.result":
          // Same ordering guarantee as `child.status`: publish before releasing
          // the drain signal so teardown cannot drop this terminal event.
          yield* emitEvent(ctx.threadId, {
            type: "task.completed",
            turnId: ctx.activeTurnId,
            payload: {
              taskId: frame.childId,
              status:
                frame.status === "done"
                  ? "completed"
                  : frame.status === "cancelled"
                    ? "stopped"
                    : "failed",
              ...(frame.finalText !== undefined ? { summary: frame.finalText } : {}),
              ...(frame.errorText !== undefined ? { error: frame.errorText } : {}),
              taskType: "subagent",
              agentKind: "agent",
              runId,
            },
          });
          yield* markChildTerminal(ctx, frame.childId);
          return;
      }
    });

  const isTerminalChildFrame = (frame: PiBridgeChildFrame): boolean =>
    frame.type === "child.result" || (frame.type === "child.status" && frame.status !== "running");

  /** Decode one `entry_appended` RPC event and route bridged child frames. */
  const handleEntryAppended = (ctx: PiSessionContext, value: Record<string, unknown>) => {
    const entry = value["entry"];
    const decoded = decodePiBridgeEntryFrame(entry);
    if (Option.isNone(decoded)) return Effect.void;
    const frame = decoded.value.data;
    // Generation guard: a frame from another process generation (a stale
    // extension still holding an older run id) never mutates this session.
    if (ctx.bridge === undefined || frame.runId !== ctx.bridge.runId) return Effect.void;
    // While a generation is retiring, only terminal child frames still matter:
    // they settle a child's real final state before the fallback reconcile, so
    // the reconcile never overwrites it (e.g. `cancelled` → `interrupted`).
    // Non-terminal frames from a stopped generation are dropped.
    if (ctx.stopped && !isTerminalChildFrame(frame)) return Effect.void;
    return handleBridgeFrame(ctx, frame);
  };

  /**
   * Wait for every accepted child to report a terminal frame before the
   * fallback reconcile. The quiesce ack alone is not enough: already-accepted
   * terminal frames may still be in flight on stdout. Bound by the same grace
   * as quiesce so a stuck child still reconciles.
   */
  const drainAcceptedChildTerminals = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      if (ctx.openChildIds.size === 0) return;
      const settlement = yield* Deferred.make<void>();
      ctx.childSettlement = settlement;
      // Re-check after registering: the pump may have drained the set while the
      // waiter was being created, in which case await would never fire.
      if (ctx.openChildIds.size === 0) {
        ctx.childSettlement = undefined;
        return;
      }
      yield* Deferred.await(settlement).pipe(
        Effect.timeoutOption(QUIESCE_GRACE_MS),
        Effect.ignore,
        Effect.ensuring(
          Effect.sync(() => {
            if (ctx.childSettlement === settlement) ctx.childSettlement = undefined;
          }),
        ),
      );
    });

  /**
   * Reconcile every still-open child to `interrupted` on session teardown,
   * scoped to this process generation's runId. Uses `task.updated` because the
   * task terminal vocabulary (task.completed) has no `interrupted` member;
   * RuntimeTaskStatus does.
   */
  const reconcileOpenChildren = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      const runId = ctx.bridge?.runId;
      if (runId === undefined || ctx.openChildIds.size === 0) return;
      for (const childId of ctx.openChildIds) {
        yield* emitEvent(ctx.threadId, {
          type: "task.updated",
          turnId: ctx.activeTurnId,
          payload: {
            taskId: childId,
            status: "interrupted",
            taskType: "subagent",
            agentKind: "agent",
            runId,
          },
        });
      }
      ctx.openChildIds.clear();
      // Unblock any drain waiting for the set to empty: teardown has decided
      // these children are interrupted.
      if (ctx.childSettlement !== undefined) {
        const settlement = ctx.childSettlement;
        ctx.childSettlement = undefined;
        yield* Deferred.succeed(settlement, undefined);
      }
    });

  // --- Session lifecycle ----------------------------------------------------

  /** Quiesce children, drain accepted terminal frames, then reconcile the rest. */
  const quiesceAndReconcileChildren = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      let quiesced = false;
      if (ctx.bridge !== undefined) {
        quiesced = yield* ctx.bridge.quiesce().pipe(
          Effect.timeoutOption(QUIESCE_GRACE_MS),
          Effect.map((option) => Option.isSome(option) && option.value),
        );
      }
      // Only drain when the companion actually acked quiescing: an ack means its
      // cancellations already published terminal frames ahead of the ack, so
      // the drain Deferred resolves deterministically. A false/absent ack means
      // no terminal frames will follow, so draining would only block until a
      // real-time timeout that teardown must not depend on.
      if (quiesced) {
        yield* drainAcceptedChildTerminals(ctx);
      }
      yield* reconcileOpenChildren(ctx).pipe(Effect.ignore);
    });

  /** Close the process/socket and unregister the in-memory session. */
  const finalizeGeneration = (ctx: PiSessionContext, reason: string) =>
    Effect.gen(function* () {
      if (ctx.pumpFiber !== undefined) {
        yield* Fiber.interrupt(ctx.pumpFiber).pipe(Effect.ignore);
      }
      yield* closePiRpcConnection(ctx.connection);
      sessions.delete(ctx.threadId);
      updateSession(ctx, { status: "closed" });
      yield* emitEvent(ctx.threadId, {
        type: "session.exited",
        payload: { exitKind: "graceful", reason },
      });
    }).pipe(Effect.ignore);

  const retireGeneration = (ctx: PiSessionContext, reason: string) =>
    Effect.gen(function* () {
      yield* resolvePendingExtensionUi(ctx).pipe(Effect.ignore);
      yield* quiesceAndReconcileChildren(ctx);
      yield* finalizeGeneration(ctx, reason);
    }).pipe(Effect.ignore);

  const stopSessionInternal = (ctx: PiSessionContext): Effect.Effect<void, never, never> =>
    Effect.gen(function* () {
      if (ctx.stopped) return;
      // Mark stopped first so any late event is suppressed while we quiesce,
      // drain, and reconcile before retiring the generation.
      ctx.stopped = true;
      yield* retireGeneration(ctx, "stopped");
    }).pipe(Effect.ignore);

  const startSession = (
    input: ProviderSessionStartInput,
  ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
    Effect.gen(function* () {
      const cwd = input.cwd ?? serverConfig.cwd;
      const existing = sessions.get(input.threadId);
      if (existing !== undefined) {
        yield* stopSessionInternal(existing);
      }
      const resume = parsePiResume(input.resumeCursor);

      // The bridge listener is created before the child process so its
      // credentials can ride the spawn environment; its scope is tied to the
      // connection scope so both retire together.
      const bridge = yield* makePiBridgeListener({
        threadId: input.threadId,
        instanceId: boundInstanceId,
      }).pipe(
        Effect.mapError(
          (cause): ProviderAdapterError =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: cause.message,
              cause,
            }),
        ),
      );

      const connection = yield* makePiRpcConnectionIn({
        binaryPath: piSettings.binaryPath,
        args: buildSpawnArgs({ resume, sessionName: input.title }),
        cwd,
        env: { ...profileEnv(), ...bridge.env },
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause): ProviderAdapterError =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: cause.message,
              cause,
            }),
        ),
        // No connection scope exists yet if the spawn fails; retire the bridge
        // listener so its TCP socket does not leak.
        Effect.tapError(() => Scope.close(bridge.scope, Exit.void).pipe(Effect.ignore)),
      );
      // Tie bridge teardown to the connection scope so the listener dies with
      // the child process (explicit scope — no ambient Scope requirement).
      yield* Scope.addFinalizer(connection.scope, Scope.close(bridge.scope, Exit.void));

      // Everything after acquisition is guarded: a get_state failure (timeout,
      // command error) fully retires the connection (and bridge via finalizer)
      // and unregisters the half-built session, so nothing leaks.
      return yield* Effect.gen(function* () {
        const state = yield* connection.request({ type: "get_state" }, { timeoutMs: 15_000 }).pipe(
          Effect.mapError(
            (cause): ProviderAdapterError =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "get_state",
                detail: cause.message,
                cause,
              }),
          ),
        );

        const createdAt = yield* nowIso;
        const ctx: PiSessionContext = {
          threadId: input.threadId,
          scope: connection.scope,
          connection,
          session: {
            provider: PROVIDER,
            ...(boundInstanceId !== undefined ? { providerInstanceId: boundInstanceId } : {}),
            status: "connecting",
            runtimeMode: input.runtimeMode,
            cwd,
            threadId: input.threadId,
            createdAt,
            updatedAt: createdAt,
          },
          turns: [],
          activeTurnId: undefined,
          promptsInFlight: 0,
          interruptedTurnIds: new Set(),
          turnOutcome: undefined,
          pumpFiber: undefined,
          pendingExtensionUi: new Map(),
          lastUsage: undefined,
          currentModel: undefined,
          currentThinkingLevel: undefined,
          bridge,
          stopped: false,
          exited: false,
          childSettlement: undefined,
          openChildIds: new Set(),
          consumedResultIdentities: new Set(),
        };

        const sessionFile =
          typeof state?.["sessionFile"] === "string" ? (state["sessionFile"] as string) : undefined;
        const sessionId =
          typeof state?.["sessionId"] === "string" ? (state["sessionId"] as string) : undefined;
        const model = state?.["model"] as Record<string, unknown> | undefined;
        const modelId =
          typeof model?.["id"] === "string" && typeof model?.["provider"] === "string"
            ? `${model["provider"]}/${model["id"]}`
            : undefined;
        const thinkingLevel =
          typeof state?.["thinkingLevel"] === "string"
            ? (state["thinkingLevel"] as string)
            : undefined;
        ctx.currentModel = modelId;
        ctx.currentThinkingLevel = thinkingLevel;

        updateSession(ctx, {
          status: "ready",
          ...(modelId !== undefined ? { model: modelId } : {}),
          resumeCursor: sessionFile
            ? { schemaVersion: PI_RESUME_VERSION, sessionFile, ...(sessionId ? { sessionId } : {}) }
            : undefined,
        });

        // Register only after get_state proved the process responsive, so a
        // failure above never leaves a half-built session behind.
        sessions.set(input.threadId, ctx);

        // Apply an explicit model selection after the session exists so the
        // picker drives Pi's own provider/model identity.
        if (input.modelSelection !== undefined) {
          yield* applyModelSelection(ctx, input.modelSelection).pipe(Effect.ignore);
        }

        yield* emitEvent(input.threadId, {
          type: "session.started",
          payload: resume !== undefined ? { resume: { sessionFile } } : {},
        });
        yield* emitEvent(input.threadId, {
          type: "session.state.changed",
          payload: { state: "ready" },
        });
        yield* emitEvent(input.threadId, {
          type: "thread.started",
          payload: sessionFile !== undefined ? { providerThreadId: sessionFile } : {},
        });

        // The extension-UI waiter forks scoped work (`Effect.forkScoped`), so
        // the pump needs this session's Scope in its context; provide it here
        // rather than leaking an ambient Scope requirement onto startSession.
        ctx.pumpFiber = yield* Effect.forkIn(
          runEventPump(ctx).pipe(Effect.provideService(Scope.Scope, connection.scope)),
          connection.scope,
        );

        return ctx.session;
      }).pipe(
        Effect.tapError(() =>
          Effect.gen(function* () {
            sessions.delete(input.threadId);
            yield* closePiRpcConnection(connection);
          }).pipe(Effect.ignore),
        ),
      );
    });

  const applyModelSelection = (
    ctx: PiSessionContext,
    selection: {
      readonly model: string;
      readonly options?: ReadonlyArray<{ id: string; value: string | boolean }>;
    },
  ) =>
    Effect.gen(function* () {
      const raw = selection.model;
      const separatorIndex = raw.indexOf("/");
      const provider = separatorIndex > 0 ? raw.slice(0, separatorIndex) : undefined;
      const modelId = separatorIndex > 0 ? raw.slice(separatorIndex + 1) : raw;
      if (provider !== undefined && modelId.length > 0) {
        yield* ctx.connection
          .request({ type: "set_model", provider, modelId }, { timeoutMs: 10_000 })
          .pipe(Effect.ignore);
      }
      const effort = selection.options?.find((option) => option.id === "reasoningEffort");
      if (typeof effort?.value === "string" && effort.value.length > 0) {
        yield* ctx.connection
          .request({ type: "set_thinking_level", level: effort.value }, { timeoutMs: 10_000 })
          .pipe(Effect.ignore);
        ctx.currentThinkingLevel = effort.value;
      }
      updateSession(ctx, { model: raw });
    });

  // --- Event pump ------------------------------------------------------------

  const settleActiveTurn = (
    ctx: PiSessionContext,
    state: "completed" | "failed" | "cancelled",
    options: { readonly stopReason?: string; readonly errorMessage?: string } = {},
  ) =>
    Effect.gen(function* () {
      const turnId = ctx.activeTurnId;
      if (turnId === undefined) return;
      if (ctx.promptsInFlight > 0) ctx.promptsInFlight -= 1;
      ctx.activeTurnId = undefined;
      updateSession(ctx, {
        status: state === "failed" ? "error" : "ready",
        activeTurnId: undefined,
      });
      const usage = ctx.lastUsage;
      yield* emitEvent(ctx.threadId, {
        type: "turn.completed",
        turnId,
        payload: {
          state,
          ...(options.stopReason !== undefined ? { stopReason: options.stopReason } : {}),
          ...(options.errorMessage !== undefined ? { errorMessage: options.errorMessage } : {}),
          ...(usage !== undefined ? { usage } : {}),
          tokenUsage: buildTurnTokenUsage(usage),
        },
      });
    });

  /**
   * Register a provider-initiated continuation run (a settled child queued a
   * follow-up turn while the parent was idle) as a real turn before any recap
   * text streams, so orchestration can persist it.
   */
  const beginProviderInitiatedTurn = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      const turnId = yield* newTurnId;
      ctx.activeTurnId = turnId;
      ctx.promptsInFlight += 1;
      ctx.turnOutcome = undefined;
      ctx.turns.push({ id: turnId, items: [] });
      updateSession(ctx, { status: "running", activeTurnId: turnId });
      yield* emitEvent(ctx.threadId, {
        type: "turn.started",
        turnId,
        payload: {
          ...(ctx.currentModel !== undefined ? { model: ctx.currentModel } : {}),
          ...(ctx.currentThinkingLevel !== undefined ? { effort: ctx.currentThinkingLevel } : {}),
        },
      });
    });

  /**
   * Unwind a freshly created turn whose prompt was rejected before acceptance.
   * Only touches the newly created turn; a rejected steering message must not
   * terminate unrelated accepted work (the caller skips this for steering).
   */
  const unwindRejectedTurn = (ctx: PiSessionContext, turnId: TurnId, detail: string) =>
    Effect.gen(function* () {
      if (ctx.activeTurnId !== turnId) return;
      if (ctx.promptsInFlight > 0) ctx.promptsInFlight -= 1;
      ctx.activeTurnId = undefined;
      ctx.turnOutcome = undefined;
      ctx.turns = ctx.turns.filter((turn) => turn.id !== turnId);
      updateSession(ctx, { status: "ready", activeTurnId: undefined });
      yield* emitEvent(ctx.threadId, {
        type: "turn.completed",
        turnId,
        payload: { state: "failed", stopReason: "prompt-rejected", errorMessage: detail },
      });
    });

  const buildTurnTokenUsage = (usage: Record<string, unknown> | undefined) => {
    if (usage === undefined) return undefined;
    const input = numberOrUndefined(usage["input"]);
    const output = numberOrUndefined(usage["output"]);
    if (input === undefined && output === undefined) return undefined;
    return {
      usageScope: "main_agent" as const,
      usageStatus: "complete" as const,
      hasSubagents: false,
      ...(input !== undefined ? { inputTokens: input } : {}),
      ...(output !== undefined ? { outputTokens: output } : {}),
      ...cachedTokens(usage),
    };
  };

  const cachedTokens = (usage: Record<string, unknown>) => {
    const cacheRead = numberOrUndefined(usage["cacheRead"]);
    return cacheRead !== undefined ? { cachedInputTokens: cacheRead } : {};
  };

  const numberOrUndefined = (value: unknown): number | undefined =>
    typeof value === "number" && Number.isFinite(value) ? value : undefined;

  const runEventPump = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      // Drain until the process exits. Unlike `Stream.runDrain` over an
      // unbounded queue this loop can end on its own, so a crashed process does
      // not leave a fiber blocked on the queue forever.
      while (!ctx.exited) {
        const event = yield* Queue.take(ctx.connection.events);
        yield* handleRpcEvent(ctx, event).pipe(Effect.ignore);
      }
    });

  const handleRpcEvent = (ctx: PiSessionContext, event: PiRpcEvent) =>
    Effect.gen(function* () {
      if (event._tag === "Exited") {
        yield* handleProcessExit(ctx, event.stderrTail);
        return;
      }
      const value = event.value;
      const type = typeof value["type"] === "string" ? value["type"] : "";
      // Once a generation begins retiring, only bridged child terminal frames
      // still need handling (they settle a child's real final state before the
      // fallback reconcile). Drop every other late event — most importantly a
      // stale queued parent continuation — so it can never mutate the closing
      // turn or open a fresh one.
      if (ctx.stopped && type !== "entry_appended") return;
      switch (type) {
        case "agent_start": {
          if (ctx.activeTurnId === undefined) {
            // Provider-initiated continuation (e.g. a settled child queued a
            // follow-up turn). Register a real turn before any recap text
            // streams so orchestration can persist it.
            yield* beginProviderInitiatedTurn(ctx);
            return;
          }
          updateSession(ctx, { status: "running" });
          yield* emitEvent(ctx.threadId, {
            type: "session.state.changed",
            payload: { state: "running" },
          });
          return;
        }
        case "agent_settled": {
          if (ctx.activeTurnId !== undefined && !ctx.interruptedTurnIds.has(ctx.activeTurnId)) {
            const outcome = ctx.turnOutcome;
            if (outcome?.state === "failed") {
              yield* settleActiveTurn(ctx, "failed", {
                stopReason: "error",
                errorMessage: outcome.error ?? "Assistant run failed",
              });
            } else {
              yield* settleActiveTurn(ctx, "completed", { stopReason: "stop" });
            }
          }
          ctx.turnOutcome = undefined;
          return;
        }
        case "message_start": {
          yield* handleMessageStart(ctx, value);
          return;
        }
        case "message_update": {
          yield* handleMessageUpdate(ctx, value);
          return;
        }
        case "message_end": {
          yield* handleMessageEnd(ctx, value);
          return;
        }
        case "auto_retry_end": {
          if (value["success"] === false) {
            ctx.turnOutcome = {
              state: "failed",
              error:
                typeof value["finalError"] === "string"
                  ? value["finalError"]
                  : "Assistant run failed after retries",
            };
          }
          return;
        }
        case "tool_execution_start": {
          yield* handleToolStart(ctx, value);
          return;
        }
        case "tool_execution_update": {
          yield* handleToolUpdate(ctx, value);
          return;
        }
        case "tool_execution_end": {
          yield* handleToolEnd(ctx, value);
          return;
        }
        case "compaction_end": {
          if (value["aborted"] !== true) {
            yield* emitEvent(ctx.threadId, {
              type: "thread.state.changed",
              payload: { state: "compacted" },
            });
          }
          return;
        }
        case "extension_error": {
          yield* emitEvent(ctx.threadId, {
            type: "runtime.warning",
            payload: {
              message:
                typeof value["error"] === "string"
                  ? value["error"]
                  : "Pi extension reported an error",
            },
          });
          return;
        }
        case "extension_ui_request": {
          yield* handleExtensionUiRequest(ctx, value);
          return;
        }
        case "entry_appended": {
          yield* handleEntryAppended(ctx, value);
          return;
        }
        default:
          return;
      }
    });

  const handleMessageUpdate = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const usage = value["usage"];
      if (typeof usage === "object" && usage !== null) {
        ctx.lastUsage = usage as Record<string, unknown>;
        const totalTokens = numberOrUndefined((usage as Record<string, unknown>)["totalTokens"]);
        if (totalTokens !== undefined) {
          yield* emitEvent(ctx.threadId, {
            type: "thread.token-usage.updated",
            payload: { usage: { usedTokens: totalTokens } },
          });
        }
      }
      const delta = value["assistantMessageEvent"];
      if (typeof delta !== "object" || delta === null) return;
      const record = delta as Record<string, unknown>;
      const turnId = ctx.activeTurnId;
      if (turnId === undefined) return;
      const deltaType = record["type"];
      if (deltaType === "text_delta" && typeof record["delta"] === "string") {
        yield* emitEvent(ctx.threadId, {
          type: "content.delta",
          turnId,
          payload: { streamKind: "assistant_text", delta: record["delta"] },
        });
      } else if (deltaType === "thinking_delta" && typeof record["delta"] === "string") {
        yield* emitEvent(ctx.threadId, {
          type: "content.delta",
          turnId,
          payload: { streamKind: "reasoning_text", delta: record["delta"] },
        });
      } else if (deltaType === "toolcall_start") {
        const toolCallId = typeof record["id"] === "string" ? record["id"] : undefined;
        const toolName = typeof record["toolName"] === "string" ? record["toolName"] : "tool";
        if (toolCallId !== undefined) {
          yield* emitEvent(ctx.threadId, {
            type: "item.started",
            turnId,
            itemId: toolCallId,
            payload: {
              itemType: canonicalItemTypeForTool(toolName),
              status: "inProgress",
              title: toolName,
            },
          });
        }
      } else if (deltaType === "toolcall_end") {
        const toolCall = record["toolCall"];
        if (typeof toolCall === "object" && toolCall !== null) {
          const toolRecord = toolCall as Record<string, unknown>;
          const toolCallId = typeof toolRecord["id"] === "string" ? toolRecord["id"] : undefined;
          const toolName = typeof toolRecord["name"] === "string" ? toolRecord["name"] : "tool";
          if (toolCallId !== undefined) {
            yield* emitEvent(ctx.threadId, {
              type: "item.completed",
              turnId,
              itemId: toolCallId,
              payload: {
                itemType: canonicalItemTypeForTool(toolName),
                status: "completed",
                title: toolName,
                data: { arguments: toolRecord["arguments"] },
              },
            });
          }
        }
      }
    });

  const handleMessageEnd = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const message = value["message"];
      if (typeof message !== "object" || message === null) return;
      const record = message as Record<string, unknown>;
      if (record["role"] !== "assistant") return;
      const turnId = ctx.activeTurnId;
      if (turnId === undefined) return;
      const modelId =
        typeof record["model"] === "string" && typeof record["provider"] === "string"
          ? `${record["provider"]}/${record["model"]}`
          : undefined;
      if (modelId !== undefined && ctx.currentModel !== modelId) {
        ctx.currentModel = modelId;
        updateSession(ctx, { model: modelId });
      }
      const stopReason =
        typeof record["stopReason"] === "string" ? record["stopReason"] : undefined;
      ctx.turnOutcome =
        stopReason === "error"
          ? {
              state: "failed",
              error:
                typeof record["errorMessage"] === "string"
                  ? record["errorMessage"]
                  : "Assistant message ended with an error",
            }
          : { state: "completed" };
      yield* emitEvent(ctx.threadId, {
        type: "item.completed",
        turnId,
        payload: {
          itemType: "assistant_message",
          status: "completed",
          data: { stopReason },
        },
      });
    });

  const handleMessageStart = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const message = value["message"];
      if (typeof message !== "object" || message === null) return;
      const record = message as Record<string, unknown>;
      if (record["role"] !== "custom" || record["customType"] !== "subagent-result") return;
      const details =
        typeof record["details"] === "object" && record["details"] !== null
          ? (record["details"] as Record<string, unknown>)
          : {};
      // The companion sends both `id` and `childId` (same value), plus the
      // process generation `runId` and the per-child settle sequence.
      const childId =
        typeof details["childId"] === "string"
          ? details["childId"]
          : typeof details["id"] === "string"
            ? details["id"]
            : undefined;
      const runId = typeof details["runId"] === "string" ? details["runId"] : undefined;
      const settleSeqRaw = details["settleSeq"];
      const settleSeq = typeof settleSeqRaw === "number" ? settleSeqRaw : undefined;
      const title = typeof details["title"] === "string" ? details["title"] : undefined;
      const status = typeof details["status"] === "string" ? details["status"] : undefined;
      const content = typeof record["content"] === "string" ? record["content"] : undefined;
      if (content === undefined || content.trim().length === 0) return;
      const turnId = ctx.activeTurnId;
      if (turnId === undefined) return;
      // Receipt dedup is stable across turns, keyed by the full generation
      // identity (runId + childId + settleSeq): a reused `sa-1` in a new
      // process generation or a repeated child turn stays distinct, while a
      // replayed delivery of the same result never produces a second receipt.
      const resultIdentity = [
        runId,
        childId,
        settleSeq === undefined ? undefined : String(settleSeq),
      ]
        .filter((part): part is string => part !== undefined)
        .join(":");
      const dedupKey = resultIdentity.length > 0 ? resultIdentity : String(turnId);
      if (ctx.consumedResultIdentities.has(dedupKey)) return;
      ctx.consumedResultIdentities.add(dedupKey);
      // Persist a readable receipt for the child result so its output stays
      // visible in the parent conversation even if the recap run fails.
      yield* emitEvent(ctx.threadId, {
        type: "item.completed",
        turnId,
        itemId: `subagent-result:${dedupKey}`,
        payload: {
          itemType: "assistant_message",
          status: "completed",
          title: childId !== undefined ? `Subagent ${childId} result` : "Subagent result",
          detail: content,
          data: {
            source: "subagent-result",
            ...(childId !== undefined ? { childId } : {}),
            ...(runId !== undefined ? { runId } : {}),
            ...(settleSeq !== undefined ? { settleSeq } : {}),
            ...(title !== undefined ? { title } : {}),
            ...(status !== undefined ? { status } : {}),
          },
        },
      });
    });

  const handleToolStart = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const turnId = ctx.activeTurnId;
      const toolCallId = typeof value["toolCallId"] === "string" ? value["toolCallId"] : undefined;
      const toolName = typeof value["toolName"] === "string" ? value["toolName"] : "tool";
      if (turnId === undefined || toolCallId === undefined) return;
      yield* emitEvent(ctx.threadId, {
        type: "item.started",
        turnId,
        itemId: toolCallId,
        payload: {
          itemType: canonicalItemTypeForTool(toolName),
          status: "inProgress",
          title: toolName,
          data: { args: value["args"] },
        },
      });
    });

  const handleToolUpdate = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const turnId = ctx.activeTurnId;
      const toolCallId = typeof value["toolCallId"] === "string" ? value["toolCallId"] : undefined;
      const toolName = typeof value["toolName"] === "string" ? value["toolName"] : "tool";
      if (turnId === undefined || toolCallId === undefined) return;
      yield* emitEvent(ctx.threadId, {
        type: "item.updated",
        turnId,
        itemId: toolCallId,
        payload: {
          itemType: canonicalItemTypeForTool(toolName),
          status: "inProgress",
          title: toolName,
          data: { partialResult: value["partialResult"] },
        },
      });
    });

  const handleToolEnd = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const turnId = ctx.activeTurnId;
      const toolCallId = typeof value["toolCallId"] === "string" ? value["toolCallId"] : undefined;
      const toolName = typeof value["toolName"] === "string" ? value["toolName"] : "tool";
      if (turnId === undefined || toolCallId === undefined) return;
      yield* emitEvent(ctx.threadId, {
        type: "item.completed",
        turnId,
        itemId: toolCallId,
        payload: {
          itemType: canonicalItemTypeForTool(toolName),
          status: value["isError"] === true ? "failed" : "completed",
          title: toolName,
          data: { result: value["result"] },
        },
      });
    });

  const handleProcessExit = (ctx: PiSessionContext, stderrTail: string) =>
    Effect.gen(function* () {
      if (ctx.stopped) return;
      ctx.stopped = true;
      ctx.exited = true;
      // Release any open dialog before failing the turn; the process is gone.
      yield* resolvePendingExtensionUi(ctx);
      // Children never reported a terminal result are interrupted for this
      // generation; the process died out from under them.
      yield* reconcileOpenChildren(ctx).pipe(Effect.ignore);
      // Fail any pending turn honestly; recovery re-runs startSession with
      // the persisted resume cursor.
      if (ctx.activeTurnId !== undefined) {
        yield* settleActiveTurn(ctx, "failed", {
          stopReason: "process-exited",
          errorMessage: stderrTail.length > 0 ? stderrTail.slice(-400) : "Pi process exited",
        });
      }
      // Retire the bridge listener now that the process is gone. Do NOT close
      // `connection.scope` here: this handler runs inside the pump fiber, which
      // is forked in that scope, so closing it inline would self-interrupt. The
      // process already exited and its stream/exit fibers completed; `exited`
      // ends the pump loop on this iteration, so nothing is left behind.
      if (ctx.bridge !== undefined) {
        yield* Scope.close(ctx.bridge.scope, Exit.void).pipe(Effect.ignore);
      }
      sessions.delete(ctx.threadId);
      updateSession(ctx, { status: "error" });
      yield* emitEvent(ctx.threadId, {
        type: "session.exited",
        payload: {
          exitKind: "error",
          reason: stderrTail.length > 0 ? stderrTail.slice(-400) : "Pi process exited",
          recoverable: true,
        },
      });
    });

  // --- Extension UI requests ---------------------------------------------------

  /**
   * Resolve every open dialog as cancelled. Exit and interrupt call this so a
   * waiting answer never blocks the pump or outlives the owning turn.
   */
  const resolvePendingExtensionUi = (ctx: PiSessionContext) =>
    Effect.gen(function* () {
      if (ctx.pendingExtensionUi.size === 0) return;
      const pending = [...ctx.pendingExtensionUi.entries()];
      ctx.pendingExtensionUi.clear();
      for (const [nativeId, entry] of pending) {
        const resolved = yield* Deferred.done(entry.deferred, Exit.succeed({ cancelled: true }));
        if (!resolved) continue;
        yield* emitEvent(ctx.threadId, {
          type: "user-input.resolved",
          requestId: nativeId,
          payload: { answers: { cancelled: true } },
        });
      }
    });

  const handleExtensionUiRequest = (ctx: PiSessionContext, value: Record<string, unknown>) =>
    Effect.gen(function* () {
      const nativeId = typeof value["id"] === "string" ? value["id"] : undefined;
      const method = typeof value["method"] === "string" ? value["method"] : "";
      if (nativeId === undefined) return;
      const dialogMethods = new Set(["select", "confirm", "input", "editor"]);
      if (!dialogMethods.has(method)) {
        // Fire-and-forget notifications (notify/setStatus/setWidget/...) have
        // no T3 surface yet; ignoring them keeps the protocol honest.
        return;
      }
      const deferred = yield* Deferred.make<Record<string, unknown>, never>();
      const entry: PendingExtensionUi = { nativeId, method, deferred };
      ctx.pendingExtensionUi.set(nativeId, entry);
      const question = extensionUiQuestion(method, value);
      yield* emitEvent(ctx.threadId, {
        type: "user-input.requested",
        requestId: nativeId,
        payload: { questions: [question] },
      });
      // Fork the wait so an open dialog never blocks the pump from draining
      // shutdown/interrupt events. Exit and interrupt resolve the pending map
      // directly; the map (not this fiber) is the source of truth.
      yield* Effect.forkScoped(
        Effect.gen(function* () {
          const response = yield* Deferred.await(deferred).pipe(
            Effect.timeoutOption(10 * 60_000),
            Effect.map((option) => (option._tag === "Some" ? option.value : { cancelled: true })),
          );
          if (ctx.pendingExtensionUi.get(nativeId) !== entry) return;
          ctx.pendingExtensionUi.delete(nativeId);
          yield* emitEvent(ctx.threadId, {
            type: "user-input.resolved",
            requestId: nativeId,
            payload: { answers: response },
          });
          yield* ctx.connection
            .respondExtensionUi({ type: "extension_ui_response", id: nativeId, ...response })
            .pipe(Effect.ignore);
        }),
      );
    });

  const extensionUiQuestion = (method: string, value: Record<string, unknown>) => {
    const title = typeof value["title"] === "string" ? value["title"] : "Pi request";
    const message = typeof value["message"] === "string" ? value["message"] : "";
    if (method === "select") {
      const options = Array.isArray(value["options"])
        ? (value["options"] as unknown[]).filter(
            (entry): entry is string => typeof entry === "string",
          )
        : [];
      return {
        id: nativeQuestionId(value),
        header: title,
        question: message.length > 0 ? message : title,
        options: options.map((option) => ({ label: option, description: option })),
      };
    }
    if (method === "confirm") {
      return {
        id: nativeQuestionId(value),
        header: title,
        question: message.length > 0 ? message : title,
        options: [
          { label: "Confirm", description: "Confirm" },
          { label: "Cancel", description: "Cancel" },
        ],
      };
    }
    return {
      id: nativeQuestionId(value),
      header: title,
      question: message.length > 0 ? message : title,
      options: [],
      allowCustomAnswer: true,
    };
  };

  const nativeQuestionId = (value: Record<string, unknown>) =>
    typeof value["id"] === "string" ? value["id"] : "pi-question";

  /**
   * Normalize the actual client answer shapes into the Pi extension UI wire
   * shape. The web client sends a bare string (`{ q1: "Confirm" }`) or a
   * selection array (`{ q1: ["A", "B"] }`); the legacy approval path still
   * sends a wrapped `{ value: ... }` record.
   */
  const normalizeUserInputAnswer = (
    raw: unknown,
  ):
    | { readonly _tag: "value"; readonly value: string }
    | { readonly _tag: "selection"; readonly values: Array<string> }
    | { readonly _tag: "cancelled" } => {
    if (typeof raw === "string") {
      return raw.trim().length > 0 ? { _tag: "value", value: raw } : { _tag: "cancelled" };
    }
    if (Array.isArray(raw)) {
      const values = raw.filter((entry): entry is string => typeof entry === "string");
      return values.length > 0 ? { _tag: "selection", values } : { _tag: "cancelled" };
    }
    if (typeof raw === "object" && raw !== null) {
      const record = raw as Record<string, unknown>;
      if (record["cancelled"] === true) return { _tag: "cancelled" };
      const value = record["value"];
      if (typeof value === "string" && value.trim().length > 0) return { _tag: "value", value };
      if (Array.isArray(value)) {
        const values = value.filter((entry): entry is string => typeof entry === "string");
        if (values.length > 0) return { _tag: "selection", values };
      }
      if (typeof record["confirmed"] === "boolean") {
        return { _tag: "value", value: record["confirmed"] ? "Confirm" : "Cancel" };
      }
    }
    return { _tag: "cancelled" };
  };

  // --- Adapter surface ---------------------------------------------------------

  const requireSession = (threadId: string) => {
    const ctx = sessions.get(threadId);
    if (ctx === undefined || ctx.stopped) {
      return Option.none();
    }
    return Option.some(ctx);
  };

  const adapter: ProviderAdapterShape<ProviderAdapterError> & {
    readonly cancelChild: (input: {
      readonly threadId: ThreadId;
      readonly instanceId: ProviderInstanceId;
      readonly runId: string;
      readonly childId: string;
    }) => Effect.Effect<boolean, never, never>;
  } = {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
      supportsConversationRollback: false,
    },

    startSession,

    sendTurn: (input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const ctxOption = requireSession(input.threadId);
        if (Option.isNone(ctxOption)) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }
        const ctx = ctxOption.value;
        const text = input.input?.trim() ?? "";
        const isSteer = ctx.activeTurnId !== undefined;
        if (!isSteer && text.length === 0 && (input.attachments ?? []).length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or attachments.",
          });
        }

        const images = yield* buildImageAttachments(input);
        const turnId = isSteer ? (ctx.activeTurnId as TurnId) : yield* newTurnId;
        if (!isSteer) {
          ctx.activeTurnId = turnId;
          ctx.promptsInFlight += 1;
          ctx.turnOutcome = undefined;
          ctx.turns.push({ id: turnId, items: [] });
          updateSession(ctx, { status: "running", activeTurnId: turnId });
          yield* emitEvent(input.threadId, {
            type: "turn.started",
            turnId,
            payload: {
              ...(ctx.currentModel !== undefined ? { model: ctx.currentModel } : {}),
              ...(ctx.currentThinkingLevel !== undefined
                ? { effort: ctx.currentThinkingLevel }
                : {}),
            },
          });
        }

        if (input.modelSelection !== undefined) {
          yield* applyModelSelection(ctx, input.modelSelection).pipe(Effect.ignore);
        }

        const command: Record<string, unknown> = { type: "prompt", message: text };
        if (images.length > 0) command["images"] = images;
        if (isSteer) command["streamingBehavior"] = "steer";

        const sent = yield* ctx.connection.request(command, { timeoutMs: 30_000 }).pipe(
          Effect.mapError(
            (cause): ProviderAdapterError =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "prompt",
                detail: cause.message,
                cause,
              }),
          ),
          Effect.exit,
        );

        if (Exit.isFailure(sent)) {
          // A rejected initial prompt must not leave an active turn behind
          // (which would misclassify the next submission as steering); a
          // rejected steering message must not touch accepted work.
          if (!isSteer) {
            yield* unwindRejectedTurn(ctx, turnId, Cause.pretty(sent.cause));
          }
          return yield* Effect.failCause(sent.cause);
        }
        return {
          threadId: input.threadId,
          turnId,
          ...(ctx.session.resumeCursor !== undefined
            ? { resumeCursor: ctx.session.resumeCursor }
            : {}),
        };
      }),

    interruptTurn: (threadId) =>
      Effect.gen(function* () {
        const ctxOption = requireSession(threadId);
        if (Option.isNone(ctxOption)) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const ctx = ctxOption.value;
        const turnId = ctx.activeTurnId;
        if (turnId !== undefined) ctx.interruptedTurnIds.add(turnId);
        // Interrupt retires the process generation, it is not one native abort.
        // Mark the generation stopped FIRST so a stale queued parent
        // continuation can never open a fresh turn, then drop queued
        // continuations and abort the running parent. Children are quiesced,
        // their accepted terminal frames drained, and the rest reconciled
        // before the active turn is settled cancelled and the generation
        // retired. The saved resume cursor stays on the session ProviderService
        // already persisted, so later work resumes via a fresh startSession.
        ctx.stopped = true;
        yield* ctx.connection.send({ type: "clear_queue" }).pipe(Effect.ignore);
        yield* ctx.connection.send({ type: "abort" }).pipe(Effect.ignore);
        // Release any open dialog; the interrupted turn cannot accept answers.
        yield* resolvePendingExtensionUi(ctx).pipe(Effect.ignore);
        yield* quiesceAndReconcileChildren(ctx);
        if (turnId !== undefined) {
          yield* settleActiveTurn(ctx, "cancelled", { stopReason: "interrupted" });
        }
        yield* finalizeGeneration(ctx, "interrupted");
      }),

    respondToUserInput: (threadId, requestId, answers) =>
      Effect.gen(function* () {
        const ctxOption = requireSession(threadId);
        if (Option.isNone(ctxOption)) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const ctx = ctxOption.value;
        const pending = ctx.pendingExtensionUi.get(requestId);
        if (pending === undefined) return;
        const values = (answers as Record<string, unknown> | undefined) ?? {};
        const raw = values[requestId] ?? Object.values(values)[0];
        const answer = normalizeUserInputAnswer(raw);
        const method = pending.method;
        const response: Record<string, unknown> = {};
        if (answer._tag === "cancelled") {
          response["cancelled"] = true;
        } else if (method === "confirm") {
          const label = answer._tag === "selection" ? (answer.values[0] ?? "") : answer.value;
          response["confirmed"] = label.toLowerCase().startsWith("confirm");
        } else {
          const value = answer._tag === "selection" ? (answer.values[0] ?? "") : answer.value;
          response["value"] = value;
        }
        yield* Deferred.done(pending.deferred, Exit.succeed(response));
      }),

    respondToRequest: (threadId, requestId, decision) =>
      adapter.respondToUserInput(threadId, requestId, {
        [requestId]: { value: decision === "accept" ? "Confirm" : "Cancel" },
      } as never),

    /**
     * Optional child-control capability (ChildControlCapability): cancel one
     * bridged child. The full identity must match the live process generation
     * (`runId`), the adapter's bound instance, and a still-open child; anything
     * else is stale and must not touch a live child. The extension
     * acknowledges with the child's settlement.
     */
    cancelChild: (input) =>
      Effect.gen(function* () {
        const ctx = sessions.get(input.threadId);
        if (ctx === undefined || ctx.stopped || ctx.bridge === undefined) return false;
        if (ctx.bridge.runId !== input.runId) return false;
        if (boundInstanceId === undefined || boundInstanceId !== input.instanceId) return false;
        if (!ctx.openChildIds.has(input.childId)) return false;
        // The bridge already bounds acknowledgment at its 10s outer grace
        // (exceeding the manager's 5s cancellation drain), so no nested
        // timeout here — a 5s wrap could fire just before the ack lands.
        return yield* ctx.bridge.cancelChild(input.childId);
      }),

    stopSession: (threadId) =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (ctx === undefined) return;
        yield* stopSessionInternal(ctx);
      }),

    listSessions: () => Effect.sync(() => [...sessions.values()].map((ctx) => ctx.session)),

    hasSession: (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      }),

    readThread: (threadId) =>
      Effect.gen(function* () {
        const ctxOption = requireSession(threadId);
        if (Option.isNone(ctxOption)) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const ctx = ctxOption.value;
        return {
          threadId,
          turns: ctx.turns.map((turn) => ({ id: turn.id, items: turn.items })),
        };
      }),

    rollbackThread: (threadId) =>
      Effect.gen(function* () {
        void threadId;
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "fork",
          detail: "Pi sessions do not support provider-side rollback in T3 Code yet.",
        });
      }),

    stopAll: (): Effect.Effect<void, never, never> =>
      Effect.forEach([...sessions.values()], (ctx) => stopSessionInternal(ctx), {
        discard: true,
      }),

    streamEvents: Stream.fromPubSub(runtimeEventPubSub),
  };

  const newTurnId: Effect.Effect<TurnId, ProviderAdapterError> = Effect.map(
    randomUUIDv4,
    (id) => id as unknown as TurnId,
  );

  const buildImageAttachments = (input: ProviderSendTurnInput) =>
    Effect.forEach(
      (input.attachments ?? []).filter((attachment) => attachment.type === "image"),
      (attachment) =>
        Effect.gen(function* () {
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (attachmentPath === null) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          }
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause): ProviderAdapterError =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "prompt",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          return {
            type: "image",
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          };
        }),
    );

  yield* Effect.addFinalizer(() => adapter.stopAll().pipe(Effect.ignore));

  return adapter;
});
