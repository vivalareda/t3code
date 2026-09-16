import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId, type ChildAgentState } from "@t3tools/contracts";
import type { AgentPanelModel, RuntimeSubagent } from "./subagentRuntime.ts";
import { deriveAgentPanelModel } from "./subagentRuntime.ts";
import {
  appendTranscriptChunks,
  childAgentIdentityKey,
  createChildAgentRosterController,
  createChildCancelController,
  createChildTranscriptController,
  deriveTranscriptRows,
  mergeProviderChildren,
  type ChildAgentIdentity,
  type ChildAgentRosterListResult,
  type ChildTranscriptChunk,
  type ChildTranscriptPage,
  type ChildTranscriptReadRequest,
} from "./childAgents.ts";

const ENV_A = EnvironmentId.make("env-a");
const ENV_B = EnvironmentId.make("env-b");

function childState(fields: {
  threadId: string;
  instanceId: string;
  runId: string;
  childId: string;
  status?: ChildAgentState["status"];
  title?: string | null;
  tokens?: number | null;
}): ChildAgentState {
  return {
    threadId: fields.threadId,
    instanceId: fields.instanceId,
    runId: fields.runId,
    childId: fields.childId,
    title: fields.title ?? null,
    backend: null,
    model: null,
    effort: null,
    status: fields.status ?? "running",
    summary: null,
    errorText: null,
    tokens: fields.tokens ?? null,
    startedAt: null,
    settledAt: null,
  } as unknown as ChildAgentState;
}

function identity(
  overrides: Partial<Record<keyof ChildAgentIdentity, string>> = {},
): ChildAgentIdentity {
  return {
    threadId: "thread-a",
    instanceId: "pi",
    runId: "run-1",
    childId: "sa-1",
    ...overrides,
  } as unknown as ChildAgentIdentity;
}

function directAgent(id: string, childRunId?: string): RuntimeSubagent {
  return {
    id,
    kind: "subagent",
    title: id,
    role: null,
    model: null,
    effort: null,
    status: "running",
    activationCount: 1,
    usage: null,
    progress: null,
    lastToolName: null,
    result: null,
    error: null,
    outputFile: null,
    parentAgentId: null,
    agentIndex: null,
    phaseIndex: null,
    phaseTitle: null,
    attempt: null,
    workflowName: null,
    phases: [],
    runHandles: null,
    childRunId: childRunId ?? null,
    recentActivity: [],
    firstSeenAt: "2026-08-01T10:00:00.000Z",
    startedAt: null,
    completedAt: null,
    updatedAt: "2026-08-01T10:00:00.000Z",
  } as unknown as RuntimeSubagent;
}

/** A model whose counts are consistent with its direct agents. */
function panelWithDirectAgents(agents: ReadonlyArray<RuntimeSubagent>): AgentPanelModel {
  return deriveAgentPanelModel({ agents });
}

/** An explicitly empty panel model. */
function emptyPanel(): AgentPanelModel {
  return deriveAgentPanelModel({ agents: [] });
}

function chunk(
  seq: number,
  chunkValue: ChildTranscriptChunk,
): { readonly seq: number; readonly chunk: ChildTranscriptChunk } {
  return { seq, chunk: chunkValue };
}

function append(seq: number, itemId: string, text: string, kind: "text" | "thinking" = "text") {
  return chunk(seq, { op: "append", itemId, kind, text });
}

function finalizeText(seq: number, itemId: string, text: string) {
  return chunk(seq, { op: "finalize", itemId, item: { kind: "text", text } });
}

function emptyPage(): ChildTranscriptPage {
  return { chunks: [], hasMore: false, lastSeq: null };
}

function page(
  chunks: ReadonlyArray<{ readonly seq: number; readonly chunk: ChildTranscriptChunk }>,
  hasMore = false,
): ChildTranscriptPage {
  return { chunks, hasMore, lastSeq: chunks.at(-1)?.seq ?? null };
}

describe("childAgentIdentityKey", () => {
  it("distinguishes the full identity, not just childId", () => {
    const a = childAgentIdentityKey(identity());
    const b = childAgentIdentityKey(identity({ runId: "run-2" }));
    const c = childAgentIdentityKey(identity({ instanceId: "codex" }));
    const d = childAgentIdentityKey(identity());
    expect(a).not.toBe(b);
    expect(a).not.toBe(c);
    expect(a).toBe(d);
  });
});

describe("mergeProviderChildren", () => {
  it("shows a persisted child when the ordinary roster is empty", () => {
    const child = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
    });
    const merged = mergeProviderChildren(emptyPanel(), [child]);
    expect(merged.hasAgents).toBe(true);
    expect(merged.providerChildren).toHaveLength(1);
    expect(merged.providerChildren[0]).toBe(child);
    expect(merged.runningCount).toBe(1);
    expect(merged.liveCount).toBe(1);
    expect(merged.settledCount).toBe(0);
  });

  it("counts settled children and tokens without inflating live", () => {
    const done = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
      status: "done",
      tokens: 1200,
    });
    const merged = mergeProviderChildren(emptyPanel(), [done]);
    expect(merged.liveCount).toBe(0);
    expect(merged.settledCount).toBe(1);
    expect(merged.totalTokens).toBe(1200);
  });

  it("deduplicates duplicate deliveries by full identity", () => {
    const child = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
    });
    const merged = mergeProviderChildren(emptyPanel(), [child, child]);
    expect(merged.providerChildren).toHaveLength(1);
    expect(merged.runningCount).toBe(1);
  });

  it("keeps the same childId across environments as distinct children", () => {
    const envA = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
    });
    const envB = childState({
      threadId: "thread-b",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
    });
    const merged = mergeProviderChildren(emptyPanel(), [envA, envB]);
    expect(merged.providerChildren).toHaveLength(2);
  });

  it("prefers durable rows by exact tuple and never dedups on bare childId", () => {
    // The canonical task row for the LIVE generation (run-2) is in the activity
    // roster; both durable generations must survive, with the run-2 activity
    // twin removed and the run-1 generation preserved alongside.
    const activityRow = directAgent("sa-1", "run-2");
    const run1 = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
      status: "done",
      tokens: 100,
    });
    const run2 = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-2",
      childId: "sa-1",
      status: "running",
    });

    const merged = mergeProviderChildren(panelWithDirectAgents([activityRow]), [run1, run2]);

    expect(merged.directAgents).toHaveLength(0);
    expect(merged.providerChildren.map((child) => child.runId)).toEqual(["run-1", "run-2"]);
    expect(merged.runningCount).toBe(1);
    expect(merged.settledCount).toBe(1);
    expect(merged.liveCount).toBe(1);
    expect(merged.totalTokens).toBe(100);
  });

  it("preserves a durable child whose activity twin carries no run id (no heuristic drop)", () => {
    const legacy = directAgent("sa-1", undefined);
    const durable = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
      status: "running",
    });
    const merged = mergeProviderChildren(panelWithDirectAgents([legacy]), [durable]);
    expect(merged.directAgents).toHaveLength(1);
    expect(merged.providerChildren).toHaveLength(1);
    expect(merged.runningCount).toBe(2);
  });

  it("keeps an activity row from a different generation alongside the durable child", () => {
    const oldActivity = directAgent("sa-1", "run-1");
    const durable = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-2",
      childId: "sa-1",
      status: "running",
    });
    const merged = mergeProviderChildren(panelWithDirectAgents([oldActivity]), [durable]);
    expect(merged.directAgents).toHaveLength(1);
    expect(merged.providerChildren).toHaveLength(1);
    expect(merged.runningCount).toBe(2);
  });

  it("preserves existing provider rosters (workflows and direct agents)", () => {
    const child = childState({
      threadId: "thread-a",
      instanceId: "pi",
      runId: "run-1",
      childId: "sa-1",
    });
    const existing = directAgent("codex-child", undefined);
    const merged = mergeProviderChildren(panelWithDirectAgents([existing]), [child]);
    expect(merged.directAgents).toEqual([existing]);
    expect(merged.providerChildren.map((entry) => entry.childId)).toEqual(["sa-1"]);
  });
});

describe("deriveTranscriptRows", () => {
  it("accumulates appended text into one assistant row", () => {
    const rows = deriveTranscriptRows([append(0, "m1", "Hello"), append(1, "m1", " world")]);
    expect(rows).toEqual([{ key: "text:m1", kind: "assistant", label: null, text: "Hello world" }]);
  });

  it("renders thinking as a distinct row kind", () => {
    const rows = deriveTranscriptRows([append(0, "t1", "planning", "thinking")]);
    expect(rows[0]).toMatchObject({ key: "thinking:t1", kind: "thinking", text: "planning" });
  });

  it("finalize replaces streamed text instead of duplicating it", () => {
    const rows = deriveTranscriptRows([
      append(0, "m1", "partial"),
      finalizeText(1, "m1", "final full text"),
    ]);
    expect(rows).toEqual([
      { key: "text:m1", kind: "assistant", label: null, text: "final full text" },
    ]);
  });

  it("renders tool calls and tool results with distinct keys", () => {
    const rows = deriveTranscriptRows([
      chunk(0, {
        op: "add",
        itemId: "m1:call:1",
        item: { kind: "toolCall", toolId: "t1", name: "Read", argsPreview: "{ path }" },
      }),
      chunk(1, {
        op: "add",
        itemId: "m1:result:1",
        item: {
          kind: "toolResult",
          toolId: "t1",
          name: "Read",
          isError: false,
          outputPreview: "ok",
        },
      }),
    ]);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ kind: "tool", label: "Read", text: "{ path }" });
    expect(rows[0]!.isError).toBeUndefined();
    expect(rows[1]).toMatchObject({ kind: "tool", label: "Read", text: "ok", isError: false });
    expect(rows[0]!.key).not.toBe(rows[1]!.key);
  });

  it("renders user messages and error tool results", () => {
    const rows = deriveTranscriptRows([
      chunk(0, { op: "add", itemId: "u1", item: { kind: "user", text: "go on" } }),
      chunk(1, {
        op: "add",
        itemId: "r1",
        item: {
          kind: "toolResult",
          toolId: "t1",
          name: "Bash",
          isError: true,
          outputPreview: "boom",
        },
      }),
    ]);
    expect(rows[0]).toMatchObject({ key: "user:u1", kind: "user", text: "go on" });
    expect(rows[1]).toMatchObject({ kind: "tool", isError: true, text: "boom" });
  });

  it("replaces a live tool result with its finalized form instead of duplicating it", () => {
    const rows = deriveTranscriptRows([
      chunk(0, {
        op: "add",
        itemId: "r1",
        item: {
          kind: "toolResult",
          toolId: "t1",
          name: "Bash",
          isError: false,
          outputPreview: "partial…",
        },
      }),
      chunk(1, {
        op: "add",
        itemId: "r1",
        item: {
          kind: "toolResult",
          toolId: "t1",
          name: "Bash",
          isError: true,
          outputPreview: "command failed",
        },
      }),
    ]);
    expect(rows).toEqual([
      {
        key: "toolResult:r1:t1",
        kind: "tool",
        label: "Bash",
        text: "command failed",
        isError: true,
      },
    ]);
  });

  it("replaces duplicate text add updates on the same stable id", () => {
    const rows = deriveTranscriptRows([
      chunk(0, { op: "add", itemId: "m1", item: { kind: "text", text: "draft" } }),
      chunk(1, { op: "add", itemId: "m1", item: { kind: "text", text: "final" } }),
    ]);
    expect(rows).toEqual([{ key: "text:m1", kind: "assistant", label: null, text: "final" }]);
  });

  it("ignores an append that arrives after finalization (no duplicate reopened item)", () => {
    const rows = deriveTranscriptRows([
      append(0, "m1", "partial"),
      finalizeText(1, "m1", "final"),
      append(2, "m1", "late tail"),
    ]);
    expect(rows).toEqual([{ key: "text:m1", kind: "assistant", label: null, text: "final" }]);
  });
});

describe("appendTranscriptChunks", () => {
  it("deduplicates by sequence and keeps ascending order", () => {
    const result = appendTranscriptChunks(
      [append(0, "a", "0"), append(1, "b", "1"), append(2, "c", "2")],
      [append(2, "c", "2-dup"), append(3, "d", "3")],
    );
    expect(result.map((entry) => entry.seq)).toEqual([0, 1, 2, 3]);
    expect(result[2]!.chunk).toEqual({ op: "append", itemId: "c", kind: "text", text: "2" });
  });

  it("preserves a 200+ chunk transcript in one fold", () => {
    const incoming = Array.from({ length: 250 }, (_, index) =>
      append(index, `m${index}`, `t${index}`),
    );
    const result = appendTranscriptChunks([], incoming);
    expect(result).toHaveLength(250);
    expect(result.at(-1)!.seq).toBe(249);
  });
});

describe("createChildTranscriptController", () => {
  it("loads an empty first page, then follows output via catch-up", async () => {
    let calls = 0;
    const reader = {
      readPage: async (): Promise<ChildTranscriptPage> => {
        calls += 1;
        if (calls === 1) return emptyPage();
        return page([append(0, "m1", "hello"), append(1, "m1", " world")]);
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.loadInitial();
    expect(controller.getState().chunks).toHaveLength(0);
    expect(controller.getState().loading).toBe(false);
    await controller.catchUp();
    expect(controller.getState().chunks).toHaveLength(2);
    expect(deriveTranscriptRows(controller.getState().chunks)[0]?.text).toBe("hello world");
  });

  it("sequentially catches up a transcript exceeding the page size", async () => {
    const first = page(
      Array.from({ length: 200 }, (_, index) => append(index, `m${index}`, `t${index}`)),
      true,
    );
    const second = page(
      Array.from({ length: 50 }, (_, index) =>
        append(200 + index, `m${200 + index}`, `t${200 + index}`),
      ),
      false,
    );
    const pages = [first, second];
    let calls = 0;
    const reader = {
      readPage: async (request: ChildTranscriptReadRequest): Promise<ChildTranscriptPage> => {
        const result = pages[calls] ?? emptyPage();
        calls += 1;
        expect(request.afterSeq).toBe(calls === 1 ? null : 199);
        return result;
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.catchUp();
    expect(controller.getState().chunks).toHaveLength(250);
    expect(controller.getState().hasMore).toBe(false);
    expect(controller.getState().lastSeq).toBe(249);
  });

  it("deduplicates overlapping/duplicate page deliveries", async () => {
    const pages = [
      page([append(0, "a", "0"), append(1, "b", "1"), append(2, "c", "2")], true),
      page([append(2, "c", "2"), append(3, "d", "3")], false),
    ];
    let calls = 0;
    const reader = { readPage: async () => pages[calls++] ?? emptyPage() };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.catchUp();
    expect(controller.getState().chunks.map((entry) => entry.seq)).toEqual([0, 1, 2, 3]);
  });

  it("routes reads to the owning environment for each reset", async () => {
    const seen: string[] = [];
    const reader = {
      readPage: async (request: ChildTranscriptReadRequest): Promise<ChildTranscriptPage> => {
        seen.push(request.environmentId);
        return emptyPage();
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.loadInitial();
    controller.reset({ environmentId: ENV_B, identity: identity() });
    await controller.loadInitial();
    expect(seen).toEqual(["env-a", "env-b"]);
  });

  it("drops a stale read after switching identity while the request is pending", async () => {
    let resolveFirst!: (value: ChildTranscriptPage) => void;
    const first = new Promise<ChildTranscriptPage>((resolve) => {
      resolveFirst = resolve;
    });
    const reader = {
      readPage: (request: ChildTranscriptReadRequest): Promise<ChildTranscriptPage> =>
        request.identity.childId === "sa-1"
          ? first
          : Promise.resolve(page([append(0, "m2", "second child")])),
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity({ childId: "sa-1" }) });
    const pending = controller.loadInitial();
    controller.reset({ environmentId: ENV_A, identity: identity({ childId: "sa-2" }) });
    await controller.loadInitial();
    resolveFirst(page([append(0, "m1", "stale child output")]));
    await pending;
    expect(controller.getState().chunks.map((entry) => entry.seq)).toEqual([0]);
    expect(deriveTranscriptRows(controller.getState().chunks)[0]?.text).toBe("second child");
  });

  it("exposes a read error and recovers on retry", async () => {
    let fail = true;
    const reader = {
      readPage: async (): Promise<ChildTranscriptPage> => {
        if (fail) throw new Error("boom");
        return page([append(0, "m1", "recovered")]);
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.loadInitial();
    expect(controller.getState().loading).toBe(false);
    expect(controller.getState().error).toBe("boom");
    fail = false;
    await controller.loadMore();
    expect(controller.getState().error).toBeNull();
    expect(controller.getState().chunks).toHaveLength(1);
  });

  it("queues one trailing read when a notice lands during an in-flight read", async () => {
    let reads = 0;
    let releaseFirst!: (value: ChildTranscriptPage) => void;
    const first = new Promise<ChildTranscriptPage>((resolve) => {
      releaseFirst = resolve;
    });
    const reader = {
      readPage: async (): Promise<ChildTranscriptPage> => {
        reads += 1;
        if (reads === 1) return first;
        return page([append(1, "m0", " tail")]);
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    const pending = controller.loadInitial();
    // A change notice arrives while the first read is still in flight.
    await controller.catchUp();
    expect(reads).toBe(1);
    releaseFirst(page([append(0, "m0", "head")]));
    // loadInitial must run exactly one trailing read after its own response.
    await pending;
    expect(reads).toBe(2);
    expect(deriveTranscriptRows(controller.getState().chunks).map((row) => row.text)).toEqual([
      "head tail",
    ]);
  });

  it("drains an invalidation that lands during the trailing read", async () => {
    let releaseFirst!: (value: ChildTranscriptPage) => void;
    const first = new Promise<ChildTranscriptPage>((resolve) => {
      releaseFirst = resolve;
    });
    let releaseSecond!: (value: ChildTranscriptPage) => void;
    const second = new Promise<ChildTranscriptPage>((resolve) => {
      releaseSecond = resolve;
    });
    let secondStarted!: () => void;
    const waitingForSecond = new Promise<void>((resolve) => {
      secondStarted = resolve;
    });
    let reads = 0;
    const reader = {
      readPage: async (): Promise<ChildTranscriptPage> => {
        reads += 1;
        if (reads === 1) return first;
        if (reads === 2) {
          secondStarted();
          return second;
        }
        return page([append(2, "m0", " tail2")]);
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    const pending = controller.loadInitial();
    await controller.catchUp(); // notice during the first read
    releaseFirst(page([append(0, "m0", "head")]));
    await waitingForSecond; // the trailing read is now in flight
    await controller.catchUp(); // notice during the trailing read
    releaseSecond(page([append(1, "m0", " tail1")]));
    await pending;
    expect(reads).toBe(3);
    expect(deriveTranscriptRows(controller.getState().chunks).map((row) => row.text)).toEqual([
      "head tail1 tail2",
    ]);
  });

  it("abandons the old generation's loop and preserves the new generation's queued read", async () => {
    let releaseA!: (value: ChildTranscriptPage) => void;
    const pendingA = new Promise<ChildTranscriptPage>((resolve) => {
      releaseA = resolve;
    });
    let releaseB!: (value: ChildTranscriptPage) => void;
    const pendingB = new Promise<ChildTranscriptPage>((resolve) => {
      releaseB = resolve;
    });
    let readsB = 0;
    const reader = {
      readPage: async (request: ChildTranscriptReadRequest): Promise<ChildTranscriptPage> => {
        if (request.identity.childId === "sa-a") {
          return pendingA;
        }
        readsB += 1;
        if (readsB === 1) return pendingB;
        return page([append(1, "m1", "more after queued read")]);
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity({ childId: "sa-a" }) });
    const old = controller.catchUp();

    controller.reset({ environmentId: ENV_B, identity: identity({ childId: "sa-b" }) });
    const loadB = controller.loadInitial();
    controller.loadMore(); // queues a read on the NEW generation's run
    expect(readsB).toBe(1);

    // The old read resolves late; its loop must not start reads or drain the new run.
    releaseA(page([append(0, "m0", "stale old output")]));
    await old;
    expect(readsB).toBe(1);

    releaseB(page([append(0, "m0", "new head")], true));
    await loadB;
    expect(readsB).toBe(2);
    expect(deriveTranscriptRows(controller.getState().chunks).map((row) => row.text)).toEqual([
      "new head",
      "more after queued read",
    ]);
  });

  it("reconnect catch-up preserves the cursor instead of clearing and re-fetching", async () => {
    const pages = [
      page([append(0, "m1", "first load")], true),
      page([append(1, "m2", "more after reconnect")]),
    ];
    const seenAfterSeq: Array<number | null> = [];
    let calls = 0;
    const reader = {
      readPage: async (request: ChildTranscriptReadRequest): Promise<ChildTranscriptPage> => {
        seenAfterSeq.push(request.afterSeq);
        return pages[calls++] ?? emptyPage();
      },
    };
    const controller = createChildTranscriptController(reader);
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.loadInitial();
    expect(controller.getState().chunks).toHaveLength(1);
    await controller.catchUp(); // reconnect: continue from cursor, not null
    expect(controller.getState().chunks).toHaveLength(2);
    expect(seenAfterSeq[0]).toBeNull();
    expect(seenAfterSeq[1]).toBe(0);
  });

  it("unsubscribing a listener stops notifications (subscription lifetime)", () => {
    const controller = createChildTranscriptController({ readPage: async () => emptyPage() });
    let notified = 0;
    const dispose = controller.subscribe(() => {
      notified += 1;
    });
    controller.reset({ environmentId: ENV_A, identity: identity() });
    dispose();
    controller.reset({ environmentId: ENV_A, identity: identity() });
    expect(notified).toBe(1);
  });
});

describe("createChildCancelController", () => {
  it("reports a confirmed cancellation", async () => {
    const controller = createChildCancelController({ cancel: async () => ({ cancelled: true }) });
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.cancel();
    expect(controller.getState().status).toBe("cancelled");
    expect(controller.getState().message).toBeNull();
  });

  it("reports an unsuccessful cancellation when cancelled is false", async () => {
    const controller = createChildCancelController({ cancel: async () => ({ cancelled: false }) });
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.cancel();
    expect(controller.getState().status).toBe("not-cancelled");
    expect(controller.getState().message).toContain("could not be cancelled");
  });

  it("surfaces unsupported-cancel errors and retries to a confirmed outcome", async () => {
    let fail = true;
    const controller = createChildCancelController({
      cancel: async () => {
        if (fail) throw new Error("Child control is unavailable.");
        return { cancelled: true };
      },
    });
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await controller.cancel();
    expect(controller.getState().status).toBe("failed");
    expect(controller.getState().message).toBe("Child control is unavailable.");
    fail = false;
    await controller.cancel();
    expect(controller.getState().status).toBe("cancelled");
  });

  it("discards a cancel that resolves after the selection changed", async () => {
    let release!: (value: { readonly cancelled: boolean }) => void;
    const slow = new Promise<{ readonly cancelled: boolean }>((resolve) => {
      release = resolve;
    });
    const controller = createChildCancelController({
      cancel: async (request) =>
        request.identity.childId === "sa-1" ? slow : Promise.resolve({ cancelled: true }),
    });
    controller.reset({ environmentId: ENV_A, identity: identity({ childId: "sa-1" }) });
    const stale = controller.cancel();
    controller.reset({ environmentId: ENV_A, identity: identity({ childId: "sa-2" }) });
    await controller.cancel();
    release({ cancelled: true });
    await stale;
    // The old sa-1 response must not overwrite the new sa-2 outcome.
    expect(controller.getState().status).toBe("cancelled");
  });

  it("single-flights concurrent cancel calls", async () => {
    let calls = 0;
    const controller = createChildCancelController({
      cancel: async () => {
        calls += 1;
        return { cancelled: true };
      },
    });
    controller.reset({ environmentId: ENV_A, identity: identity() });
    await Promise.all([controller.cancel(), controller.cancel()]);
    expect(calls).toBe(1);
    expect(controller.getState().status).toBe("cancelled");
  });
});

describe("createChildAgentRosterController", () => {
  const THREAD_A = ThreadId.make("thread-a");
  const THREAD_B = ThreadId.make("thread-b");
  const rosterChild = (childId: string): ChildAgentState =>
    childState({ threadId: "thread-a", instanceId: "pi", runId: "run-1", childId });

  it("coalesces a refresh that lands during an in-flight read into one trailing read", async () => {
    let releaseFirst!: (value: ChildAgentRosterListResult) => void;
    const first = new Promise<ChildAgentRosterListResult>((resolve) => {
      releaseFirst = resolve;
    });
    let calls = 0;
    const controller = createChildAgentRosterController({
      list: async () => {
        calls += 1;
        if (calls === 1) return first;
        return { _tag: "Success", children: [rosterChild("sa-2")] };
      },
    });
    controller.reset({ environmentId: ENV_A, threadId: THREAD_A });
    const pending = controller.refresh();
    controller.refresh(); // notice during the in-flight read
    expect(calls).toBe(1);
    releaseFirst({ _tag: "Success", children: [rosterChild("sa-1")] });
    await pending;
    expect(calls).toBe(2);
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-2"]);
  });

  it("keeps loaded rows on a retryable failure and recovers on the next refresh", async () => {
    let calls = 0;
    const controller = createChildAgentRosterController({
      list: async () => {
        calls += 1;
        if (calls === 1) return { _tag: "Success", children: [rosterChild("sa-1")] };
        if (calls === 2) return { _tag: "Failed", message: "boom" };
        return { _tag: "Success", children: [rosterChild("sa-1"), rosterChild("sa-2")] };
      },
    });
    controller.reset({ environmentId: ENV_A, threadId: THREAD_A });
    await controller.refresh();
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-1"]);
    expect(controller.getState().error).toBeNull();
    await controller.refresh();
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-1"]);
    expect(controller.getState().error).toBe("boom");
    await controller.refresh();
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-1", "sa-2"]);
    expect(controller.getState().error).toBeNull();
  });

  it("ignores an intentional interruption without blanking rows or surfacing an error", async () => {
    let calls = 0;
    const controller = createChildAgentRosterController({
      list: async () => {
        calls += 1;
        if (calls === 1) return { _tag: "Success", children: [rosterChild("sa-1")] };
        return { _tag: "Interrupted" };
      },
    });
    controller.reset({ environmentId: ENV_A, threadId: THREAD_A });
    await controller.refresh();
    await controller.refresh();
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-1"]);
    expect(controller.getState().error).toBeNull();
    expect(controller.getState().loading).toBe(false);
  });

  it("drops an in-flight read from a superseded scope so it cannot regress rows", async () => {
    let releaseA!: (value: ChildAgentRosterListResult) => void;
    const pendingA = new Promise<ChildAgentRosterListResult>((resolve) => {
      releaseA = resolve;
    });
    let callsB = 0;
    const controller = createChildAgentRosterController({
      list: async (scope) => {
        if (scope.threadId === THREAD_B) {
          callsB += 1;
          return { _tag: "Success", children: [rosterChild("sa-b")] };
        }
        return pendingA;
      },
    });
    controller.reset({ environmentId: ENV_A, threadId: THREAD_A });
    const refreshA = controller.refresh();
    controller.reset({ environmentId: ENV_B, threadId: THREAD_B });
    const refreshB = controller.refresh();
    expect(callsB).toBe(1);
    releaseA({ _tag: "Success", children: [rosterChild("sa-a")] });
    await refreshA;
    await refreshB;
    expect(controller.getState().scope).toBe("env-b:thread-b");
    expect(controller.getState().children.map((child) => child.childId)).toEqual(["sa-b"]);
  });
});
