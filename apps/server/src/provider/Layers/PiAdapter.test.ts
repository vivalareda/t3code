// @effect-diagnostics nodeBuiltinImport:off - fixture files are written directly, matching other fake-CLI tests.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { afterAll } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makePiAdapter } from "./PiAdapter.ts";

afterAll(() => {
  for (const directory of fixtureDirectories) {
    try {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup; a leaked temp dir is harmless.
    }
  }
});

const fixtureDirectories: Array<string> = [];
const REQUEST_ID = ApprovalRequestId.make("q1");
const THREAD_ID = ThreadId.make("thread-pi-adapter-1");
const TEST_INSTANCE_ID = ProviderInstanceId.make("pi-test");

/**
 * Scriptable fake `pi --mode rpc` process. Behaviour is driven by JSONL
 * scenario files and a command log, all passed through environment variables:
 *   FAKE_PI_LOG                path appended with every received command
 *   FAKE_PI_START_EVENTS       JSONL events emitted right after `get_state`
 *   FAKE_PI_PROMPT_EVENTS      JSONL events emitted after a successful `prompt`
 *   FAKE_PI_EXIT_AFTER_MS      exit after emitting prompt/start events
 *   FAKE_PI_LATE_EVENTS        JSONL events emitted after a delay (late work)
 *   FAKE_PI_LATE_EVENTS_DELAY_MS
 */
const FAKE_PI_SOURCE = `
import { readFileSync, appendFileSync } from "node:fs";

const logPath = process.env.FAKE_PI_LOG;
const startEventsPath = process.env.FAKE_PI_START_EVENTS;
const promptEventsPath = process.env.FAKE_PI_PROMPT_EVENTS;
const lateEventsPath = process.env.FAKE_PI_LATE_EVENTS;
const exitAfterMs = Number(process.env.FAKE_PI_EXIT_AFTER_MS ?? "0");
const lateDelayMs = Number(process.env.FAKE_PI_LATE_EVENTS_DELAY_MS ?? "0");

const send = (payload) => process.stdout.write(JSON.stringify(payload) + "\\n");
const logCommand = (cmd) => {
  if (logPath) { try { appendFileSync(logPath, JSON.stringify(cmd) + "\\n"); } catch {} }
};
const respond = (id, command, success, data, error) => {
  const payload = { id, type: "response", command, success };
  if (data !== undefined) payload.data = data;
  if (error !== undefined) payload.error = error;
  send(payload);
};
const emitFile = (path) => {
  if (!path) return;
  let text = "";
  try { text = readFileSync(path, "utf8"); } catch { return; }
  for (const line of text.split("\\n")) {
    if (line.trim().length === 0) continue;
    send(JSON.parse(line));
  }
};
const scheduleExit = () => {
  if (exitAfterMs > 0) setTimeout(() => process.exit(0), exitAfterMs);
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let i = buffer.indexOf("\\n");
  while (i >= 0) {
    const line = buffer.slice(0, i);
    buffer = buffer.slice(i + 1);
    if (line.trim().length > 0) handleLine(line);
    i = buffer.indexOf("\\n");
  }
});

function handleLine(line) {
  let cmd;
  try { cmd = JSON.parse(line); } catch { return; }
  logCommand(cmd);
  const id = typeof cmd.id === "string" ? cmd.id : undefined;
  const type = String(cmd.type ?? "");
  if (type === "get_state") {
    respond(id, type, true, {
      sessionFile: "/tmp/pi-adapter-test-session.jsonl",
      sessionId: "sess-1",
      model: { id: "test-model", provider: "test" },
      isStreaming: false,
    });
    emitFile(startEventsPath);
    scheduleExit();
  } else if (type === "prompt") {
    if (String(cmd.message ?? "").includes("REJECT_ME")) {
      respond(id, type, false, undefined, "prompt rejected by fixture");
      return;
    }
    respond(id, type, true);
    emitFile(promptEventsPath);
    if (lateEventsPath && lateDelayMs > 0) {
      setTimeout(() => emitFile(lateEventsPath), lateDelayMs);
    }
    scheduleExit();
  } else {
    respond(id, type, true);
  }
}
`;

/**
 * Fake `pi --mode rpc` that also connects to the bridge as the subagents
 * companion, so the interrupt path exercises a real quiesce handshake, an
 * accepted terminal child frame, and a stale queued parent continuation.
 */
const INTERRUPT_FAKE_SOURCE = `
import { appendFileSync } from "node:fs";
import { createConnection } from "node:net";

const logPath = process.env.FAKE_PI_LOG;
const runId = process.env.T3CODE_PI_BRIDGE_RUN_ID;
const bridgePort = Number(process.env.T3CODE_PI_BRIDGE_PORT ?? "0");
const bridgeToken = process.env.T3CODE_PI_BRIDGE_TOKEN;
let bridgeSocket = null;
let bridgeReady = false;
let childOpen = false;

const send = (payload) => process.stdout.write(JSON.stringify(payload) + "\\n");
const logCommand = (cmd) => {
  if (logPath) { try { appendFileSync(logPath, JSON.stringify(cmd) + "\\n"); } catch {} }
};
const respond = (id, command, success, data, error) => {
  const payload = { id, type: "response", command, success };
  if (data !== undefined) payload.data = data;
  if (error !== undefined) payload.error = error;
  send(payload);
};
const entry = (data) => ({
  type: "entry_appended",
  entry: {
    type: "custom",
    id: "entry-" + data.type + "-" + data.childId,
    customType: "t3-bridge",
    data: Object.assign({ ts: Date.now(), runId }, data),
  },
});

function connectBridge() {
  if (!bridgePort || !bridgeToken || !runId) return;
  bridgeSocket = createConnection({ host: "127.0.0.1", port: bridgePort });
  bridgeSocket.setNoDelay(true);
  bridgeSocket.on("connect", () => {
    bridgeSocket.write(
      JSON.stringify({ type: "hello", protocol: 1, token: bridgeToken, runId, pid: process.pid }) + "\\n",
    );
  });
  let buf = "";
  bridgeSocket.on("data", (chunk) => {
    buf += chunk;
    let index = buf.indexOf("\\n");
    while (index >= 0) {
      const line = buf.slice(0, index).replace(/\\r$/, "");
      buf = buf.slice(index + 1);
      let msg;
      try { msg = JSON.parse(line); } catch { index = buf.indexOf("\\n"); continue; }
      if (msg.type === "hello_ok") {
        bridgeReady = true;
        // Marker for the test to wait on before interrupting: quiesce now
        // reaches a connected companion deterministically.
        send({ type: "extension_error", error: "bridge-ready" });
      } else if (msg.type === "quiesce") {
        bridgeSocket.write(
          JSON.stringify({ type: "ack", reqId: msg.reqId, accepted: true, runId }) + "\\n",
        );
        if (childOpen) {
          childOpen = false;
          send(entry({ type: "child.status", childId: "sa-1", status: "cancelled", settledAt: Date.now() }));
        }
      } else if (msg.type === "cancel") {
        bridgeSocket.write(
          JSON.stringify({ type: "ack", reqId: msg.reqId, accepted: true, runId }) + "\\n",
        );
      }
      index = buf.indexOf("\\n");
    }
  });
  bridgeSocket.on("error", () => {});
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let index = buffer.indexOf("\\n");
  while (index >= 0) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (line.trim().length > 0) handleLine(line);
    index = buffer.indexOf("\\n");
  }
});

function handleLine(line) {
  let cmd;
  try { cmd = JSON.parse(line); } catch { return; }
  logCommand(cmd);
  const id = typeof cmd.id === "string" ? cmd.id : undefined;
  if (cmd.type === "get_state") {
    connectBridge();
    respond(id, "get_state", true, {
      sessionFile: "/tmp/pi-interrupt-session.jsonl",
      sessionId: "sess-interrupt",
      model: { id: "test-model", provider: "test" },
      isStreaming: false,
    });
  } else if (cmd.type === "prompt") {
    respond(id, "prompt", true);
    // Open a running parent turn with one live child and leave it unsettled,
    // so interrupt is exercised against a genuinely active turn.
    send({ type: "agent_start" });
    send(entry({ type: "child.started", childId: "sa-1", title: "Live child", backend: "pi" }));
    childOpen = true;
    send({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "working on it" } });
    send({ type: "message_end", message: { role: "assistant", stopReason: "stop" } });
  } else if (cmd.type === "abort") {
    respond(id, "abort", true);
    // A child follow-up was already queued while the parent ran; surface it now
    // to prove interrupt suppresses the stale parent continuation.
    send({ type: "agent_start" });
    send({
      type: "message_start",
      message: {
        role: "custom", customType: "subagent-result", content: "late child output", display: true,
        details: { id: "sa-1", childId: "sa-1", title: "recap", status: "done", runId },
      },
    });
    send({
      type: "message_end",
      message: {
        role: "custom", customType: "subagent-result", content: "late child output", display: true,
        details: { id: "sa-1", childId: "sa-1", title: "recap", status: "done", runId },
      },
    });
    send({ type: "message_start", message: { role: "assistant" } });
    send({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "late recap" } });
    send({ type: "message_end", message: { role: "assistant", stopReason: "stop" } });
    send({ type: "agent_settled" });
  } else {
    respond(id, String(cmd.type ?? ""), true);
  }
}
`;

function makeFakePi(
  dir: string,
  env: Record<string, string> = {},
  source = FAKE_PI_SOURCE,
): string {
  return writeFakeCli({
    directory: dir,
    name: "pi",
    source,
    env,
  });
}

const writeScenario = (dir: string, name: string, events: Array<Record<string, unknown>>) => {
  const path = NodePath.join(dir, name);
  NodeFS.writeFileSync(
    path,
    events.map((event) => JSON.stringify(event)).join("\n") + "\n",
    "utf8",
  );
  return path;
};

const textDelta = (delta: string): Record<string, unknown> => ({
  type: "message_update",
  assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta },
});

const assistantMessageEnd = (
  stopReason: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "message_end",
  message: { role: "assistant", stopReason, ...extra },
});

const subagentResultMessage = (childId: string, content: string): Record<string, unknown> => ({
  role: "custom",
  customType: "subagent-result",
  content,
  display: true,
  details: { id: childId, title: "recap", status: "done" },
});

const subagentResultMessageWithRun = (
  childId: string,
  runId: string,
  content: string,
): Record<string, unknown> => ({
  role: "custom",
  customType: "subagent-result",
  content,
  display: true,
  details: { id: childId, runId, title: "recap", status: "done" },
});

const extensionUiRequest = (method: string, id = "q1"): Record<string, unknown> => ({
  type: "extension_ui_request",
  id,
  method,
  title: "Pi asks",
  message: "Choose",
  ...(method === "select" ? { options: ["Allow", "Block"] } : {}),
});

interface AdapterHarness {
  readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
  /** Wait until the accumulated event set satisfies `predicate`. */
  readonly waitUntil: (
    predicate: (events: ReadonlyArray<ProviderRuntimeEvent>) => boolean,
  ) => Effect.Effect<void>;
  /** Read the events accumulated so far. */
  readonly events: Effect.Effect<ReadonlyArray<ProviderRuntimeEvent>>;
}

/** A persistent collector that never misses events between waits. */
function makeCollector(adapter: ProviderAdapterShape<ProviderAdapterError>) {
  return Effect.gen(function* () {
    const ref = yield* Ref.make<ReadonlyArray<ProviderRuntimeEvent>>([]);
    const waiters = yield* Ref.make<
      Array<{
        readonly predicate: (events: ReadonlyArray<ProviderRuntimeEvent>) => boolean;
        readonly deferred: Deferred.Deferred<void>;
      }>
    >([]);
    yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.gen(function* () {
        const events = yield* Ref.updateAndGet(ref, (arr) => [...arr, event]);
        const pending = yield* Ref.get(waiters);
        if (pending.length === 0) return;
        const satisfied = pending.filter((waiter) => waiter.predicate(events));
        if (satisfied.length === 0) return;
        yield* Ref.set(
          waiters,
          pending.filter((waiter) => !satisfied.includes(waiter)),
        );
        for (const waiter of satisfied) {
          yield* Deferred.succeed(waiter.deferred, undefined);
        }
      }),
    ).pipe(Effect.forkScoped);

    return {
      events: Ref.get(ref),
      waitUntil: (predicate: (events: ReadonlyArray<ProviderRuntimeEvent>) => boolean) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(ref);
          if (predicate(current)) return;
          const deferred = yield* Deferred.make<void>();
          yield* Ref.update(waiters, (ws) => [...ws, { predicate, deferred }]);
          yield* Deferred.await(deferred);
        }),
    };
  });
}

const runWithFake = (
  scenario: {
    readonly startEvents?: Array<Record<string, unknown>>;
    readonly promptEvents?: Array<Record<string, unknown>>;
    readonly lateEvents?: Array<Record<string, unknown>>;
    readonly lateDelayMs?: number;
    readonly exitAfterMs?: number;
  },
  fn: (harness: AdapterHarness) => Effect.Effect<void, ProviderAdapterError>,
) => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-adapter-fixture-"));
  fixtureDirectories.push(dir);
  const env: Record<string, string> = {
    FAKE_PI_LOG: NodePath.join(dir, "commands.jsonl"),
  };
  if (scenario.startEvents !== undefined) {
    env["FAKE_PI_START_EVENTS"] = writeScenario(dir, "start.jsonl", scenario.startEvents);
  }
  if (scenario.promptEvents !== undefined) {
    env["FAKE_PI_PROMPT_EVENTS"] = writeScenario(dir, "prompt.jsonl", scenario.promptEvents);
  }
  if (scenario.lateEvents !== undefined) {
    env["FAKE_PI_LATE_EVENTS"] = writeScenario(dir, "late.jsonl", scenario.lateEvents);
    env["FAKE_PI_LATE_EVENTS_DELAY_MS"] = String(scenario.lateDelayMs ?? 0);
  }
  if (scenario.exitAfterMs !== undefined) {
    env["FAKE_PI_EXIT_AFTER_MS"] = String(scenario.exitAfterMs);
  }

  const launcher = makeFakePi(dir, env);
  const layer = Layer.provideMerge(ServerConfig.layerTest(dir, dir), NodeServices.layer);

  return Effect.gen(function* () {
    const adapter = yield* makePiAdapter(
      { enabled: true, binaryPath: launcher, profileDir: "", customModels: [] },
      { instanceId: TEST_INSTANCE_ID },
    );
    const collector = yield* makeCollector(adapter);
    yield* adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" });
    return yield* fn({
      adapter,
      waitUntil: collector.waitUntil,
      events: collector.events,
    });
  }).pipe(Effect.provide(layer));
};

const runWithInterruptFake = (
  fn: (harness: AdapterHarness) => Effect.Effect<void, ProviderAdapterError>,
) => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-interrupt-fixture-"));
  fixtureDirectories.push(dir);
  const launcher = makeFakePi(
    dir,
    { FAKE_PI_LOG: NodePath.join(dir, "commands.jsonl") },
    INTERRUPT_FAKE_SOURCE,
  );
  const layer = Layer.provideMerge(ServerConfig.layerTest(dir, dir), NodeServices.layer);

  return Effect.gen(function* () {
    const adapter = yield* makePiAdapter(
      { enabled: true, binaryPath: launcher, profileDir: "", customModels: [] },
      { instanceId: TEST_INSTANCE_ID },
    );
    const collector = yield* makeCollector(adapter);
    return yield* fn({ adapter, waitUntil: collector.waitUntil, events: collector.events });
  }).pipe(Effect.provide(layer));
};

const completedTurns = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter((event) => event.type === "turn.completed");

const startedTurns = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter((event) => event.type === "turn.started");

const hasRequested = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.some((event) => event.type === "user-input.requested");

const resolvedFor = (requestId: string) => (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.some((event) => event.type === "user-input.resolved" && event.requestId === requestId);

describe("PiAdapter", () => {
  it.effect("maps the web client's bare confirm answer to a confirmed response", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("confirm", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: "Confirm" });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          expect(answer?.type).toBe("user-input.resolved");
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ confirmed: true });
        }),
    ),
  );

  it.effect("maps Cancel to confirmed: false", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("confirm", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: "Cancel" });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ confirmed: false });
        }),
    ),
  );

  it.effect("maps a select answer string to value", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("select", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: "Allow" });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ value: "Allow" });
        }),
    ),
  );

  it.effect("maps a multi-select array to the first selected value", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("select", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: ["Allow", "Block"] });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ value: "Allow" });
        }),
    ),
  );

  it.effect("maps text input to value", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("input", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: "hello world" });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ value: "hello world" });
        }),
    ),
  );

  it.effect("still accepts the legacy wrapped { value } answer shape", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("confirm", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.respondToUserInput(THREAD_ID, REQUEST_ID, { q1: { value: "Confirm" } });
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ confirmed: true });
        }),
    ),
  );

  it.effect("interrupt releases a pending dialog without waiting for its timeout", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("confirm", "q1")] },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* adapter.interruptTurn(THREAD_ID);
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          expect(answer?.type).toBe("user-input.resolved");
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ cancelled: true });
        }),
    ),
  );

  it.effect("process exit releases a pending dialog without waiting for its timeout", () =>
    runWithFake(
      { startEvents: [extensionUiRequest("confirm", "q1")], exitAfterMs: 300 },
      ({ waitUntil, events }) =>
        Effect.gen(function* () {
          yield* waitUntil(hasRequested);
          yield* waitUntil(resolvedFor("q1"));
          const answer = (yield* events).find(
            (event) => event.type === "user-input.resolved" && event.requestId === "q1",
          );
          expect(answer?.type).toBe("user-input.resolved");
          if (answer?.type !== "user-input.resolved") return;
          expect(answer.payload.answers).toEqual({ cancelled: true });
        }),
    ),
  );

  it.effect("settles an error-ending assistant message as a failed turn", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("partial work"),
          assistantMessageEnd("error", { errorMessage: "boom" }),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const completed = completedTurns(yield* events)[0];
          expect(completed?.type).toBe("turn.completed");
          if (completed?.type !== "turn.completed") return;
          expect(completed.payload.state).toBe("failed");
          expect(completed.payload.errorMessage).toBe("boom");
        }),
    ),
  );

  it.effect("settles exhausted model retries as a failed turn with the final error", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("first try"),
          assistantMessageEnd("error"),
          {
            type: "auto_retry_end",
            success: false,
            attempt: 3,
            finalError: "529 overloaded_error",
          },
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const completed = completedTurns(yield* events)[0];
          expect(completed?.type).toBe("turn.completed");
          if (completed?.type !== "turn.completed") return;
          expect(completed.payload.state).toBe("failed");
          expect(completed.payload.errorMessage).toBe("529 overloaded_error");
        }),
    ),
  );

  it.effect("recovers a successful retry to a completed turn", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("first try"),
          assistantMessageEnd("error"),
          { type: "auto_retry_end", success: true, attempt: 2 },
          textDelta("recovered"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const completed = completedTurns(yield* events)[0];
          expect(completed?.type).toBe("turn.completed");
          if (completed?.type !== "turn.completed") return;
          expect(completed.payload.state).toBe("completed");
        }),
    ),
  );

  it.effect("unwinds a rejected initial prompt and treats the next send as a new turn", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("fresh turn"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          const rejected = yield* adapter
            .sendTurn({ threadId: THREAD_ID, input: "REJECT_ME please" })
            .pipe(Effect.exit);
          expect(Exit.isFailure(rejected)).toBe(true);
          if (Exit.isFailure(rejected)) {
            expect(String(rejected.cause)).toContain("prompt rejected by fixture");
          }

          // The rejected turn must be settled (failed), not left active.
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const rejectedCompletion = completedTurns(yield* events)[0];
          expect(rejectedCompletion?.type).toBe("turn.completed");
          if (rejectedCompletion?.type !== "turn.completed") return;
          expect(rejectedCompletion.payload.state).toBe("failed");
          expect(rejectedCompletion.payload.stopReason).toBe("prompt-rejected");

          // The next submission opens a fresh turn, not a steering continuation.
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "do it now" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          const starts = startedTurns(yield* events);
          expect(starts.length).toBe(2);
          const ids = starts.map((event) =>
            event.type === "turn.started" ? String(event.turnId) : "",
          );
          expect(ids[0]).not.toBe(ids[1]);
        }),
    ),
  );

  it.effect("a rejected steering message does not terminate the active turn", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("still running"),
          assistantMessageEnd("stop"),
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          const turn = yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) =>
            seen.some(
              (event) =>
                event.type === "content.delta" &&
                event.payload.streamKind === "assistant_text" &&
                event.payload.delta === "still running",
            ),
          );

          const rejected = yield* adapter
            .sendTurn({ threadId: THREAD_ID, input: "REJECT_ME steering" })
            .pipe(Effect.exit);
          expect(Exit.isFailure(rejected)).toBe(true);

          // The active turn is untouched: no turn.completed, session still running.
          expect(completedTurns(yield* events).length).toBe(0);
          const sessions = yield* adapter.listSessions();
          expect(sessions.length).toBe(1);
          expect(sessions[0]?.status).toBe("running");
          expect(String(sessions[0]?.activeTurnId)).toBe(String(turn.turnId));
        }),
    ),
  );

  it.effect("registers a provider-initiated continuation turn and persists its recap text", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("Spawned sa-1. I will relay its output."),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
          { type: "agent_start" },
          {
            type: "message_start",
            message: subagentResultMessage(
              "sa-1",
              "The subagent found a monorepo with server, web, desktop, and mobile apps.",
            ),
          },
          {
            type: "message_end",
            message: subagentResultMessage(
              "sa-1",
              "The subagent found a monorepo with server, web, desktop, and mobile apps.",
            ),
          },
          { type: "message_start", message: { role: "assistant" } },
          textDelta("The subagent found a monorepo with server, web, desktop, and mobile apps."),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "recap the codebase" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          const seen = yield* events;

          expect(startedTurns(seen).length).toBe(2);
          expect(completedTurns(seen).length).toBe(2);

          // The recap text must land in the second (continuation) turn.
          const turnIds = startedTurns(seen).map((event) =>
            event.type === "turn.started" ? String(event.turnId) : "",
          );
          const recapDelta = seen.find(
            (event) =>
              event.type === "content.delta" &&
              event.payload.streamKind === "assistant_text" &&
              event.payload.delta.startsWith("The subagent found"),
          );
          expect(recapDelta?.type).toBe("content.delta");
          expect(String(recapDelta?.turnId)).toBe(turnIds[1]);
          expect(String(recapDelta?.turnId)).not.toBe(turnIds[0]);

          // The child result is captured as a readable receipt in that turn.
          const receipt = seen.find(
            (event) =>
              event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              (event.payload.data as Record<string, unknown> | undefined)?.source ===
                "subagent-result",
          );
          expect(receipt?.type).toBe("item.completed");
          expect(String(receipt?.turnId)).toBe(turnIds[1]);
        }),
    ),
  );

  it.effect("does not start a new turn from a continuation arriving after Stop", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("done"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
        lateEvents: [
          { type: "agent_start" },
          {
            type: "message_start",
            message: subagentResultMessage("sa-1", "late child output"),
          },
          {
            type: "message_end",
            message: subagentResultMessage("sa-1", "late child output"),
          },
          textDelta("late recap"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
        lateDelayMs: 200,
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          yield* adapter.stopSession(THREAD_ID);
          yield* waitUntil((seen) => seen.some((event) => event.type === "session.exited"));
          const seen = yield* events;
          expect(startedTurns(seen).length).toBe(1);
          expect(completedTurns(seen).length).toBe(1);
          expect(
            seen.some(
              (event) => event.type === "content.delta" && event.payload.delta === "late recap",
            ),
          ).toBe(false);
        }),
    ),
  );

  it.effect("captures the run generation on the child-result receipt", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("acknowledged"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
          { type: "agent_start" },
          {
            type: "message_start",
            message: subagentResultMessageWithRun("sa-1", "run-a", "child output"),
          },
          {
            type: "message_end",
            message: subagentResultMessageWithRun("sa-1", "run-a", "child output"),
          },
          { type: "message_start", message: { role: "assistant" } },
          textDelta("recap text"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "go" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          const receipt = (yield* events).find(
            (event) =>
              event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              (event.payload.data as Record<string, unknown> | undefined)?.source ===
                "subagent-result",
          );
          expect(receipt?.type).toBe("item.completed");
          if (receipt?.type !== "item.completed") return;
          expect(String(receipt.itemId)).toBe("subagent-result:run-a:sa-1");
          expect((receipt.payload.data as Record<string, unknown>).runId).toBe("run-a");
        }),
    ),
  );

  it.effect("emits one receipt for a duplicate child-result delivery within a turn", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("acknowledged"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
          { type: "agent_start" },
          {
            type: "message_start",
            message: subagentResultMessage("sa-1", "child output"),
          },
          {
            type: "message_start",
            message: subagentResultMessage("sa-1", "child output"),
          },
          {
            type: "message_end",
            message: subagentResultMessage("sa-1", "child output"),
          },
          { type: "message_start", message: { role: "assistant" } },
          textDelta("recap"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "go" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          const receipts = (yield* events).filter(
            (event) =>
              event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              (event.payload.data as Record<string, unknown> | undefined)?.source ===
                "subagent-result",
          );
          expect(receipts.length).toBe(1);
        }),
    ),
  );

  it.effect("Stop retires the process generation and a later user turn resumes fresh", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("first"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "first" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);

          yield* adapter.stopSession(THREAD_ID);
          yield* waitUntil((seen) => seen.some((event) => event.type === "session.exited"));
          // The retired generation is gone: no lingering adapter session.
          expect((yield* adapter.listSessions()).length).toBe(0);
          expect(yield* adapter.hasSession(THREAD_ID)).toBe(false);

          // A fresh generation resumes the saved session and serves a new turn.
          const resumed = yield* adapter.startSession({
            threadId: THREAD_ID,
            runtimeMode: "full-access",
          });
          expect(resumed.status).toBe("ready");
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "second" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          expect(startedTurns(yield* events).length).toBe(2);
        }),
    ),
  );

  it.effect(
    "interrupt retires the generation, keeps a live child cancelled, and resumes a fresh run",
    () =>
      runWithInterruptFake(({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          const first = yield* adapter.startSession({
            threadId: THREAD_ID,
            runtimeMode: "full-access",
          });
          // Wait for the bridge handshake so quiesce reaches a connected
          // companion deterministically for the rest of this test.
          yield* waitUntil((seen) =>
            seen.some(
              (event) =>
                event.type === "runtime.warning" &&
                (event.payload as { message?: string }).message === "bridge-ready",
            ),
          );

          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "spawn a live child" });
          yield* waitUntil((seen) =>
            seen.some(
              (event) =>
                event.type === "task.started" &&
                (event.payload as { taskId?: string }).taskId === "sa-1",
            ),
          );

          yield* adapter.interruptTurn(THREAD_ID);
          // The generation is retired, not merely aborted in place.
          expect(yield* adapter.hasSession(THREAD_ID)).toBe(false);
          expect((yield* adapter.listSessions()).length).toBe(0);

          // The interrupt fiber publishes teardown events asynchronously; wait
          // until the collector has observed the teardown marker, the settled
          // turn, and the child's terminal state before snapshotting.
          yield* waitUntil((s) => s.some((event) => event.type === "session.exited"));
          yield* waitUntil((s) => s.some((event) => event.type === "turn.completed"));
          yield* waitUntil((s) =>
            s.some(
              (event) =>
                event.type === "task.updated" &&
                (event.payload as { taskId?: string }).taskId === "sa-1",
            ),
          );

          const seen = yield* events;

          // The active turn settles cancelled rather than being abandoned.
          const completed = completedTurns(seen);
          expect(completed.length).toBe(1);
          expect(completed[0]?.type).toBe("turn.completed");
          if (completed[0]?.type === "turn.completed") {
            expect(completed[0].payload.state).toBe("cancelled");
            expect(completed[0].payload.stopReason).toBe("interrupted");
          }

          // The quiesced child reports its real terminal state (`cancelled`)
          // because its accepted terminal frame drained before the fallback
          // reconcile; it is never overwritten with `interrupted`.
          const childUpdates = seen.filter(
            (event) =>
              event.type === "task.updated" &&
              (event.payload as { taskId?: string }).taskId === "sa-1",
          );
          expect(
            childUpdates.some(
              (event) => (event.payload as { status?: string }).status === "cancelled",
            ),
          ).toBe(true);
          expect(
            childUpdates.some(
              (event) => (event.payload as { status?: string }).status === "interrupted",
            ),
          ).toBe(false);

          // The generation emitted its teardown marker.
          expect(seen.some((event) => event.type === "session.exited")).toBe(true);

          // A stale queued parent continuation never opens a turn or streams.
          expect(startedTurns(seen).length).toBe(1);
          expect(
            seen.some(
              (event) =>
                event.type === "content.delta" &&
                (event.payload as { delta?: string }).delta === "late recap",
            ),
          ).toBe(false);

          // A fresh startSession resumes the saved cursor into a new generation.
          const oldRunId = (
            seen.find((event) => event.type === "task.started")?.payload as
              | { runId?: string }
              | undefined
          )?.runId;
          const resumed = yield* adapter.startSession({
            threadId: THREAD_ID,
            runtimeMode: "full-access",
            resumeCursor: first.resumeCursor,
          });
          expect(resumed.status).toBe("ready");
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "resume work" });
          yield* waitUntil(
            (after) => after.filter((event) => event.type === "task.started").length >= 2,
          );
          const runIds = (yield* events)
            .filter((event) => event.type === "task.started")
            .map((event) => (event.payload as { runId?: string }).runId);
          expect(runIds[1]).toBeDefined();
          expect(runIds[1]).not.toBe(oldRunId);
        }),
      ),
  );
});
