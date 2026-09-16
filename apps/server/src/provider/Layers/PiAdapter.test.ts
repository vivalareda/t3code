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
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
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
// Process-group SIGTERM signalling differs on Windows; the OS-level
// process-reaped assertion is skipped there.
const windowsHost = HostProcessPlatform.defaultValue() === "win32";

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
    if (String(cmd.message ?? "").startsWith("FAKE_EVENTS:")) {
      for (const event of JSON.parse(cmd.message.slice("FAKE_EVENTS:".length))) send(event);
    } else {
      emitFile(promptEventsPath);
    }
    if (process.env.FAKE_PI_EXIT_AFTER_PROMPT === "1") {
      process.stdout.write("\\n", () => process.exit(9));
      return;
    }
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

const messageUpdateWithUsage = (usage: Record<string, unknown>): Record<string, unknown> => ({
  type: "message_update",
  usage,
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

type PiFakeScenario = {
  readonly startEvents?: Array<Record<string, unknown>>;
  readonly promptEvents?: Array<Record<string, unknown>>;
  readonly lateEvents?: Array<Record<string, unknown>>;
  readonly lateDelayMs?: number;
  readonly exitAfterMs?: number;
  readonly exitAfterPrompt?: boolean;
};

/** Build the adapter + collector against a fake `pi` without auto-starting. */
const makeFakeHarness = (scenario: PiFakeScenario) => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-adapter-fixture-"));
  fixtureDirectories.push(dir);
  const env: Record<string, string> = {
    FAKE_PI_LOG: NodePath.join(dir, "commands.jsonl"),
    FAKE_PI_EXIT_AFTER_PROMPT: scenario.exitAfterPrompt ? "1" : "0",
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
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const released = yield* Deferred.make<void>();
    const observedSpawner = ChildProcessSpawner.make((command) =>
      spawner
        .spawn(command)
        .pipe(Effect.tap(() => Effect.addFinalizer(() => Deferred.succeed(released, undefined)))),
    );
    const adapter = yield* makePiAdapter(
      { enabled: true, binaryPath: launcher, profileDir: "", customModels: [] },
      { instanceId: TEST_INSTANCE_ID },
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner));
    const collector = yield* makeCollector(adapter);
    return {
      adapter,
      waitUntil: collector.waitUntil,
      events: collector.events,
      released: Deferred.await(released),
      dir,
    };
  }).pipe(Effect.provide(layer));
};

const runWithFake = (
  scenario: PiFakeScenario,
  fn: (harness: AdapterHarness) => Effect.Effect<void, ProviderAdapterError>,
) =>
  Effect.gen(function* () {
    const harness = yield* makeFakeHarness(scenario);
    yield* harness.adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" });
    return yield* fn(harness);
  });

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

/** A fake `pi` that reports its pid then hangs forever on `get_state`. */
const HANG_GET_STATE_SOURCE = `
import { appendFileSync } from "node:fs";

const logPath = process.env.FAKE_PI_LOG;
const pidPath = process.env.FAKE_PI_PID_FILE;
if (pidPath) { try { appendFileSync(pidPath, String(process.pid)); } catch {} }

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let i = buffer.indexOf("\\n");
  while (i >= 0) {
    const line = buffer.slice(0, i);
    buffer = buffer.slice(i + 1);
    if (line.trim().length > 0) {
      let cmd;
      try { cmd = JSON.parse(line); } catch { cmd = null; }
      if (cmd !== null && logPath) {
        try { appendFileSync(logPath, JSON.stringify(cmd) + "\\n"); } catch {}
      }
      // Deliberately never answer get_state: startSession stays suspended so
      // an interrupt can land mid-startup.
    }
    i = buffer.indexOf("\\n");
  }
});
`;

const runWithHangingFake = () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-hang-fixture-"));
  fixtureDirectories.push(dir);
  const pidFile = NodePath.join(dir, "pid.txt");
  const launcher = makeFakePi(
    dir,
    {
      FAKE_PI_LOG: NodePath.join(dir, "commands.jsonl"),
      FAKE_PI_PID_FILE: pidFile,
    },
    HANG_GET_STATE_SOURCE,
  );
  const layer = Layer.provideMerge(ServerConfig.layerTest(dir, dir), NodeServices.layer);
  return Effect.gen(function* () {
    const adapter = yield* makePiAdapter(
      { enabled: true, binaryPath: launcher, profileDir: "", customModels: [] },
      { instanceId: TEST_INSTANCE_ID },
    );
    return { adapter, pidFile };
  }).pipe(Effect.provide(layer));
};

/**
 * Event-driven rendezvous on the fake child's marker file. `fs.watch` fires on
 * the directory when the child creates the file, so this never polls or sleeps;
 * the 10s bound is only a hang guard. The already-written case is checked once
 * up front so a child that finished before the watcher attached also resolves.
 */
const waitForFileContent = (path: string) =>
  Effect.promise<string>(
    () =>
      new Promise((resolve, reject) => {
        const dir = NodePath.dirname(path);
        let done = false;
        let watcher: NodeFS.FSWatcher | undefined;
        let timeout: NodeJS.Timeout;

        function cleanup(): void {
          clearTimeout(timeout);
          if (watcher !== undefined) {
            watcher.close();
            watcher = undefined;
          }
        }

        function tryResolve(): boolean {
          try {
            const content = NodeFS.readFileSync(path, "utf8").trim();
            if (content.length > 0) {
              done = true;
              cleanup();
              resolve(content);
              return true;
            }
          } catch {
            // not written yet
          }
          return false;
        }

        function fail(): void {
          if (done) return;
          done = true;
          cleanup();
          reject(new Error(`Timed out waiting for file content at ${path}`));
        }

        // @effect-diagnostics-next-line globalTimers:off - fs.watch rendezvous hang guard, not an Effect-schedulable timer.
        timeout = setTimeout(fail, 10_000);
        (timeout as unknown as { unref?: () => void }).unref?.();

        watcher = NodeFS.watch(dir, () => {
          tryResolve();
        });
        tryResolve();
      }),
  );

const completedTurns = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter((event) => event.type === "turn.completed");

const startedTurns = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter((event) => event.type === "turn.started");

const hasRequested = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.some((event) => event.type === "user-input.requested");

const resolvedFor = (requestId: string) => (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.some((event) => event.type === "user-input.resolved" && event.requestId === requestId);

describe("PiAdapter", () => {
  it.effect("sums authoritative call usage without counting streaming snapshots twice", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          { type: "message_update", usage: { input: 999, output: 999, totalTokens: 1998 } },
          assistantMessageEnd("toolUse", {
            usage: { input: 5, output: 3, cacheRead: 2, cacheWrite: 3 },
          }),
          { type: "message_update", usage: { input: 999, output: 999, totalTokens: 1998 } },
          assistantMessageEnd("stop", {
            usage: { input: 7, output: 4, cacheRead: 1, cacheWrite: 0 },
          }),
          { type: "agent_settled" },
        ],
      },
      (h) =>
        Effect.gen(function* () {
          yield* h.adapter.sendTurn({ threadId: THREAD_ID, input: "review" });
          yield* h.waitUntil((events) => events.some((event) => event.type === "turn.completed"));
          const completed = (yield* h.events).find((event) => event.type === "turn.completed");
          expect(completed?.payload).toMatchObject({
            usage: { input: 12, output: 7, cacheRead: 3, cacheWrite: 3, totalTokens: 25 },
            tokenUsage: {
              usageStatus: "complete",
              inputTokens: 18,
              outputTokens: 7,
              cachedInputTokens: 3,
              cacheCreationTokens: 3,
            },
          });
        }),
    ),
  );

  it.effect(
    "resets usage for explicit turns and automatic continuations, including zero totals",
    () =>
      runWithFake({}, (h) =>
        Effect.gen(function* () {
          const sendEvents = (events: Array<Record<string, unknown>>) =>
            h.adapter.sendTurn({
              threadId: THREAD_ID,
              input: `FAKE_EVENTS:${JSON.stringify(events)}`,
            });
          yield* sendEvents([
            { type: "agent_start" },
            assistantMessageEnd("stop", { usage: { input: 10, output: 2 } }),
            { type: "agent_settled" },
            { type: "agent_start" },
            assistantMessageEnd("stop", {
              usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            }),
            { type: "agent_settled" },
          ]);
          yield* h.waitUntil(
            (events) => events.filter((event) => event.type === "turn.completed").length === 2,
          );
          yield* sendEvents([
            { type: "agent_start" },
            assistantMessageEnd("error", { errorMessage: "Provider unavailable" }),
            { type: "agent_settled" },
          ]);
          yield* h.waitUntil(
            (events) => events.filter((event) => event.type === "turn.completed").length === 3,
          );
          const completed = (yield* h.events).filter((event) => event.type === "turn.completed");
          expect(completed.map((event) => event.payload.tokenUsage)).toEqual([
            {
              usageScope: "main_agent",
              usageStatus: "complete",
              hasSubagents: false,
              inputTokens: 10,
              outputTokens: 2,
            },
            {
              usageScope: "main_agent",
              usageStatus: "complete",
              hasSubagents: false,
              inputTokens: 0,
              outputTokens: 0,
            },
            { usageScope: "main_agent", usageStatus: "unavailable", hasSubagents: false },
          ]);
          expect(completed[2]?.payload.state).toBe("failed");
          expect(completed[2]?.payload.usage).toBeUndefined();
        }),
      ),
  );

  it.effect("reports partial totals when any assistant call is missing usage", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          assistantMessageEnd("toolUse", { usage: { input: 10, output: 2 } }),
          assistantMessageEnd("stop", { usage: { output: 3 } }),
          { type: "agent_settled" },
        ],
      },
      (h) =>
        Effect.gen(function* () {
          yield* h.adapter.sendTurn({ threadId: THREAD_ID, input: "review" });
          yield* h.waitUntil((events) => events.some((event) => event.type === "turn.completed"));
          const completed = (yield* h.events).find((event) => event.type === "turn.completed");
          expect(completed?.payload.tokenUsage).toMatchObject({
            usageStatus: "partial",
            inputTokens: 10,
            outputTokens: 5,
          });
        }),
    ),
  );

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

  it.effect("closes the connection scope after an unexpected process exit", () =>
    Effect.gen(function* () {
      const harness = yield* makeFakeHarness({
        exitAfterPrompt: true,
        promptEvents: [{ type: "agent_start" }, textDelta("unfinished work")],
      });
      yield* harness.adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" });
      yield* harness.adapter.sendTurn({ threadId: THREAD_ID, input: "crash during this turn" });
      yield* harness.waitUntil((events) => events.some((event) => event.type === "session.exited"));
      // Process exit and map deletion alone do not prove resource cleanup:
      // wait on a finalizer registered inside the connection-owned scope.
      yield* harness.released;
      expect(yield* harness.adapter.hasSession(THREAD_ID)).toBe(false);
      expect(completedTurns(yield* harness.events)[0]?.payload).toMatchObject({
        state: "failed",
        stopReason: "process-exited",
      });
    }),
  );

  it.effect("concurrent startSession for the same thread spawns a single process", () =>
    Effect.gen(function* () {
      const { adapter, dir } = yield* makeFakeHarness({});
      const [first, second] = yield* Effect.all(
        [
          adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" }),
          adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" }),
        ],
        { concurrency: "unbounded" },
      );
      // The second caller reuses the first's session instead of spawning again.
      expect(first).toBe(second);
      expect(first.status).toBe("ready");

      const commands = NodeFS.readFileSync(NodePath.join(dir, "commands.jsonl"), "utf8")
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0)
        .map((line) => JSON.parse(line) as { readonly type?: string });
      expect(commands.filter((command) => command.type === "get_state").length).toBe(1);
    }),
  );

  it.effect.skipIf(windowsHost)(
    "interrupting startSession before get_state resolves reaps the spawned process",
    () =>
      Effect.gen(function* () {
        const { adapter, pidFile } = yield* runWithHangingFake();
        const startFiber = yield* adapter
          .startSession({ threadId: THREAD_ID, runtimeMode: "full-access" })
          .pipe(Effect.forkChild);

        // Rendezvous with the child before interrupting.
        const pid = Number.parseInt(yield* waitForFileContent(pidFile), 10);
        yield* Fiber.interrupt(startFiber);
        const exit = yield* Fiber.await(startFiber);
        expect(Exit.isFailure(exit)).toBe(true);

        // No half-built session survives the interrupted start.
        expect((yield* adapter.listSessions()).length).toBe(0);
        expect(yield* adapter.hasSession(THREAD_ID)).toBe(false);

        // All-cause cleanup awaited the child's exit before completing, so the
        // process is already reaped.
        expect(() => process.kill(pid, 0)).toThrow();
      }),
  );

  it.effect("accumulates authoritative message_end usage across assistant calls", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("first call"),
          assistantMessageEnd("toolUse", {
            usage: { input: 100, output: 10, cacheRead: 0, cacheWrite: 0 },
          }),
          textDelta("second call"),
          assistantMessageEnd("stop", {
            usage: { input: 50, output: 5, cacheRead: 20, cacheWrite: 30 },
          }),
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
          expect(completed.payload.tokenUsage).toEqual({
            usageScope: "main_agent",
            usageStatus: "complete",
            hasSubagents: false,
            inputTokens: 200,
            outputTokens: 15,
            cachedInputTokens: 20,
            cacheCreationTokens: 30,
          });
        }),
    ),
  );

  it.effect("resets turn usage at each explicit turn boundary", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("reply"),
          assistantMessageEnd("stop", {
            usage: { input: 100, output: 10, cacheRead: 5, cacheWrite: 5 },
          }),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "first" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "second" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 2);
          const completed = completedTurns(yield* events);
          expect(completed.length).toBe(2);
          expect(completed[1]?.type).toBe("turn.completed");
          if (completed[1]?.type !== "turn.completed") return;
          // The second turn starts a fresh accumulator, not the first doubled.
          expect(completed[1].payload.tokenUsage).toEqual({
            usageScope: "main_agent",
            usageStatus: "complete",
            hasSubagents: false,
            inputTokens: 110,
            outputTokens: 10,
            cachedInputTokens: 5,
            cacheCreationTokens: 5,
          });
        }),
    ),
  );

  it.effect("marks a failed turn's accumulated usage partial, not complete", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("partial work"),
          assistantMessageEnd("error", {
            errorMessage: "boom",
            usage: { input: 100, output: 10, cacheRead: 0, cacheWrite: 0 },
          }),
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
          expect(completed.payload.tokenUsage).toEqual({
            usageScope: "main_agent",
            usageStatus: "partial",
            hasSubagents: false,
            inputTokens: 100,
            outputTokens: 10,
          });
        }),
    ),
  );

  it.effect("ignores live message_update usage for the authoritative turn total", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          messageUpdateWithUsage({
            input: 999,
            output: 999,
            cacheRead: 999,
            cacheWrite: 999,
            totalTokens: 3996,
          }),
          textDelta("hi"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil, events }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const seen = yield* events;
          // The live thread-level snapshot still fires from message_update...
          expect(seen.some((event) => event.type === "thread.token-usage.updated")).toBe(true);
          const completed = completedTurns(seen)[0];
          expect(completed?.type).toBe("turn.completed");
          if (completed?.type !== "turn.completed") return;
          // ...but the authoritative turn total comes only from message_end usage.
          expect(completed.payload.tokenUsage).toEqual({
            usageScope: "main_agent",
            usageStatus: "unavailable",
            hasSubagents: false,
          });
        }),
    ),
  );

  it.effect("a synthetic model slug does not overwrite the Pi-native session model", () =>
    runWithFake(
      {
        promptEvents: [
          { type: "agent_start" },
          textDelta("reply"),
          assistantMessageEnd("stop"),
          { type: "agent_settled" },
        ],
      },
      ({ adapter, waitUntil }) =>
        Effect.gen(function* () {
          yield* adapter.sendTurn({
            threadId: THREAD_ID,
            input: "hello",
            modelSelection: { instanceId: TEST_INSTANCE_ID, model: "pi-default" },
          });
          yield* waitUntil((seen) => completedTurns(seen).length >= 1);
          const sessions = yield* adapter.listSessions();
          expect(sessions.length).toBe(1);
          // get_state reported provider/id = "test/test-model"; the synthetic
          // slug must not clobber it.
          expect(sessions[0]?.model).toBe("test/test-model");
        }),
    ),
  );
});
