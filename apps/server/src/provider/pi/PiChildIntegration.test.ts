// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  PiSettings,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { Deferred, Effect, Layer, PubSub, Schema, Stream } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "@effect/vitest";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ChildAgentControl, make as makeChildControl } from "../Services/ChildAgentControl.ts";
import * as ChildAgentChangeHub from "../Services/ChildAgentChangeHub.ts";
import { ProviderSessionDirectoryLive } from "../Layers/ProviderSessionDirectory.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProjectionChildTranscriptRepositoryLive } from "../../persistence/ProjectionChildTranscripts.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEngineLive } from "../../orchestration/Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "../../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../../orchestration/Layers/ProjectionSnapshotQuery.ts";
import { ProviderRuntimeIngestionLive } from "../../orchestration/Layers/ProviderRuntimeIngestion.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProviderRuntimeIngestionService } from "../../orchestration/Services/ProviderRuntimeIngestion.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../../orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../../orchestration/ThreadPlanProgress.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";

const threadId = ThreadId.make("pi-integration-thread");
const instanceId = ProviderInstanceId.make("pi-integration-instance");
const provider = ProviderDriverKind.make("pi");
const createdAt = "2026-09-14T00:00:00.000Z";
const decodePiSettings = Schema.decodeSync(PiSettings);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));

// A real subprocess emits exactly the extension's RPC envelope. Generation
// identity comes from the adapter's spawn environment, never a test constant.
const fixtureSource = `
let buffer = "";
const send = (value) => process.stdout.write(JSON.stringify(value) + "\\n");
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let index;
  while ((index = buffer.indexOf("\\n")) >= 0) {
    const command = JSON.parse(buffer.slice(0, index));
    buffer = buffer.slice(index + 1);
    const response = {type: "response", id: command.id, command: command.type, success: true};
    if (command.type === "get_state") response.data = {
      sessionFile: "/tmp/pi-integration-session.jsonl", sessionId: "integration",
      model: {provider: "fixture", id: "model"}
    };
    send(response);
    if (command.type === "prompt") {
      const batch = JSON.parse(command.message);
      for (const event of batch.events) {
        // Only stamp this process generation onto frames that sent the
        // placeholder. A frame carrying an explicit (old) runId survives so
        // tests can exercise late-generation rejection.
        if (event.type === "entry_appended" && event.entry?.data?.runId === "$RUN") {
          event.entry.data.runId = process.env.T3CODE_PI_BRIDGE_RUN_ID;
        }
        if (event.message?.details?.runId === "$RUN") {
          event.message.details.runId = process.env.T3CODE_PI_BRIDGE_RUN_ID;
        }
        send(event);
      }
      send({type: "extension_error", error: batch.receipt});
    }
  }
});
`;

const entry = (data: Record<string, unknown>, runId = "$RUN") => ({
  type: "entry_appended",
  entry: {
    type: "custom",
    id: `entry-${data.type}-${data.childId}-${data.seq ?? "state"}`,
    customType: "t3-bridge",
    data: { ts: 1, runId, ...data },
  },
});

const subagentResultMessage = (
  childId: string,
  content: string,
  opts: { readonly settleSeq?: number; readonly runId?: string } = {},
): Record<string, unknown> => ({
  role: "custom",
  customType: "subagent-result",
  content,
  display: true,
  details: {
    id: childId,
    childId,
    title: "Explore",
    status: "done",
    ...(opts.settleSeq !== undefined ? { settleSeq: opts.settleSeq } : {}),
    runId: opts.runId ?? "$RUN",
  },
});
const text = (delta: string) => ({
  type: "message_update",
  assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta },
});
const messageEnd = (content: string) => ({
  type: "message_end",
  message: { role: "assistant", content: [{ type: "text", text: content }], stopReason: "stop" },
});

type Receipts = Map<string, Deferred.Deferred<void>>;

function withHarness<E>(
  run: (harness: Effect.Success<ReturnType<typeof initializeHarness>>) => Effect.Effect<void, E>,
) {
  return Effect.gen(function* () {
    const directory = yield* Effect.acquireRelease(
      Effect.sync(() =>
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-child-integration-")),
      ),
      (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
    );
    const receipts: Receipts = new Map();
    const events: ProviderRuntimeEvent[] = [];
    yield* initializeHarness(directory, receipts, events).pipe(
      Effect.flatMap(run),
      Effect.provide(makeTestLayer(directory, receipts, events)),
    );
  });
}

function makeTestLayer(directory: string, receipts: Receipts, events: ProviderRuntimeEvent[]) {
  const launcher = writeFakeCli({ directory, name: "pi", source: fixtureSource });
  const adapterLayer = Layer.effect(
    ProviderService,
    Effect.gen(function* () {
      const adapter = yield* makePiAdapter(decodePiSettings({ binaryPath: launcher }), {
        instanceId,
      });
      const unsupported = () =>
        Effect.die(new Error("Unexpected provider operation in integration fixture"));
      return ProviderService.of({
        startSession: (_threadId, input) => adapter.startSession(input).pipe(Effect.orDie),
        sendTurn: (input) => adapter.sendTurn(input).pipe(Effect.orDie),
        stopSession: (input) => adapter.stopSession(input.threadId).pipe(Effect.orDie),
        listSessions: adapter.listSessions,
        interruptTurn: (input) =>
          adapter.interruptTurn(input.threadId, input.turnId).pipe(Effect.orDie),
        respondToRequest: (input) =>
          adapter
            .respondToRequest(input.threadId, input.requestId, input.decision)
            .pipe(Effect.orDie),
        respondToUserInput: (input) =>
          adapter
            .respondToUserInput(input.threadId, input.requestId, input.answers)
            .pipe(Effect.orDie),
        compactThread: unsupported,
        getCapabilities: () => Effect.succeed(adapter.capabilities),
        assertConversationRollbackSupported: unsupported,
        getInstanceInfo: () =>
          Effect.succeed({
            instanceId,
            driverKind: provider,
            displayName: undefined,
            enabled: true,
            continuationIdentity: { driverKind: provider, continuationKey: "pi-integration" },
          }),
        rollbackConversation: unsupported,
        uploadFeedback: unsupported,
        streamEvents: adapter.streamEvents.pipe(
          Stream.tap((event) => {
            events.push(event);
            const receipt =
              event.type === "runtime.warning" ? receipts.get(event.payload.message) : undefined;
            return receipt ? Deferred.succeed(receipt, undefined) : Effect.void;
          }),
        ),
      } satisfies ProviderServiceShape);
    }),
  );
  const registryLayer = Layer.effect(
    ProviderInstanceRegistry,
    Effect.gen(function* () {
      const changes = yield* PubSub.unbounded<void>();
      return ProviderInstanceRegistry.of({
        getInstance: () => Effect.succeed(undefined),
        listInstances: Effect.succeed([]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.fromPubSub(changes),
        subscribeChanges: PubSub.subscribe(changes),
      });
    }),
  );
  const engineLayer = OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provide(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
  );
  const childControlLayer = Layer.effect(ChildAgentControl, makeChildControl).pipe(
    Layer.provide(registryLayer),
    Layer.provide(ProviderSessionDirectoryLive.pipe(Layer.provide(ProviderSessionRuntime.layer))),
  );
  return Layer.mergeAll(ProviderRuntimeIngestionLive, childControlLayer).pipe(
    Layer.provideMerge(engineLayer),
    Layer.provideMerge(
      OrchestrationProjectionSnapshotQueryLive.pipe(
        Layer.provide(RepositoryIdentityResolver.layer),
      ),
    ),
    Layer.provideMerge(ProjectionChildTranscriptRepositoryLive),
    Layer.provideMerge(ChildAgentChangeHub.layer),
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provideMerge(ThreadPlanProgress.layer),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(adapterLayer),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))),
    Layer.provideMerge(VcsProcess.layer),
    Layer.provideMerge(ServerConfig.layerTest(directory, directory)),
    Layer.provideMerge(NodeServices.layer),
  );
}

const initializeHarness = Effect.fn(function* (
  directory: string,
  receipts: Receipts,
  events: ProviderRuntimeEvent[],
) {
  const engine = yield* OrchestrationEngineService;
  const ingestion = yield* ProviderRuntimeIngestionService;
  const providerService = yield* ProviderService;
  const control = yield* ChildAgentControl;
  const query = yield* ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  yield* ingestion.start();
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("pi-create-project"),
    projectId: ProjectId.make("pi-project"),
    title: "Pi integration",
    workspaceRoot: directory,
    defaultModelSelection: { instanceId, model: "fixture/model" },
    createdAt,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make("pi-create-thread"),
    threadId,
    projectId: ProjectId.make("pi-project"),
    title: "Pi thread",
    modelSelection: { instanceId, model: "fixture/model" },
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    runtimeMode: "approval-required",
    branch: null,
    worktreePath: null,
    createdAt,
  });
  const start = (resumeCursor?: unknown) =>
    providerService.startSession(threadId, {
      threadId,
      providerInstanceId: instanceId,
      runtimeMode: "approval-required",
      cwd: directory,
      ...(resumeCursor !== undefined ? { resumeCursor } : {}),
    });
  let receiptId = 0;
  const send = Effect.fn(function* (rpcEvents: unknown[]) {
    const receipt = `integration-receipt-${++receiptId}`;
    const done = yield* Deferred.make<void>();
    receipts.set(receipt, done);
    yield* providerService.sendTurn({
      threadId,
      input: encodeJson({ events: rpcEvents, receipt }),
    });
    // The marker is read only after preceding events were enqueued.
    yield* Deferred.await(done);
    yield* ingestion.drain;
    receipts.delete(receipt);
  });
  yield* start();
  return { start, send, providerService, control, events, read: () => query.getSnapshot(), sql };
});

describe("Pi child integration", () => {
  it.effect("persists extension-shaped incremental output and a provider-initiated recap", () =>
    withHarness((h) =>
      Effect.gen(function* () {
        yield* h.send([
          { type: "agent_start" },
          text("Spawned the background child."),
          messageEnd("Spawned the background child."),
          entry({
            type: "child.started",
            childId: "sa-1",
            title: "Explore",
            backend: "pi",
            cwd: "/tmp",
            ts: 1,
          }),
          entry({
            type: "child.transcript",
            childId: "sa-1",
            seq: 0,
            chunk: { op: "append", itemId: "answer", kind: "text", text: "A monorepo" },
          }),
          entry({
            type: "child.transcript",
            childId: "sa-1",
            seq: 1,
            chunk: { op: "append", itemId: "answer", kind: "text", text: " with four apps." },
          }),
          { type: "agent_settled" },
          entry({
            type: "child.result",
            childId: "sa-1",
            status: "done",
            finalText: "A monorepo with four apps.",
          }),
          // Real Pi emits agent_start before the followUp's custom message:
          // sendCustomMessage(triggerTurn) runs _runAgentPrompt, so the
          // provider-initiated turn opens first, then the subagent-result
          // message streams as its input message.
          { type: "agent_start" },
          {
            type: "message_start",
            message: subagentResultMessage("sa-1", "A monorepo with four apps.", { settleSeq: 1 }),
          },
          {
            type: "message_end",
            message: subagentResultMessage("sa-1", "A monorepo with four apps.", { settleSeq: 1 }),
          },
          text("The child found a monorepo with four apps."),
          messageEnd("The child found a monorepo with four apps."),
          { type: "agent_settled" },
        ]);
        const listed = yield* h.control.list({ threadId });
        expect(listed.children).toHaveLength(1);
        const child = listed.children[0]!;
        expect(child).toMatchObject({
          instanceId,
          childId: "sa-1",
          status: "done",
          summary: "A monorepo with four apps.",
        });
        expect(child.runId).not.toBe("current");
        const page = yield* h.control.transcript({ ...child, afterSeq: undefined, limit: 200 });
        expect(page.chunks.map((chunk) => chunk.seq)).toEqual([0, 1]);
        const snapshot = yield* h.read();
        const thread = snapshot.threads.find((candidate) => candidate.id === threadId)!;
        expect(
          thread.messages
            .filter((message) => message.role === "assistant")
            .map((message) => message.text)
            .sort(),
        ).toEqual([
          "A monorepo with four apps.",
          "Spawned the background child.",
          "The child found a monorepo with four apps.",
        ]);
        // The serialized conversation must NOT fold child transcript chunks
        // into the parent timeline; the child's readable output arrives only
        // through the subagent-result receipt.
        expect(
          h.events.some(
            (event) =>
              event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              (event.payload.data as Record<string, unknown> | undefined)?.source ===
                "subagent-result",
          ),
        ).toBe(true);
        expect(h.events.filter((event) => event.type === "turn.started")).toHaveLength(2);
        expect(h.events.filter((event) => event.type === "turn.completed")).toHaveLength(2);
        expect(
          thread.activities.some((activity) => encodeJson(activity).includes('"op":"append"')),
        ).toBe(false);
      }),
    ),
  );

  it.effect(
    "keeps reused child ids distinct across process generations and pages duplicate delivery once",
    () =>
      withHarness((h) =>
        Effect.gen(function* () {
          const transcript = Array.from({ length: 205 }, (_, seq) =>
            entry({
              type: "child.transcript",
              childId: "sa-1",
              seq,
              chunk: { op: "append", itemId: "long-answer", kind: "text", text: `${seq} ` },
            }),
          );
          yield* h.send([
            { type: "agent_start" },
            entry({ type: "child.started", childId: "sa-1", title: "Old child", backend: "pi" }),
            ...transcript,
            ...transcript.slice(-5),
            entry({
              type: "child.started",
              childId: "sa-2",
              title: "Unfinished child",
              backend: "pi",
            }),
            entry({
              type: "child.result",
              childId: "sa-1",
              status: "done",
              finalText: "Old history",
            }),
            { type: "agent_settled" },
          ]);
          const oldSession = (yield* h.providerService.listSessions())[0]!;
          const old = (yield* h.control.list({ threadId })).children.find(
            (child) => child.childId === "sa-1",
          )!;
          const first = yield* h.control.transcript({ ...old, afterSeq: undefined, limit: 200 });
          expect(first.chunks).toHaveLength(200);
          expect(first.hasMore).toBe(true);
          const tail = yield* h.control.transcript({
            ...old,
            afterSeq: first.lastSeq ?? undefined,
            limit: 200,
          });
          expect(tail.chunks.map((chunk) => chunk.seq)).toEqual([200, 201, 202, 203, 204]);
          expect(tail.hasMore).toBe(false);
          yield* h.providerService.stopSession({ threadId });
          yield* h.start(oldSession.resumeCursor);
          yield* h.send([
            { type: "agent_start" },
            entry({ type: "child.started", childId: "sa-1", title: "New child", backend: "pi" }),
            entry({
              type: "child.transcript",
              childId: "sa-1",
              seq: 0,
              chunk: { op: "append", itemId: "new", kind: "text", text: "New history" },
            }),
            { type: "agent_settled" },
          ]);
          const children = (yield* h.control.list({ threadId })).children;
          expect(children).toHaveLength(3);
          expect(children.find((child) => child.childId === "sa-2")?.status).toBe("interrupted");
          expect(
            children.find((child) => child.runId === old.runId && child.childId === "sa-1")?.status,
          ).toBe("done");
          const fresh = children.find((child) => child.runId !== old.runId)!;
          const freshPage = yield* h.control.transcript({
            ...fresh,
            afterSeq: undefined,
            limit: 200,
          });
          expect(freshPage.chunks).toHaveLength(1);
          expect(freshPage.chunks[0]?.chunk).toMatchObject({ text: "New history" });
        }),
      ),
  );

  it.effect("tracks four concurrent children with distinct transcripts", () =>
    withHarness((h) =>
      Effect.gen(function* () {
        const spawn = [0, 1, 2, 3].flatMap((n) => [
          entry({
            type: "child.started",
            childId: `sa-${n}`,
            title: `Child ${n}`,
            backend: "pi",
          }),
          entry({
            type: "child.transcript",
            childId: `sa-${n}`,
            seq: 0,
            chunk: { op: "append", itemId: `answer-${n}`, kind: "text", text: `child ${n} line 0` },
          }),
          entry({
            type: "child.transcript",
            childId: `sa-${n}`,
            seq: 1,
            chunk: { op: "append", itemId: `answer-${n}`, kind: "text", text: `child ${n} line 1` },
          }),
        ]);
        const results = [0, 1, 2, 3].map((n) =>
          entry({
            type: "child.result",
            childId: `sa-${n}`,
            status: "done",
            finalText: `Child ${n} done`,
          }),
        );
        yield* h.send([{ type: "agent_start" }, ...spawn, ...results, { type: "agent_settled" }]);
        const children = (yield* h.control.list({ threadId })).children;
        expect(children).toHaveLength(4);
        for (let n = 0; n < 4; n += 1) {
          const child = children.find((candidate) => candidate.childId === `sa-${n}`)!;
          expect(child.status).toBe("done");
          expect(child.summary).toBe(`Child ${n} done`);
          const page = yield* h.control.transcript({ ...child, afterSeq: undefined, limit: 200 });
          expect(page.chunks.map((chunk) => chunk.chunk)).toMatchObject([
            { text: `child ${n} line 0` },
            { text: `child ${n} line 1` },
          ]);
        }
      }),
    ),
  );

  it.effect(
    "drops a late stale-generation frame after restart without mutating the new child",
    () =>
      withHarness((h) =>
        Effect.gen(function* () {
          yield* h.send([
            { type: "agent_start" },
            entry({ type: "child.started", childId: "sa-1", title: "Old child", backend: "pi" }),
            entry({
              type: "child.transcript",
              childId: "sa-1",
              seq: 0,
              chunk: { op: "append", itemId: "old", kind: "text", text: "old history" },
            }),
            entry({ type: "child.result", childId: "sa-1", status: "done", finalText: "old done" }),
            { type: "agent_settled" },
          ]);
          const oldRunId = (
            h.events.find((event) => event.type === "task.started")?.payload as
              | { runId?: string }
              | undefined
          )?.runId;
          const oldSession = (yield* h.providerService.listSessions())[0]!;
          yield* h.providerService.stopSession({ threadId });
          yield* h.start(oldSession.resumeCursor);

          yield* h.send([
            { type: "agent_start" },
            entry({ type: "child.started", childId: "sa-1", title: "New child", backend: "pi" }),
            entry({
              type: "child.transcript",
              childId: "sa-1",
              seq: 0,
              chunk: { op: "append", itemId: "new", kind: "text", text: "new history" },
            }),
            // A stale frame from the previous generation is dropped by the
            // adapter's generation guard, so the new child is never mutated.
            entry(
              { type: "child.result", childId: "sa-1", status: "done", finalText: "stale" },
              oldRunId,
            ),
            { type: "agent_settled" },
          ]);

          const children = (yield* h.control.list({ threadId })).children;
          expect(children).toHaveLength(2);
          const oldChild = children.find((child) => child.runId === oldRunId)!;
          expect(oldChild.status).toBe("done");
          expect(oldChild.summary).toBe("old done");
          const newChild = children.find((child) => child.runId !== oldRunId)!;
          // The stale result never landed: the new child stays running.
          expect(newChild.status).toBe("running");
          const page = yield* h.control.transcript({
            ...newChild,
            afterSeq: undefined,
            limit: 200,
          });
          expect(page.chunks.map((chunk) => chunk.seq)).toEqual([0]);
          expect(page.chunks[0]?.chunk).toMatchObject({ text: "new history" });
        }),
      ),
  );

  it.effect(
    "persists canonical runId on ordinary task activities and scopes stable progress ids per generation",
    () =>
      withHarness((h) =>
        Effect.gen(function* () {
          yield* h.send([
            { type: "agent_start" },
            entry({
              type: "child.started",
              childId: "sa-1",
              title: "Explorer Alpha",
              backend: "pi",
            }),
            entry({ type: "child.status", childId: "sa-1", status: "running" }),
            entry({ type: "child.usage", childId: "sa-1", tokens: 1200 }),
            entry({
              type: "child.started",
              childId: "sa-2",
              title: "Explorer Beta",
              backend: "pi",
            }),
            entry({ type: "child.status", childId: "sa-2", status: "running" }),
            entry({ type: "child.usage", childId: "sa-2", tokens: 800 }),
            entry({
              type: "child.status",
              childId: "sa-1",
              status: "cancelled",
              errorText: "Run was aborted",
            }),
            entry({
              type: "child.result",
              childId: "sa-1",
              status: "cancelled",
              errorText: "Run was aborted",
            }),
            { type: "agent_settled" },
          ]);

          const firstRun = (
            h.events.find((event) => event.type === "task.started")?.payload as
              | { runId?: string }
              | undefined
          )?.runId;
          expect(firstRun).toBeDefined();

          const sql = h.sql;
          const readRows = sql<{
            readonly activityId: string;
            readonly kind: string;
            readonly payloadJson: string;
          }>`
            SELECT activity_id AS "activityId", kind, payload_json AS "payloadJson"
            FROM projection_thread_activities
            WHERE thread_id = ${threadId}
              AND kind IN ('task.started', 'task.progress', 'task.updated', 'task.completed')
            ORDER BY created_at ASC
          `;
          const firstRows = yield* readRows;

          // Every ordinary task.* activity for the bridged children carries the
          // canonical process-generation run id (the upstream fix under test).
          const taskRows = firstRows.filter(
            (row) =>
              ((decodeJson(row.payloadJson) as Record<string, unknown>).taskType as unknown) ===
              "subagent",
          );
          expect(taskRows.length).toBeGreaterThan(0);
          for (const row of taskRows) {
            expect((decodeJson(row.payloadJson) as Record<string, unknown>).runId).toBe(firstRun);
          }

          // Stable usage ids are generation-aware: one row per task, scoped by run id.
          const usageRows = taskRows.filter(
            (row) =>
              (decodeJson(row.payloadJson) as Record<string, unknown>).usageSnapshot === true,
          );
          expect(usageRows.map((row) => row.activityId).sort()).toEqual(
            [
              `task-usage:${threadId}:sa-1:${firstRun}`,
              `task-usage:${threadId}:sa-2:${firstRun}`,
            ].sort(),
          );

          // Reuse sa-1 in a fresh process generation: the stable id must write a
          // second row instead of clobbering the prior generation's retained row.
          const oldSession = (yield* h.providerService.listSessions())[0]!;
          yield* h.providerService.stopSession({ threadId });
          yield* h.start(oldSession.resumeCursor);
          yield* h.send([
            { type: "agent_start" },
            entry({ type: "child.started", childId: "sa-1", title: "New child", backend: "pi" }),
            entry({ type: "child.usage", childId: "sa-1", tokens: 500 }),
            { type: "agent_settled" },
          ]);

          const secondRows = yield* readRows;
          const sa1UsageRows = secondRows.filter((row) => {
            const payload = decodeJson(row.payloadJson) as Record<string, unknown>;
            return payload.taskId === "sa-1" && payload.usageSnapshot === true;
          });
          expect(sa1UsageRows).toHaveLength(2);
          expect(
            new Set(
              sa1UsageRows.map(
                (row) => (decodeJson(row.payloadJson) as Record<string, unknown>).runId,
              ),
            ).size,
          ).toBe(2);

          // Durable children stay distinct: old cancelled, the abandoned sibling
          // interrupted on teardown, and the new generation running.
          const children = (yield* h.control.list({ threadId })).children;
          expect(children).toHaveLength(3);
          expect(
            children.find((child) => child.childId === "sa-1" && child.runId === firstRun)?.status,
          ).toBe("cancelled");
          expect(children.find((child) => child.childId === "sa-2")?.status).toBe("interrupted");
          expect(
            children.find((child) => child.childId === "sa-1" && child.runId !== firstRun)?.status,
          ).toBe("running");
        }),
      ),
  );
});
