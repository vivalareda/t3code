import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import {
  ChildAgentChangeHub,
  layer as ChildAgentChangeHubLayer,
} from "../provider/Services/ChildAgentChangeHub.ts";
import { SqlitePersistenceMemory } from "./Layers/Sqlite.ts";
import {
  ProjectionChildTranscriptRepository,
  ProjectionChildTranscriptRepositoryLive,
} from "./ProjectionChildTranscripts.ts";

// The repository, change hub, and sqlite are all exposed and memoized together
// so the repository and tests see the same shared hub instance (the same
// provideMerge composition server.ts uses for its persistence layer).
const testLayer = ProjectionChildTranscriptRepositoryLive.pipe(
  Layer.provideMerge(ChildAgentChangeHubLayer),
  Layer.provideMerge(SqlitePersistenceMemory),
);

const CHUNK = {
  op: "append" as const,
  itemId: "item-1",
  kind: "text" as const,
  text: "hello",
};

// The in-memory SQLite is shared across the suite, so every test owns a
// distinct (threadId, instanceId) namespace to stay isolated from its
// neighbours.
const baseFor = (threadId: string) => ({
  threadId: threadId as never,
  instanceId: "pi" as never,
});

const layer = it.layer(testLayer);

layer("ProjectionChildTranscriptRepository", (it) => {
  it.effect("persists chunks and states scoped by run", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = {
        ...baseFor("th-persist"),
        runId: "run-a",
        childId: "sa-1",
      };

      yield* repository.upsertState({
        ...base,
        title: "Explore repo",
        backend: "pi",
        cwd: "/tmp",
        model: "anthropic/claude-sonnet-4",
        effort: null,
        status: "running",
        outcomeStatus: null,
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T00:00:00.000Z",
        settledAt: null,
        updatedAt: "2026-09-14T00:00:00.000Z",
      });
      yield* repository.appendChunk({
        ...base,
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:01.000Z",
      });
      yield* repository.appendChunk({
        ...base,
        seq: 1,
        chunk: { op: "add", itemId: "item-2", item: { kind: "user", text: "hi" } },
        createdAt: "2026-09-14T00:00:02.000Z",
      });

      // Same child id under a different run is a different generation.
      yield* repository.upsertState({
        ...base,
        runId: "run-b",
        title: "Explore repo",
        backend: "pi",
        cwd: null,
        model: null,
        effort: null,
        status: "running",
        outcomeStatus: null,
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T01:00:00.000Z",
        settledAt: null,
        updatedAt: "2026-09-14T01:00:00.000Z",
      });

      const pageA = yield* repository.listChunks({ ...base, limit: 10 });
      expect(pageA.chunks).toHaveLength(2);
      expect(pageA.lastSeq).toBe(1);
      expect(pageA.hasMore).toBe(false);

      const pageB = yield* repository.listChunks({
        ...base,
        runId: "run-b",
        limit: 10,
      });
      expect(pageB.chunks).toHaveLength(0);

      // Incremental reads: afterSeq skips what the viewer already has.
      const tail = yield* repository.listChunks({ ...base, afterSeq: 0, limit: 10 });
      expect(tail.chunks).toHaveLength(1);
      expect(tail.chunks[0]?.seq).toBe(1);

      // Bounded page.
      const limited = yield* repository.listChunks({ ...base, limit: 1 });
      expect(limited.chunks).toHaveLength(1);
      expect(limited.hasMore).toBe(true);

      // Terminal states overwrite; completed stays completed.
      yield* repository.upsertState({
        ...base,
        title: null,
        backend: null,
        cwd: null,
        model: null,
        effort: null,
        status: "done",
        outcomeStatus: "completed",
        summary: "all done",
        errorText: null,
        tokens: 42,
        contextWindow: null,
        startedAt: null,
        settledAt: "2026-09-14T00:05:00.000Z",
        updatedAt: "2026-09-14T00:05:00.000Z",
      });
      const states = yield* repository.listStates({ threadId: base.threadId });
      expect(states).toHaveLength(2);
      const runA = states.find((state) => state.runId === "run-a");
      expect(runA?.status).toBe("done");
      expect(runA?.summary).toBe("all done");
      expect(runA?.tokens).toBe(42);

      // markUnfinishedInterrupted only touches running rows of the run.
      yield* repository.markUnfinishedInterrupted({
        threadId: base.threadId,
        instanceId: base.instanceId,
        runId: "run-b",
        updatedAt: "2026-09-14T02:00:00.000Z",
      });
      const afterSweep = yield* repository.listStates({ threadId: base.threadId });
      const runAAfter = afterSweep.find((state) => state.runId === "run-a");
      const runBAfter = afterSweep.find((state) => state.runId === "run-b");
      expect(runAAfter?.status).toBe("done");
      expect(runBAfter?.status).toBe("interrupted");
    }),
  );

  it.effect("rejects chunks from another thread", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = {
        ...baseFor("th-scope"),
        runId: "run-a",
        childId: "sa-1",
      };
      yield* repository.appendChunk({
        ...base,
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:01.000Z",
      });
      const otherThread = yield* repository.listChunks({
        ...base,
        threadId: "th-scope-other" as never,
        limit: 10,
      });
      expect(otherThread.chunks).toHaveLength(0);
    }),
  );

  it.effect("never resurrects a terminal state from a late running write", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = {
        ...baseFor("th-terminal"),
        runId: "run-a",
        childId: "sa-1",
      };
      const state = {
        title: null,
        backend: null,
        cwd: null,
        model: null,
        effort: null,
        status: "running" as const,
        outcomeStatus: null,
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T00:00:00.000Z",
        settledAt: null,
        updatedAt: "2026-09-14T00:00:00.000Z",
      };
      yield* repository.upsertState({ ...base, ...state });
      yield* repository.upsertState({
        ...base,
        ...state,
        status: "done",
        outcomeStatus: "completed",
        summary: "finished",
        settledAt: "2026-09-14T00:05:00.000Z",
        updatedAt: "2026-09-14T00:05:00.000Z",
      });
      // A replayed task.started (running) after the terminal write must not
      // resurrect the row or clobber its terminal outcome.
      yield* repository.upsertState({
        ...base,
        ...state,
        title: "Late title",
        updatedAt: "2026-09-14T00:06:00.000Z",
      });
      yield* repository.upsertState({
        ...base,
        ...state,
        status: "interrupted",
        outcomeStatus: "interrupted",
        summary: "late teardown",
        errorText: "process exited",
        settledAt: "2026-09-14T00:07:00.000Z",
        updatedAt: "2026-09-14T00:07:00.000Z",
      });
      const found = yield* repository.getState({ ...base });
      expect(Option.isSome(found)).toBe(true);
      if (Option.isSome(found)) {
        expect(found.value.status).toBe("done");
        expect(found.value.outcomeStatus).toBe("completed");
        expect(found.value.summary).toBe("finished");
        expect(found.value.errorText).toBe(null);
        expect(found.value.settledAt).toBe("2026-09-14T00:05:00.000Z");
        expect(found.value.updatedAt).toBe("2026-09-14T00:05:00.000Z");
      }
    }),
  );

  it.effect("bounds roster summaries without truncating readable transcripts", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = { ...baseFor("th-preview"), runId: "run-a", childId: "sa-1" };
      const text = "😀".repeat(5000);
      const state = {
        ...base,
        title: "Review",
        backend: "pi",
        cwd: null,
        model: null,
        effort: null,
        status: "done" as const,
        outcomeStatus: "completed",
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T00:00:00.000Z",
        settledAt: "2026-09-14T00:05:00.000Z",
        updatedAt: "2026-09-14T00:05:00.000Z",
      };
      // The terminal status can precede its result metadata.
      yield* repository.upsertState(state);
      yield* repository.upsertState({ ...state, summary: text });
      yield* repository.appendChunk({
        ...base,
        seq: 1,
        chunk: { op: "add", itemId: "answer", item: { kind: "text", text } },
        createdAt: state.updatedAt,
      });
      const rows = yield* repository.listStates({ threadId: base.threadId });
      expect(rows[0]?.summary).toBe("😀".repeat(2000));
      const page = yield* repository.listChunks({ ...base, limit: 1 });
      expect(page.chunks[0]?.chunk).toEqual({
        op: "add",
        itemId: "answer",
        item: { kind: "text", text },
      });
    }),
  );

  it.effect("markAllUnfinishedInterrupted sweeps running rows only", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const state = (runId: string, status: "running" | "done") => ({
        title: null,
        backend: null,
        cwd: null,
        model: null,
        effort: null,
        status,
        outcomeStatus: status === "done" ? "completed" : null,
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T00:00:00.000Z",
        settledAt: status === "done" ? "2026-09-14T00:01:00.000Z" : null,
        updatedAt: "2026-09-14T00:00:00.000Z",
      });
      const base = baseFor("th-sweep");
      yield* repository.upsertState({
        ...base,
        runId: "run-a",
        childId: "sa-1",
        ...state("run-a", "running"),
      });
      yield* repository.upsertState({
        ...base,
        runId: "run-b",
        childId: "sa-2",
        ...state("run-b", "done"),
      });
      yield* repository.markAllUnfinishedInterrupted({ updatedAt: "2026-09-14T02:00:00.000Z" });
      const rows = yield* repository.listStates({ threadId: base.threadId });
      expect(rows.find((row) => row.childId === "sa-1")?.status).toBe("interrupted");
      expect(rows.find((row) => row.childId === "sa-2")?.status).toBe("done");
    }),
  );

  it.effect("getState resolves only the exact full tuple", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = {
        ...baseFor("th-tuple"),
        runId: "run-a",
        childId: "sa-1",
      };
      yield* repository.upsertState({
        ...base,
        title: null,
        backend: null,
        cwd: null,
        model: null,
        effort: null,
        status: "running",
        outcomeStatus: null,
        summary: null,
        errorText: null,
        tokens: null,
        contextWindow: null,
        startedAt: "2026-09-14T00:00:00.000Z",
        settledAt: null,
        updatedAt: "2026-09-14T00:00:00.000Z",
      });
      expect(Option.isSome(yield* repository.getState({ ...base }))).toBe(true);
      expect(Option.isNone(yield* repository.getState({ ...base, runId: "run-b" }))).toBe(true);
    }),
  );

  it.effect("rejects out-of-bounds transcript page limits", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const base = {
        ...baseFor("th-bounds"),
        runId: "run-a",
        childId: "sa-1",
      };
      yield* repository.appendChunk({
        ...base,
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:01.000Z",
      });

      const zero = yield* Effect.exit(repository.listChunks({ ...base, limit: 0 }));
      expect(Exit.isFailure(zero)).toBe(true);
      const tooLarge = yield* Effect.exit(repository.listChunks({ ...base, limit: 501 }));
      expect(Exit.isFailure(tooLarge)).toBe(true);

      const ok = yield* repository.listChunks({ ...base, limit: 500 });
      expect(ok.chunks).toHaveLength(1);
      expect(ok.hasMore).toBe(false);
    }),
  );

  it.effect("suppresses duplicate chunk delivery change notices", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const hub = yield* ChildAgentChangeHub;
      const base = {
        ...baseFor("th-dup-notify"),
        runId: "run-a",
        childId: "sa-1",
      };
      const pull = yield* Stream.toPull(hub.subscribe(base.threadId));
      const [reset] = yield* pull;
      expect(reset).toMatchObject({ type: "reset", threadId: base.threadId });

      yield* repository.appendChunk({
        ...base,
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:01.000Z",
      });
      const [first] = yield* pull;
      expect(first).toMatchObject({ type: "changed", childId: "sa-1" });

      // Same tuple + seq is an idempotent no-op insert: no notice is published.
      yield* repository.appendChunk({
        ...base,
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:01.000Z",
      });
      // A different child then makes the next expected notice unambiguous: if the
      // duplicate had been published, this sa-2 notice would arrive second.
      yield* repository.appendChunk({
        ...base,
        childId: "sa-2",
        seq: 0,
        chunk: CHUNK,
        createdAt: "2026-09-14T00:00:02.000Z",
      });
      const [second] = yield* pull;
      expect(second).toMatchObject({ type: "changed", childId: "sa-2" });
    }),
  );
});
