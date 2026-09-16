/**
 * Durable child-agent transcript and state storage, projected from
 * `task.*` runtime events (including the bridged `task.transcript` chunks).
 *
 * Scope invariants: every read and mutation is keyed by the owning
 * (threadId, instanceId, runId, childId) tuple. Clients never address
 * children by filesystem paths, and a run id namespaces child ids so a
 * reused `sa-1` after a process restart cannot collide with history.
 *
 * State transitions are monotonic: once a row reaches a terminal status it is
 * never resurrected by a replayed or late `running` write. Chunks are
 * idempotent by (tuple, seq), so duplicate delivery and reconnect cannot
 * duplicate rows.
 *
 * @module persistence/ProjectionChildTranscripts
 */
import {
  IsoDateTime,
  NonNegativeInt,
  ProviderInstanceId,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import * as Struct from "effect/Struct";

import { ChildAgentChangeHub } from "../provider/Services/ChildAgentChangeHub.ts";
import { toPersistenceSqlError, type ProjectionRepositoryError } from "./Errors.ts";

export const ChildTranscriptStatus = Schema.Literals([
  "running",
  "done",
  "error",
  "cancelled",
  "interrupted",
]);
export type ChildTranscriptStatus = typeof ChildTranscriptStatus.Type;

export const ProjectionChildState = Schema.Struct({
  threadId: ThreadId,
  instanceId: ProviderInstanceId,
  runId: TrimmedNonEmptyString,
  childId: TrimmedNonEmptyString,
  title: Schema.NullOr(TrimmedNonEmptyString),
  backend: Schema.NullOr(TrimmedNonEmptyString),
  cwd: Schema.NullOr(TrimmedNonEmptyString),
  model: Schema.NullOr(TrimmedNonEmptyString),
  effort: Schema.NullOr(TrimmedNonEmptyString),
  status: ChildTranscriptStatus,
  outcomeStatus: Schema.NullOr(TrimmedNonEmptyString),
  summary: Schema.NullOr(Schema.String),
  errorText: Schema.NullOr(Schema.String),
  tokens: Schema.NullOr(NonNegativeInt),
  contextWindow: Schema.NullOr(NonNegativeInt),
  startedAt: Schema.NullOr(IsoDateTime),
  settledAt: Schema.NullOr(IsoDateTime),
  updatedAt: IsoDateTime,
});
export type ProjectionChildState = typeof ProjectionChildState.Type;

export const ProjectionChildTranscriptChunk = Schema.Struct({
  threadId: ThreadId,
  instanceId: ProviderInstanceId,
  runId: TrimmedNonEmptyString,
  childId: TrimmedNonEmptyString,
  seq: NonNegativeInt,
  chunk: Schema.Unknown,
  createdAt: IsoDateTime,
});
export type ProjectionChildTranscriptChunk = typeof ProjectionChildTranscriptChunk.Type;

const ProjectionChildTranscriptChunkDbRow = ProjectionChildTranscriptChunk.mapFields(
  Struct.assign({
    chunk: Schema.fromJsonString(Schema.Unknown),
  }),
);

export const ListChildTranscriptInput = Schema.Struct({
  threadId: ThreadId,
  instanceId: ProviderInstanceId,
  runId: TrimmedNonEmptyString,
  childId: TrimmedNonEmptyString,
  /** Exclusive lower bound; omit to read from the beginning. */
  afterSeq: Schema.optional(NonNegativeInt),
  limit: NonNegativeInt.check(Schema.isGreaterThan(0)).check(Schema.isLessThanOrEqualTo(500)),
});
export type ListChildTranscriptInput = typeof ListChildTranscriptInput.Type;

export interface ListChildTranscriptPage {
  readonly chunks: ReadonlyArray<ProjectionChildTranscriptChunk>;
  /** Sequence of the last returned chunk; undefined when the page is empty. */
  readonly lastSeq: number | undefined;
  readonly hasMore: boolean;
}

export const ListChildStatesInput = Schema.Struct({
  threadId: ThreadId,
  instanceId: Schema.optional(ProviderInstanceId),
});
export type ListChildStatesInput = typeof ListChildStatesInput.Type;

export const GetChildStateInput = Schema.Struct({
  threadId: ThreadId,
  instanceId: ProviderInstanceId,
  runId: TrimmedNonEmptyString,
  childId: TrimmedNonEmptyString,
});
export type GetChildStateInput = typeof GetChildStateInput.Type;

export class ProjectionChildTranscriptRepository extends Context.Service<
  ProjectionChildTranscriptRepository,
  {
    readonly appendChunk: (
      chunk: ProjectionChildTranscriptChunk,
    ) => Effect.Effect<void, ProjectionRepositoryError>;
    readonly upsertState: (
      state: ProjectionChildState,
    ) => Effect.Effect<void, ProjectionRepositoryError>;
    readonly getState: (
      input: GetChildStateInput,
    ) => Effect.Effect<Option.Option<ProjectionChildState>, ProjectionRepositoryError>;
    readonly listChunks: (
      input: ListChildTranscriptInput,
    ) => Effect.Effect<ListChildTranscriptPage, ProjectionRepositoryError>;
    readonly listStates: (
      input: ListChildStatesInput,
    ) => Effect.Effect<ReadonlyArray<ProjectionChildState>, ProjectionRepositoryError>;
    readonly markUnfinishedInterrupted: (input: {
      readonly threadId: ThreadId;
      readonly instanceId: ProviderInstanceId;
      readonly runId: string;
      readonly updatedAt: IsoDateTime;
    }) => Effect.Effect<void, ProjectionRepositoryError>;
    /**
     * Startup reconciliation: interrupt every still-running durable child,
     * independent of any live provider binding. Completed and already
     * interrupted rows are preserved.
     */
    readonly markAllUnfinishedInterrupted: (input: {
      readonly updatedAt: IsoDateTime;
    }) => Effect.Effect<void, ProjectionRepositoryError>;
    readonly deleteByThreadId: (
      threadId: ThreadId,
    ) => Effect.Effect<void, ProjectionRepositoryError>;
  }
>()("t3/persistence/ProjectionChildTranscripts/ProjectionChildTranscriptRepository") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // The hub is a required dependency so every writer and subscriber shares one
  // notification stream. Production, startup reconciliation, and tests all
  // build against the same hub; there is no silent no-notification fallback.
  const changeHub = yield* ChildAgentChangeHub;
  const notifyChanged = (input: {
    readonly threadId: ThreadId;
    readonly instanceId: ProviderInstanceId;
    readonly runId: string;
    readonly childId: string;
  }) => changeHub.publishChanged(input);

  // `RETURNING seq` reports whether the insert actually happened: an
  // `ON CONFLICT DO NOTHING` no-op (duplicate delivery, reconnect replay)
  // returns no rows, so we suppress the redundant `changed` notice rather than
  // waking subscribers for a write that changed nothing.
  const insertChunk = SqlSchema.findAll({
    Request: ProjectionChildTranscriptChunk,
    Result: Schema.Struct({ seq: NonNegativeInt }),
    execute: (chunk) => sql`
      INSERT INTO projection_child_transcripts (
        thread_id, instance_id, run_id, child_id, seq, chunk_json, created_at
      )
      VALUES (
        ${chunk.threadId}, ${chunk.instanceId}, ${chunk.runId}, ${chunk.childId},
        ${chunk.seq}, ${JSON.stringify(chunk.chunk)}, ${chunk.createdAt}
      )
      ON CONFLICT (thread_id, instance_id, run_id, child_id, seq) DO NOTHING
      RETURNING seq AS "seq"
    `,
  });

  // The roster is a lightweight preview. Full answers remain in transcript
  // chunks, rather than being resent on every list refresh as another summary.
  const upsertStateRow = SqlSchema.void({
    Request: ProjectionChildState,
    execute: (state) => sql`
      INSERT INTO projection_child_states (
        thread_id, instance_id, run_id, child_id,
        title, backend, cwd, model, effort,
        status, outcome_status, summary, error_text,
        tokens, context_window, started_at, settled_at, updated_at
      )
      VALUES (
        ${state.threadId}, ${state.instanceId}, ${state.runId}, ${state.childId},
        ${state.title}, ${state.backend}, ${state.cwd}, ${state.model}, ${state.effort},
        ${state.status}, ${state.outcomeStatus}, substr(${state.summary}, 1, 2000), ${state.errorText},
        ${state.tokens}, ${state.contextWindow}, ${state.startedAt}, ${state.settledAt}, ${state.updatedAt}
      )
      ON CONFLICT (thread_id, instance_id, run_id, child_id)
      DO UPDATE SET
        title = COALESCE(excluded.title, projection_child_states.title),
        backend = COALESCE(excluded.backend, projection_child_states.backend),
        cwd = COALESCE(excluded.cwd, projection_child_states.cwd),
        model = COALESCE(excluded.model, projection_child_states.model),
        effort = COALESCE(excluded.effort, projection_child_states.effort),
        -- Monotonic: a terminal row never returns to running (and terminal
        -- outcomes are sticky), so replay/late delivery cannot resurrect it.
        status = CASE
          WHEN projection_child_states.status = 'running' THEN excluded.status
          ELSE projection_child_states.status
        END,
        outcome_status = COALESCE(excluded.outcome_status, projection_child_states.outcome_status),
        summary = COALESCE(excluded.summary, projection_child_states.summary),
        error_text = COALESCE(excluded.error_text, projection_child_states.error_text),
        tokens = COALESCE(excluded.tokens, projection_child_states.tokens),
        context_window = COALESCE(excluded.context_window, projection_child_states.context_window),
        started_at = COALESCE(projection_child_states.started_at, excluded.started_at),
        settled_at = COALESCE(excluded.settled_at, projection_child_states.settled_at),
        updated_at = excluded.updated_at
      -- A matching terminal result may fill in summary/usage after its status
      -- frame. A conflicting replay or teardown must not rewrite settled history.
      WHERE projection_child_states.status = 'running'
         OR excluded.status = projection_child_states.status
    `,
  });

  const getStateRow = SqlSchema.findOneOption({
    Request: GetChildStateInput,
    Result: ProjectionChildState,
    execute: ({ threadId, instanceId, runId, childId }) => sql`
      SELECT
        thread_id AS "threadId", instance_id AS "instanceId", run_id AS "runId",
        child_id AS "childId", title, backend, cwd, model, effort,
        status, outcome_status AS "outcomeStatus", summary, error_text AS "errorText",
        tokens, context_window AS "contextWindow",
        started_at AS "startedAt", settled_at AS "settledAt", updated_at AS "updatedAt"
      FROM projection_child_states
      WHERE thread_id = ${threadId}
        AND instance_id = ${instanceId}
        AND run_id = ${runId}
        AND child_id = ${childId}
      LIMIT 1
    `,
  });

  const listChunkRows = SqlSchema.findAll({
    Request: ListChildTranscriptInput,
    Result: ProjectionChildTranscriptChunkDbRow,
    execute: ({ threadId, instanceId, runId, childId, afterSeq, limit }) =>
      afterSeq === undefined
        ? sql`
            SELECT
              thread_id AS "threadId", instance_id AS "instanceId", run_id AS "runId",
              child_id AS "childId", seq, chunk_json AS "chunk", created_at AS "createdAt"
            FROM projection_child_transcripts
            WHERE thread_id = ${threadId}
              AND instance_id = ${instanceId}
              AND run_id = ${runId}
              AND child_id = ${childId}
            ORDER BY seq ASC
            LIMIT ${limit + 1}
          `
        : sql`
            SELECT
              thread_id AS "threadId", instance_id AS "instanceId", run_id AS "runId",
              child_id AS "childId", seq, chunk_json AS "chunk", created_at AS "createdAt"
            FROM projection_child_transcripts
            WHERE thread_id = ${threadId}
              AND instance_id = ${instanceId}
              AND run_id = ${runId}
              AND child_id = ${childId}
              AND seq > ${afterSeq}
            ORDER BY seq ASC
            LIMIT ${limit + 1}
          `,
  });

  const listStateRows = SqlSchema.findAll({
    Request: ListChildStatesInput,
    Result: ProjectionChildState,
    execute: ({ threadId, instanceId }) =>
      instanceId === undefined
        ? sql`
            SELECT
              thread_id AS "threadId", instance_id AS "instanceId", run_id AS "runId",
              child_id AS "childId", title, backend, cwd, model, effort,
              status, outcome_status AS "outcomeStatus", summary, error_text AS "errorText",
              tokens, context_window AS "contextWindow",
              started_at AS "startedAt", settled_at AS "settledAt", updated_at AS "updatedAt"
            FROM projection_child_states
            WHERE thread_id = ${threadId}
            ORDER BY started_at ASC, child_id ASC
          `
        : sql`
            SELECT
              thread_id AS "threadId", instance_id AS "instanceId", run_id AS "runId",
              child_id AS "childId", title, backend, cwd, model, effort,
              status, outcome_status AS "outcomeStatus", summary, error_text AS "errorText",
              tokens, context_window AS "contextWindow",
              started_at AS "startedAt", settled_at AS "settledAt", updated_at AS "updatedAt"
            FROM projection_child_states
            WHERE thread_id = ${threadId}
              AND instance_id = ${instanceId}
            ORDER BY started_at ASC, child_id ASC
          `,
  });

  // Teardown sweep returns the interrupted child ids so each row publishes a
  // small `changed` notice (persistence-before-notify) for subscribed clients.
  const markUnfinishedInterruptedRows = SqlSchema.findAll({
    Request: Schema.Struct({
      threadId: ThreadId,
      instanceId: ProviderInstanceId,
      runId: TrimmedNonEmptyString,
      updatedAt: IsoDateTime,
    }),
    Result: Schema.Struct({ childId: TrimmedNonEmptyString }),
    execute: ({ threadId, instanceId, runId, updatedAt }) => sql`
      UPDATE projection_child_states
      SET status = 'interrupted',
          outcome_status = 'interrupted',
          updated_at = ${updatedAt}
      WHERE thread_id = ${threadId}
        AND instance_id = ${instanceId}
        AND run_id = ${runId}
        AND status = 'running'
      RETURNING child_id AS "childId"
    `,
  });

  const repository = ProjectionChildTranscriptRepository.of({
    appendChunk: (chunk) =>
      insertChunk(chunk).pipe(
        Effect.mapError((cause) =>
          toPersistenceSqlError(`append child transcript chunk ${chunk.childId}#${chunk.seq}`)(
            cause,
          ),
        ),
        // Persistence before notify, and only after an actual insert: a
        // duplicate (idempotent replay) writes nothing and must not wake the
        // viewer for a change that did not happen.
        Effect.flatMap((inserted) =>
          inserted.length === 0
            ? Effect.void
            : notifyChanged({
                threadId: chunk.threadId,
                instanceId: chunk.instanceId,
                runId: chunk.runId,
                childId: chunk.childId,
              }),
        ),
      ),
    upsertState: (state) =>
      upsertStateRow(state).pipe(
        Effect.mapError((cause) =>
          toPersistenceSqlError(`upsert child state ${state.childId}`)(cause),
        ),
        Effect.tap(() =>
          notifyChanged({
            threadId: state.threadId,
            instanceId: state.instanceId,
            runId: state.runId,
            childId: state.childId,
          }),
        ),
      ),
    getState: (input) =>
      getStateRow(input).pipe(
        Effect.mapError((cause) =>
          toPersistenceSqlError(`get child state ${input.childId}`)(cause),
        ),
      ),
    listChunks: (input) =>
      listChunkRows(input).pipe(
        Effect.map((rows) => {
          const chunks = rows.length > input.limit ? rows.slice(0, input.limit) : rows;
          const last = chunks.at(-1);
          return {
            chunks,
            lastSeq: last?.seq,
            hasMore: rows.length > input.limit,
          };
        }),
        Effect.mapError((cause) =>
          toPersistenceSqlError(`list child transcript chunks ${input.childId}`)(cause),
        ),
      ),
    listStates: (input) =>
      listStateRows(input).pipe(
        Effect.mapError((cause) =>
          toPersistenceSqlError(`list child states for thread ${input.threadId}`)(cause),
        ),
      ),
    markUnfinishedInterrupted: ({ threadId, instanceId, runId, updatedAt }) =>
      Effect.gen(function* () {
        const rows = yield* markUnfinishedInterruptedRows({
          threadId,
          instanceId,
          runId,
          updatedAt,
        }).pipe(
          Effect.mapError((cause) =>
            toPersistenceSqlError(`mark unfinished children interrupted ${threadId}`)(cause),
          ),
        );
        for (const row of rows) {
          yield* notifyChanged({ threadId, instanceId, runId, childId: row.childId });
        }
      }),
    markAllUnfinishedInterrupted: ({ updatedAt }) =>
      sql`
        UPDATE projection_child_states
        SET status = 'interrupted',
            outcome_status = 'interrupted',
            updated_at = ${updatedAt}
        WHERE status = 'running'
      `.pipe(
        Effect.asVoid,
        Effect.mapError((cause) =>
          toPersistenceSqlError(`mark all unfinished children interrupted`)(cause),
        ),
      ),
    deleteByThreadId: (threadId) =>
      Effect.all([
        sql`DELETE FROM projection_child_transcripts WHERE thread_id = ${threadId}`,
        sql`DELETE FROM projection_child_states WHERE thread_id = ${threadId}`,
      ]).pipe(
        Effect.asVoid,
        Effect.mapError((cause) =>
          toPersistenceSqlError(`delete child transcripts for thread ${threadId}`)(cause),
        ),
      ),
  });

  return repository;
});

export const ProjectionChildTranscriptRepositoryLive = Layer.effect(
  ProjectionChildTranscriptRepository,
  make,
);
