/**
 * Child-agent transcript and control contracts.
 *
 * Pi owns execution of its subagent children; T3 records readable display
 * state and exposes typed controls. Every address is scoped to the owning
 * (threadId, providerInstanceId, runId, childId) tuple — clients never
 * resolve filesystem paths, and a run id namespaces child ids so a reused
 * `sa-1` after a process restart cannot collide with history.
 *
 * @module childAgents
 */
import * as Schema from "effect/Schema";
import { ProviderInstanceId } from "./providerInstance.ts";
import { NonNegativeInt, PositiveInt, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const ChildId = TrimmedNonEmptyString.check(Schema.isMaxLength(128));
export type ChildId = typeof ChildId.Type;

export const RunId = TrimmedNonEmptyString.check(Schema.isMaxLength(128));
export type RunId = typeof RunId.Type;

export const ChildAgentStatus = Schema.Literals([
  "running",
  "done",
  "error",
  "cancelled",
  "interrupted",
]);
export type ChildAgentStatus = typeof ChildAgentStatus.Type;

/**
 * Full public child identity. Required on every transcript read and cancel;
 * list results return these fields so clients can address rows exactly.
 */
export const ChildAgentIdentity = Schema.Struct({
  threadId: ThreadId,
  instanceId: ProviderInstanceId,
  runId: RunId,
  childId: ChildId,
});
export type ChildAgentIdentity = typeof ChildAgentIdentity.Type;

/**
 * One typed transcript item. `text`/`thinking` carry assistant output;
 * `user` records a user turn; `toolCall` and `toolResult` record tool
 * lifecycle. Item ids distinguish assistant message/part and tool call vs
 * result at the emitter.
 */
export const ChildTranscriptItem = Schema.Union([
  Schema.Struct({
    kind: Schema.Literals(["text", "thinking"]),
    text: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("user"),
    text: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("toolCall"),
    toolId: TrimmedNonEmptyString,
    name: TrimmedNonEmptyString,
    argsPreview: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("toolResult"),
    toolId: TrimmedNonEmptyString,
    name: TrimmedNonEmptyString,
    isError: Schema.Boolean,
    outputPreview: Schema.optional(Schema.String),
  }),
]);
export type ChildTranscriptItem = typeof ChildTranscriptItem.Type;

/**
 * One projected transcript chunk. `op` mirrors the extension's incremental
 * protocol: `append` streams text into an open item, `finalize` closes one,
 * and `add` records a terminal fact (user message, tool result). `finalize`
 * replaces/ends an existing item and never duplicates text.
 */
export const ChildTranscriptChunk = Schema.Union([
  Schema.Struct({
    op: Schema.Literal("append"),
    itemId: TrimmedNonEmptyString,
    kind: Schema.Literals(["text", "thinking"]),
    text: Schema.String,
  }),
  Schema.Struct({
    op: Schema.Literal("finalize"),
    itemId: TrimmedNonEmptyString,
    item: ChildTranscriptItem,
  }),
  Schema.Struct({
    op: Schema.Literal("add"),
    itemId: TrimmedNonEmptyString,
    item: ChildTranscriptItem,
  }),
]);
export type ChildTranscriptChunk = typeof ChildTranscriptChunk.Type;

export const ChildAgentState = Schema.Struct({
  ...ChildAgentIdentity.fields,
  title: Schema.NullOr(TrimmedNonEmptyString),
  backend: Schema.NullOr(TrimmedNonEmptyString),
  model: Schema.NullOr(TrimmedNonEmptyString),
  effort: Schema.NullOr(TrimmedNonEmptyString),
  status: ChildAgentStatus,
  summary: Schema.NullOr(Schema.String),
  errorText: Schema.NullOr(Schema.String),
  tokens: Schema.NullOr(Schema.Number),
  startedAt: Schema.NullOr(Schema.String),
  settledAt: Schema.NullOr(Schema.String),
});
export type ChildAgentState = typeof ChildAgentState.Type;

export const ChildAgentTranscriptPage = Schema.Struct({
  childId: ChildId,
  chunks: Schema.Array(
    Schema.Struct({
      seq: NonNegativeInt,
      chunk: ChildTranscriptChunk,
    }),
  ),
  lastSeq: Schema.NullOr(NonNegativeInt),
  hasMore: Schema.Boolean,
});

/** Hard ceiling for one transcript page; the public `limit` is validated to it. */
export const CHILD_AGENT_TRANSCRIPT_MAX_PAGE = 500;

export const ChildTranscriptLimit = PositiveInt.check(
  Schema.isLessThanOrEqualTo(CHILD_AGENT_TRANSCRIPT_MAX_PAGE),
);
export type ChildTranscriptLimit = typeof ChildTranscriptLimit.Type;
export type ChildAgentTranscriptPage = typeof ChildAgentTranscriptPage.Type;

// --- RPC payloads ---------------------------------------------------------------

export const ChildAgentListInput = Schema.Struct({
  threadId: ThreadId,
});
export type ChildAgentListInput = typeof ChildAgentListInput.Type;

export const ChildAgentListResult = Schema.Struct({
  children: Schema.Array(ChildAgentState),
});
export type ChildAgentListResult = typeof ChildAgentListResult.Type;

export const ChildAgentTranscriptInput = Schema.Struct({
  ...ChildAgentIdentity.fields,
  afterSeq: Schema.optional(Schema.NullOr(NonNegativeInt)),
  limit: Schema.optional(Schema.NullOr(ChildTranscriptLimit)),
});
export type ChildAgentTranscriptInput = typeof ChildAgentTranscriptInput.Type;

export const ChildAgentCancelInput = Schema.Struct({
  ...ChildAgentIdentity.fields,
});
export type ChildAgentCancelInput = typeof ChildAgentCancelInput.Type;

export const ChildAgentCancelResult = Schema.Struct({
  cancelled: Schema.Literal(true),
});
export type ChildAgentCancelResult = typeof ChildAgentCancelResult.Type;

/**
 * Small change notice streamed to subscribed clients. `reset` is emitted on
 * subscribe/reconnect so a client re-reads the roster before consuming
 * `changed`; `changed` names one durable child write. No transcript chunks
 * ride this stream — clients re-read pages after a change.
 */
export const ChildAgentChangeEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("reset"),
    threadId: ThreadId,
  }),
  Schema.Struct({
    type: Schema.Literal("changed"),
    ...ChildAgentIdentity.fields,
  }),
]);
export type ChildAgentChangeEvent = typeof ChildAgentChangeEvent.Type;

export const ChildAgentSubscribeInput = Schema.Struct({
  threadId: ThreadId,
});
export type ChildAgentSubscribeInput = typeof ChildAgentSubscribeInput.Type;

export class ChildAgentNotFoundError extends Schema.TaggedError<ChildAgentNotFoundError>()(
  "ChildAgentNotFoundError",
  {
    ...ChildAgentIdentity.fields,
  },
) {
  override get message(): string {
    return `Child agent '${this.childId}' (run ${this.runId}) was not found on thread ${this.threadId}.`;
  }
}

export class ChildAgentControlUnavailableError extends Schema.TaggedError<ChildAgentControlUnavailableError>()(
  "ChildAgentControlUnavailableError",
  {
    ...ChildAgentIdentity.fields,
  },
) {
  override get message(): string {
    return `Child agent '${this.childId}' (run ${this.runId}) cannot be cancelled from T3 right now.`;
  }
}

/**
 * A child-agent read failed against durable storage. Surfaced instead of an
 * empty transcript/roster so a transient persistence error is never rendered
 * as "no children" or "no output".
 */
export class ChildAgentReadError extends Schema.TaggedError<ChildAgentReadError>()(
  "ChildAgentReadError",
  {
    operation: Schema.String,
    detail: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return this.detail === undefined
      ? `Failed to read child-agent data (${this.operation}).`
      : `Failed to read child-agent data (${this.operation}): ${this.detail}`;
  }
}
