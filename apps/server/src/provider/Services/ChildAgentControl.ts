/**
 * ChildAgentControl — server-side read and control surface for provider-owned
 * child agents (Pi subagents today).
 *
 * Reads are scoped by the full (thread, instance, run, child) identity from
 * the durable projection. Cancellation passes through the provider adapter's
 * optional child-control capability; adapters without it — or requests that do
 * not match the live generation — fail with an explicit error rather than
 * pretending to cancel.
 *
 * @module provider/Services/ChildAgentControl
 */
import {
  type ChildAgentCancelResult,
  type ChildAgentChangeEvent,
  ChildAgentControlUnavailableError,
  type ChildAgentListResult,
  ChildAgentNotFoundError,
  ChildAgentReadError,
  ChildAgentState,
  type ChildAgentTranscriptPage,
  ChildTranscriptChunk,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ProjectionChildTranscriptRepository,
  type ProjectionChildState,
} from "../../persistence/ProjectionChildTranscripts.ts";
import { ChildAgentChangeHub } from "./ChildAgentChangeHub.ts";
import { ProviderInstanceRegistry } from "./ProviderInstanceRegistry.ts";
import { ProviderSessionDirectory } from "./ProviderSessionDirectory.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * Optional per-adapter capability: cancel one running child. Adapters whose
 * provider does not expose child controls simply do not implement this and
 * the service reports the capability unavailable. The adapter validates the
 * run id against its live generation and returns false on a mismatch.
 */
export interface ChildControlCapability {
  readonly cancelChild: (input: {
    readonly threadId: ThreadId;
    readonly instanceId: ProviderInstanceId;
    readonly runId: string;
    readonly childId: string;
  }) => Effect.Effect<boolean>;
}

const hasChildControl = (
  adapter: ProviderAdapterShape<unknown>,
): adapter is ProviderAdapterShape<unknown> & ChildControlCapability =>
  typeof (adapter as Partial<ChildControlCapability>).cancelChild === "function";

const toChildAgentState = Schema.decodeSync(ChildAgentState);
const isTranscriptChunk = Schema.is(ChildTranscriptChunk);

/** Map a persistence failure into a typed read error that reaches the client. */
const toChildAgentReadError = (operation: string) => (error: unknown) =>
  new ChildAgentReadError({
    operation,
    ...(error instanceof Error ? { detail: error.message } : {}),
  });

const projectState = (row: ProjectionChildState) =>
  toChildAgentState({
    threadId: row.threadId,
    instanceId: row.instanceId,
    runId: row.runId,
    childId: row.childId,
    title: row.title,
    backend: row.backend,
    model: row.model,
    effort: row.effort,
    status: row.status,
    summary: row.summary,
    errorText: row.errorText,
    tokens: row.tokens,
    startedAt: row.startedAt,
    settledAt: row.settledAt,
  });

export class ChildAgentControl extends Context.Service<
  ChildAgentControl,
  {
    readonly list: (input: {
      readonly threadId: ThreadId;
    }) => Effect.Effect<ChildAgentListResult, ChildAgentReadError>;
    readonly transcript: (input: {
      readonly threadId: ThreadId;
      readonly instanceId: ProviderInstanceId;
      readonly runId: string;
      readonly childId: string;
      readonly afterSeq: number | undefined;
      readonly limit: number | undefined;
    }) => Effect.Effect<ChildAgentTranscriptPage, ChildAgentNotFoundError | ChildAgentReadError>;
    readonly cancel: (input: {
      readonly threadId: ThreadId;
      readonly instanceId: ProviderInstanceId;
      readonly runId: string;
      readonly childId: string;
    }) => Effect.Effect<
      ChildAgentCancelResult,
      ChildAgentNotFoundError | ChildAgentControlUnavailableError | ChildAgentReadError
    >;
    readonly subscribeChanges: (input: {
      readonly threadId: ThreadId;
    }) => Stream.Stream<ChildAgentChangeEvent>;
  }
>()("t3/provider/Services/ChildAgentControl") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const repository = yield* ProjectionChildTranscriptRepository;
  const registry = yield* ProviderInstanceRegistry;
  const sessionDirectory = yield* ProviderSessionDirectory;
  // Required so control and every writer share the notification stream; there
  // is no silent no-subscription fallback.
  const changeHub = yield* ChildAgentChangeHub;

  const resolveAdapter = (threadId: ThreadId, instanceId: ProviderInstanceId) =>
    Effect.gen(function* () {
      const bindingOption = yield* sessionDirectory
        .getBinding(threadId)
        .pipe(Effect.mapError(toChildAgentReadError("resolving the child provider binding")));
      const binding = Option.getOrUndefined(bindingOption);
      // Controls must target the live, owning instance generation. A durable
      // row from a prior instance never routes a cancel to the current one.
      if (
        binding === undefined ||
        binding.providerInstanceId === undefined ||
        binding.providerInstanceId !== instanceId
      ) {
        return Option.none();
      }
      const instance = yield* registry.getInstance(binding.providerInstanceId);
      if (instance === undefined) return Option.none();
      return Option.some({
        instanceId: binding.providerInstanceId,
        adapter: instance.adapter,
      });
    });

  const list = (input: { readonly threadId: ThreadId }) =>
    Effect.gen(function* () {
      const rows = yield* repository
        .listStates({ threadId: input.threadId })
        .pipe(Effect.mapError(toChildAgentReadError("listing child agents")));
      return { children: rows.map(projectState) };
    });

  const transcript = (input: {
    readonly threadId: ThreadId;
    readonly instanceId: ProviderInstanceId;
    readonly runId: string;
    readonly childId: string;
    readonly afterSeq: number | undefined;
    readonly limit: number | undefined;
  }) =>
    Effect.gen(function* () {
      const rowOption = yield* repository
        .getState({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        })
        .pipe(Effect.mapError(toChildAgentReadError("reading child state")));
      if (Option.isNone(rowOption)) {
        return yield* new ChildAgentNotFoundError({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        });
      }
      const page = yield* repository
        .listChunks({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
          ...(input.afterSeq !== undefined ? { afterSeq: input.afterSeq } : {}),
          limit: input.limit ?? 200,
        })
        .pipe(Effect.mapError(toChildAgentReadError("reading child transcript")));
      const chunks: Array<{
        seq: number;
        chunk: ChildAgentTranscriptPage["chunks"][number]["chunk"];
      }> = [];
      for (const chunk of page.chunks) {
        // A persisted chunk that fails to decode is corrupt, not "no output".
        // Surface it as a read error instead of silently truncating the page.
        if (!isTranscriptChunk(chunk.chunk)) {
          return yield* new ChildAgentReadError({
            operation: "decoding child transcript",
            detail: `invalid persisted chunk at seq ${chunk.seq} for ${input.childId}`,
          });
        }
        chunks.push({ seq: chunk.seq, chunk: chunk.chunk });
      }
      return {
        childId: input.childId,
        chunks,
        lastSeq: page.lastSeq ?? null,
        hasMore: page.hasMore,
      } satisfies ChildAgentTranscriptPage;
    });

  const cancel = (input: {
    readonly threadId: ThreadId;
    readonly instanceId: ProviderInstanceId;
    readonly runId: string;
    readonly childId: string;
  }) =>
    Effect.gen(function* () {
      // The durable row must exist for exactly this generation; anything else
      // is a stale or unknown identity and must not touch the live child.
      const rowOption = yield* repository
        .getState({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        })
        .pipe(Effect.mapError(toChildAgentReadError("reading child state")));
      if (Option.isNone(rowOption)) {
        return yield* new ChildAgentNotFoundError({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        });
      }

      const resolved = yield* resolveAdapter(input.threadId, input.instanceId);
      if (Option.isNone(resolved) || !hasChildControl(resolved.value.adapter)) {
        return yield* new ChildAgentControlUnavailableError({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        });
      }

      const cancelled = yield* resolved.value.adapter
        .cancelChild({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        })
        .pipe(Effect.catch(() => Effect.succeed(false)));

      if (!cancelled) {
        return yield* new ChildAgentControlUnavailableError({
          threadId: input.threadId,
          instanceId: input.instanceId,
          runId: input.runId,
          childId: input.childId,
        });
      }
      return { cancelled: true } satisfies ChildAgentCancelResult;
    });

  const subscribeChanges = (input: { readonly threadId: ThreadId }) =>
    changeHub.subscribe(input.threadId);

  return ChildAgentControl.of({ list, transcript, cancel, subscribeChanges });
});

// The repository, change hub, instance registry, and session directory are all
// provided once at the top level (server.ts) so every writer, subscriber, and
// the control service share the same instances.
export const ChildAgentControlLive = Layer.effect(ChildAgentControl, make);
