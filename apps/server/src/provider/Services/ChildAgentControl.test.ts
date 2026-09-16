import { expect, it } from "@effect/vitest";
import {
  ChildAgentControlUnavailableError,
  ChildAgentNotFoundError,
  ChildAgentReadError,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { ProviderSessionDirectoryPersistenceError } from "../Errors.ts";
import { PersistenceSqlError } from "../../persistence/Errors.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import {
  ProjectionChildTranscriptRepository,
  ProjectionChildTranscriptRepositoryLive,
  type ProjectionChildState,
} from "../../persistence/ProjectionChildTranscripts.ts";
import { layer as ChildAgentChangeHubLayer } from "./ChildAgentChangeHub.ts";
import { ChildAgentControl, make as makeChildControl } from "./ChildAgentControl.ts";
import { ProviderInstanceRegistry } from "./ProviderInstanceRegistry.ts";
import { ProviderSessionDirectory } from "./ProviderSessionDirectory.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

const threadId = ThreadId.make("control-thread");
const instanceId = ProviderInstanceId.make("pi");

const seedState = (overrides: Partial<ProjectionChildState> = {}): ProjectionChildState => ({
  threadId,
  instanceId,
  runId: "run-a",
  childId: "sa-1",
  title: "Explore",
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
  startedAt: "2026-09-14T00:00:00.000Z",
  settledAt: null,
  updatedAt: "2026-09-14T00:00:00.000Z",
  ...overrides,
});

const adapterWithControl = (accepted: boolean) =>
  ({
    cancelChild: () => Effect.succeed(accepted),
  }) as unknown as ProviderAdapterShape<unknown>;

const registryLayer = (adapter?: ProviderAdapterShape<unknown>) =>
  Layer.effect(
    ProviderInstanceRegistry,
    Effect.gen(function* () {
      const changes = yield* PubSub.unbounded<void>();
      return ProviderInstanceRegistry.of({
        getInstance: () =>
          Effect.succeed(adapter === undefined ? undefined : ({ instanceId, adapter } as never)),
        listInstances: Effect.succeed([]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.fromPubSub(changes),
        subscribeChanges: PubSub.subscribe(changes),
      });
    }),
  );

const directoryLayer = (
  adapter?: ProviderAdapterShape<unknown>,
  bindingInstanceId: ProviderInstanceId = instanceId,
  bindingFailure?: ProviderSessionDirectoryPersistenceError,
) =>
  Layer.succeed(
    ProviderSessionDirectory,
    ProviderSessionDirectory.of({
      getBinding: () =>
        bindingFailure !== undefined
          ? Effect.fail(bindingFailure)
          : Effect.succeed(
              adapter === undefined
                ? Option.none()
                : Option.some({ providerInstanceId: bindingInstanceId } as never),
            ),
      upsert: () => Effect.void,
      recordImportedTranscript: () => Effect.void,
      getProvider: () => Effect.die("unused"),
      listThreadIds: () => Effect.succeed([]),
      listBindings: () => Effect.succeed([]),
    }),
  );

const controlLayer = (
  adapter?: ProviderAdapterShape<unknown>,
  bindingInstanceId: ProviderInstanceId = instanceId,
  bindingFailure?: ProviderSessionDirectoryPersistenceError,
) =>
  Layer.effect(ChildAgentControl, makeChildControl).pipe(
    Layer.provideMerge(ProjectionChildTranscriptRepositoryLive),
    Layer.provide(ChildAgentChangeHubLayer),
    Layer.provide(registryLayer(adapter)),
    Layer.provide(directoryLayer(adapter, bindingInstanceId, bindingFailure)),
    Layer.provideMerge(SqlitePersistenceMemory),
  );

const withControl = <A, E>(
  adapter: ProviderAdapterShape<unknown> | undefined,
  run: (
    control: ChildAgentControl["Service"],
    repository: ProjectionChildTranscriptRepository["Service"],
  ) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const control = yield* ChildAgentControl;
    const repository = yield* ProjectionChildTranscriptRepository;
    return yield* run(control, repository);
  }).pipe(Effect.provide(controlLayer(adapter)));

const failureOf = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(
    Effect.match({
      onSuccess: () => undefined as E | undefined,
      onFailure: (error) => error,
    }),
  );

const dbDown = new PersistenceSqlError({ operation: "simulated db failure", detail: "db down" });

const failingRepository: ProjectionChildTranscriptRepository["Service"] = {
  appendChunk: () => Effect.void,
  upsertState: () => Effect.void,
  getState: () => Effect.fail(dbDown),
  listChunks: () => Effect.fail(dbDown),
  listStates: () => Effect.fail(dbDown),
  markUnfinishedInterrupted: () => Effect.void,
  markAllUnfinishedInterrupted: () => Effect.void,
  deleteByThreadId: () => Effect.void,
};

it.layer(controlLayer(undefined))("ChildAgentControl", (it) => {
  it.effect("lists children with their full identity", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const control = yield* ChildAgentControl;
      yield* repository.upsertState(seedState());
      const result = yield* control.list({ threadId });
      expect(result.children).toHaveLength(1);
      expect(result.children[0]).toMatchObject({
        threadId,
        instanceId,
        runId: "run-a",
        childId: "sa-1",
      });
    }),
  );

  it.effect("rejects a transcript read from the wrong generation", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const control = yield* ChildAgentControl;
      yield* repository.upsertState(seedState());
      const error = yield* failureOf(
        control.transcript({
          threadId,
          instanceId,
          runId: "run-other",
          childId: "sa-1",
          afterSeq: undefined,
          limit: 200,
        }),
      );
      expect(error).toBeInstanceOf(ChildAgentNotFoundError);
    }),
  );

  it.effect("reports a missing child as not found instead of faking cancellation", () =>
    Effect.gen(function* () {
      const control = yield* ChildAgentControl;
      const error = yield* failureOf(
        control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-absent" }),
      );
      expect(error).toBeInstanceOf(ChildAgentNotFoundError);
    }),
  );

  it.effect("reports unavailable control when the adapter cannot cancel", () =>
    withControl(undefined, (control, repository) =>
      Effect.gen(function* () {
        yield* repository.upsertState(seedState());
        const error = yield* failureOf(
          control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
        );
        expect(error).toBeInstanceOf(ChildAgentControlUnavailableError);
      }),
    ),
  );

  it.effect("cancels through the matching adapter and surfaces rejection", () =>
    withControl(adapterWithControl(false), (control, repository) =>
      Effect.gen(function* () {
        yield* repository.upsertState(seedState());
        const error = yield* failureOf(
          control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
        );
        expect(error).toBeInstanceOf(ChildAgentControlUnavailableError);
      }),
    ),
  );

  it.effect("accepts a cancel from the matching generation", () =>
    withControl(adapterWithControl(true), (control, repository) =>
      Effect.gen(function* () {
        yield* repository.upsertState(seedState());
        expect(
          yield* control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
        ).toEqual({ cancelled: true });
      }),
    ),
  );

  it.effect("streams a reset then changed notices after durable writes", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const control = yield* ChildAgentControl;
      const pull = yield* Stream.toPull(control.subscribeChanges({ threadId }));
      const [reset] = yield* pull;
      expect(reset).toMatchObject({ type: "reset", threadId });
      yield* repository.upsertState(seedState({ childId: "sa-2" }));
      const [changed] = yield* pull;
      expect(changed).toMatchObject({
        type: "changed",
        threadId,
        instanceId,
        runId: "run-a",
        childId: "sa-2",
      });
    }),
  );

  it.effect("refuses to cancel when the live binding owns a different instance", () =>
    Effect.gen(function* () {
      const control = yield* ChildAgentControl;
      const repository = yield* ProjectionChildTranscriptRepository;
      yield* repository.upsertState(seedState());
      const error = yield* failureOf(
        control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
      );
      expect(error).toBeInstanceOf(ChildAgentControlUnavailableError);
    }).pipe(
      Effect.provide(
        controlLayer(adapterWithControl(true), ProviderInstanceId.make("other-instance")),
      ),
    ),
  );

  it.effect("surfaces binding lookup failure instead of claiming control is unavailable", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionChildTranscriptRepository;
      const control = yield* ChildAgentControl;
      yield* repository.upsertState(seedState());
      const error = yield* failureOf(
        control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
      );
      expect(error).toBeInstanceOf(ChildAgentReadError);
      expect(error?.message).toContain("binding");
    }).pipe(
      Effect.provide(
        controlLayer(
          adapterWithControl(true),
          instanceId,
          new ProviderSessionDirectoryPersistenceError({
            operation: "getBinding",
            detail: "database unavailable",
          }),
        ),
      ),
    ),
  );

  it.effect("surfaces a persistence failure when listing child agents", () =>
    Effect.gen(function* () {
      const control = yield* ChildAgentControl;
      const error = yield* failureOf(control.list({ threadId }));
      expect(error).toBeInstanceOf(ChildAgentReadError);
    }).pipe(
      Effect.provide(
        Layer.effect(ChildAgentControl, makeChildControl).pipe(
          Layer.provideMerge(Layer.succeed(ProjectionChildTranscriptRepository, failingRepository)),
          Layer.provide(ChildAgentChangeHubLayer),
          Layer.provide(registryLayer(undefined)),
          Layer.provide(directoryLayer(undefined)),
          Layer.provideMerge(SqlitePersistenceMemory),
        ),
      ),
    ),
  );

  it.effect("surfaces a persistence failure when cancelling a child", () =>
    Effect.gen(function* () {
      const control = yield* ChildAgentControl;
      const error = yield* failureOf(
        control.cancel({ threadId, instanceId, runId: "run-a", childId: "sa-1" }),
      );
      expect(error).toBeInstanceOf(ChildAgentReadError);
    }).pipe(
      Effect.provide(
        Layer.effect(ChildAgentControl, makeChildControl).pipe(
          Layer.provideMerge(Layer.succeed(ProjectionChildTranscriptRepository, failingRepository)),
          Layer.provide(ChildAgentChangeHubLayer),
          Layer.provide(registryLayer(undefined)),
          Layer.provide(directoryLayer(undefined)),
          Layer.provideMerge(SqlitePersistenceMemory),
        ),
      ),
    ),
  );

  it.effect("surfaces a persistence failure instead of an empty transcript", () =>
    Effect.gen(function* () {
      const control = yield* ChildAgentControl;
      const error = yield* failureOf(
        control.transcript({
          threadId,
          instanceId,
          runId: "run-a",
          childId: "sa-1",
          afterSeq: undefined,
          limit: 200,
        }),
      );
      expect(error).toBeInstanceOf(ChildAgentReadError);
    }).pipe(
      Effect.provide(
        Layer.effect(ChildAgentControl, makeChildControl).pipe(
          Layer.provideMerge(Layer.succeed(ProjectionChildTranscriptRepository, failingRepository)),
          Layer.provide(ChildAgentChangeHubLayer),
          Layer.provide(registryLayer(undefined)),
          Layer.provide(directoryLayer(undefined)),
          Layer.provideMerge(SqlitePersistenceMemory),
        ),
      ),
    ),
  );
});
