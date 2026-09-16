// @effect-diagnostics nodeBuiltinImport:off - creates a platform-native fake CLI for the RPC probe
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { PI_DEFAULT_MODEL, PiSettings } from "@t3tools/contracts";
import { Deferred, Effect, Fiber, FileSystem, Path, Schema, Scope, Sink, Stream } from "effect";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import {
  buildInitialPiProviderSnapshot,
  checkPiProviderStatus,
  discoverPiModelsViaRpc,
  makePiModelCatalog,
  mergePiModels,
  parsePiAvailableModels,
} from "./PiProvider.ts";

const decodeSettings = Schema.decodeSync(PiSettings);

it.layer(NodeServices.layer)("PiProvider", (it) => {
  it.effect("never advertises automatic text generation on initial or refreshed snapshots", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({
        directory,
        name: "pi",
        source: 'process.stdout.write("0.85.1\\n");',
      });
      const settings = decodeSettings({ enabled: true, binaryPath: launcher });
      expect((yield* buildInitialPiProviderSnapshot(settings)).supportsTextGeneration).toBe(false);
      const ready = yield* checkPiProviderStatus(settings);
      expect(ready.supportsTextGeneration).toBe(false);
      expect(ready.version).toBe("0.85.1");
      expect(
        (yield* buildInitialPiProviderSnapshot(decodeSettings({ enabled: false })))
          .supportsTextGeneration,
      ).toBe(false);
    }),
  );

  it.effect("discovers models from an existing platform temp directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({
        directory,
        name: "pi",
        source: `
      let buffer = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (text) => {
        buffer += text;
        let index;
        while ((index = buffer.indexOf("\\n")) >= 0) {
          const command = JSON.parse(buffer.slice(0, index));
          buffer = buffer.slice(index + 1);
          process.stdout.write(JSON.stringify({ type: "response", id: command.id, success: true,
            data: {models: [{provider: "fixture", id: "model", name: "Fixture model"}]}
          }) + "\\n");
        }
      });
    `,
      });
      const models = yield* discoverPiModelsViaRpc(decodeSettings({ binaryPath: launcher }), {
        ...process.env,
        TMPDIR: path.join(directory, "does-not-exist"),
      });
      expect(models).toMatchObject([{ slug: "fixture/model", name: "Fixture model" }]);
    }),
  );

  it.effect("closes an explicit discovery scope when its model request is interrupted", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const testScope = yield* Scope.Scope;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({
        directory,
        name: "pi-hung-catalog",
        source: "process.stdin.resume();",
      });
      const requestWritten = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const observedSpawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(command);
          // Safety cleanup if the regression fails: this is separate from the
          // connection-owned finalizer whose completion is asserted below.
          yield* Scope.addFinalizer(testScope, handle.kill().pipe(Effect.ignore));
          yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined));
          return {
            ...handle,
            stdin: handle.stdin.pipe(
              Sink.mapInputEffect((chunk: Uint8Array) =>
                Deferred.succeed(requestWritten, undefined).pipe(Effect.as(chunk)),
              ),
            ),
          };
        }),
      );
      const discovery = yield* discoverPiModelsViaRpc(
        decodeSettings({ binaryPath: launcher }),
        process.env,
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner),
        Effect.forkScoped,
      );
      yield* Deferred.await(requestWritten);
      yield* Fiber.interrupt(discovery);
      expect(yield* Deferred.isDone(released)).toBe(true);
    }),
  );

  it.effect("parsePiAvailableModels skips malformed entries and keeps valid ones", () =>
    Effect.sync(() =>
      parsePiAvailableModels({
        models: [
          { provider: "fixture", id: "ok", name: "OK model" },
          null,
          "not-an-object",
          { id: "missing-provider" },
          { provider: "missing-id" },
          { provider: "", id: "empty-provider" },
          { provider: "fixture", id: "" },
          { provider: "reasoning", id: "thinker", reasoning: true },
          { provider: "fixture", id: "unnamed" },
        ],
      }),
    ).pipe(
      Effect.map((models) => {
        expect(models).toMatchObject([
          { slug: "fixture/ok", name: "OK model", isCustom: false },
          { slug: "reasoning/thinker", name: "thinker", isCustom: false },
          { slug: "fixture/unnamed", name: "unnamed", isCustom: false },
        ]);
        expect(models[1]?.capabilities).toMatchObject({
          optionDescriptors: [{ id: "reasoningEffort" }],
        });
        expect(models[0]?.capabilities).toMatchObject({ optionDescriptors: [] });
      }),
    ),
  );

  it.effect("keeps Pi's native default ahead of discovered and custom models", () =>
    Effect.sync(() => {
      const discovered = parsePiAvailableModels({
        models: [{ provider: "fixture", id: "available", name: "Available model" }],
      });
      const models = mergePiModels(discovered, [PI_DEFAULT_MODEL, "fixture/custom"]);
      expect(models.map((model) => model.slug)).toEqual([
        PI_DEFAULT_MODEL,
        "fixture/available",
        "fixture/custom",
      ]);
      expect(models[0]).toMatchObject({ name: "Pi default", isDefault: true, isCustom: false });
      expect(mergePiModels([], [])[0]?.slug).toBe(PI_DEFAULT_MODEL);
    }),
  );

  it.effect("makePiModelCatalog publishes set values to its changes stream", () =>
    Effect.gen(function* () {
      const catalog = yield* makePiModelCatalog;
      const value = parsePiAvailableModels({
        models: [{ provider: "fixture", id: "live-model" }],
      });
      // SubscriptionRef replays the current value on subscribe; wait until the
      // subscriber has observed that initial emission before writing, so the
      // assertion is deterministic rather than racing set vs. subscription.
      const initialSeen = yield* Deferred.make<void>();
      const fiber = yield* catalog.changes.pipe(
        Stream.tap(() => Deferred.succeed(initialSeen, undefined).pipe(Effect.asVoid)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(initialSeen);
      yield* catalog.set(value);
      const collected = yield* Fiber.join(fiber);
      expect(collected).toEqual([[], value]);
    }),
  );
});
