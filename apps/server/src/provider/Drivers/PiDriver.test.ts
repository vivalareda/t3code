// @effect-diagnostics nodeBuiltinImport:off - platform-native fake CLI fixture
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { PI_DEFAULT_MODEL, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { Effect, Fiber, FileSystem, Layer, Option, Path, Stream } from "effect";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { PiDriver } from "./PiDriver.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-driver-test-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
);

it.layer(testLayer)("PiDriver", (it) => {
  it.effect(
    "publishes live models without refresh or another RPC process, preserving Pi's default",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const directory = yield* fs.makeTempDirectoryScoped();
        const logPath = path.join(directory, "commands.jsonl");
        const launcher = writeFakeCli({
          directory,
          name: "pi",
          source: `
import { appendFileSync } from "node:fs";
if (process.argv.includes("--version")) {
  console.log("0.85.1");
} else {
  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    buffer += chunk;
    let end;
    while ((end = buffer.indexOf("\\n")) >= 0) {
      const command = JSON.parse(buffer.slice(0, end));
      buffer = buffer.slice(end + 1);
      appendFileSync(process.env.FAKE_PI_COMMANDS, JSON.stringify(command) + "\\n");
      const data = command.type === "get_state"
        ? { model: { provider: "fixture", id: "native" }, sessionFile: "/tmp/pi-driver-session.jsonl" }
        : command.type === "get_available_models"
          ? { models: [{ provider: "fixture", id: "native", name: "Native model" }] }
          : {};
      process.stdout.write(JSON.stringify({ type: "response", id: command.id, success: true, data }) + "\\n");
    }
  });
}
`,
        });
        const launches: ReadonlyArray<string>[] = [];
        const observedSpawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            if (command._tag === "StandardCommand") launches.push(command.args);
            return yield* spawner.spawn(command);
          }),
        );
        const instance = yield* PiDriver.create({
          instanceId: ProviderInstanceId.make("pi-personal"),
          displayName: "Personal Pi",
          enabled: true,
          config: { ...PiDriver.defaultConfig(), binaryPath: launcher },
          environment: [{ name: "FAKE_PI_COMMANDS", value: logPath, sensitive: false }],
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner));
        const updated = yield* instance.snapshot.streamChanges.pipe(
          Stream.filter((snapshot) =>
            snapshot.models.some((model) => model.slug === "fixture/native"),
          ),
          Stream.runHead,
          Effect.forkScoped,
        );
        const threadId = ThreadId.make("live-catalog-thread");
        const session = yield* instance.adapter.startSession({
          threadId,
          cwd: directory,
          runtimeMode: "approval-required",
          modelSelection: {
            instanceId: ProviderInstanceId.make("pi-personal"),
            model: PI_DEFAULT_MODEL,
          },
        });
        expect(session.model).toBe("fixture/native");
        const snapshot = Option.getOrThrow(yield* Fiber.join(updated));
        expect(snapshot.models.map((model) => model.slug)).toEqual([
          PI_DEFAULT_MODEL,
          "fixture/native",
        ]);
        expect(snapshot.models.find((model) => model.isDefault)?.slug).toBe(PI_DEFAULT_MODEL);
        expect(launches.filter((args) => args.includes("--mode"))).toHaveLength(1);
        expect(launches.some((args) => args.includes("--no-session"))).toBe(false);
        const commands = yield* fs.readFileString(logPath);
        expect(commands).toContain('"get_available_models"');
        expect(commands).not.toContain('"set_model"');
        yield* instance.adapter.stopSession(threadId);
      }),
  );
});
