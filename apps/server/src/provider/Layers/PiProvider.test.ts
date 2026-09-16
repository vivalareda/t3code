// @effect-diagnostics nodeBuiltinImport:off - creates a platform-native fake CLI for the RPC probe
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import { Effect, FileSystem, Path, Schema } from "effect";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import {
  buildInitialPiProviderSnapshot,
  checkPiProviderStatus,
  discoverPiModelsViaRpc,
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
});
