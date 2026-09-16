/**
 * PiDriver — driver SPI implementation for the Pi coding agent.
 *
 * One instance owns its configuration (binary path, profile directory) and
 * the instance-shared model catalog that live sessions and the explicit
 * refresh probe populate. Text generation (commit messages, titles, …) is
 * reported unsupported: a normal Pi coding session boots every extension and
 * tool, and a title prompt must never be able to launch subagents.
 *
 * @module provider/Drivers/PiDriver
 */
import { PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import type { FileSystem } from "effect/FileSystem";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { makePiTextGeneration } from "../../textGeneration/PiTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import {
  buildInitialPiProviderSnapshot,
  checkPiProviderStatus,
  discoverPiModelsViaRpc,
  makePiModelCatalog,
  mergePiModels,
  type PiModelCatalog,
} from "../Layers/PiProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import { ServerSettingsService } from "../../serverSettings.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const DRIVER_KIND = ProviderDriverKind.make("pi");
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type PiDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Pi",
    supportsMultipleInstances: true,
  },
  configSchema: PiSettings,
  defaultConfig: (): PiSettings => decodePiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverConfig = yield* ServerConfig;
      const serverSettings = yield* ServerSettingsService;
      const path = yield* Path.Path;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies PiSettings;
      const modelCatalog: PiModelCatalog = yield* makePiModelCatalog;

      const adapter = yield* makePiAdapter(effectiveConfig, {
        environment: processEnv,
        sessionDir: path.join(serverConfig.stateDir, "pi-sessions"),
        instanceId,
        modelCatalog,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      const textGeneration = yield* makePiTextGeneration;

      const checkProvider = checkPiProviderStatus(effectiveConfig, processEnv, modelCatalog).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.map(stampIdentity),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<PiSettings>>({
        resolveMaintenance: () => Effect.succeed(MAINTENANCE_CAPABILITIES),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialPiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        // Live sessions (via the adapter hook) call `modelCatalog.set`; fold
        // those writes into the managed snapshot immediately so the model
        // picker reflects a newly discovered catalog without the health timer
        // or an explicit refresh. This only republishes the current snapshot
        // with the updated models — it never re-probes — so it cannot collide
        // with the version/health check or the explicit refresh path.
        enrichSnapshot: ({ getSnapshot, publishSnapshot }) =>
          modelCatalog.changes.pipe(
            Stream.mapEffect((models) =>
              getSnapshot.pipe(
                Effect.map((current) => ({
                  ...current,
                  models: mergePiModels(models, effectiveConfig.customModels),
                })),
                Effect.flatMap(publishSnapshot),
              ),
            ),
            Stream.runDrain,
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Pi snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      const refreshModels = () =>
        Effect.gen(function* () {
          // Explicit setup/refresh path: a short-lived --no-session RPC
          // process lists models without creating a session file.
          const discovered = yield* discoverPiModelsViaRpc(effectiveConfig, processEnv).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          );
          yield* modelCatalog.set(discovered);
          yield* snapshot.refresh;
        }).pipe(
          Effect.scoped,
          Effect.mapError(
            (cause) =>
              new ProviderDriverError({
                driver: DRIVER_KIND,
                instanceId,
                detail: `Failed to refresh Pi models: ${String(cause)}`,
                cause,
              }),
          ),
        );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        refreshModels,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
