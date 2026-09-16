/**
 * Pi provider status probing.
 *
 * Health checks stay cheap: a `pi --version` spawn never creates a session,
 * runs extension hooks, or opens a login flow. Pi's model catalog is
 * provider-config dependent and only knowable from a live RPC session, so it
 * is populated by the adapter during real sessions and by the explicit
 * model-refresh probe — never by guessing that `--list-models` is
 * side-effect-free (current pi startup initializes runtime services before
 * printing that list).
 *
 * @module provider/Layers/PiProvider
 */
import * as NodeOS from "node:os";
import {
  type CustomModelSetting,
  type PiSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import { createModelCapabilities } from "@t3tools/shared/model";

import { expandHomePath } from "../../pathExpansion.ts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { closePiRpcConnection, makePiRpcConnectionIn } from "../pi/PiRpcConnection.ts";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Scope from "effect/Scope";

import {
  buildServerProvider as buildProviderSnapshot,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const buildPiProvider = (
  input: Parameters<typeof buildProviderSnapshot>[0],
): ServerProviderDraft => ({
  ...buildProviderSnapshot(input),
  supportsTextGeneration: false,
});

const PI_PRESENTATION = {
  displayName: "Pi",
  supportsConversationRollback: false,
  showInteractionModeToggle: false,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;

const EMPTY_CAPABILITIES = createModelCapabilities({
  optionDescriptors: [],
});

/**
 * Instance-scoped model catalog. The adapter fills it from live sessions
 * (`get_available_models`); the explicit refresh probe fills it from a
 * short-lived `--no-session` RPC process.
 */
export interface PiModelCatalog {
  readonly get: Effect.Effect<ReadonlyArray<ServerProviderModel>>;
  readonly set: (models: ReadonlyArray<ServerProviderModel>) => Effect.Effect<void>;
}

export const makePiModelCatalog = Effect.gen(function* () {
  const ref = yield* Ref.make<ReadonlyArray<ServerProviderModel>>([]);
  return {
    get: Ref.get(ref),
    set: (models) => Ref.set(ref, models),
  } satisfies PiModelCatalog;
});

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    if (!piSettings.enabled) {
      return buildPiProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models: [],
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }
    return buildPiProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: [],
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Pi CLI availability...",
      },
    });
  });
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  catalog?: PiModelCatalog | undefined,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
  if (!piSettings.enabled) {
    return buildPiProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const binaryPath = piSettings.binaryPath;
  const versionProbe = spawnVersionProbe(binaryPath, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
  );
  const probeOutcome = yield* versionProbe;

  if (probeOutcome._tag === "None") {
    return buildPiProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: yield* catalogModels(catalog, piSettings),
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Installed but the version check timed out.",
      },
    });
  }
  const result = probeOutcome.value;
  if (typeof result === "string") {
    // Command-missing sentinel from spawnVersionProbe.
    return buildPiProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: yield* catalogModels(catalog, piSettings),
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message:
          "`pi` is not installed or not on PATH. Install Pi manually, then run `pi` and use `/login`.",
      },
    });
  }

  const version = parseGenericCliVersion(result.stdout);
  const message =
    result.code !== 0
      ? `Installed but the version check exited with code ${result.code}.`
      : undefined;
  return buildPiProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: yield* catalogModels(catalog, piSettings),
    probe: {
      installed: true,
      version,
      status: result.code !== 0 ? "warning" : "ready",
      // Pi delegates authentication to its own model providers (env keys or
      // `/login`); T3 cannot verify it without starting a session.
      auth: { status: "unknown" },
      ...(message !== undefined ? { message } : {}),
    },
  });
});

const catalogModels = (catalog: PiModelCatalog | undefined, piSettings: PiSettings) =>
  Effect.gen(function* () {
    const discovered = catalog !== undefined ? yield* catalog.get : [];
    return mergePiModels(discovered, piSettings.customModels);
  });

type VersionProbeResult =
  | { readonly stdout: string; readonly stderr: string; readonly code: number }
  | "command-missing";

const spawnVersionProbe = Effect.fn("spawnVersionProbe")(function* (
  binaryPath: string,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<VersionProbeResult, never, ChildProcessSpawner.ChildProcessSpawner> {
  const spawnCommand = yield* resolveSpawnCommand(binaryPath, ["--version"], {
    env: environment,
  });
  return yield* spawnAndCollect(
    binaryPath,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      shell: spawnCommand.shell,
    }),
  ).pipe(
    Effect.catch((error): Effect.Effect<VersionProbeResult, never> =>
      isCommandMissingCause(error)
        ? Effect.succeed("command-missing")
        : Effect.succeed({ stdout: "", stderr: String(error), code: 1 }),
    ),
  );
});

/** Discovered models first (they carry real names), then settings customs. */
export function mergePiModels(
  discovered: ReadonlyArray<ServerProviderModel>,
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set(discovered.map((model) => model.slug));
  const customEntries: ServerProviderModel[] = [];
  for (const entry of customModels ?? []) {
    const slug = typeof entry === "string" ? entry : entry.slug;
    if (seen.has(slug)) continue;
    seen.add(slug);
    customEntries.push({
      slug,
      ...(typeof entry !== "string" && entry.name !== undefined
        ? { name: entry.name }
        : { name: slug }),
      isCustom: true,
      capabilities:
        typeof entry !== "string" ? (entry.capabilities ?? EMPTY_CAPABILITIES) : EMPTY_CAPABILITIES,
    });
  }
  return [...discovered, ...customEntries];
}

/**
 * Explicit model discovery. Spawns a short-lived `pi --mode rpc --no-session`
 * process, lists the configured models, and tears it down. This runs only on
 * explicit setup/refresh — never as a health-check side effect — because pi
 * startup initializes runtime services and loads extensions.
 */
export const discoverPiModelsViaRpc = Effect.fn("discoverPiModelsViaRpc")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ReadonlyArray<ServerProviderModel>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const resolvedProfile = piSettings.profileDir ? expandHomePath(piSettings.profileDir) : undefined;
  const env = resolvedProfile
    ? { ...environment, PI_CODING_AGENT_DIR: resolvedProfile }
    : environment;
  const connection = yield* makePiRpcConnectionIn({
    binaryPath: piSettings.binaryPath,
    args: ["--mode", "rpc", "--no-session"],
    cwd: NodeOS.tmpdir(),
    env,
  }).pipe(
    Effect.catch(() => Effect.succeed(null)),
    Effect.map((value) => value as import("../pi/PiRpcConnection.ts").PiRpcConnectionHandle | null),
  );
  if (connection === null) return [];
  const outcome = yield* connection
    .request({ type: "get_available_models" }, { timeoutMs: 20_000 })
    .pipe(Effect.exit);
  yield* closePiRpcConnection(connection);
  if (Exit.isFailure(outcome)) return [];
  const data = outcome.value;
  const models = data?.["models"];
  if (!Array.isArray(models)) return [];
  const resolved: ServerProviderModel[] = [];
  for (const entry of models) {
    if (typeof entry !== "object" || entry === null) continue;
    const record = entry as Record<string, unknown>;
    const id = typeof record["id"] === "string" ? record["id"] : undefined;
    const provider = typeof record["provider"] === "string" ? record["provider"] : undefined;
    if (id === undefined || provider === undefined) continue;
    resolved.push({
      slug: `${provider}/${id}`,
      name: typeof record["name"] === "string" ? record["name"] : id,
      isCustom: false,
      capabilities: createModelCapabilities({
        optionDescriptors:
          record["reasoning"] === true
            ? [
                {
                  id: "reasoningEffort",
                  label: "Reasoning",
                  type: "select" as const,
                  options: PI_REASONING_LEVELS.map((level) => ({ id: level, label: level })),
                },
              ]
            : [],
      }),
    });
  }
  return resolved;
});

const PI_REASONING_LEVELS = ["off", "minimal", "low", "medium", "high"] as const;
