// @effect-diagnostics nodeBuiltinImport:off - writes fake CLI launchers and probes a loopback bridge port directly.
import * as NodeNet from "node:net";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, type ProviderRuntimeEvent, ThreadId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { Deferred, Effect, Exit, Fiber, FileSystem, Ref, Scope, Sink } from "effect";
import * as Stream from "effect/Stream";
import { ChildProcess } from "effect/unstable/process";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makePiAdapter } from "./PiAdapter.ts";

const THREAD_ID = ThreadId.make("thread-pi-adapter-lifecycle");
const TEST_INSTANCE_ID = ProviderInstanceId.make("pi-lifecycle-test");
const windowsHost = HostProcessPlatform.defaultValue() === "win32";
const BRIDGE_PORT_ENV = "T3CODE_PI_BRIDGE_PORT";

type ObservedSpawner = ReturnType<typeof ChildProcessSpawner.make>;

/** Replies to `get_state` and every other command with success. */
const SIMPLE_STATE_SOURCE = `
const send = (payload) => process.stdout.write(JSON.stringify(payload) + "\\n");
const respond = (id, type, success, data, error) => {
  const payload = { id, type: "response", command: type, success };
  if (data !== undefined) payload.data = data;
  if (error !== undefined) payload.error = error;
  send(payload);
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let i = buffer.indexOf("\\n");
  while (i >= 0) {
    const line = buffer.slice(0, i);
    buffer = buffer.slice(i + 1);
    if (line.trim().length > 0) {
      let cmd;
      try { cmd = JSON.parse(line); } catch { cmd = null; }
      if (cmd !== null) {
        const id = typeof cmd.id === "string" ? cmd.id : undefined;
        if (cmd.type === "get_state") {
          respond(id, "get_state", true, {
            sessionFile: "/tmp/pi-lifecycle-session.jsonl",
            sessionId: "sess-lifecycle"
          });
        } else {
          respond(id, String(cmd.type ?? ""), true);
        }
      }
    }
    i = buffer.indexOf("\\n");
  }
});
`;

/** Replies to `get_state`, then exits code 1 right after a successful `prompt`. */
const UNEXPECTED_EXIT_SOURCE = `
const send = (payload) => process.stdout.write(JSON.stringify(payload) + "\\n");
const respond = (id, type, success, data, error) => {
  const payload = { id, type: "response", command: type, success };
  if (data !== undefined) payload.data = data;
  if (error !== undefined) payload.error = error;
  send(payload);
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let i = buffer.indexOf("\\n");
  while (i >= 0) {
    const line = buffer.slice(0, i);
    buffer = buffer.slice(i + 1);
    if (line.trim().length > 0) {
      let cmd;
      try { cmd = JSON.parse(line); } catch { cmd = null; }
      if (cmd !== null) {
        const id = typeof cmd.id === "string" ? cmd.id : undefined;
        if (cmd.type === "get_state") {
          respond(id, "get_state", true, {
            sessionFile: "/tmp/pi-unexpected-exit-session.jsonl",
            sessionId: "sess-unexpected"
          });
        } else if (cmd.type === "prompt") {
          respond(id, "prompt", true);
          process.stdout.end(() => process.exit(1));
        } else {
          respond(id, String(cmd.type ?? ""), true);
        }
      }
    }
    i = buffer.indexOf("\\n");
  }
});
`;

/** A live `pi` that never answers: startSession hangs on `get_state` until interrupted. */
const HANG_SOURCE = "process.stdin.resume();";

const makeAdapter = (launcher: string, observedSpawner: ObservedSpawner, dir: string) =>
  makePiAdapter(
    { enabled: true, binaryPath: launcher, profileDir: "", customModels: [] },
    { instanceId: TEST_INSTANCE_ID },
  ).pipe(
    Effect.provide(ServerConfig.layerTest(dir, dir)),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner),
  );

interface Collector {
  readonly events: Effect.Effect<ReadonlyArray<ProviderRuntimeEvent>>;
  readonly waitUntil: (
    predicate: (events: ReadonlyArray<ProviderRuntimeEvent>) => boolean,
  ) => Effect.Effect<void>;
}

const makeCollector = (adapter: ProviderAdapterShape<ProviderAdapterError>) =>
  Effect.gen(function* () {
    const ref = yield* Ref.make<ReadonlyArray<ProviderRuntimeEvent>>([]);
    const waiters = yield* Ref.make<
      Array<{
        readonly predicate: (events: ReadonlyArray<ProviderRuntimeEvent>) => boolean;
        readonly deferred: Deferred.Deferred<void>;
      }>
    >([]);
    yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.gen(function* () {
        const events = yield* Ref.updateAndGet(ref, (arr) => [...arr, event]);
        const pending = yield* Ref.get(waiters);
        if (pending.length === 0) return;
        const satisfied = pending.filter((waiter) => waiter.predicate(events));
        if (satisfied.length === 0) return;
        yield* Ref.set(
          waiters,
          pending.filter((waiter) => !satisfied.includes(waiter)),
        );
        for (const waiter of satisfied) {
          yield* Deferred.succeed(waiter.deferred, undefined);
        }
      }),
    ).pipe(Effect.forkScoped);

    return {
      events: Ref.get(ref),
      waitUntil: (predicate) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(ref);
          if (predicate(current)) return;
          const deferred = yield* Deferred.make<void>();
          yield* Ref.update(waiters, (ws) => [...ws, { predicate, deferred }]);
          yield* Deferred.await(deferred);
        }),
    } satisfies Collector;
  });

const bridgePortOf = (command: ChildProcess.Command): number | undefined => {
  if (command._tag !== "StandardCommand") return undefined;
  const raw = command.options.env?.[BRIDGE_PORT_ENV];
  return typeof raw === "string" && raw.length > 0 ? Number(raw) : undefined;
};

/** Bind + immediately close a server on `port`; resolves false on EADDRINUSE. */
const portIsRebindable = (port: number) =>
  Effect.promise(
    () =>
      new Promise<boolean>((resolve) => {
        const server = NodeNet.createServer();
        let settled = false;
        const finish = (value: boolean) => {
          if (settled) return;
          settled = true;
          resolve(value);
        };
        server.once("error", () => finish(false));
        server.listen({ host: "127.0.0.1", port, backlog: 1 }, () => {
          server.close(() => finish(true));
        });
      }),
  );

it.layer(NodeServices.layer)("PiAdapterLifecycle", (it) => {
  it.effect("concurrent startSession for the same thread spawns a single process", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const testScope = yield* Scope.Scope;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({ directory, name: "pi", source: SIMPLE_STATE_SOURCE });

      const spawnCount = yield* Ref.make(0);
      const observedSpawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          yield* Ref.update(spawnCount, (n) => n + 1);
          if (command._tag !== "StandardCommand") {
            return yield* Effect.die("Unexpected process pipeline");
          }
          const handle = yield* spawner.spawn(command);
          yield* Scope.addFinalizer(testScope, handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );

      const adapter = yield* makeAdapter(launcher, observedSpawner, directory);
      const [first, second] = yield* Effect.all(
        [
          adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" }),
          adapter.startSession({ threadId: THREAD_ID, runtimeMode: "full-access" }),
        ],
        { concurrency: "unbounded" },
      );
      // Both callers succeed and share the first winner's session object.
      expect(first).toBe(second);
      expect(first.status).toBe("ready");
      // The one-flight lock means only the first caller reached the spawner.
      expect(yield* Ref.get(spawnCount)).toBe(1);
    }),
  );

  it.effect("unexpected process exit closes the explicit connection scope", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const testScope = yield* Scope.Scope;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({ directory, name: "pi", source: UNEXPECTED_EXIT_SOURCE });

      const scopeClosed = yield* Deferred.make<void>();
      const capturedScope = yield* Ref.make<Scope.Scope | undefined>(undefined);
      const observedSpawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          yield* Ref.set(capturedScope, scope);
          // Prove the connection-owned scope (not merely the process) is closed.
          yield* Scope.addFinalizer(
            scope,
            Deferred.succeed(scopeClosed, undefined).pipe(Effect.asVoid),
          );
          if (command._tag !== "StandardCommand") {
            return yield* Effect.die("Unexpected process pipeline");
          }
          const handle = yield* spawner.spawn(command);
          yield* Scope.addFinalizer(testScope, handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      // Safety: close the captured scope at teardown if an assertion fails first.
      yield* Scope.addFinalizer(
        testScope,
        Effect.gen(function* () {
          const scope = yield* Ref.get(capturedScope);
          if (scope !== undefined) yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
        }),
      );

      const adapter = yield* makeAdapter(launcher, observedSpawner, directory);
      const collector = yield* makeCollector(adapter);
      const session = yield* adapter.startSession({
        threadId: THREAD_ID,
        runtimeMode: "full-access",
      });
      expect(session.status).toBe("ready");

      yield* adapter.sendTurn({ threadId: THREAD_ID, input: "hello" });
      yield* collector.waitUntil((events) => events.some((e) => e.type === "session.exited"));
      yield* Deferred.await(scopeClosed);

      const exited = (yield* collector.events).find((e) => e.type === "session.exited");
      expect(exited?.type).toBe("session.exited");
      if (exited?.type === "session.exited") {
        expect(exited.payload.exitKind).toBe("error");
      }
    }),
  );

  it.effect("startup interrupted while spawning closes the connection scope and bridge", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const testScope = yield* Scope.Scope;
      const directory = yield* fs.makeTempDirectoryScoped();
      const launcher = writeFakeCli({ directory, name: "pi", source: HANG_SOURCE });

      const spawnEntered = yield* Deferred.make<void>();
      const scopeClosed = yield* Deferred.make<void>();
      const portRef = yield* Ref.make<number | undefined>(undefined);
      const capturedScope = yield* Ref.make<Scope.Scope | undefined>(undefined);
      const observedSpawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          const scope = yield* Scope.Scope;
          yield* Ref.set(capturedScope, scope);
          yield* Scope.addFinalizer(
            scope,
            Deferred.succeed(scopeClosed, undefined).pipe(Effect.asVoid),
          );
          const port = bridgePortOf(command);
          if (port !== undefined) yield* Ref.set(portRef, port);
          yield* Deferred.succeed(spawnEntered, undefined);
          // Never actually hand control to the real spawner: startup is stuck
          // acquiring the process, so interruption must retire both scopes.
          return yield* Effect.never;
        }),
      );
      yield* Scope.addFinalizer(
        testScope,
        Effect.gen(function* () {
          const scope = yield* Ref.get(capturedScope);
          if (scope !== undefined) yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
        }),
      );

      const adapter = yield* makeAdapter(launcher, observedSpawner, directory);
      const startFiber = yield* adapter
        .startSession({ threadId: THREAD_ID, runtimeMode: "full-access" })
        .pipe(Effect.forkChild);

      yield* Deferred.await(spawnEntered);
      yield* Fiber.interrupt(startFiber);
      const exit = yield* Fiber.await(startFiber);
      expect(Exit.isFailure(exit)).toBe(true);

      // The connection scope was closed explicitly during the interrupted spawn.
      yield* Deferred.await(scopeClosed);
      expect((yield* adapter.listSessions()).length).toBe(0);

      // The bridge listener released its ephemeral port: it can be bound again.
      const port = yield* Ref.get(portRef);
      expect(port).toBeDefined();
      expect(port).toBeGreaterThan(0);
      if (port !== undefined) {
        expect(yield* portIsRebindable(port)).toBe(true);
      }
    }),
  );

  it.effect(
    "startup interrupted while get_state is pending closes the acquired process and scope",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const testScope = yield* Scope.Scope;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const directory = yield* fs.makeTempDirectoryScoped();
        const launcher = writeFakeCli({ directory, name: "pi", source: HANG_SOURCE });

        const requestWritten = yield* Deferred.make<void>();
        const scopeClosed = yield* Deferred.make<void>();
        const pidRef = yield* Ref.make<number | undefined>(undefined);
        const capturedScope = yield* Ref.make<Scope.Scope | undefined>(undefined);
        const observedSpawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            const scope = yield* Scope.Scope;
            yield* Ref.set(capturedScope, scope);
            yield* Scope.addFinalizer(
              scope,
              Deferred.succeed(scopeClosed, undefined).pipe(Effect.asVoid),
            );
            if (command._tag !== "StandardCommand") {
              return yield* Effect.die("Unexpected process pipeline");
            }
            const handle = yield* spawner.spawn(command);
            yield* Ref.set(pidRef, Number(handle.pid));
            yield* Scope.addFinalizer(testScope, handle.kill().pipe(Effect.ignore));
            // Deterministic gate: resolve once the `get_state` command actually
            // reached the child's stdin, so the interrupt lands mid-request.
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
        yield* Scope.addFinalizer(
          testScope,
          Effect.gen(function* () {
            const scope = yield* Ref.get(capturedScope);
            if (scope !== undefined) yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
          }),
        );

        const adapter = yield* makeAdapter(launcher, observedSpawner, directory);
        const startFiber = yield* adapter
          .startSession({ threadId: THREAD_ID, runtimeMode: "full-access" })
          .pipe(Effect.forkChild);

        yield* Deferred.await(requestWritten);
        yield* Fiber.interrupt(startFiber);
        const exit = yield* Fiber.await(startFiber);
        expect(Exit.isFailure(exit)).toBe(true);

        // The explicit connection scope (and its process-kill finalizer) closed.
        yield* Deferred.await(scopeClosed);
        expect((yield* adapter.listSessions()).length).toBe(0);

        const pid = yield* Ref.get(pidRef);
        expect(pid).toBeDefined();
        // The acquired process was actually reaped, not merely flagged dead.
        if (!windowsHost && pid !== undefined) {
          expect(() => process.kill(pid, 0)).toThrow();
        }
      }),
  );
});
