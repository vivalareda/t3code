// @effect-diagnostics preferSchemaOverJson:off - the RPC wire is untyped upstream JSON; schemas belong to the adapter boundary above.
/**
 * PiRpcConnection — one `pi --mode rpc` child process speaking JSONL over
 * stdio.
 *
 * Pi's RPC framing is strict JSONL with LF (`\n`) as the only record
 * delimiter (see pi docs/rpc.md): records split on `\n` only, an optional
 * trailing `\r` is stripped, and generic Unicode line readers must not be
 * used because U+2028/U+2029 are valid inside JSON strings. `readline` is
 * therefore off-limits here.
 *
 * Commands carry a correlation `id`; Pi answers with a `type: "response"`
 * line echoing that id. Everything else on stdout is an event. Extension UI
 * dialogs arrive as `extension_ui_request` lines and are answered by the
 * owner through `respondExtensionUi`.
 *
 * The connection is scoped: closing the scope kills the child and fails all
 * pending requests. Pi's RPC mode owns stdout, so nothing here may write to
 * the child's stdout or assume `console.log` inside pi produces events.
 *
 * @module provider/pi/PiRpcConnection
 */
import { Data, Deferred, Effect, Exit, Fiber, Queue, Ref, Scope, Sink, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

export class PiRpcSpawnError extends Data.TaggedError("PiRpcSpawnError")<{
  readonly command: string;
  readonly cause: unknown;
}> {
  override get message(): string {
    return `Failed to spawn Pi RPC process: ${this.command}`;
  }
}

export class PiRpcClosedError extends Data.TaggedError("PiRpcClosedError")<{
  readonly reason?: string | undefined;
}> {
  override get message(): string {
    return this.reason === undefined
      ? "Pi RPC process has exited."
      : `Pi RPC process has exited: ${this.reason}`;
  }
}

export class PiRpcCommandError extends Data.TaggedError("PiRpcCommandError")<{
  readonly command: string;
  readonly error: string;
}> {
  override get message(): string {
    return `Pi RPC command '${this.command}' failed: ${this.error}`;
  }
}

export type PiRpcEvent =
  | { readonly _tag: "Event"; readonly value: Record<string, unknown> }
  | { readonly _tag: "Exited"; readonly exitCode: number | null; readonly stderrTail: string };

export interface PiRpcSpawnInput {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  /** Environment for the child; `undefined` inherits the server environment. */
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly extendEnv?: boolean | undefined;
}

export interface PiRpcConnection {
  /**
   * Send a command and await its correlated response. Fails with
   * `PiRpcCommandError` when Pi answers `success: false`.
   */
  readonly request: (
    command: Record<string, unknown>,
    options?: { readonly timeoutMs?: number | undefined },
  ) => Effect.Effect<Record<string, unknown> | undefined, PiRpcClosedError | PiRpcCommandError>;
  /** Send a command without awaiting a response (fire-and-forget writes). */
  readonly send: (command: Record<string, unknown>) => Effect.Effect<void, PiRpcClosedError>;
  /** Answer an `extension_ui_request` previously surfaced on the event queue. */
  readonly respondExtensionUi: (
    response: Record<string, unknown>,
  ) => Effect.Effect<void, PiRpcClosedError>;
  /** Every non-response stdout line, plus a terminal `Exited` marker. */
  readonly events: Queue.Queue<PiRpcEvent>;
  /** Resolves once the child process has exited. */
  readonly exited: Deferred.Deferred<PiRpcEvent & { readonly _tag: "Exited" }, never>;
  /** Force-kill the child (bounded). */
  readonly kill: Effect.Effect<void>;
  /** Recent stderr tail, for diagnostics when the process misbehaves. */
  readonly stderrTail: Effect.Effect<string>;
}

const STDERR_TAIL_MAX_CHARS = 8_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 120_000;
const lineEncoder = new TextEncoder();

/** Take the tail of an accumulated string, bounded for diagnostics. */
const tail = (value: string): string =>
  value.length <= STDERR_TAIL_MAX_CHARS ? value : value.slice(-STDERR_TAIL_MAX_CHARS);

/**
 * Strict LF framing for pi RPC stdout. Returns complete records with the
 * delimiter removed and an optional trailing `\r` stripped; keeps the
 * remainder buffered. A trailing unterminated fragment is emitted only when
 * `flush` is set (process exit).
 */
export const createLfFramer = () => {
  let buffer = "";
  return {
    push(chunk: string): Array<string> {
      buffer += chunk;
      const records: Array<string> = [];
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        let record = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        if (record.endsWith("\r")) record = record.slice(0, -1);
        records.push(record);
        index = buffer.indexOf("\n");
      }
      return records;
    },
    flush(): string | undefined {
      if (buffer.length === 0) return undefined;
      const record = buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer;
      buffer = "";
      return record;
    },
  };
};

type PiRpcResponseOutcome =
  | { readonly _tag: "Success"; readonly data: Record<string, unknown> | undefined }
  | { readonly _tag: "CommandError"; readonly error: string };

export const makePiRpcConnection = Effect.fn("makePiRpcConnection")(function* (
  input: PiRpcSpawnInput,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const events = yield* Queue.unbounded<PiRpcEvent>();
  const exited = yield* Deferred.make<PiRpcEvent & { readonly _tag: "Exited" }, never>();
  const pending = yield* Ref.make(new Map<string, Deferred.Deferred<PiRpcResponseOutcome>>());
  const stderrTailRef = yield* Ref.make("");
  let nextRequestId = 0;
  let closed = false;

  const failAllPending = (reason?: string) =>
    Effect.gen(function* () {
      const open = yield* Ref.getAndSet(pending, new Map());
      for (const deferred of open.values()) {
        yield* Deferred.done(
          deferred,
          Exit.fail(new PiRpcClosedError(reason === undefined ? {} : { reason })),
        );
      }
    });

  const handleLine = (line: string) =>
    Effect.gen(function* () {
      if (line.length === 0) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        // Non-JSON stdout output (a stray console write) is not protocol
        // traffic; keep it out of the event stream but leave a breadcrumb.
        yield* Ref.update(stderrTailRef, (current) => tail(`${current}\n[unparsed] ${line}`));
        return;
      }
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        yield* Ref.update(stderrTailRef, (current) => tail(`${current}\n[non-object] ${line}`));
        return;
      }
      const record = parsed as Record<string, unknown>;
      if (record.type === "response") {
        const id = typeof record.id === "string" ? record.id : undefined;
        if (id === undefined) return;
        const open = yield* Ref.get(pending);
        const deferred = open.get(id);
        if (!deferred) return;
        if (record.success === true) {
          const data =
            typeof record.data === "object" && record.data !== null
              ? (record.data as Record<string, unknown>)
              : undefined;
          yield* Deferred.done(deferred, Exit.succeed({ _tag: "Success" as const, data }));
        } else {
          const error = typeof record.error === "string" ? record.error : "unknown Pi RPC failure";
          yield* Deferred.done(deferred, Exit.succeed({ _tag: "CommandError" as const, error }));
        }
        return;
      }
      yield* Queue.offer(events, { _tag: "Event" as const, value: record });
    });

  const spawnCommand = yield* resolveSpawnCommand(input.binaryPath, [...input.args], {
    ...(input.env ? { env: input.env } : {}),
    extendEnv: input.extendEnv ?? true,
  });
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: input.cwd,
        // Keep the RPC pipe open across individual writes; the default
        // `endOnDone: true` closes stdin after the first write.
        stdin: { stream: "pipe", endOnDone: false },
        ...(input.env ? { env: input.env } : {}),
        extendEnv: input.extendEnv ?? true,
        shell: spawnCommand.shell,
      }),
    )
    .pipe(Effect.mapError((cause) => new PiRpcSpawnError({ command: input.binaryPath, cause })));

  // One framer for the whole stdout stream: a record may straddle chunks.
  const framer = createLfFramer();

  const stdoutPump = child.stdout.pipe(
    Stream.decodeText(),
    Stream.flatMap((chunk) => Stream.fromIterable(framer.push(chunk))),
    Stream.mapEffect((line) => handleLine(line)),
    Stream.runDrain,
    Effect.andThen(
      Effect.gen(function* () {
        const lastLine = framer.flush();
        if (lastLine !== undefined) yield* handleLine(lastLine);
      }),
    ),
    Effect.ignore,
  );

  const stderrPump = child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) => Ref.update(stderrTailRef, (current) => tail(current + chunk))),
    Effect.ignore,
  );

  const stdoutFiber = yield* Effect.forkScoped(stdoutPump);
  const stderrFiber = yield* Effect.forkScoped(stderrPump);
  const exitWatch = Effect.gen(function* () {
    const code = yield* child.exitCode.pipe(Effect.map((value) => Number(value)));
    // Process exit can precede pipe EOF. The terminal marker must follow all
    // buffered responses and events, or the adapter drops the final answer.
    yield* Fiber.await(stdoutFiber);
    yield* Fiber.await(stderrFiber);
    closed = true;
    const tailText = yield* Ref.get(stderrTailRef);
    const exitEvent: PiRpcEvent & { readonly _tag: "Exited" } = {
      _tag: "Exited",
      exitCode: Number.isFinite(code) ? code : null,
      stderrTail: tailText,
    };
    yield* failAllPending(`process exited with code ${exitEvent.exitCode ?? "unknown"}`);
    yield* Queue.offer(events, exitEvent);
    yield* Deferred.succeed(exited, exitEvent);
  }).pipe(Effect.ignore);

  yield* Effect.forkScoped(exitWatch);

  const writeLine = (payload: string) =>
    Stream.make(lineEncoder.encode(`${payload}\n`)).pipe(
      Stream.run(child.stdin as Sink.Sink<void, Uint8Array, never>),
      Effect.as(true),
      Effect.catch(() => Effect.succeed(false)),
    );

  const connection: PiRpcConnection = {
    events,
    exited,
    stderrTail: Ref.get(stderrTailRef),
    request: (command, options) =>
      Effect.gen(function* () {
        if (closed) return yield* new PiRpcClosedError({});
        const id = `t3-${++nextRequestId}`;
        const deferred = yield* Deferred.make<PiRpcResponseOutcome>();
        yield* Ref.update(pending, (map) => {
          const next = new Map(map);
          next.set(id, deferred);
          return next;
        });
        const outcome = yield* Effect.gen(function* () {
          if (closed) return yield* new PiRpcClosedError({});
          const written = yield* writeLine(JSON.stringify({ ...command, id }));
          if (!written) {
            return yield* new PiRpcClosedError({ reason: "child stdin is closed" });
          }
          return yield* Deferred.await(deferred).pipe(
            Effect.timeoutOption(options?.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS),
          );
        }).pipe(
          Effect.ensuring(
            Ref.update(pending, (map) => {
              const next = new Map(map);
              next.delete(id);
              return next;
            }),
          ),
        );
        if (outcome._tag === "None") {
          return yield* new PiRpcClosedError({
            reason: `timed out waiting for response to '${String(command.type)}'`,
          });
        }
        switch (outcome.value._tag) {
          case "Success":
            return outcome.value.data;
          case "CommandError":
            return yield* new PiRpcCommandError({
              command: String(command.type ?? "unknown"),
              error: outcome.value.error,
            });
        }
      }),
    send: (command) =>
      Effect.gen(function* () {
        if (closed) return yield* new PiRpcClosedError({});
        const written = yield* writeLine(JSON.stringify(command));
        if (!written) return yield* new PiRpcClosedError({ reason: "child stdin is closed" });
      }),
    respondExtensionUi: (response) => connection.send(response),
    kill: child.kill({ forceKillAfter: "2 seconds" }).pipe(Effect.ignore),
  };

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closed = true;
      yield* failAllPending("connection closed");
      yield* connection.kill;
    }),
  );

  return connection;
});

export interface PiRpcConnectionHandle extends PiRpcConnection {
  readonly scope: Scope.Closeable;
}

/**
 * Spawn a connection tied to an explicitly managed scope so an adapter can
 * close it deterministically during stop, independent of the caller's scope.
 */
export const makePiRpcConnectionIn = (input: PiRpcSpawnInput) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const connection = yield* makePiRpcConnection(input).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.onError(() => Scope.close(scope, Exit.void)),
    );
    return { ...connection, scope } satisfies PiRpcConnectionHandle;
  });

/** Close an explicitly managed connection scope, bounded. */
export const closePiRpcConnection = (handle: PiRpcConnectionHandle) =>
  Scope.close(handle.scope, Exit.void).pipe(Effect.timeoutOption("5 seconds"), Effect.ignore);
