// @effect-diagnostics nodeBuiltinImport:off - fixture files are written directly, matching other fake-CLI tests.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";

import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { afterAll } from "vite-plus/test";

afterAll(() => {
  for (const directory of fixtureDirectories) {
    try {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup; a leaked temp dir is harmless.
    }
  }
});
import {
  closePiRpcConnection,
  createLfFramer,
  makePiRpcConnection,
  makePiRpcConnectionIn,
  type PiRpcEvent,
} from "./PiRpcConnection.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));

const fixtureDirectories: Array<string> = [];

const makeFakePi = (extraEnv: Record<string, string> = {}) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pi-rpc-fixture-"));
  const scriptPath = NodePath.join(directory, "fake-pi-rpc.ts");
  NodeFS.writeFileSync(scriptPath, NodeFS.readFileSync(`${__dirname}/fakePiRpc.ts`, "utf8"));
  const launcher = writeFakeCli({
    directory,
    name: "pi",
    source: `await import(${JSON.stringify(NodeURL.pathToFileURL(scriptPath).href)});`,
    env: extraEnv,
  });
  fixtureDirectories.push(directory);
  return { directory, launcher };
};

describe("createLfFramer", () => {
  it("splits on LF only and strips a trailing CR", () => {
    const framer = createLfFramer();
    expect(framer.push('{"a":1}\n{"b":"x"}\r\n{"c":2}\n')).toEqual([
      '{"a":1}',
      '{"b":"x"}',
      '{"c":2}',
    ]);
    expect(framer.push('{"d":')).toEqual([]);
    expect(framer.push("3}\n")).toEqual(['{"d":3}']);
    expect(framer.flush()).toBeUndefined();
  });

  it("flushes a trailing unterminated fragment", () => {
    const framer = createLfFramer();
    framer.push('{"partial":');
    expect(framer.flush()).toBe('{"partial":');
  });
});

it.layer(NodeServices.layer)("makePiRpcConnection", (it) => {
  it.effect("correlates a request/response round trip and surfaces events", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnection({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const state = yield* connection.request({ type: "get_state" }, { timeoutMs: 10_000 });
      expect(state?.["sessionFile"]).toBe("/tmp/fake-session.jsonl");
      expect(state?.["sessionId"]).toBe("abc123");

      // The get_state handler emits an event with U+2028/U+2029 inside a
      // string; it must arrive as one intact event.
      const firstEvent = yield* Queue.take(connection.events);
      expect(firstEvent._tag).toBe("Event");
      if (firstEvent._tag === "Event") {
        expect((firstEvent.value as Record<string, unknown>)["type"]).toBe("message_update");
        expect((firstEvent.value as Record<string, unknown>)["note"]).toContain("line\u2028sep");
      }
    }),
  );

  it.effect("assembles a response fragmented across stdout chunks", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnection({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const data = yield* connection.request({ type: "fragmented" }, { timeoutMs: 10_000 });
      expect(data?.["ok"]).toBe(true);
      yield* connection.kill;
    }),
  );

  it.effect("fails a rejected command with PiRpcCommandError", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnection({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const result = yield* connection
        .request({ type: "prompt", message: "REJECT_ME please" }, { timeoutMs: 10_000 })
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("prompt rejected by fixture");
      }
      yield* connection.kill;
    }),
  );

  it.effect("fails pending requests when the process exits", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnectionIn({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const pendingFiber = yield* Effect.forkScoped(
        connection.request({ type: "hang" }, { timeoutMs: 30_000 }).pipe(Effect.exit),
      );
      yield* connection.request({ type: "exit", code: 0 }, { timeoutMs: 10_000 });
      const exitEvent = yield* Deferred.await(connection.exited);
      expect(exitEvent._tag).toBe("Exited");
      expect(exitEvent.exitCode).toBe(0);
      const pendingOutcome = yield* Fiber.join(pendingFiber);
      expect(Exit.isFailure(pendingOutcome)).toBe(true);
      yield* closePiRpcConnection(connection);
    }),
  );

  it.effect("drains final stdout and stderr before publishing process exit", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnectionIn({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      yield* connection.request({ type: "exit_with_output" });
      for (let seq = 0; seq < 300; seq += 1) {
        const event = yield* Queue.take(connection.events);
        expect(event).toEqual({
          _tag: "Event",
          value: { type: "message_update", seq, delta: "héllo 🌍 ".repeat(256) },
        });
      }
      expect(yield* Queue.take(connection.events)).toEqual({
        _tag: "Event",
        value: { type: "agent_settled" },
      });
      expect(yield* Queue.take(connection.events)).toMatchObject({
        _tag: "Exited",
        exitCode: 0,
        stderrTail: "final diagnostic",
      });
      const afterExit = yield* connection.request({ type: "get_state" }).pipe(Effect.exit);
      expect(Exit.isFailure(afterExit)).toBe(true);
      yield* closePiRpcConnection(connection);
    }),
  );

  it.effect("closing the connection releases pending requests without their timeout", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnectionIn({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const pending = yield* Effect.forkScoped(
        connection.request({ type: "hang" }).pipe(Effect.exit),
      );
      // This receipt proves the fixture has processed the earlier hang command.
      yield* connection.request({ type: "get_state" });
      yield* closePiRpcConnection(connection);
      const result = yield* Fiber.join(pending);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) expect(String(result.cause)).toContain("connection closed");
    }),
  );

  it.effect("times out a request against a hung process", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi();
      const connection = yield* makePiRpcConnectionIn({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      const outcome = yield* connection
        .request({ type: "hang" }, { timeoutMs: 150 })
        .pipe(Effect.exit, TestClock.withLive);
      expect(Exit.isFailure(outcome)).toBe(true);
      if (Exit.isFailure(outcome)) {
        expect(String(outcome.cause)).toContain("timed out");
      }
      yield* closePiRpcConnection(connection);
    }),
  );

  it.effect("spawning a missing binary fails with PiRpcSpawnError", () =>
    Effect.gen(function* () {
      const outcome = yield* makePiRpcConnection({
        binaryPath: "/definitely/not/a/real/binary/pi-fixture",
        args: ["--mode", "rpc"],
        cwd: __dirname,
      }).pipe(Effect.exit);
      expect(Exit.isFailure(outcome)).toBe(true);
      if (Exit.isFailure(outcome)) {
        expect(String(outcome.cause)).toContain("PiRpcSpawnError");
      }
    }),
  );

  it.effect("records unparsable stdout lines without breaking the protocol", () =>
    Effect.gen(function* () {
      const fixture = makeFakePi({ FAKE_PI_RPC_SPAN_UNICODE_ON_START: "1" });
      const connection = yield* makePiRpcConnectionIn({
        binaryPath: fixture.launcher,
        args: ["--mode", "rpc"],
        cwd: fixture.directory,
      });
      // The startup event arrives intact despite being unicode-heavy.
      const firstEvent: PiRpcEvent = yield* Queue.take(connection.events);
      expect(firstEvent._tag).toBe("Event");
      const state = yield* connection.request({ type: "get_state" }, { timeoutMs: 10_000 });
      expect(state?.["sessionId"]).toBe("abc123");
      yield* closePiRpcConnection(connection);
    }),
  );
});
