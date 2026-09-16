// @effect-diagnostics nodeBuiltinImport:off - the bridge uses the node:net socket directly.
// @effect-diagnostics preferSchemaOverJson:off - the subject under test IS the JSON-over-LF wire protocol.
import * as NodeNet from "node:net";

import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Option, Queue, Scope } from "effect";
import { ProviderInstanceId } from "@t3tools/contracts";

import {
  decodePiBridgeEntryFrame,
  makePiBridgeListener,
  PI_BRIDGE_PROTOCOL_VERSION,
  PI_BRIDGE_CONTROL_MAX_BYTES,
} from "./PiBridge.ts";

const instanceId = ProviderInstanceId.make("pi-test");
const makeBridge = () =>
  Effect.acquireRelease(makePiBridgeListener({ threadId: "th-1", instanceId }), (bridge) =>
    Scope.close(bridge.scope, Exit.void),
  );

const appendChunk = {
  op: "append" as const,
  itemId: "answer",
  kind: "text" as const,
  text: "hello",
};

describe("decodePiBridgeEntryFrame", () => {
  it("decodes a t3-bridge child.started entry", () => {
    const decoded = decodePiBridgeEntryFrame({
      type: "custom",
      customType: "t3-bridge",
      data: {
        type: "child.started",
        runId: "run-1",
        childId: "sa-1",
        title: "Explore",
        backend: "pi",
      },
    });
    expect(Option.isSome(decoded)).toBe(true);
    if (Option.isSome(decoded)) {
      expect(decoded.value.data).toMatchObject({
        type: "child.started",
        runId: "run-1",
        childId: "sa-1",
      });
    }
  });

  it("decodes a typed transcript chunk", () => {
    const decoded = decodePiBridgeEntryFrame({
      type: "custom",
      customType: "t3-bridge",
      data: {
        type: "child.transcript",
        runId: "run-1",
        childId: "sa-1",
        seq: 0,
        chunk: appendChunk,
      },
    });
    expect(Option.isSome(decoded)).toBe(true);
    if (Option.isSome(decoded)) {
      expect(decoded.value.data).toMatchObject({ type: "child.transcript", chunk: appendChunk });
    }
  });

  it("decodes actual companion emitter frames with numeric ts on every frame", () => {
    const numericTs = 1750000000000;
    const frames = [
      {
        type: "child.started" as const,
        runId: "run-1",
        childId: "sa-1",
        title: "Explore",
        backend: "pi",
        cwd: "/tmp",
        model: "fixture/model",
        ts: numericTs,
      },
      {
        type: "child.status" as const,
        runId: "run-1",
        childId: "sa-1",
        status: "running" as const,
        ts: numericTs,
      },
      {
        type: "child.status" as const,
        runId: "run-1",
        childId: "sa-1",
        // The companion substitutes `cancelled` for an interrupted child.
        status: "cancelled" as const,
        ts: numericTs,
      },
      {
        type: "child.transcript" as const,
        runId: "run-1",
        childId: "sa-1",
        seq: 0,
        chunk: appendChunk,
        ts: numericTs,
      },
      {
        type: "child.usage" as const,
        runId: "run-1",
        childId: "sa-1",
        tokens: 42,
        contextWindow: 200000,
        ts: numericTs,
      },
      {
        type: "child.result" as const,
        runId: "run-1",
        childId: "sa-1",
        status: "done" as const,
        finalText: "done",
        ts: numericTs,
      },
    ];
    for (const data of frames) {
      const decoded = decodePiBridgeEntryFrame({
        type: "custom",
        customType: "t3-bridge",
        data,
      });
      expect(Option.isSome(decoded)).toBe(true);
      if (Option.isSome(decoded)) {
        expect(decoded.value.data).toMatchObject(data);
      }
    }
  });

  it("rejects a frame with a string ts (the wire is numeric Date.now)", () => {
    expect(
      Option.isNone(
        decodePiBridgeEntryFrame({
          type: "custom",
          customType: "t3-bridge",
          data: {
            type: "child.started",
            runId: "run-1",
            childId: "sa-1",
            title: "Explore",
            backend: "pi",
            ts: "2026-09-14T00:00:00.000Z",
          },
        }),
      ),
    ).toBe(true);
  });

  it("rejects entries that are not t3-bridge and malformed frames", () => {
    expect(
      Option.isNone(decodePiBridgeEntryFrame({ type: "custom", customType: "other", data: {} })),
    ).toBe(true);
    expect(
      Option.isNone(
        decodePiBridgeEntryFrame({
          type: "custom",
          customType: "t3-bridge",
          data: { type: "child.started" },
        }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        decodePiBridgeEntryFrame({
          type: "custom",
          customType: "t3-bridge",
          data: {
            type: "child.transcript",
            runId: "run-1",
            childId: "sa-1",
            seq: 0,
            chunk: { op: "append" },
          },
        }),
      ),
    ).toBe(true);
  });
});

interface SocketClient {
  readonly nextLine: Effect.Effect<string>;
  readonly write: (value: unknown) => Effect.Effect<void>;
  readonly rawWrite: (chunk: Buffer) => Effect.Effect<void>;
  readonly close: Effect.Effect<void>;
  readonly closed: Deferred.Deferred<void>;
}

const makeSocketClient = (port: number) =>
  Effect.gen(function* () {
    const socket = yield* Effect.acquireRelease(
      Effect.callback<NodeNet.Socket>((resume) => {
        const next = NodeNet.connect({ host: "127.0.0.1", port });
        next.once("connect", () => resume(Effect.succeed(next)));
        next.once("error", (cause) => resume(Effect.die(cause)));
      }),
      (next) => Effect.sync(() => next.destroy()),
    );
    const lines = yield* Queue.unbounded<string>();
    const closed = yield* Deferred.make<void>();
    let buffer = "";
    socket.on("data", (chunk: Buffer) => {
      buffer += chunk.toString("utf8");
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        const line = buffer.slice(0, index).replace(/\r$/, "");
        buffer = buffer.slice(index + 1);
        void Queue.offer(lines, line).pipe(Effect.runSync);
        index = buffer.indexOf("\n");
      }
    });
    socket.on("close", () => {
      void Deferred.succeed(closed, undefined).pipe(Effect.runSync);
    });
    return {
      nextLine: Queue.take(lines),
      write: (value: unknown) => Effect.sync(() => socket.write(`${JSON.stringify(value)}\n`)),
      rawWrite: (chunk: Buffer) => Effect.sync(() => socket.write(chunk)),
      close: Effect.sync(() => socket.destroy()),
      closed,
    } satisfies SocketClient;
  });

describe("makePiBridgeListener", () => {
  it.effect("closing the owner releases pending controls and the socket", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env.T3CODE_PI_BRIDGE_PORT));
      yield* client.write({
        type: "hello",
        protocol: 1,
        token: bridge.env.T3CODE_PI_BRIDGE_TOKEN,
        runId: bridge.runId,
      });
      yield* client.nextLine;
      const pending = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      yield* client.nextLine;
      yield* Scope.close(bridge.scope, Exit.void);
      expect(yield* Fiber.join(pending)).toBe(false);
      yield* Deferred.await(client.closed);
      expect(yield* bridge.connected).toBe(false);
    }),
  );

  for (const terminated of [false, true]) {
    it.effect(`rejects an oversized UTF-8 ${terminated ? "frame" : "fragment"}`, () =>
      Effect.gen(function* () {
        const bridge = yield* makeBridge();
        const client = yield* makeSocketClient(Number(bridge.env.T3CODE_PI_BRIDGE_PORT));
        const frame = JSON.stringify({
          type: "hello",
          protocol: 1,
          token: bridge.env.T3CODE_PI_BRIDGE_TOKEN,
          runId: bridge.runId,
          padding: "😀".repeat(PI_BRIDGE_CONTROL_MAX_BYTES / 3),
        });
        expect(frame.length).toBeLessThan(PI_BRIDGE_CONTROL_MAX_BYTES);
        yield* client.rawWrite(Buffer.from(frame + (terminated ? "\n" : "")));
        yield* Deferred.await(client.closed);
        expect(yield* bridge.connected).toBe(false);
      }),
    );
  }

  it.effect("accepts a coalesced batch of individually bounded controls", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env.T3CODE_PI_BRIDGE_PORT));
      yield* client.write({
        type: "hello",
        protocol: 1,
        token: bridge.env.T3CODE_PI_BRIDGE_TOKEN,
        runId: bridge.runId,
      });
      yield* client.nextLine;
      const pending = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      const command = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      const noise = JSON.stringify({ type: "ignored", padding: "x".repeat(40_000) }) + "\n";
      const ack =
        JSON.stringify({ type: "ack", reqId: command.reqId, runId: bridge.runId, accepted: true }) +
        "\n";
      yield* client.rawWrite(Buffer.from(noise.repeat(4) + ack));
      expect(yield* Fiber.join(pending)).toBe(true);
    }),
  );
  it.effect("authenticates hello and flips connected only after hello_ok", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      expect(yield* bridge.connected).toBe(false);
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
        pid: 123,
      });
      const helloOk = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      expect(helloOk).toMatchObject({
        type: "hello_ok",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        runId: bridge.runId,
      });
      expect(yield* bridge.connected).toBe(true);
    }),
  );

  it.effect("cancel and quiesce commands carry runId and resolve on ack", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
      });
      yield* client.nextLine;

      const cancelFiber = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      const cancel = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      expect(cancel).toMatchObject({ type: "cancel", childId: "sa-1", runId: bridge.runId });
      yield* client.write({
        type: "ack",
        reqId: cancel["reqId"],
        runId: bridge.runId,
        accepted: true,
      });
      expect(yield* Fiber.join(cancelFiber)).toBe(true);

      const quiesceFiber = yield* Effect.forkChild(bridge.quiesce());
      const quiesce = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      expect(quiesce).toMatchObject({ type: "quiesce", runId: bridge.runId });
      yield* client.write({
        type: "ack",
        reqId: quiesce["reqId"],
        runId: bridge.runId,
        accepted: false,
      });
      expect(yield* Fiber.join(quiesceFiber)).toBe(false);
    }),
  );

  it.effect("rejects an acknowledgment without its process generation", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
      });
      yield* client.nextLine;

      const cancelFiber = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      const cancel = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      expect(cancel).toMatchObject({ type: "cancel", childId: "sa-1", runId: bridge.runId });
      yield* client.write({ type: "ack", reqId: cancel["reqId"], accepted: true });
      yield* client.write({
        type: "ack",
        reqId: cancel["reqId"],
        runId: bridge.runId,
        accepted: false,
      });
      expect(yield* Fiber.join(cancelFiber)).toBe(false);
    }),
  );

  it.effect("rejects an ack whose runId mismatches this generation", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
      });
      yield* client.nextLine;

      const cancelFiber = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      const cancel = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      // The invalid acceptance precedes a valid rejection on the same socket:
      // only the latter may resolve this command.
      yield* client.write({
        type: "ack",
        reqId: cancel["reqId"],
        runId: "wrong-generation",
        accepted: true,
      });
      yield* client.write({
        type: "ack",
        reqId: cancel["reqId"],
        runId: bridge.runId,
        accepted: false,
      });
      expect(yield* Fiber.join(cancelFiber)).toBe(false);
    }),
  );

  it.effect("rejects a mismatched run id and never advertises readiness", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: "wrong-generation",
      });
      yield* Deferred.await(client.closed);
      expect(yield* bridge.connected).toBe(false);
    }),
  );

  it.effect("fails pending commands closed when the socket drops", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      yield* client.write({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
      });
      yield* client.nextLine;

      const pending = yield* Effect.forkChild(bridge.cancelChild("sa-1"));
      yield* client.nextLine;
      yield* client.close;
      expect(yield* Fiber.join(pending)).toBe(false);
    }),
  );

  it.effect("reassembles a multi-byte code point split across TCP chunks", () =>
    Effect.gen(function* () {
      const bridge = yield* makeBridge();
      const client = yield* makeSocketClient(Number(bridge.env[`T3CODE_PI_BRIDGE_PORT`]));
      // The emoji in pid is split across two writes; the UTF-8 decoder must
      // reassemble it before JSON.parse succeeds.
      const hello = `${JSON.stringify({
        type: "hello",
        protocol: PI_BRIDGE_PROTOCOL_VERSION,
        token: bridge.env[`T3CODE_PI_BRIDGE_TOKEN`],
        runId: bridge.runId,
        pid: "\u{1F600}",
      })}\n`;
      const bytes = Buffer.from(hello, "utf8");
      const boundary = bytes.indexOf(Buffer.from("\u{1F600}")) + 2;
      yield* client.rawWrite(bytes.subarray(0, boundary));
      yield* client.rawWrite(bytes.subarray(boundary));
      const helloOk = JSON.parse(yield* client.nextLine) as Record<string, unknown>;
      expect(helloOk).toMatchObject({ type: "hello_ok", runId: bridge.runId });
    }),
  );
});
