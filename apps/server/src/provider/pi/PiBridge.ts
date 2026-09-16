/**
 * PiBridge — the loopback TCP listener between T3 and the subagents extension
 * running inside one `pi --mode rpc` process.
 *
 * Pi's RPC stream carries the parent conversation AND child events: child
 * (subagent) frames arrive as `entry_appended` entries with
 * `customType: "t3-bridge"` and are decoded by {@link decodePiBridgeEntryFrame}.
 * This TCP socket carries authenticated controls only — cancel and quiesce.
 * A remote browser never contacts this listener; all reads and controls flow
 * through T3's authenticated transport.
 *
 * Framing is LF-delimited JSON with a hard per-frame byte bound and UTF-8-safe
 * decoding (multi-byte characters split across TCP chunks are reassembled).
 * The handshake authenticates the session token and the run id T3 generated
 * for this provider process; anything else is rejected before any child
 * control is advertised. `connected` flips true only after `hello_ok` is sent.
 *
 * Protocol (version 1):
 *   client → server:
 *     {"type":"hello","protocol":1,"token":…,"runId":…,"pid":…}
 *     {"type":"ack","reqId":…,"runId":…,"accepted":bool}
 *   server → client:
 *     {"type":"hello_ok","protocol":1,"runId":…}
 *     {"type":"cancel","reqId":…,"childId":…,"runId":…}
 *     {"type":"quiesce","reqId":…,"runId":…}
 *     {"type":"ping","reqId":…}
 *
 * Child frames (over Pi RPC `entry_appended`, `customType: "t3-bridge"`):
 *     every frame carries `runId` (generation) and a numeric `ts` (Date.now).
 *     {"type":"child.started","runId":…,"childId","title","backend","cwd?","model?","effort?"}
 *     {"type":"child.status","runId":…,"childId","status":"running|done|error|cancelled","errorText?"}
 *     {"type":"child.transcript","runId":…,"childId","seq",…chunk…}
 *     {"type":"child.usage","runId":…,"childId","tokens?","contextWindow?"}
 *     {"type":"child.result","runId":…,"childId","status":"done|error|cancelled","finalText?","errorText?"}
 *
 * @module provider/pi/PiBridge
 */
import { Data, Effect, Exit, Option, Schema, Scope } from "effect";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as NodeStringDecoder from "node:string_decoder";

import {
  ChildId,
  ChildTranscriptChunk,
  NonNegativeInt,
  ProviderInstanceId,
  RunId,
} from "@t3tools/contracts";

export const PI_BRIDGE_PROTOCOL_VERSION = 1;
export const PI_BRIDGE_ENV_PREFIX = "T3CODE_PI_BRIDGE";

/** Hard bound for one LF-delimited frame. Exceeding it drops the socket. */
export const PI_BRIDGE_CONTROL_MAX_BYTES = 64 * 1024;
/** Bounded wait for an extension acknowledgment; commands fail closed. */
const COMMAND_TIMEOUT_MS = 10_000;

export class PiBridgeBindError extends Data.TaggedError("PiBridgeBindError")<{
  readonly cause: unknown;
}> {
  override get message(): string {
    return "Failed to bind the Pi child bridge listener.";
  }
}

/** Wire shape of one `entry_appended` entry carrying a bridged child frame. */
// `ts` is the extension's `Date.now()` wall-clock millis on every frame — a
// number, not an ISO string. The schema matches the actual companion emitter
// (`createBridgePublisher`, `ts: now()` with `now ?? Date.now`).
export const PiBridgeEntryFrame = Schema.Struct({
  type: Schema.Literal("custom"),
  customType: Schema.Literal("t3-bridge"),
  data: Schema.Union([
    Schema.Struct({
      type: Schema.Literal("child.started"),
      runId: RunId,
      childId: ChildId,
      title: Schema.String,
      backend: Schema.String,
      cwd: Schema.optional(Schema.String),
      model: Schema.optional(Schema.String),
      effort: Schema.optional(Schema.String),
      ts: Schema.optional(Schema.Number),
    }),
    Schema.Struct({
      type: Schema.Literal("child.status"),
      runId: RunId,
      childId: ChildId,
      // The publisher substitutes `cancelled` for an interrupted child's
      // status so a monotone terminal handler never freezes `error` first.
      status: Schema.Literals(["running", "done", "error", "cancelled"]),
      errorText: Schema.optional(Schema.String),
      settledAt: Schema.optional(Schema.Number),
      ts: Schema.optional(Schema.Number),
    }),
    Schema.Struct({
      type: Schema.Literal("child.transcript"),
      runId: RunId,
      childId: ChildId,
      seq: NonNegativeInt,
      chunk: ChildTranscriptChunk,
      ts: Schema.optional(Schema.Number),
    }),
    Schema.Struct({
      type: Schema.Literal("child.usage"),
      runId: RunId,
      childId: ChildId,
      tokens: Schema.optional(Schema.Number),
      contextWindow: Schema.optional(Schema.Number),
      ts: Schema.optional(Schema.Number),
    }),
    Schema.Struct({
      type: Schema.Literal("child.result"),
      runId: RunId,
      childId: ChildId,
      status: Schema.Literals(["done", "error", "cancelled"]),
      finalText: Schema.optional(Schema.String),
      errorText: Schema.optional(Schema.String),
      ts: Schema.optional(Schema.Number),
    }),
  ]),
});
export type PiBridgeEntryFrame = typeof PiBridgeEntryFrame.Type;
export type PiBridgeChildFrame = PiBridgeEntryFrame["data"];

/**
 * Decode and validate the `entry` value of a Pi RPC `entry_appended` event.
 * Returns `None` for anything that is not a `t3-bridge` custom entry (or is
 * malformed), so the RPC pump can skip unrelated entries without failing.
 * The caller checks `frame.runId` against its live bridge run id.
 */
export const decodePiBridgeEntryFrame = Schema.decodeUnknownOption(PiBridgeEntryFrame);

export interface PiBridgeListener {
  /** Environment variables handed to the spawned Pi process. */
  readonly env: Readonly<Record<string, string>>;
  /** Full process-generation run id; equals `T3CODE_PI_BRIDGE_RUN_ID`. */
  readonly runId: string;
  /** Send cancel-one; resolves with the extension's acknowledgment. */
  readonly cancelChild: (childId: string) => Effect.Effect<boolean>;
  /** Quiesce: close admission + suppress automatic delivery. */
  readonly quiesce: () => Effect.Effect<boolean>;
  /**
   * True only after an authenticated hello completed (not merely after the
   * socket connected). Controls must be sent only when this is true.
   */
  readonly connected: Effect.Effect<boolean>;
  /** Closes the listener; the owner ties this to the Pi process lifetime. */
  readonly scope: Scope.Closeable;
}

interface BridgeState {
  readonly pendingCommands: Map<string, (accepted: boolean) => void>;
  socket: NodeNet.Socket | null;
  connected: boolean;
  requestCounter: number;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const makePiBridgeListener = Effect.fn("makePiBridgeListener")(function* (input: {
  readonly threadId: string;
  readonly instanceId: ProviderInstanceId | undefined;
}): Effect.fn.Return<PiBridgeListener, PiBridgeBindError> {
  const token = NodeCrypto.randomUUID();
  const runId = NodeCrypto.randomUUID();
  // The listener owns its cleanup scope so the caller never has to thread an
  // ambient Scope into its session effect; the owner closes this scope to
  // retire the listener alongside the Pi process.
  const ownScope = yield* Scope.make();

  return yield* Effect.gen(function* () {
    const state: BridgeState = {
      pendingCommands: new Map(),
      socket: null,
      connected: false,
      requestCounter: 0,
    };

    const sendToClient = (payload: Record<string, unknown>): boolean => {
      const socket = state.socket;
      if (socket === null || socket.destroyed || !state.connected) return false;
      socket.write(`${JSON.stringify(payload)}\n`);
      return true;
    };

    const failPending = () => {
      for (const [reqId, resolve] of state.pendingCommands) {
        state.pendingCommands.delete(reqId);
        resolve(false);
      }
    };

    const sendCommand = (payload: Record<string, unknown>) =>
      Effect.callback<boolean>((resume) => {
        const reqId = `req-${++state.requestCounter}`;
        let settled = false;
        const finish = (accepted: boolean) => {
          if (settled) return;
          settled = true;
          state.pendingCommands.delete(reqId);
          resume(Effect.succeed(accepted));
        };
        state.pendingCommands.set(reqId, finish);
        const sent = sendToClient({ ...payload, reqId, runId });
        if (!sent) finish(false);
        return Effect.sync(() => {
          if (!settled) state.pendingCommands.delete(reqId);
        });
      }).pipe(
        Effect.timeoutOption(COMMAND_TIMEOUT_MS),
        Effect.map((option) => Option.getOrElse(option, () => false)),
      );

    const handleMessage = (line: string) => {
      if (line.length === 0) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        return;
      }
      if (!isRecord(parsed)) return;
      if (parsed["type"] === "ack") {
        const reqId = typeof parsed["reqId"] === "string" ? parsed["reqId"] : undefined;
        if (reqId !== undefined && parsed["runId"] === runId) {
          const resolve = state.pendingCommands.get(reqId);
          if (resolve !== undefined) {
            state.pendingCommands.delete(reqId);
            resolve(parsed["accepted"] === true);
          }
        }
      }
    };

    const server = NodeNet.createServer((socket) => {
      // Single-client bridge: a second connection is rejected so a stale
      // extension cannot hijack the session.
      if (state.socket !== null && !state.socket.destroyed) {
        socket.destroy();
        return;
      }
      state.socket = socket;
      socket.setNoDelay(true);
      // Per-socket framing state: a stale partial handshake or frame from a
      // previous connection can never poison a reconnect. The decoder reassembles
      // multi-byte code points split across TCP chunks before line splitting.
      const decoder = new NodeStringDecoder.StringDecoder("utf8");
      let buffer = "";
      let handshakeDone = false;

      const handleHandshake = (line: string): boolean => {
        let hello: unknown;
        try {
          hello = JSON.parse(line);
        } catch {
          return false;
        }
        const ok =
          isRecord(hello) &&
          hello["type"] === "hello" &&
          hello["protocol"] === PI_BRIDGE_PROTOCOL_VERSION &&
          hello["token"] === token &&
          hello["runId"] === runId;
        if (!ok) return false;
        // Authenticated ready state: advertise readiness only after validation.
        state.connected = true;
        socket.write(
          `${JSON.stringify({ type: "hello_ok", protocol: PI_BRIDGE_PROTOCOL_VERSION, runId })}\n`,
        );
        return true;
      };

      socket.on("data", (chunk) => {
        buffer += decoder.write(chunk);
        let newlineIndex = buffer.indexOf("\n");
        while (newlineIndex >= 0) {
          const line = buffer.slice(0, newlineIndex).replace(/\r$/, "");
          buffer = buffer.slice(newlineIndex + 1);
          if (Buffer.byteLength(line, "utf8") > PI_BRIDGE_CONTROL_MAX_BYTES) {
            socket.destroy();
            return;
          }
          if (!handshakeDone) {
            if (!handleHandshake(line)) {
              socket.destroy();
              return;
            }
            handshakeDone = true;
          } else {
            handleMessage(line);
          }
          newlineIndex = buffer.indexOf("\n");
        }
        // Limit the remaining partial frame, not a coalesced batch of valid frames.
        if (Buffer.byteLength(buffer, "utf8") > PI_BRIDGE_CONTROL_MAX_BYTES) socket.destroy();
      });
      socket.on("close", () => {
        if (state.socket === socket) {
          state.socket = null;
          state.connected = false;
          // Pending commands fail closed: controls no longer reach children.
          failPending();
        }
      });
      socket.on("error", () => {
        socket.destroy();
      });
    });

    // Register cleanup before the asynchronous bind, including failed/interrupted acquisition.
    yield* Scope.addFinalizer(
      ownScope,
      Effect.callback<void>((resume) => {
        state.socket?.destroy();
        failPending();
        server.close(() => resume(Effect.void));
      }),
    );

    const listening = yield* Effect.callback<{ port: number } | undefined>((resume) => {
      server.once("error", () => resume(Effect.succeed(undefined)));
      server.listen({ host: "127.0.0.1", port: 0, backlog: 1 }, () => {
        const address = server.address();
        resume(
          Effect.succeed(
            address !== null && typeof address === "object" ? { port: address.port } : undefined,
          ),
        );
      });
    });

    if (listening === undefined) {
      return yield* new PiBridgeBindError({ cause: "listener failed" });
    }

    return {
      scope: ownScope,
      runId,
      env: {
        [`${PI_BRIDGE_ENV_PREFIX}_PORT`]: String(listening.port),
        [`${PI_BRIDGE_ENV_PREFIX}_TOKEN`]: token,
        [`${PI_BRIDGE_ENV_PREFIX}_RUN_ID`]: runId,
        [`${PI_BRIDGE_ENV_PREFIX}_THREAD`]: input.threadId,
        ...(input.instanceId !== undefined
          ? { [`${PI_BRIDGE_ENV_PREFIX}_INSTANCE`]: input.instanceId }
          : {}),
      },
      cancelChild: (childId) => sendCommand({ type: "cancel", childId }),
      quiesce: () => sendCommand({ type: "quiesce" }),
      connected: Effect.sync(() => state.connected),
    } satisfies PiBridgeListener;
  }).pipe(Effect.onError(() => Scope.close(ownScope, Exit.void)));
});
