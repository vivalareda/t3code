/**
 * Fake `pi --mode rpc` process used by PiRpcConnection tests.
 *
 * Speaks strict LF JSONL on stdio: commands in, `type: "response"` lines out
 * (echoing the caller's `id`), plus events. Behaviour knobs arrive through
 * `FAKE_PI_RPC_*` environment variables so one stub serves many scenarios.
 */

const send = (payload: unknown) => {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
};

const respond = (
  id: unknown,
  command: string,
  success: boolean,
  data?: unknown,
  error?: string,
) => {
  const payload: Record<string, unknown> = { id, type: "response", command, success };
  if (data !== undefined) payload.data = data;
  if (error !== undefined) payload.error = error;
  send(payload);
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let index = buffer.indexOf("\n");
  while (index >= 0) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    handleLine(line);
    index = buffer.indexOf("\n");
  }
});
process.stdin.on("end", () => {
  if (process.env.FAKE_PI_RPC_EXIT_ON_STDIN_END === "1") process.exit(0);
});

function handleLine(line: string) {
  if (line.length === 0) return;
  let command: Record<string, unknown>;
  try {
    command = JSON.parse(line);
  } catch {
    send({ type: "response", command: "parse", success: false, error: "Failed to parse command" });
    return;
  }
  const id = typeof command.id === "string" ? command.id : undefined;
  const type = String(command.type ?? "");
  switch (type) {
    case "get_state":
      // Emit an event with U+2028/U+2029 inside JSON strings to prove the
      // client frames on LF only, then answer.
      send({ type: "message_update", note: "line\u2028sep\u2029inside" });
      respond(id, type, true, {
        sessionFile: "/tmp/fake-session.jsonl",
        sessionId: "abc123",
        model: { id: "test-model", provider: "test" },
        isStreaming: false,
      });
      break;
    case "prompt": {
      const message = String(command.message ?? "");
      if (message.includes("REJECT_ME")) {
        respond(id, type, false, undefined, "prompt rejected by fixture");
        return;
      }
      // Emit a streaming event before acknowledging acceptance.
      send({ type: "agent_start" });
      send({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "hello " },
      });
      respond(id, type, true);
      break;
    }
    case "echo_unicode":
      respond(id, type, true, { text: "héllo — line\u2028sep   inside" });
      break;
    case "fragmented":
      // Write one response in three stdout chunks with the JSON split at
      // arbitrary byte positions to exercise the client's chunk reassembly.
      const payload = `${JSON.stringify({ id, type: "response", command: type, success: true, data: { ok: true } })}\n`;
      const mid = Math.floor(payload.length / 3);
      process.stdout.write(payload.slice(0, mid));
      setImmediate(() => {
        process.stdout.write(payload.slice(mid, mid * 2));
        setImmediate(() => process.stdout.write(payload.slice(mid * 2)));
      });
      break;
    case "exit_with_output": {
      respond(id, type, true);
      for (let seq = 0; seq < 300; seq += 1) {
        send({ type: "message_update", seq, delta: "héllo 🌍 ".repeat(256) });
      }
      // End naturally so every byte is handed to the pipes before exit.
      process.stdout.end(JSON.stringify({ type: "agent_settled" }));
      process.stderr.end("final diagnostic");
      process.stdin.destroy();
      break;
    }
    case "exit":
      respond(id, type, true);
      setImmediate(() => process.exit(Number(command.code ?? 0)));
      break;
    case "hang":
      // Never respond; used to test request timeouts against a live process.
      break;
    default:
      respond(id, type, true, { echoed: command });
  }
}

if (process.env.FAKE_PI_RPC_SPAN_UNICODE_ON_START === "1") {
  send({ type: "message_update", note: "startup line" });
}
