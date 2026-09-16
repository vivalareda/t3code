import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { CHANGES_CAPACITY, ChildAgentChangeHub, layer } from "./ChildAgentChangeHub.ts";

const quietThread = ThreadId.make("quiet");
const busyThread = ThreadId.make("busy");
const instanceId = ProviderInstanceId.make("pi");

const publishChanged = (hub: ChildAgentChangeHub["Service"], threadId: ThreadId, childId: string) =>
  hub.publishChanged({ threadId, instanceId, runId: "run", childId });

it.layer(layer)("ChildAgentChangeHub", (it) => {
  it.effect("attaches before the initial reset and scopes changed to the thread", () =>
    Effect.gen(function* () {
      const hub = yield* ChildAgentChangeHub;
      const pull = yield* Stream.toPull(hub.subscribe(quietThread));

      // The reset is delivered first, after the subscription is attached.
      const [reset] = yield* pull;
      expect(reset).toMatchObject({ type: "reset", threadId: quietThread });

      // A write for the subscribed thread is delivered with full identity.
      yield* publishChanged(hub, quietThread, "sa-1");
      const [changed] = yield* pull;
      expect(changed).toMatchObject({
        type: "changed",
        threadId: quietThread,
        instanceId,
        runId: "run",
        childId: "sa-1",
      });

      // A write for another thread is filtered out: publishing a busy-thread
      // change followed by a quiet-thread change, the next delivered event is
      // the quiet one, proving the busy one never reached this subscriber.
      yield* publishChanged(hub, busyThread, "sa-busy");
      yield* publishChanged(hub, quietThread, "sa-2");
      const [changed2] = yield* pull;
      expect(changed2).toMatchObject({ type: "changed", childId: "sa-2" });
    }),
  );

  it.effect("recovers a quiet thread's evicted terminal notice after global overflow", () =>
    Effect.gen(function* () {
      const hub = yield* ChildAgentChangeHub;
      const quiet = yield* Stream.toPull(hub.subscribe(quietThread));
      const [reset1] = yield* quiet;
      expect(reset1).toMatchObject({ type: "reset", threadId: quietThread });

      // The quiet thread reaches a terminal state — a notice we must not lose.
      yield* publishChanged(hub, quietThread, "sa-terminal");

      // Flood the single shared sliding buffer from a busy thread without
      // consuming the quiet subscription, evicting the terminal notice above.
      for (let i = 0; i < CHANGES_CAPACITY + 8; i += 1) {
        yield* publishChanged(hub, busyThread, `sa-${i}`);
      }

      // The first element the quiet thread observes after the flood must be a
      // reset: the sequence gap proves the terminal notice was evicted, and the
      // reset makes the client re-read so the terminal state is not lost.
      const [recovered] = yield* quiet;
      expect(recovered).toMatchObject({ type: "reset", threadId: quietThread });
    }),
  );
});
