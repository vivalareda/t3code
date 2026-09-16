/**
 * ChildAgentChangeHub — bounded in-process change notices for the Agents
 * surface. Writers publish a small `changed` notice after a durable child
 * write (persistence-before-notify); subscribers receive a `reset` notice on
 * attach followed by `changed` notices scoped to their thread. No transcript
 * chunks ride this stream; clients re-read pages after a change.
 *
 * Overflow recovery: the underlying pubsub is a single bounded sliding hub of
 * fixed capacity. A burst of changes from busy threads can evict a quiet
 * thread's terminal `changed` before its subscriber reads it, after which the
 * viewer would keep showing a stale "running" child forever. Each published
 * change is stamped with a monotonic global sequence; a subscriber that
 * observes a sequence gap (some earlier published change was evicted) emits an
 * extra `reset` for its own thread *before* filtering by thread, so the client
 * re-reads and cannot miss the evicted write. Publishing never blocks
 * ingestion (sliding drop) and memory stays bounded by the capacity.
 *
 * The subscription is attached to the pubsub *before* the `reset` notice is
 * emitted, so a write landing between the reset and the first read is still
 * delivered — the client re-reads on reset and then applies the deltas.
 *
 * @module provider/Services/ChildAgentChangeHub
 */
import { ChildAgentChangeEvent, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

/** Bounded window of outstanding notices. Sliding keeps the newest changes. */
export const CHANGES_CAPACITY = 1024;

/** A `changed` payload plus the global sequence assigned at publish time. */
interface StampedChange {
  readonly seq: number;
  readonly threadId: ThreadId;
  readonly instanceId: ProviderInstanceId;
  readonly runId: string;
  readonly childId: string;
}

export class ChildAgentChangeHub extends Context.Service<
  ChildAgentChangeHub,
  {
    readonly publishChanged: (input: {
      readonly threadId: ThreadId;
      readonly instanceId: ProviderInstanceId;
      readonly runId: string;
      readonly childId: string;
    }) => Effect.Effect<void>;
    readonly subscribe: (threadId: ThreadId) => Stream.Stream<ChildAgentChangeEvent>;
  }
>()("t3/provider/Services/ChildAgentChangeHub") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const pubsub = yield* Effect.acquireRelease(
    PubSub.sliding<StampedChange>(CHANGES_CAPACITY),
    (ps) => PubSub.shutdown(ps),
  );
  const nextSeq = yield* Ref.make(0);

  const publishChanged: ChildAgentChangeHub["Service"]["publishChanged"] = (input) =>
    Effect.gen(function* () {
      // Sequence is assigned after the durable write already committed upstream
      // (persistence-before-notify). If two publishers interleave between
      // `Ref.modify` and `PubSub.publish` the pubsub order can differ from
      // sequence order; that only produces an extra reset, never a lost change,
      // because a reset makes the client re-read state that was durably written
      // before either sequence was assigned.
      const seq = yield* Ref.modify(nextSeq, (n) => [n + 1, n + 1] as const);
      yield* PubSub.publish(pubsub, {
        seq,
        threadId: input.threadId,
        instanceId: input.instanceId,
        runId: input.runId,
        childId: input.childId,
      });
    });

  const subscribe: ChildAgentChangeHub["Service"]["subscribe"] = (threadId) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // Attach before reading the baseline so no published change is ever
        // missed: events with seq <= baseline are covered by the reset, events
        // with seq > baseline are applied as deltas.
        const subscription = yield* PubSub.subscribe(pubsub);
        const baseline = yield* Ref.get(nextSeq);
        return Stream.concat(
          Stream.make({ type: "reset", threadId }),
          Stream.fromSubscription(subscription).pipe(
            Stream.mapAccum(
              () => baseline,
              (
                lastSeen: number,
                event: StampedChange,
              ): readonly [number, ReadonlyArray<ChildAgentChangeEvent>] => {
                // seq <= lastSeen: published before our baseline read and already
                // covered by the reset we emitted above.
                if (event.seq <= lastSeen) {
                  return [lastSeen, []];
                }
                // A gap means an earlier published change was evicted by the
                // sliding buffer. Emit a reset for the requested thread before
                // filtering so the client re-reads and recovers any evicted
                // terminal notice, regardless of which thread it belonged to.
                const gapReset: ReadonlyArray<ChildAgentChangeEvent> =
                  event.seq > lastSeen + 1 ? [{ type: "reset", threadId }] : [];
                const changed: ReadonlyArray<ChildAgentChangeEvent> =
                  event.threadId === threadId
                    ? [
                        {
                          type: "changed",
                          threadId: event.threadId,
                          instanceId: event.instanceId,
                          runId: event.runId,
                          childId: event.childId,
                        },
                      ]
                    : [];
                return [event.seq, [...gapReset, ...changed]];
              },
            ),
          ),
        );
      }),
    );

  return ChildAgentChangeHub.of({ publishChanged, subscribe });
});

export const layer = Layer.effect(ChildAgentChangeHub, make);
