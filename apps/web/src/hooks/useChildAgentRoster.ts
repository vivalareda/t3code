import type { ChildAgentState, EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  type ChildAgentRosterListResult,
  type ChildAgentRosterScope,
  createChildAgentRosterController,
} from "@t3tools/client-runtime/state/childAgents";
import { isAtomCommandInterrupted } from "@t3tools/client-runtime/state/runtime";
import * as Cause from "effect/Cause";
import { useCallback, useEffect, useState, useSyncExternalStore } from "react";

import { serverEnvironment } from "~/state/server";
import { useEnvironmentQuery } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";

const EMPTY_CHILDREN: ReadonlyArray<ChildAgentState> = [];

/**
 * Durable provider-child roster for one thread, shared by the Agents panel and
 * the ChatView activity indicator so the two never disagree.
 *
 * The list is scoped by `{threadId}` (the owning environment is the outer
 * command destination). `childAgent.subscribeChanges` notices — a `reset` on
 * subscribe/reconnect and a `changed` after each durable write — trigger a
 * serialized, coalesced refresh, never a poll or a fan-out.
 *
 * The read lives in a small controller so it is testable without rendering:
 * rows reset synchronously on scope change, stale responses are dropped by a
 * read generation, retryable failures keep the previously-loaded rows, and an
 * intentional interruption is not surfaced as a user error.
 */
export interface ChildAgentRoster {
  readonly children: ReadonlyArray<ChildAgentState>;
  readonly error: string | null;
  readonly loading: boolean;
  readonly refresh: () => void;
}

export function useChildAgentRoster(
  environmentId: EnvironmentId | null,
  threadId: ThreadId | null,
): ChildAgentRoster {
  const listChildAgents = useAtomCommand(serverEnvironment.listChildAgents, {
    label: "child agent list",
    reportFailure: false,
  });
  const canLoad = environmentId !== null && threadId !== null;
  const scope = canLoad ? `${environmentId}:${threadId}` : null;

  const [controller] = useState(() =>
    createChildAgentRosterController({
      list: async (target: ChildAgentRosterScope): Promise<ChildAgentRosterListResult> => {
        const result = await listChildAgents({
          environmentId: target.environmentId,
          input: { threadId: target.threadId },
        });
        if (result._tag === "Success") {
          return { _tag: "Success", children: result.value.children };
        }
        if (isAtomCommandInterrupted(result)) {
          return { _tag: "Interrupted" };
        }
        const cause = Cause.squash(result.cause);
        const message =
          cause instanceof Error && cause.message.trim().length > 0
            ? cause.message
            : "Could not load provider children.";
        return { _tag: "Failed", message };
      },
    }),
  );

  const state = useSyncExternalStore(controller.subscribe, controller.getState);

  // Attach the subscription atom before the first list read: a reconnect
  // `reset` therefore triggers catch-up rather than racing the initial fetch.
  const changeQuery = useEnvironmentQuery(
    canLoad
      ? serverEnvironment.childAgentSubscribeChanges({ environmentId, input: { threadId } })
      : null,
  );
  const notice = changeQuery.data;

  // Reset on scope change/loss, then load. Reset bumps the read generation
  // synchronously, so a previous scope's in-flight read is dropped and its rows
  // leave the panel in the same commit. The cleanup releases any in-flight read
  // on unmount.
  useEffect(() => {
    controller.reset(canLoad ? { environmentId, threadId } : null);
    if (canLoad) {
      void controller.refresh();
    }
    return () => controller.reset(null);
  }, [controller, canLoad, environmentId, threadId]);

  // Serialized coalesced catch-up on change/reset notices.
  useEffect(() => {
    if (notice === null) {
      return;
    }
    void controller.refresh();
  }, [controller, notice]);

  // Derive against the scope that owns the loaded data so a thread/environment
  // change empties the roster in the same commit instead of after an effect.
  const current = state.scope === scope;
  const refresh = useCallback(() => {
    void controller.refresh();
  }, [controller]);

  return {
    children: current ? state.children : EMPTY_CHILDREN,
    error: current ? state.error : null,
    loading: current ? state.loading : canLoad,
    refresh,
  };
}
