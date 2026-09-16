/**
 * ChildAgentTranscriptView — read-only transcript for one provider-owned
 * child agent (Pi subagents today), with an individual Cancel control.
 *
 * Reads are scoped by the owning environment and the full child identity
 * `{threadId, instanceId, runId, childId}`. The viewer fetches bounded pages
 * after a sequence cursor, follows live growth from `childAgent.subscribeChanges`
 * notices (no polling), and exposes an explicit retry / unsuccessful-cancel
 * outcome. The child's live status is derived from the shared roster; this
 * component only stores the identity and transcript state.
 */
import type { ChildAgentState, EnvironmentId } from "@t3tools/contracts";
import {
  childAgentIdentityKey,
  createChildCancelController,
  createChildTranscriptController,
  deriveTranscriptRows,
  type ChildAgentIdentity,
  type TranscriptRow,
} from "@t3tools/client-runtime/state/childAgents";
import * as Cause from "effect/Cause";
import { ChevronLeft, Loader2, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

import { ExpandableText } from "~/components/settings/ExpandableText";
import { Button } from "~/components/ui/button";
import { cn } from "~/lib/utils";
import { useEnvironmentQuery } from "~/state/query";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";

const STATUS_LABEL: Record<ChildAgentState["status"], string> = {
  running: "Running",
  done: "Completed",
  error: "Failed",
  cancelled: "Stopped",
  interrupted: "Stopped",
};

function TranscriptRowView({ row }: { row: TranscriptRow }) {
  if (row.kind === "user") {
    return (
      <div className="rounded-md border border-border/60 bg-muted/40 px-2.5 py-1.5 text-xs">
        <ExpandableText
          text={row.text}
          collapsedClassName="line-clamp-6"
          expandLabel="Show full message"
          className="text-foreground/90"
        />
      </div>
    );
  }
  if (row.kind === "thinking") {
    return (
      <div className="border-l-2 border-border/60 pl-2.5 text-xs italic text-muted-foreground/80">
        <ExpandableText
          text={row.text}
          collapsedClassName="line-clamp-4"
          expandLabel="Show full thinking"
        />
      </div>
    );
  }
  if (row.kind === "tool") {
    return (
      <div className="font-mono text-[.7rem] text-muted-foreground">
        {row.label ? <span className="text-foreground/70">▸ {row.label}</span> : null}
        {row.text ? (
          <span className={cn("ml-2", row.isError && "text-destructive-foreground")}>
            {row.text}
          </span>
        ) : null}
      </div>
    );
  }
  return (
    <div className="whitespace-pre-wrap break-words text-xs leading-relaxed text-foreground/90">
      {row.text}
    </div>
  );
}

function toRpcIdentity(identity: ChildAgentIdentity) {
  return {
    threadId: identity.threadId,
    instanceId: identity.instanceId,
    runId: identity.runId,
    childId: identity.childId,
  };
}

export function ChildAgentTranscriptView({
  environmentId,
  identity,
  child,
  onBack,
}: {
  environmentId: EnvironmentId;
  identity: ChildAgentIdentity;
  child: ChildAgentState | null;
  onBack: () => void;
}) {
  const readTranscript = useAtomCommand(serverEnvironment.readChildAgentTranscript, {
    label: "child agent transcript read",
    reportFailure: false,
  });
  const cancelChild = useAtomCommand(serverEnvironment.cancelChildAgent, {
    label: "child agent cancel",
  });

  const [transcriptController] = useState(() =>
    createChildTranscriptController({
      readPage: async (request) => {
        const result = await readTranscript({
          environmentId: request.environmentId,
          input: {
            ...toRpcIdentity(request.identity),
            ...(request.afterSeq === null ? {} : { afterSeq: request.afterSeq }),
            limit: request.limit,
          },
        });
        if (result._tag === "Failure") {
          const cause = Cause.squash(result.cause);
          throw cause instanceof Error ? cause : new Error("The transcript request failed.");
        }
        return {
          chunks: result.value.chunks,
          hasMore: result.value.hasMore,
          lastSeq: result.value.lastSeq,
        };
      },
    }),
  );

  const [cancelController] = useState(() =>
    createChildCancelController({
      cancel: async (request) => {
        const result = await cancelChild({
          environmentId: request.environmentId,
          input: toRpcIdentity(request.identity),
        });
        if (result._tag === "Failure") {
          const cause = Cause.squash(result.cause);
          throw cause instanceof Error ? cause : new Error("Cancellation failed.");
        }
        return { cancelled: result.value.cancelled };
      },
    }),
  );

  const transcript = useSyncExternalStore(
    transcriptController.subscribe,
    transcriptController.getState,
  );
  const cancelState = useSyncExternalStore(cancelController.subscribe, cancelController.getState);

  // Reset synchronously on identity/environment change, then load the first page.
  useEffect(() => {
    transcriptController.reset({ environmentId, identity });
    cancelController.reset({ environmentId, identity });
    void transcriptController.loadInitial();
  }, [transcriptController, cancelController, environmentId, identity]);

  const changeQuery = useEnvironmentQuery(
    serverEnvironment.childAgentSubscribeChanges({
      environmentId,
      input: { threadId: identity.threadId },
    }),
  );
  const notice = changeQuery.data;

  // Follow live growth without polling. A `reset` (subscribe/reconnect) and a
  // `changed` for this FULL identity (thread + instance + run + child) both
  // catch up from the existing cursor — a reconnect never clears/refetches all,
  // and a reused child id in another run/thread must not re-target this view.
  useEffect(() => {
    if (notice === null) {
      return;
    }
    if (notice.type === "reset") {
      void transcriptController.catchUp();
    } else if (childAgentIdentityKey(notice) === childAgentIdentityKey(identity)) {
      void transcriptController.catchUp();
    }
  }, [transcriptController, identity, notice]);

  const live = child?.status === "running";
  const prevLiveRef = useRef(live);
  useEffect(() => {
    const wasLive = prevLiveRef.current;
    prevLiveRef.current = live;
    // Terminal transition: one final bounded catch-up so the settled tail and
    // any trailing chunks land without reopening the panel.
    if (wasLive && !live) {
      void transcriptController.catchUp();
    }
  }, [transcriptController, live]);

  // Keep the fold memoized: it is O(history) and must not re-run for every
  // unrelated roster/cancel re-render.
  const rows = useMemo(() => deriveTranscriptRows(transcript.chunks), [transcript.chunks]);
  const statusLabel = child === null ? "Unknown" : STATUS_LABEL[child.status];
  const title = child?.title ?? identity.childId;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <header className="flex items-center gap-2 border-b border-border/60 px-2 py-1.5">
        <Button
          variant="ghost"
          size="icon"
          className="size-6"
          onClick={onBack}
          aria-label="Back to agents"
        >
          <ChevronLeft className="size-4" />
        </Button>
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium">{title}</div>
          <div className="truncate font-mono text-[.65rem] text-muted-foreground">
            {[child?.backend, child?.model, statusLabel].filter(Boolean).join(" · ") ||
              identity.childId}
          </div>
        </div>
        {live ? (
          <Button
            variant="ghost"
            size="sm"
            className="h-6 gap-1 px-2 text-xs"
            onClick={() => void cancelController.cancel()}
            disabled={cancelState.status === "cancelling"}
          >
            {cancelState.status === "cancelling" ? (
              <Loader2 aria-hidden className="size-3 animate-none" />
            ) : (
              <X aria-hidden className="size-3" />
            )}
            {cancelState.status === "cancelling" ? "Cancelling…" : "Cancel"}
          </Button>
        ) : null}
      </header>

      {cancelState.status === "not-cancelled" || cancelState.status === "failed" ? (
        <div className="flex items-center gap-2 border-b border-border/60 bg-destructive/10 px-2 py-1 text-xs text-destructive-foreground">
          <span className="min-w-0 flex-1 truncate">
            {cancelState.message ?? "Cancellation did not succeed."}
          </span>
          <Button
            size="sm"
            variant="ghost"
            className="h-6 px-2 text-xs"
            onClick={() => void cancelController.cancel()}
          >
            Retry
          </Button>
        </div>
      ) : null}

      <div className="min-h-0 flex-1 overflow-y-auto p-2">
        {rows.length === 0 ? (
          transcript.error !== null ? (
            <div className="flex flex-col items-center gap-2 p-4 text-center text-xs text-destructive-foreground">
              <span>{transcript.error}</span>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => void transcriptController.loadInitial()}
              >
                Retry
              </Button>
            </div>
          ) : transcript.loading ? (
            <div className="p-4 text-center text-xs text-muted-foreground">Loading transcript…</div>
          ) : (
            <div className="p-4 text-center text-xs text-muted-foreground">No transcript yet.</div>
          )
        ) : (
          <>
            <div className="flex flex-col gap-2">
              {rows.map((row) => (
                <TranscriptRowView key={row.key} row={row} />
              ))}
            </div>
            {transcript.error !== null ? (
              <div className="mt-2 flex items-center justify-between gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-2.5 py-1.5 text-xs text-destructive-foreground">
                <span className="min-w-0 flex-1 truncate">{transcript.error}</span>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={() => void transcriptController.catchUp()}
                >
                  Retry
                </Button>
              </div>
            ) : null}
          </>
        )}
        {transcript.hasMore && transcript.error === null ? (
          <div className="pt-2 text-center">
            <Button
              size="sm"
              variant="ghost"
              className="h-6 px-2 text-xs"
              onClick={() => void transcriptController.loadMore()}
              disabled={transcript.loading}
            >
              {transcript.loading ? "Loading…" : "Load more transcript"}
            </Button>
          </div>
        ) : null}
        {child?.errorText ? (
          <div className="mt-2 rounded-md border border-destructive/40 bg-destructive/10 px-2.5 py-1.5 text-xs text-destructive-foreground">
            {child.errorText}
          </div>
        ) : null}
      </div>
    </div>
  );
}
