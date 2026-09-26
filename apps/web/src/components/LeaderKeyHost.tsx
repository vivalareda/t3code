import { useEffect, useEffectEvent, useMemo, useRef, useState } from "react";

import { openCommandPalette } from "../commandPaletteBus";
import { useComposerHandleContext } from "../composerHandleContext";
import { useHandleNewThread } from "../hooks/useHandleNewThread";
import { useClientSettings, useLegacySidebarEnabled } from "../hooks/useSettings";
import { startNewThreadFromContext } from "../lib/chatThreadActions";
import { isComposerVimIdleNormal } from "../lib/composerVim";
import { isEditableFocused } from "../lib/editableFocus";
import { isFloatingLayerOpen } from "../lib/floatingLayer";
import { leaderContinuations, stepLeaderKey, type LeaderCommand } from "../lib/leaderKey";
import { isTerminalFocused } from "../lib/terminalFocus";
import { selectProjectGroupingSettings } from "../logicalProject";
import { buildSidebarProjectSnapshots } from "../sidebarProjectGrouping";
import { useProjects } from "../state/entities";
import { usePrimaryEnvironmentId } from "../state/environments";
import { Kbd, KbdGroup } from "./ui/kbd";

/** How long a sequence rests before the which-key popup lists its continuations. */
const WHICH_KEY_DELAY_MS = 400;

/**
 * Listens for the Space leader on every screen and runs its commands. Sits
 * inside the command palette so it can reach the composer handle; commands
 * that need a conversation do nothing without one.
 */
export function LeaderKeyHost() {
  const composerHandleRef = useComposerHandleContext();
  const { activeDraftThread, activeThread, defaultProjectRef, handleNewThread } =
    useHandleNewThread();
  const legacySidebarEnabled = useLegacySidebarEnabled();
  const projectGroupingSettings = useClientSettings(selectProjectGroupingSettings);
  const projects = useProjects();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const projectGroupCount = useMemo(
    () =>
      buildSidebarProjectSnapshots({
        projects,
        settings: projectGroupingSettings,
        primaryEnvironmentId,
        resolveEnvironmentLabel: () => null,
      }).length,
    [primaryEnvironmentId, projectGroupingSettings, projects],
  );
  const [hintSequence, setHintSequence] = useState<string | null>(null);
  const pendingRef = useRef<string | null>(null);
  const hintTimerRef = useRef<number | null>(null);
  const swallowSpaceKeyUpRef = useRef(false);

  const runCommand = useEffectEvent((command: LeaderCommand) => {
    switch (command) {
      case "palette.open":
        openCommandPalette();
        return;
      case "project.add":
        openCommandPalette({ open: "add-project" });
        return;
      case "modelPicker.open":
        composerHandleRef?.current?.openModelPicker();
        return;
      case "thread.new":
        // Mirrors the sidebar's + button and chat.new: a real choice of
        // project goes through the palette picker, otherwise create now.
        if (!legacySidebarEnabled && projectGroupCount > 1) {
          openCommandPalette({ open: "new-thread-in" });
          return;
        }
        void startNewThreadFromContext({
          activeDraftThread,
          activeThread: activeThread ?? undefined,
          defaultProjectRef,
          handleNewThread,
        });
        return;
    }
  });

  useEffect(() => {
    const clearHintTimer = () => {
      if (hintTimerRef.current !== null) window.clearTimeout(hintTimerRef.current);
      hintTimerRef.current = null;
    };
    const settle = (sequence: string | null) => {
      pendingRef.current = sequence;
      clearHintTimer();
      setHintSequence(null);
      if (sequence === null) return;
      hintTimerRef.current = window.setTimeout(() => setHintSequence(sequence), WHICH_KEY_DELAY_MS);
    };
    const cancel = () => {
      if (pendingRef.current !== null) settle(null);
    };
    const consume = (event: KeyboardEvent) => {
      event.preventDefault();
      event.stopPropagation();
      // Buttons and role="button" rows click on the Space keyup.
      if (event.key === " ") swallowSpaceKeyUpRef.current = true;
    };

    const onKeyDown = (event: KeyboardEvent) => {
      const step = stepLeaderKey(pendingRef.current, event, () => ({
        editableFocus: isEditableFocused(event.target) || isTerminalFocused(),
        composerNormalMode: isComposerVimIdleNormal(event.target),
        floatingLayerOpen: isFloatingLayerOpen(),
      }));
      switch (step.kind) {
        case "pass":
          return;
        case "pending":
          consume(event);
          if (step.sequence !== pendingRef.current) settle(step.sequence);
          return;
        case "command":
          consume(event);
          settle(null);
          runCommand(step.command);
          return;
        case "cancel":
          if (step.consume) consume(event);
          settle(null);
          return;
      }
    };
    const onKeyUp = (event: KeyboardEvent) => {
      if (event.key !== " " || !swallowSpaceKeyUpRef.current) return;
      swallowSpaceKeyUpRef.current = false;
      event.preventDefault();
      event.stopPropagation();
    };
    const onBlur = () => {
      swallowSpaceKeyUpRef.current = false;
      cancel();
    };

    // Capture phase: the leader wins over focused buttons, sidebar rows and
    // the composer's own key handling.
    window.addEventListener("keydown", onKeyDown, true);
    window.addEventListener("keyup", onKeyUp, true);
    window.addEventListener("pointerdown", cancel, true);
    window.addEventListener("blur", onBlur);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
      window.removeEventListener("keyup", onKeyUp, true);
      window.removeEventListener("pointerdown", cancel, true);
      window.removeEventListener("blur", onBlur);
      clearHintTimer();
    };
  }, []);

  if (hintSequence === null) return null;
  return <LeaderKeyHints sequence={hintSequence} />;
}

/** The which-key popup: the keys typed so far and what may follow. Static, no motion. */
function LeaderKeyHints({ sequence }: { sequence: string }) {
  const hints = leaderContinuations(sequence);
  return (
    <div
      role="status"
      aria-label="Leader key"
      data-leader-key-hints=""
      className="dropdown-glass pointer-events-none fixed right-4 bottom-4 z-[130] min-w-44 rounded-lg py-1 text-sm text-popover-foreground shadow-md"
    >
      <div className="px-2.5 py-1">
        <KbdGroup>
          <Kbd>Space</Kbd>
          {Array.from(sequence, (typed, index) => (
            <Kbd key={index}>{typed}</Kbd>
          ))}
        </KbdGroup>
      </div>
      <ul>
        {hints.map((hint) => (
          <li key={hint.key} className="flex items-center gap-2.5 px-2.5 py-1">
            <Kbd>{hint.key}</Kbd>
            <span className={hint.prefix ? "text-muted-foreground" : undefined}>
              {hint.prefix ? `${hint.label}…` : hint.label}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
