import { useEffect, useEffectEvent, useMemo, useState } from "react";

import { openCommandPalette } from "../commandPaletteBus";
import { useComposerHandleContext } from "../composerHandleContext";
import { useHandleNewThread } from "../hooks/useHandleNewThread";
import { useClientSettings, useLegacySidebarEnabled } from "../hooks/useSettings";
import { threadJumpIndexFromCommand } from "../keybindings";
import { useLeaderKeyStore } from "../leaderKeyStore";
import { startNewThreadFromContext } from "../lib/chatThreadActions";
import { armComposerNormalMode, dropComposerNormalMode } from "../lib/composerRestMode";
import { COMPOSER_VIM_EDITOR_SELECTOR, isComposerVimIdleNormal } from "../lib/composerVim";
import { isEditableFocused } from "../lib/editableFocus";
import { isFloatingLayerOpen } from "../lib/floatingLayer";
import { leaderContinuations, stepLeaderKey, type LeaderCommand } from "../lib/leaderKey";
import { isTerminalFocused } from "../lib/terminalFocus";
import { selectProjectGroupingSettings } from "../logicalProject";
import { buildSidebarProjectSnapshots } from "../sidebarProjectGrouping";
import { useProjects } from "../state/entities";
import { usePrimaryEnvironmentId } from "../state/environments";
import { requestThreadJump } from "../threadJumpBus";
import { Kbd, KbdGroup } from "./ui/kbd";

/** How long a sequence rests before the which-key popup lists its continuations. */
const WHICH_KEY_DELAY_MS = 400;

function isPlainEscape(event: KeyboardEvent): boolean {
  return (
    event.key === "Escape" && !event.metaKey && !event.ctrlKey && !event.altKey && !event.shiftKey
  );
}

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

  const runCommand = useEffectEvent((command: LeaderCommand) => {
    const jumpIndex = threadJumpIndexFromCommand(command);
    if (jumpIndex !== null) {
      requestThreadJump(jumpIndex);
      return;
    }
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
      default:
        return;
    }
  });

  useEffect(() => {
    const { setPending } = useLeaderKeyStore.getState();
    const readPending = () => useLeaderKeyStore.getState().pending;
    let hintTimer: number | null = null;
    let swallowSpaceKeyUp = false;

    const clearHintTimer = () => {
      if (hintTimer !== null) window.clearTimeout(hintTimer);
      hintTimer = null;
    };
    const settle = (sequence: string | null) => {
      setPending(sequence);
      clearHintTimer();
      setHintSequence(null);
      if (sequence === null) return;
      hintTimer = window.setTimeout(() => setHintSequence(sequence), WHICH_KEY_DELAY_MS);
    };
    const cancel = () => {
      if (readPending() !== null) settle(null);
    };
    const consume = (event: KeyboardEvent) => {
      event.preventDefault();
      event.stopPropagation();
      // Buttons and role="button" rows click on the Space keyup.
      if (event.key === " ") swallowSpaceKeyUp = true;
    };

    const onKeyDown = (event: KeyboardEvent) => {
      // Escape closing a popup: the composer rests in normal mode when focus
      // comes back to it, so another sequence can follow straight away.
      if (isPlainEscape(event) && isFloatingLayerOpen()) armComposerNormalMode();
      const pending = readPending();
      const step = stepLeaderKey(pending, event, () => ({
        editableFocus: isEditableFocused(event.target) || isTerminalFocused(),
        composerNormalMode: isComposerVimIdleNormal(event.target),
        floatingLayerOpen: isFloatingLayerOpen(),
      }));
      switch (step.kind) {
        case "pass":
          return;
        case "pending":
          consume(event);
          if (step.sequence !== pending) settle(step.sequence);
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
      if (event.key !== " " || !swallowSpaceKeyUp) return;
      swallowSpaceKeyUp = false;
      event.preventDefault();
      event.stopPropagation();
    };
    const onFocusIn = (event: FocusEvent) => {
      // Focus landing anywhere but the composer ends the Escape hand-off.
      const target = event.target;
      if (!(target instanceof Element && target.closest(COMPOSER_VIM_EDITOR_SELECTOR))) {
        dropComposerNormalMode();
      }
    };
    const onPointerDown = () => {
      dropComposerNormalMode();
      cancel();
    };
    const onBlur = () => {
      swallowSpaceKeyUp = false;
      cancel();
    };

    // Capture phase: the leader wins over focused buttons, sidebar rows and
    // the composer's own key handling.
    window.addEventListener("keydown", onKeyDown, true);
    window.addEventListener("keyup", onKeyUp, true);
    window.addEventListener("focusin", onFocusIn, true);
    window.addEventListener("pointerdown", onPointerDown, true);
    window.addEventListener("blur", onBlur);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
      window.removeEventListener("keyup", onKeyUp, true);
      window.removeEventListener("focusin", onFocusIn, true);
      window.removeEventListener("pointerdown", onPointerDown, true);
      window.removeEventListener("blur", onBlur);
      clearHintTimer();
      setPending(null);
      dropComposerNormalMode();
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
