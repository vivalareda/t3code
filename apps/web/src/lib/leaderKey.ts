/**
 * The Space leader: app-level key sequences in the spirit of a vim
 * `mapleader`. Space starts a sequence when nothing editable has the
 * keyboard, or when the composer's vim editor is resting in normal mode. The
 * sequence then waits, with no timeout, for the next key: a mapped key runs
 * its command, a prefix waits for more, and Esc or any other key cancels.
 */

import {
  THREAD_JUMP_KEYBINDING_COMMANDS,
  type ThreadJumpKeybindingCommand,
} from "@t3tools/contracts";

export type LeaderCommand =
  | "palette.open"
  | "modelPicker.open"
  | "thread.new"
  | "project.add"
  | ThreadJumpKeybindingCommand;

export interface LeaderBinding {
  /** Keys pressed after Space, in order. */
  readonly keys: string;
  readonly label: string;
  readonly command: LeaderCommand;
  /** Which-key row shared by a run of keys, such as `1–9`; those keys collapse into one entry. */
  readonly hint?: string;
}

export const LEADER_BINDINGS: ReadonlyArray<LeaderBinding> = [
  { keys: "ff", label: "Find thread", command: "palette.open" },
  { keys: "m", label: "Choose model", command: "modelPicker.open" },
  { keys: "n", label: "New thread", command: "thread.new" },
  { keys: "a", label: "Add project", command: "project.add" },
  // Space 1–9 opens the sidebar's first nine threads, like ⌘1–9.
  ...THREAD_JUMP_KEYBINDING_COMMANDS.map((command, index) => ({
    keys: String(index + 1),
    label: "Jump to thread",
    command,
    hint: "1–9",
  })),
];

/** Labels for sequences that only lead to further keys. */
const LEADER_PREFIX_LABELS: Readonly<Record<string, string>> = { f: "Find" };

export interface LeaderHint {
  readonly key: string;
  readonly label: string;
  /** The key leads to more keys rather than running a command. */
  readonly prefix: boolean;
}

/** The keys that may follow `sequence`, in table order, for the which-key popup. */
export function leaderContinuations(sequence: string): ReadonlyArray<LeaderHint> {
  const hints: LeaderHint[] = [];
  for (const binding of LEADER_BINDINGS) {
    if (!binding.keys.startsWith(sequence) || binding.keys.length === sequence.length) continue;
    const nextKey = binding.keys[sequence.length]!;
    const prefix = binding.keys.length > sequence.length + 1;
    const key = !prefix && binding.hint ? binding.hint : nextKey;
    if (hints.some((hint) => hint.key === key)) continue;
    hints.push({
      key,
      label: prefix ? (LEADER_PREFIX_LABELS[sequence + nextKey] ?? nextKey) : binding.label,
      prefix,
    });
  }
  return hints;
}

export interface LeaderKeyEvent {
  readonly key: string;
  readonly metaKey: boolean;
  readonly ctrlKey: boolean;
  readonly altKey: boolean;
  readonly shiftKey: boolean;
  readonly repeat?: boolean;
  readonly isComposing?: boolean;
}

export interface LeaderKeyContext {
  /** A text field, editor, terminal or the keybinding recorder owns the keyboard. */
  readonly editableFocus: boolean;
  /** The composer's vim editor is resting in normal mode, where Space is free. */
  readonly composerNormalMode: boolean;
  /** A dialog, menu or other popup is open. */
  readonly floatingLayerOpen: boolean;
}

export type LeaderKeyStep =
  /** Not the leader's key. The event proceeds untouched. */
  | { readonly kind: "pass" }
  /** Consumed; the sequence so far waits for the next key. */
  | { readonly kind: "pending"; readonly sequence: string }
  /** Consumed; the sequence is complete. */
  | { readonly kind: "command"; readonly command: LeaderCommand }
  /** The sequence is abandoned. `consume` is false when the key should still reach the app. */
  | { readonly kind: "cancel"; readonly consume: boolean };

const MODIFIER_KEYS = new Set(["Shift", "Control", "Alt", "AltGraph", "Meta", "OS", "CapsLock"]);

/**
 * Advances the leader state machine by one keydown. `pending` is null with no
 * sequence in flight, otherwise the keys typed since Space. `readContext` is
 * only consulted for a plain Space, since it walks the DOM.
 */
export function stepLeaderKey(
  pending: string | null,
  event: LeaderKeyEvent,
  readContext: () => LeaderKeyContext,
): LeaderKeyStep {
  if (pending === null) {
    if (event.key !== " " || event.repeat || event.isComposing) return { kind: "pass" };
    if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return { kind: "pass" };
    const context = readContext();
    if (context.floatingLayerOpen) return { kind: "pass" };
    if (context.editableFocus && !context.composerNormalMode) return { kind: "pass" };
    return { kind: "pending", sequence: "" };
  }
  // A held-down key repeats; the sequence stays where it is.
  if (event.repeat) return { kind: "pending", sequence: pending };
  if (MODIFIER_KEYS.has(event.key)) return { kind: "pass" };
  // A chord such as ⌘K belongs to the app's own shortcuts.
  if (event.metaKey || event.ctrlKey || event.altKey) return { kind: "cancel", consume: false };
  if (event.key === "Escape" || event.key.length !== 1 || event.isComposing) {
    return { kind: "cancel", consume: true };
  }
  const sequence = pending + event.key;
  const exact = LEADER_BINDINGS.find((binding) => binding.keys === sequence);
  if (exact) return { kind: "command", command: exact.command };
  if (LEADER_BINDINGS.some((binding) => binding.keys.startsWith(sequence))) {
    return { kind: "pending", sequence };
  }
  return { kind: "cancel", consume: true };
}
