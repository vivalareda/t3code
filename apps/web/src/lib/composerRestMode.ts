/**
 * Escape closing a popup hands the keyboard back to the composer. The next
 * time the composer takes focus it rests in vim normal mode, ready for
 * another leader sequence, instead of dropping into insert mode as a plain
 * focus does. The hand-off is one-shot: that focus takes it, or it is dropped
 * when focus or a pointer goes anywhere else first.
 */
let armed = false;

export function armComposerNormalMode(): void {
  armed = true;
}

export function dropComposerNormalMode(): void {
  armed = false;
}

export function takeComposerNormalMode(): boolean {
  const value = armed;
  armed = false;
  return value;
}
