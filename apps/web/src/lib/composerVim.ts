import { EditorView } from "@codemirror/view";
import { getCM, Vim, type CodeMirrorV } from "@replit/codemirror-vim";

import { parseVimrc, type ParsedVimrc } from "./vimrc";

type VimState = NonNullable<CodeMirrorV["state"]["vim"]>;

/** The composer's CodeMirror root; set through `EditorView.editorAttributes`. */
export const COMPOSER_VIM_EDITOR_SELECTOR = ".composer-vim";

/** Normal mode with nothing typed yet: no operator, count, or partial key. */
export function isVimIdleNormal(vim: VimState | null | undefined): boolean {
  return (
    vim != null &&
    !vim.insertMode &&
    !vim.visualMode &&
    !vim.inputState.operator &&
    vim.inputState.keyBuffer.length === 0
  );
}

/**
 * Whether `target` is the composer's vim editor resting in normal mode, where
 * Space is the app leader rather than a motion.
 */
export function isComposerVimIdleNormal(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const editor = target.closest<HTMLElement>(COMPOSER_VIM_EDITOR_SELECTOR);
  const view = editor ? EditorView.findFromDOM(editor) : null;
  const cm = view ? (getCM(view) as CodeMirrorV | null) : null;
  return isVimIdleNormal(cm?.state.vim);
}

export function isKnownVimOption(name: string): boolean {
  return name.length > 0 && !(Vim.getOption(name) instanceof Error);
}

export function parseComposerVimrc(source: string): ParsedVimrc {
  return parseVimrc(source, isKnownVimOption);
}

/**
 * Mappings are global in codemirror-vim, so each apply starts from a clean
 * keymap and replays the whole vimrc.
 */
export function applyComposerVimrc(cm: CodeMirrorV, source: string): void {
  Vim.mapclear();
  for (const command of parseComposerVimrc(source).commands) {
    try {
      Vim.handleEx(cm, command);
    } catch {
      // Settings already lists what the parser rejected; a runtime failure
      // leaves the rest of the vimrc applied.
    }
  }
}

let clipboardInstalled = false;

/**
 * `set clipboard=unnamedplus`: the unnamed register mirrors the OS clipboard.
 * Yanks and deletes write it; `p`/`P` read it, keeping linewise-ness when the
 * clipboard still holds the last yank.
 */
export function installComposerVimClipboard(): void {
  if (clipboardInstalled) return;
  clipboardInstalled = true;

  const controller = Vim.getRegisterController();
  const pushText = controller.pushText.bind(controller);
  controller.pushText = (registerName, operator, text, linewise, blockwise) => {
    pushText(registerName, operator, text, linewise, blockwise);
    if (!registerName || registerName === '"') {
      void navigator.clipboard?.writeText(text).catch(() => {});
    }
  };

  type PasteRegister = { linewise?: boolean; blockwise?: boolean; toString(): string };
  type PasteActions = {
    continuePaste: (
      cm: CodeMirrorV,
      args: unknown,
      vim: unknown,
      text: string,
      register: PasteRegister,
    ) => void;
  };
  Vim.defineAction("paste", function (this: PasteActions, cm, args, vim) {
    const name = args.registerName;
    const register = Vim.getRegisterController().getRegister(name);
    const pasteRegister = () => this.continuePaste(cm, args, vim, register.toString(), register);
    if ((name && name !== '"' && name !== "+") || !navigator.clipboard) {
      pasteRegister();
      return;
    }
    navigator.clipboard.readText().then((text) => {
      if (text === register.toString()) {
        pasteRegister();
        return;
      }
      this.continuePaste(cm, args, vim, text, {
        linewise: text.endsWith("\n"),
        blockwise: false,
        toString: () => text,
      });
    }, pasteRegister);
  });
}
