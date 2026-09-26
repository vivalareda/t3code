import { Vim, type CodeMirrorV } from "@replit/codemirror-vim";

import { parseVimrc, type ParsedVimrc } from "./vimrc";

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
