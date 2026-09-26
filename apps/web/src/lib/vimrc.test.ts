import { describe, expect, it } from "vite-plus/test";

import { parseVimrc } from "./vimrc";

const known = new Set(["textwidth", "tw", "pcre"]);
const parse = (source: string) => parseVimrc(source, (name) => known.has(name));

describe("parseVimrc", () => {
  it("turns map-family lines into ex commands and normalizes short and x-mode names", () => {
    expect(
      parse(`inoremap jk <Esc>\nnn H ^\n:xnoremap < <gv\nnmap <silent> gh :noh<CR>`).commands,
    ).toEqual(["inoremap jk <Esc>", "nnoremap H ^", "vnoremap < <gv", "nmap gh :noh<CR>"]);
  });

  it("expands <leader> with the mapleader in effect when the map is defined", () => {
    const { commands, warnings } = parse(
      [
        "nnoremap <leader>a A",
        'let mapleader = " "',
        "nnoremap <Leader>w :w<CR>",
        "let g:mapleader = ','",
        "nnoremap <leader>q :q<CR>",
      ].join("\n"),
    );
    expect(commands).toEqual(["nnoremap \\a A", "nnoremap <Space>w :w<CR>", "nnoremap ,q :q<CR>"]);
    expect(warnings).toEqual([]);
  });

  it("keeps known set options, including no-prefixed booleans, and flags unknown ones", () => {
    const { commands, warnings } = parse("set tw=72 nopcre number\n");
    expect(commands).toEqual(["set tw=72", "set nopcre"]);
    expect(warnings).toEqual([
      { line: 1, text: "set tw=72 nopcre number", reason: "Unknown option `number`" },
    ]);
  });

  it("skips comments and blanks, and warns on lines it cannot apply", () => {
    const { commands, warnings } = parse(
      [
        '" comment',
        "",
        "syntax on",
        "nnoremap <expr> j v:count ? 'j' : 'gj'",
        "nmap <leader>f <Plug>(fzf)",
        "let g:loaded = 1",
        "nnoremap x",
      ].join("\n"),
    );
    expect(commands).toEqual([]);
    expect(warnings.map((warning) => warning.line)).toEqual([3, 4, 5, 6, 7]);
  });
});
