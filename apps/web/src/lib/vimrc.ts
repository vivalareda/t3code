/**
 * Parses the vimrc subset the composer's codemirror-vim mode can apply:
 * map-family commands (with `<leader>` expanded), `let mapleader`, and `set`
 * for options codemirror-vim defines. Everything else becomes a warning so
 * Settings can show which lines were ignored.
 */

export interface VimrcWarning {
  readonly line: number;
  readonly text: string;
  readonly reason: string;
}

export interface ParsedVimrc {
  /** Ex commands to run through `Vim.handleEx`, in order. */
  readonly commands: ReadonlyArray<string>;
  readonly warnings: ReadonlyArray<VimrcWarning>;
}

const MAP_COMMANDS: Record<string, string> = {
  map: "map",
  nmap: "nmap",
  nm: "nmap",
  vmap: "vmap",
  vm: "vmap",
  xmap: "vmap",
  xm: "vmap",
  imap: "imap",
  im: "imap",
  omap: "omap",
  om: "omap",
  noremap: "noremap",
  no: "noremap",
  nnoremap: "nnoremap",
  nn: "nnoremap",
  vnoremap: "vnoremap",
  vn: "vnoremap",
  xnoremap: "vnoremap",
  xn: "vnoremap",
  inoremap: "inoremap",
  ino: "inoremap",
  onoremap: "onoremap",
  ono: "onoremap",
};

const CLEAR_COMMANDS = new Set([
  "unmap",
  "mapclear",
  "nmapclear",
  "vmapclear",
  "imapclear",
  "omapclear",
]);

const SET_COMMANDS = new Set(["set", "se", "setlocal", "setl", "setglobal", "setg"]);

/** Map flags that change nothing in a single-buffer composer. */
const IGNORED_MAP_FLAGS = new Set(["<silent>", "<nowait>", "<unique>", "<buffer>", "<script>"]);

export function parseVimrc(source: string, isKnownOption: (name: string) => boolean): ParsedVimrc {
  const commands: string[] = [];
  const warnings: VimrcWarning[] = [];
  let leader = "\\";

  source.split(/\r?\n/).forEach((rawLine, index) => {
    const line = index + 1;
    const text = rawLine.trim().replace(/^:+/, "");
    if (text.length === 0 || text.startsWith('"')) return;
    const warn = (reason: string) => warnings.push({ line, text: rawLine.trim(), reason });

    const [command = "", ...rest] = text.split(/\s+/);

    if (command === "let") {
      const match = /^let\s+(?:g:)?mapleader\s*=\s*(["'])(.*)\1\s*$/.exec(text);
      if (!match) return warn("Only `let mapleader` is supported");
      const value = match[2]!;
      if (value === " " || /^\\<space>$/i.test(value)) leader = "<Space>";
      else if (value.length === 1) leader = value;
      else return warn("Leader must be a single key");
      return;
    }

    const mapCommand = MAP_COMMANDS[command];
    if (mapCommand) {
      let args = rest;
      while (args[0] && args[0].startsWith("<") && args[0].endsWith(">")) {
        const flag = args[0].toLowerCase();
        if (flag === "<expr>") return warn("Expression mappings are not supported");
        if (!IGNORED_MAP_FLAGS.has(flag)) break;
        args = args.slice(1);
      }
      const [lhs, ...rhsParts] = args;
      if (!lhs || rhsParts.length === 0) return warn("Mapping needs a left and right side");
      const rhs = rhsParts.join(" ");
      if (/<plug>/i.test(rhs)) return warn("`<Plug>` mappings need plugins");
      const expand = (keys: string) => keys.replace(/<leader>/gi, leader);
      commands.push(`${mapCommand} ${expand(lhs)} ${expand(rhs)}`);
      return;
    }

    if (CLEAR_COMMANDS.has(command)) {
      commands.push(text.replace(/<leader>/gi, leader));
      return;
    }

    if (SET_COMMANDS.has(command)) {
      if (rest.length === 0) return warn("`set` needs an option");
      for (const arg of rest) {
        const name = arg.replace(/[!?&]$/, "").split(/[+\-^]?=/)[0]!;
        const bare = name.replace(/^(no|inv)/, "");
        if (isKnownOption(name) || isKnownOption(bare)) {
          commands.push(`${command} ${arg}`);
        } else {
          warnings.push({ line, text: rawLine.trim(), reason: `Unknown option \`${name}\`` });
        }
      }
      return;
    }

    warn(`\`${command}\` is not supported`);
  });

  return { commands, warnings };
}
