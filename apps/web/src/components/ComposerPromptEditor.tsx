import { ComposerPromptEditorVim } from "./ComposerPromptEditorVim";
import type { ComposerPromptEditorProps } from "./ComposerPromptEditorTiptap";

export type {
  ComposerCitationCommentRequest,
  ComposerPromptEditorHandle,
  ComposerPromptEditorProps,
} from "./ComposerPromptEditorTiptap";

/**
 * The composer editor. This fork always runs the vim editor (CodeMirror +
 * codemirror-vim over plain Markdown); the Tiptap editor stays in the tree
 * untouched so upstream merges stay cheap.
 */
export function ComposerPromptEditor(props: ComposerPromptEditorProps) {
  return <ComposerPromptEditorVim {...props} />;
}
