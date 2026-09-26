import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { Compartment, EditorState, Prec, StateField, type Range } from "@codemirror/state";
import { Decoration, type DecorationSet, EditorView, keymap } from "@codemirror/view";
import { Vim, getCM, vim, type CodeMirrorV } from "@replit/codemirror-vim";
import {
  useCallback,
  useEffect,
  useImperativeHandle,
  useLayoutEffect,
  useRef,
  useState,
} from "react";

import {
  clampCollapsedComposerCursor,
  collapseExpandedComposerCursor,
  expandCollapsedComposerCursor,
  isCollapsedCursorAdjacentToInlineToken,
} from "~/composer-logic";
import { collectComposerPromptInlineTokens } from "~/composer-editor-mentions";
import { usePrimarySettings } from "~/hooks/useSettings";
import { applyComposerVimrc, installComposerVimClipboard } from "~/lib/composerVim";
import { collectInlineContextIds } from "~/lib/composerContextReferences";
import { cn } from "~/lib/utils";
import { getTimelinePageScrollKey } from "./chat/pageScrollController";
import { importPastedComposerText } from "./composerInlineTokenPaste";
import { didComposerSelectionChangeVisibly } from "./composerSelection";
import type { ComposerPromptEditorProps } from "./ComposerPromptEditorTiptap";

type Snapshot = { value: string; cursor: number; expandedCursor: number; contextIds: string[] };

/**
 * Inline tokens (@mentions, $skills, citations, context references) stay
 * plain text: highlighted, and atomic so the caret and deletes treat each as
 * one unit.
 */
const inlineTokenField = StateField.define<DecorationSet>({
  create: (state) => inlineTokenDecorations(state.doc.toString()),
  update: (decorations, tr) =>
    tr.docChanged ? inlineTokenDecorations(tr.newDoc.toString()) : decorations,
  provide: (field) => [
    EditorView.decorations.from(field),
    EditorView.atomicRanges.of((view) => view.state.field(field)),
  ],
});

const inlineTokenMark = Decoration.mark({ class: "composer-vim-token" });

function inlineTokenDecorations(text: string): DecorationSet {
  const ranges: Range<Decoration>[] = collectComposerPromptInlineTokens(text)
    .filter((token) => token.end > token.start)
    .map((token) => inlineTokenMark.range(token.start, token.end));
  return Decoration.set(ranges, true);
}

function isPlainKey(event: KeyboardEvent): boolean {
  return !event.shiftKey && !event.altKey && !event.metaKey && !event.ctrlKey;
}

function vimOf(view: EditorView): CodeMirrorV["state"]["vim"] | null {
  return (getCM(view) as CodeMirrorV | null)?.state.vim ?? null;
}

function enterInsertMode(view: EditorView): void {
  const cm = getCM(view) as CodeMirrorV | null;
  const state = cm?.state.vim;
  if (!cm || !state || state.insertMode) return;
  if (state.visualMode) Vim.exitVisualMode(cm);
  Vim.handleKey(cm, "i", "api");
}

/** Smallest single change turning `from` into `to`, so undo and marks survive rewrites. */
function minimalChange(from: string, to: string) {
  let start = 0;
  const max = Math.min(from.length, to.length);
  while (start < max && from.charCodeAt(start) === to.charCodeAt(start)) start++;
  let end = 0;
  while (
    end < max - start &&
    from.charCodeAt(from.length - 1 - end) === to.charCodeAt(to.length - 1 - end)
  ) {
    end++;
  }
  return { from: start, to: from.length - end, insert: to.slice(start, to.length - end) };
}

/**
 * The composer editor in vim mode: CodeMirror 6 + codemirror-vim over the
 * plain Markdown prompt. Implements the same handle as the Tiptap editor so
 * ChatComposer stays engine-agnostic.
 */
export function ComposerPromptEditorVim(props: ComposerPromptEditorProps) {
  const {
    value,
    cursor,
    contextRecords,
    importContextFragment,
    disabled,
    placeholder,
    containerClassName,
    className,
    placeholderClassName,
    onChange,
    onVisibleSelectionChange,
    onCommandKeyDown,
    onPageScrollKeyDown,
    onPageScrollKeyUp,
    onPageScrollRelease,
    onPaste,
    editorRef,
  } = props;
  const vimrc = usePrimarySettings((settings) => settings.vimrc);

  const hostRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onVisibleSelectionChangeRef = useRef(onVisibleSelectionChange);
  const onCommandKeyDownRef = useRef(onCommandKeyDown);
  const importFragmentRef = useRef(importContextFragment);
  const latestValueRef = useRef(value);
  const vimrcRef = useRef(vimrc);
  const isApplyingControlledUpdateRef = useRef(false);
  const editableCompartment = useRef(new Compartment()).current;
  const attributesCompartment = useRef(new Compartment()).current;
  const [isEmpty, setIsEmpty] = useState(value.length === 0);

  useEffect(() => {
    onChangeRef.current = onChange;
    onVisibleSelectionChangeRef.current = onVisibleSelectionChange;
    onCommandKeyDownRef.current = onCommandKeyDown;
    importFragmentRef.current = importContextFragment;
  }, [importContextFragment, onChange, onCommandKeyDown, onVisibleSelectionChange]);
  useLayoutEffect(() => {
    latestValueRef.current = value;
  }, [value]);

  const initialCursor = clampCollapsedComposerCursor(value, cursor);
  const snapshotRef = useRef<Snapshot>({
    value,
    cursor: initialCursor,
    expandedCursor: expandCollapsedComposerCursor(value, initialCursor),
    contextIds: collectInlineContextIds(value),
  });
  const selectionRangeRef = useRef({
    start: snapshotRef.current.expandedCursor,
    end: snapshotRef.current.expandedCursor,
  });

  const snapshotFromState = useCallback((state: EditorState): Snapshot => {
    const nextValue = state.doc.toString();
    const expandedCursor = state.selection.main.head;
    return {
      value: nextValue,
      cursor: clampCollapsedComposerCursor(
        nextValue,
        collapseExpandedComposerCursor(nextValue, expandedCursor),
      ),
      expandedCursor,
      contextIds: collectInlineContextIds(nextValue),
    };
  }, []);

  const editorAttributes = useCallback(
    (placeholderText: string, extraClassName: string | undefined) => [
      EditorView.editorAttributes.of({
        class: cn(
          "composer-vim -m-1 max-h-52 min-h-19.5 whitespace-pre-wrap wrap-break-word leading-relaxed text-foreground",
          extraClassName,
        ),
      }),
      EditorView.contentAttributes.of({
        "data-testid": "composer-editor",
        "aria-placeholder": placeholderText,
      }),
    ],
    [],
  );

  // One EditorView for the component's lifetime; props reach it through refs
  // and compartments.
  useLayoutEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    installComposerVimClipboard();

    const handleKeyDown = (event: KeyboardEvent, view: EditorView): boolean => {
      if (event.isComposing || event.keyCode === 229) return false;
      const handler = onCommandKeyDownRef.current;
      const vimState = vimOf(view);
      const insertMode = vimState?.insertMode ?? true;
      const consume = () => {
        event.preventDefault();
        event.stopPropagation();
        return true;
      };

      // Ctrl+J / Ctrl+K walk an open completion menu.
      if (
        event.ctrlKey &&
        !event.metaKey &&
        !event.altKey &&
        !event.shiftKey &&
        (event.key === "j" || event.key === "k")
      ) {
        return handler?.(event.key === "j" ? "ArrowDown" : "ArrowUp", event) ? consume() : false;
      }

      if (event.key === "Enter") {
        // Enter sends (or accepts a menu item) in every mode.
        if (handler?.("Enter", event)) return consume();
        if (event.shiftKey && !event.altKey && !event.metaKey && !event.ctrlKey) {
          if (!insertMode) return consume();
          view.dispatch(view.state.replaceSelection("\n"), { scrollIntoView: true });
          return consume();
        }
        return false;
      }

      if (event.key === "Tab") {
        // Tab only accepts a menu item; it never inserts or moves focus.
        handler?.("Tab", event);
        return consume();
      }

      if (event.key === "Escape" && isPlainKey(event)) {
        if (handler?.("Escape", event)) return consume();
        const idle =
          vimState !== null &&
          !vimState.insertMode &&
          !vimState.visualMode &&
          !vimState.inputState.operator &&
          vimState.inputState.keyBuffer.length === 0;
        if (idle) {
          view.contentDOM.blur();
          return consume();
        }
        return false;
      }

      if ((event.key === "ArrowUp" || event.key === "ArrowDown") && handler?.(event.key, event)) {
        return consume();
      }
      return false;
    };

    const handlePaste = (event: ClipboardEvent, view: EditorView): boolean => {
      if (event.defaultPrevented) return true;
      const clipboardData = event.clipboardData;
      if (!clipboardData || clipboardData.files.length > 0) return false;
      const pastedText = clipboardData.getData("text/plain");
      if (!pastedText) return false;
      event.preventDefault();
      const importFragment = importFragmentRef.current;
      let text = importFragment
        ? importPastedComposerText(clipboardData, importFragment)
        : pastedText;
      // Complete tokens at paste boundaries just as autocomplete does.
      const tokens = collectComposerPromptInlineTokens(`${text}\n`);
      const lastToken = tokens.at(-1);
      if (
        (lastToken?.type === "mention" || lastToken?.type === "skill") &&
        lastToken.end === text.length
      ) {
        text += " ";
      }
      const from = view.state.selection.main.from;
      if (
        (tokens[0]?.type === "mention" || tokens[0]?.type === "skill") &&
        tokens[0].start === 0 &&
        from > 0 &&
        !/\s/.test(view.state.sliceDoc(from - 1, from))
      ) {
        text = ` ${text}`;
      }
      view.dispatch(view.state.replaceSelection(text), { scrollIntoView: true });
      return true;
    };

    const view = new EditorView({
      parent: host,
      state: EditorState.create({
        doc: value,
        selection: { anchor: snapshotRef.current.expandedCursor },
        extensions: [
          Prec.highest(EditorView.domEventHandlers({ keydown: handleKeyDown, paste: handlePaste })),
          vim(),
          history(),
          keymap.of([...defaultKeymap, ...historyKeymap]),
          inlineTokenField,
          editableCompartment.of([
            EditorView.editable.of(!disabled),
            EditorState.readOnly.of(disabled),
          ]),
          attributesCompartment.of(editorAttributes(placeholder, className)),
          EditorView.updateListener.of((update) => {
            if (!update.docChanged && !update.selectionSet) return;
            const next = snapshotFromState(update.state);
            const range = update.state.selection.main;
            const previousRange = selectionRangeRef.current;
            const nextRange = { start: range.from, end: range.to };
            selectionRangeRef.current = nextRange;
            setIsEmpty(next.value.length === 0);
            if (isApplyingControlledUpdateRef.current) return;
            const previous = snapshotRef.current;
            if (
              previous.value === next.value &&
              previous.cursor === next.cursor &&
              previous.expandedCursor === next.expandedCursor
            ) {
              if (didComposerSelectionChangeVisibly(previousRange, nextRange)) {
                onVisibleSelectionChangeRef.current?.();
              }
              return;
            }
            snapshotRef.current = next;
            onChangeRef.current(
              next.value,
              next.cursor,
              next.expandedCursor,
              isCollapsedCursorAdjacentToInlineToken(next.value, next.cursor, "left") ||
                isCollapsedCursorAdjacentToInlineToken(next.value, next.cursor, "right"),
              next.contextIds,
            );
          }),
        ],
      }),
    });
    viewRef.current = view;
    const cm = getCM(view) as CodeMirrorV | null;
    if (cm) {
      applyComposerVimrc(cm, vimrcRef.current);
      enterInsertMode(view);
    }
    return () => {
      viewRef.current = null;
      view.destroy();
    };
    // The view is created once; later prop changes flow through the effects below.
  }, []);

  useEffect(() => {
    vimrcRef.current = vimrc;
    const view = viewRef.current;
    const cm = view ? (getCM(view) as CodeMirrorV | null) : null;
    if (cm) applyComposerVimrc(cm, vimrc);
  }, [vimrc]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: editableCompartment.reconfigure([
        EditorView.editable.of(!disabled),
        EditorState.readOnly.of(disabled),
      ]),
    });
  }, [disabled, editableCompartment]);

  // Layout effect: ChatComposer measures resting geometry in layout effects
  // and clamps the prompt through `className`.
  useLayoutEffect(() => {
    viewRef.current?.dispatch({
      effects: attributesCompartment.reconfigure(editorAttributes(placeholder, className)),
    });
  }, [attributesCompartment, className, editorAttributes, placeholder]);

  // Controlled value/cursor from the store (history recall, chip insertion, send…).
  useLayoutEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const normalizedCursor = clampCollapsedComposerCursor(value, cursor);
    const previous = snapshotRef.current;
    if (previous.value === value && previous.cursor === normalizedCursor) return;
    const expandedCursor = expandCollapsedComposerCursor(value, normalizedCursor);
    snapshotRef.current = {
      value,
      cursor: normalizedCursor,
      expandedCursor,
      contextIds: collectInlineContextIds(value),
    };
    selectionRangeRef.current = { start: expandedCursor, end: expandedCursor };
    setIsEmpty(value.length === 0);
    const focused = view.hasFocus;
    if (previous.value === value && !focused) return;
    const current = view.state.doc.toString();
    isApplyingControlledUpdateRef.current = true;
    try {
      view.dispatch({
        ...(current === value ? {} : { changes: minimalChange(current, value) }),
        selection: { anchor: expandedCursor },
        scrollIntoView: focused,
      });
    } finally {
      isApplyingControlledUpdateRef.current = false;
    }
    // A sent or cleared prompt starts the next one in insert mode.
    if (value.length === 0) enterInsertMode(view);
  }, [cursor, value]);

  const readSnapshot = useCallback(() => {
    const view = viewRef.current;
    if (!view) return snapshotRef.current;
    const next = snapshotFromState(view.state);
    const range = view.state.selection.main;
    selectionRangeRef.current = { start: range.from, end: range.to };
    snapshotRef.current = next;
    return next;
  }, [snapshotFromState]);

  const focusAt = useCallback((nextCursor: number) => {
    const view = viewRef.current;
    if (!view) return;
    view.focus();
    enterInsertMode(view);
    // A newer prompt is waiting to be applied; its rewrite places the caret.
    if (snapshotRef.current.value !== latestValueRef.current) return;
    const snapshot = snapshotRef.current;
    const boundedCursor = clampCollapsedComposerCursor(snapshot.value, nextCursor);
    const expandedCursor = expandCollapsedComposerCursor(snapshot.value, boundedCursor);
    view.dispatch({ selection: { anchor: expandedCursor }, scrollIntoView: true });
  }, []);

  useImperativeHandle(
    editorRef,
    () => ({
      focus: () => focusAt(snapshotRef.current.cursor),
      focusAt,
      focusAtEnd: () =>
        focusAt(
          collapseExpandedComposerCursor(
            snapshotRef.current.value,
            snapshotRef.current.value.length,
          ),
        ),
      readSelectionRange: () => {
        readSnapshot();
        return selectionRangeRef.current;
      },
      // Citations stay plain text here; there is no chip to open a comment on.
      requestCitationComment: () => {},
      readSnapshot,
      isCaretOnVisualEdge: (edge) => {
        const view = viewRef.current;
        if (!view) return false;
        const { main } = view.state.selection;
        if (view.state.doc.length === 0) return true;
        if (!main.empty) return false;
        const caret = view.coordsAtPos(main.head);
        const boundary = view.coordsAtPos(edge === "start" ? 0 : view.state.doc.length);
        if (!caret || !boundary) return false;
        const threshold = (caret.bottom - caret.top) / 2;
        return edge === "start"
          ? caret.top - boundary.top < threshold
          : boundary.bottom - caret.bottom < threshold;
      },
    }),
    [focusAt, readSnapshot],
  );

  return (
    <div
      className={cn(
        "relative flow-root font-(family-name:--font-composer,var(--font-sans)) text-(length:--font-size-prompt,var(--text-sm))",
        containerClassName,
      )}
      onKeyDown={(event) => {
        if (
          event.key === "Control" ||
          event.key === "Meta" ||
          event.key === "Alt" ||
          event.key === "Shift"
        ) {
          onPageScrollRelease?.();
        }
        if (event.key !== "PageUp" && event.key !== "PageDown") return;
        const scroller = viewRef.current?.scrollDOM;
        if (!scroller) return;
        const pageScrollKey = getTimelinePageScrollKey({
          altKey: event.altKey,
          clientHeight: scroller.clientHeight,
          ctrlKey: event.ctrlKey,
          defaultPrevented: event.defaultPrevented,
          isComposing: event.nativeEvent.isComposing,
          key: event.key,
          keyCode: event.keyCode,
          metaKey: event.metaKey,
          scrollHeight: scroller.scrollHeight,
          scrollTop: scroller.scrollTop,
          shiftKey: event.shiftKey,
        });
        if (!pageScrollKey) {
          onPageScrollRelease?.();
          return;
        }
        if (!onPageScrollKeyDown) return;
        event.preventDefault();
        onPageScrollKeyDown(pageScrollKey);
      }}
      onKeyUp={(event) => onPageScrollKeyUp?.(event.key)}
      onBlur={onPageScrollRelease}
      onPasteCapture={onPaste}
    >
      <div ref={hostRef} />
      {isEmpty && contextRecords.size === 0 && placeholder ? (
        <div
          className={cn(
            "pointer-events-none absolute inset-0 leading-relaxed text-placeholder/75",
            placeholderClassName,
          )}
        >
          {placeholder}
        </div>
      ) : null}
    </div>
  );
}
