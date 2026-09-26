import { describe, expect, it } from "vite-plus/test";

import {
  leaderContinuations,
  stepLeaderKey,
  type LeaderKeyContext,
  type LeaderKeyEvent,
} from "./leaderKey";

const free: LeaderKeyContext = {
  editableFocus: false,
  composerNormalMode: false,
  floatingLayerOpen: false,
};

function key(value: string, overrides: Partial<LeaderKeyEvent> = {}): LeaderKeyEvent {
  return {
    key: value,
    metaKey: false,
    ctrlKey: false,
    altKey: false,
    shiftKey: false,
    ...overrides,
  };
}

const space = key(" ");

describe("stepLeaderKey", () => {
  it("starts a sequence on a plain Space when nothing editable has focus", () => {
    expect(stepLeaderKey(null, space, () => free)).toEqual({ kind: "pending", sequence: "" });
  });

  it("starts a sequence from the composer's vim normal mode only", () => {
    const composer = { ...free, editableFocus: true };
    expect(stepLeaderKey(null, space, () => composer)).toEqual({ kind: "pass" });
    expect(stepLeaderKey(null, space, () => ({ ...composer, composerNormalMode: true }))).toEqual({
      kind: "pending",
      sequence: "",
    });
  });

  it("leaves Space alone in popups, with modifiers, on repeat, and for other keys", () => {
    expect(stepLeaderKey(null, space, () => ({ ...free, floatingLayerOpen: true }))).toEqual({
      kind: "pass",
    });
    expect(stepLeaderKey(null, key(" ", { shiftKey: true }), () => free)).toEqual({ kind: "pass" });
    expect(stepLeaderKey(null, key(" ", { metaKey: true }), () => free)).toEqual({ kind: "pass" });
    expect(stepLeaderKey(null, key(" ", { repeat: true }), () => free)).toEqual({ kind: "pass" });
    expect(stepLeaderKey(null, key("Enter"), () => free)).toEqual({ kind: "pass" });
  });

  it("does not read the context for keys that are not a plain Space", () => {
    const readContext = () => {
      throw new Error("context read");
    };
    expect(stepLeaderKey(null, key("f"), readContext)).toEqual({ kind: "pass" });
    expect(stepLeaderKey("", key("f"), readContext)).toEqual({ kind: "pending", sequence: "f" });
  });

  it("walks prefixes to commands", () => {
    expect(stepLeaderKey("", key("f"), () => free)).toEqual({ kind: "pending", sequence: "f" });
    expect(stepLeaderKey("f", key("f"), () => free)).toEqual({
      kind: "command",
      command: "palette.open",
    });
    expect(stepLeaderKey("", key("m"), () => free)).toEqual({
      kind: "command",
      command: "modelPicker.open",
    });
    expect(stepLeaderKey("", key("n"), () => free)).toEqual({
      kind: "command",
      command: "thread.new",
    });
    expect(stepLeaderKey("", key("a"), () => free)).toEqual({
      kind: "command",
      command: "project.add",
    });
  });

  it("cancels on Esc or an unmapped key and swallows it", () => {
    expect(stepLeaderKey("", key("Escape"), () => free)).toEqual({ kind: "cancel", consume: true });
    expect(stepLeaderKey("f", key("x"), () => free)).toEqual({ kind: "cancel", consume: true });
    expect(stepLeaderKey("", key("F", { shiftKey: true }), () => free)).toEqual({
      kind: "cancel",
      consume: true,
    });
    expect(stepLeaderKey("", key("ArrowDown"), () => free)).toEqual({
      kind: "cancel",
      consume: true,
    });
  });

  it("cancels on a chord but lets the chord reach the app", () => {
    expect(stepLeaderKey("", key("k", { metaKey: true }), () => free)).toEqual({
      kind: "cancel",
      consume: false,
    });
  });

  it("ignores modifier presses and held keys while a sequence is pending", () => {
    expect(stepLeaderKey("f", key("Shift"), () => free)).toEqual({ kind: "pass" });
    expect(stepLeaderKey("f", key(" ", { repeat: true }), () => free)).toEqual({
      kind: "pending",
      sequence: "f",
    });
  });
});

describe("leaderContinuations", () => {
  it("lists the next keys after Space, marking prefixes", () => {
    expect(leaderContinuations("")).toEqual([
      { key: "f", label: "Find", prefix: true },
      { key: "m", label: "Choose model", prefix: false },
      { key: "n", label: "New thread", prefix: false },
      { key: "a", label: "Add project", prefix: false },
    ]);
  });

  it("lists the keys that complete a prefix", () => {
    expect(leaderContinuations("f")).toEqual([{ key: "f", label: "Find thread", prefix: false }]);
    expect(leaderContinuations("ff")).toEqual([]);
  });
});
