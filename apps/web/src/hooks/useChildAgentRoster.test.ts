import {
  type ChildAgentState,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { act, createElement, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { type ChildAgentRoster, useChildAgentRoster } from "./useChildAgentRoster";

type ListResult = {
  _tag: "Success";
  value: { children: ReadonlyArray<ChildAgentState> };
};
const mocks = vi.hoisted(() => ({
  list: vi.fn<() => Promise<ListResult>>(),
  activeSubscriptions: new Set<string>(),
  subscribe: vi.fn(
    (scope: { environmentId: string; input: { threadId: string } }) =>
      `${scope.environmentId}:${scope.input.threadId}`,
  ),
}));

vi.mock("~/state/server", () => ({
  serverEnvironment: {
    listChildAgents: Symbol.for("list-child-agents-command"),
    childAgentSubscribeChanges: mocks.subscribe,
  },
}));
vi.mock("~/state/use-atom-command", () => ({ useAtomCommand: () => mocks.list }));
vi.mock("~/state/query", async () => {
  const { useEffect } = await import("react");
  return {
    useEnvironmentQuery: (scope: string | null) => {
      useEffect(() => {
        if (scope === null) return;
        mocks.activeSubscriptions.add(scope);
        return () => {
          mocks.activeSubscriptions.delete(scope);
        };
      }, [scope]);
      return { data: null };
    },
  };
});

const environmentId = EnvironmentId.make("env-1");
const threadId = ThreadId.make("thread-1");
const child: ChildAgentState = {
  threadId,
  instanceId: ProviderInstanceId.make("pi-personal"),
  runId: "run-1",
  childId: "sa-1",
  title: "Historical review",
  backend: "pi",
  model: null,
  effort: null,
  status: "done",
  summary: "Review complete",
  errorText: null,
  tokens: 100,
  startedAt: null,
  settledAt: null,
};
const result: ListResult = { _tag: "Success", value: { children: [child] } };
let renderer: ReactTestRenderer | undefined;
let roster: ChildAgentRoster;

type ProbeProps = {
  driver: ProviderDriverKind | null;
  environmentId?: EnvironmentId | null;
  threadId?: ThreadId | null;
};
function Probe(props: ProbeProps) {
  const value = useChildAgentRoster(
    props.environmentId === undefined ? environmentId : props.environmentId,
    props.threadId === undefined ? threadId : props.threadId,
    props.driver,
  );
  useLayoutEffect(() => {
    roster = value;
  });
  return null;
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  mocks.list.mockReset();
  mocks.list.mockResolvedValue(result);
  mocks.subscribe.mockClear();
  mocks.activeSubscriptions.clear();
});
afterEach(() => {
  act(() => renderer?.unmount());
  renderer = undefined;
  expect(mocks.activeSubscriptions.size).toBe(0);
  vi.unstubAllGlobals();
});

describe("useChildAgentRoster provider gating", () => {
  it("does no child RPC work for Codex or Claude, including manual refresh", async () => {
    await act(async () => {
      renderer = create(createElement(Probe, { driver: ProviderDriverKind.make("codex") }));
    });
    await act(async () => {
      roster.refresh();
      renderer!.update(createElement(Probe, { driver: ProviderDriverKind.make("claudeAgent") }));
    });
    expect(mocks.subscribe).not.toHaveBeenCalled();
    expect(mocks.list).not.toHaveBeenCalled();
    expect(roster.children).toEqual([]);
    expect(roster.loading).toBe(false);
  });

  it("unsubscribes on provider change, drops the old reply, and reloads Pi history", async () => {
    let resolvePending!: (value: ListResult) => void;
    const pending = new Promise<ListResult>((resolve) => {
      resolvePending = resolve;
    });
    mocks.list.mockReturnValueOnce(pending);
    await act(async () => {
      renderer = create(createElement(Probe, { driver: ProviderDriverKind.make("pi") }));
    });
    expect(mocks.list).toHaveBeenCalledTimes(1);
    expect([...mocks.activeSubscriptions]).toEqual([`${environmentId}:${threadId}`]);
    expect(roster.loading).toBe(true);

    await act(async () => {
      renderer!.update(createElement(Probe, { driver: ProviderDriverKind.make("codex") }));
    });
    expect(mocks.activeSubscriptions.size).toBe(0);
    expect(roster.loading).toBe(false);
    await act(async () => resolvePending(result));
    expect(roster.children).toEqual([]);
    expect(mocks.list).toHaveBeenCalledTimes(1);

    await act(async () => {
      renderer!.update(createElement(Probe, { driver: ProviderDriverKind.make("pi") }));
    });
    expect(mocks.list).toHaveBeenCalledTimes(2);
    expect(roster.children).toEqual([child]);
    expect(roster.loading).toBe(false);
    expect([...mocks.activeSubscriptions]).toEqual([`${environmentId}:${threadId}`]);
  });

  it.each<ProbeProps>([
    { driver: null },
    { driver: ProviderDriverKind.make("pi"), environmentId: null },
    { driver: ProviderDriverKind.make("pi"), threadId: null },
  ])("does not load when the committed scope is incomplete: %o", async (props) => {
    await act(async () => {
      renderer = create(createElement(Probe, props));
    });
    expect(mocks.subscribe).not.toHaveBeenCalled();
    expect(mocks.list).not.toHaveBeenCalled();
    expect(roster.loading).toBe(false);
  });
});
