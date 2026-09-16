import {
  EventId,
  ProviderDriverKind,
  RuntimeTaskId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { runtimeEventToActivities } from "./ProviderRuntimeIngestion.ts";

const base = {
  provider: ProviderDriverKind.make("codex"),
  createdAt: "2026-08-06T00:00:00.000Z",
  threadId: ThreadId.make("thread-1"),
};

describe("runtimeEventToActivities task progress", () => {
  it("persists usage independently from replaceable activity", () => {
    const taskId = RuntimeTaskId.make("agent-1");
    const usageOnly = {
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-usage"),
      payload: {
        taskId,
        description: "Agent one",
        typedUsage: { totalTokens: 73_700_000 },
      },
    } satisfies ProviderRuntimeEvent;
    const command = {
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-command"),
      payload: {
        taskId,
        description: "Agent one",
        summary: "Running tests",
        lastToolName: "exec_command",
      },
    } satisfies ProviderRuntimeEvent;

    const usageActivities = runtimeEventToActivities(usageOnly);
    const commandActivities = runtimeEventToActivities(command);

    expect(usageActivities.map((activity) => activity.id)).toEqual(["task-usage:thread-1:agent-1"]);
    expect(commandActivities.map((activity) => activity.id)).toEqual([
      "task-progress:thread-1:agent-1",
    ]);
    const usagePayload = usageActivities[0]?.payload as Record<string, unknown> | undefined;
    expect(usagePayload?.typedUsage).toEqual({ totalTokens: 73_700_000 });
    expect(usagePayload?.usageSnapshot).toBe(true);
  });

  it("splits combined progress and usage into their independent snapshots", () => {
    const event = {
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-combined"),
      payload: {
        taskId: RuntimeTaskId.make("agent-2"),
        description: "Agent two",
        summary: "Inspecting the panel",
        typedUsage: { totalTokens: 4_200, toolUses: 7 },
        status: "running",
      },
    } satisfies ProviderRuntimeEvent;

    const activities = runtimeEventToActivities(event);
    const progressPayload = activities[0]?.payload as Record<string, unknown>;
    const usagePayload = activities[1]?.payload as Record<string, unknown>;

    expect(activities.map((activity) => activity.id)).toEqual([
      "task-progress:thread-1:agent-2",
      "task-usage:thread-1:agent-2",
    ]);
    expect(progressPayload.summary).toBe("Inspecting the panel");
    expect(progressPayload.status).toBe("running");
    expect(progressPayload).not.toHaveProperty("typedUsage");
    expect(usagePayload.typedUsage).toEqual({ totalTokens: 4_200, toolUses: 7 });
    expect(usagePayload.usageSnapshot).toBe(true);
    expect(usagePayload).not.toHaveProperty("status");
  });
});
describe("runtimeEventToActivities canonical child generation", () => {
  it("persists runId on every task lifecycle activity payload", () => {
    const taskId = RuntimeTaskId.make("sa-1");
    const runId = "run-gen-42";
    const started = runtimeEventToActivities({
      ...base,
      type: "task.started",
      eventId: EventId.make("evt-started"),
      payload: { taskId, taskType: "subagent", description: "Explore", runId },
    } satisfies ProviderRuntimeEvent);
    const updated = runtimeEventToActivities({
      ...base,
      type: "task.updated",
      eventId: EventId.make("evt-updated"),
      payload: { taskId, taskType: "subagent", status: "running", runId },
    } satisfies ProviderRuntimeEvent);
    const completed = runtimeEventToActivities({
      ...base,
      type: "task.completed",
      eventId: EventId.make("evt-completed"),
      payload: { taskId, taskType: "subagent", status: "completed", runId },
    } satisfies ProviderRuntimeEvent);

    const startedPayload = started[0]?.payload as Record<string, unknown> | undefined;
    const updatedPayload = updated[0]?.payload as Record<string, unknown> | undefined;
    const completedPayload = completed[0]?.payload as Record<string, unknown> | undefined;
    expect(startedPayload?.runId).toBe(runId);
    expect(updatedPayload?.runId).toBe(runId);
    expect(completedPayload?.runId).toBe(runId);
  });

  it("scopes task.progress/task-usage stable ids by generation, and keeps legacy ids when unstamped", () => {
    const taskId = RuntimeTaskId.make("sa-1");
    const first = runtimeEventToActivities({
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-progress-a"),
      payload: {
        taskId,
        description: "Explore",
        summary: "Working",
        typedUsage: { totalTokens: 10 },
        runId: "run-a",
      },
    } satisfies ProviderRuntimeEvent);
    const second = runtimeEventToActivities({
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-progress-b"),
      payload: {
        taskId,
        description: "Explore",
        summary: "Working",
        typedUsage: { totalTokens: 20 },
        runId: "run-b",
      },
    } satisfies ProviderRuntimeEvent);
    const legacy = runtimeEventToActivities({
      ...base,
      type: "task.progress",
      eventId: EventId.make("evt-progress-legacy"),
      payload: {
        taskId,
        description: "Explore",
        summary: "Working",
        typedUsage: { totalTokens: 30 },
      },
    } satisfies ProviderRuntimeEvent);

    expect(first.map((activity) => activity.id)).toEqual([
      "task-progress:thread-1:sa-1:run-a",
      "task-usage:thread-1:sa-1:run-a",
    ]);
    expect(second.map((activity) => activity.id)).toEqual([
      "task-progress:thread-1:sa-1:run-b",
      "task-usage:thread-1:sa-1:run-b",
    ]);
    expect(legacy.map((activity) => activity.id)).toEqual([
      "task-progress:thread-1:sa-1",
      "task-usage:thread-1:sa-1",
    ]);
    const firstPayload = first[0]?.payload as Record<string, unknown> | undefined;
    expect(firstPayload?.runId).toBe("run-a");
  });
});

describe("runtimeEventToActivities tool streaming persistence", () => {
  const accumulatedStdout = [
    "first line of output",
    ...Array.from({ length: 500 }, (_, index) => `Capturing frame ${index}/9028`),
  ].join("\n");
  const streamingData = {
    toolCallId: "tool-call-1",
    kind: "execute",
    command: "blender --render",
    rawOutput: { stdout: accumulatedStdout },
    content: [{ type: "content", content: { type: "text", text: accumulatedStdout } }],
  };

  it("persists tool.updated with the wire projection of data, not the accumulated stream", () => {
    const event = {
      ...base,
      type: "item.updated",
      eventId: EventId.make("evt-tool-streaming-updated"),
      payload: {
        itemType: "command_execution",
        status: "inProgress",
        title: "Render",
        detail: accumulatedStdout,
        data: streamingData,
      },
    } satisfies ProviderRuntimeEvent;

    const activities = runtimeEventToActivities(event);

    expect(activities).toHaveLength(1);
    const payload = activities[0]?.payload as Record<string, unknown>;
    const data = payload.data as Record<string, unknown>;
    expect(payload.status).toBe("inProgress");
    expect(data.toolCallId).toBe("tool-call-1");
    expect(data.command).toBe("blender --render");
    expect(data.rawOutput).toEqual({ content: "first line of output" });
    expect(data.content).toBeUndefined();
    expect(JSON.stringify(data).length).toBeLessThan(1_000);
  });

  it("persists the full terminal payload on tool.completed", () => {
    const event = {
      ...base,
      type: "item.completed",
      eventId: EventId.make("evt-tool-streaming-completed"),
      payload: {
        itemType: "command_execution",
        status: "completed",
        title: "Render",
        data: streamingData,
      },
    } satisfies ProviderRuntimeEvent;

    const activities = runtimeEventToActivities(event);

    expect(activities).toHaveLength(1);
    const payload = activities[0]?.payload as Record<string, unknown>;
    expect(payload.data).toEqual(streamingData);
  });
});
