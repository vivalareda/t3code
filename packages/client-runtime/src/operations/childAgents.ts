/**
 * Child-agent RPC helpers — typed wrappers over the `childAgent.*` commands.
 *
 * Reads and controls are scoped server-side by the full child identity
 * `{threadId, instanceId, runId, childId}`; the caller supplies the owning
 * environment as the outer command destination, never a bridge address.
 *
 * @module operations/childAgents
 */
import type {
  ChildAgentCancelInput,
  ChildAgentListInput,
  ChildAgentTranscriptInput,
  ThreadId,
} from "@t3tools/contracts";
import { WS_METHODS } from "@t3tools/contracts";

import {
  type EnvironmentRpcFailure,
  type EnvironmentRpcStreamFailure,
  type EnvironmentRpcStreamValue,
  type EnvironmentRpcSuccess,
  request,
  subscribe,
} from "../rpc/client.ts";

export const listChildAgents = (input: ChildAgentListInput) =>
  request(WS_METHODS.childAgentList, input);

export const readChildAgentTranscript = (input: ChildAgentTranscriptInput) =>
  request(WS_METHODS.childAgentTranscript, input);

export const cancelChildAgent = (input: ChildAgentCancelInput) =>
  request(WS_METHODS.childAgentCancel, input);

/** Streaming change notices: `{type: reset}` or `{type: changed, ...identity}`. */
export const subscribeChildAgentChanges = (input: { readonly threadId: ThreadId }) =>
  subscribe(WS_METHODS.childAgentSubscribeChanges, input);

export type ChildAgentListResponse = EnvironmentRpcSuccess<"childAgent.list">;
export type ChildAgentTranscriptResponse = EnvironmentRpcSuccess<"childAgent.transcript">;
export type ChildAgentCancelResponse = EnvironmentRpcSuccess<"childAgent.cancel">;
export type ChildAgentSubscribeChangesValue =
  EnvironmentRpcStreamValue<"childAgent.subscribeChanges">;
export type ChildAgentSubscribeChangesFailure =
  EnvironmentRpcStreamFailure<"childAgent.subscribeChanges">;
export type ChildAgentRequestFailure = EnvironmentRpcFailure<
  "childAgent.list" | "childAgent.transcript" | "childAgent.cancel"
>;
