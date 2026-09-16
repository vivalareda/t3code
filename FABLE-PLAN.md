# Pi provider: fix plan

Working file for the `feat/pi-provider` branch. Not for commit (see AGENTS.md
"Plans and work artifacts"); delete before opening a PR.

State of the branch when this was written: the parent RPC adapter is sound and
matches Pi 0.85.1's CLI flags and RPC protocol. Typecheck and lint are clean.
The child-agent half (bridge, projection, RPCs, change hub, web panel) has been
extended since the first review with live change subscriptions, typed
transcript items, and adapter tests, and two earlier findings are already
fixed on the branch: the extension-UI dialog no longer blocks the event pump,
and the panel and transcript view now follow live growth. What follows is what
is still wrong, in the order it should be fixed.

## Phase 0: make the test runner work again

Nothing below can be verified until this is done. Every test file in this
worktree fails to load, including untouched ones such as
`apps/server/src/persistence/Errors.test.ts`, with either
`Vitest failed to find the current suite` or
`Cannot read properties of undefined (reading 'config')`. Reinstalling with
`vp i` did not help.

1. Compare `pnpm-lock.yaml` against `main`. The branch changes two lines in the
   lockfile; check whether that pulled a second `vitest` or `vite-plus` copy so
   that `vite-plus/test` and `@effect/vitest` resolve to different vitest
   instances. `ls node_modules/.pnpm | grep -E 'vitest|vite-plus'` should show
   one of each.
2. If the lockfile drift is the cause, revert it to `main`'s version and rerun
   `vp i`. If not, run the control test in the main checkout to confirm the
   runner works there, then diff `node_modules/.pnpm` between the two.
3. Acceptance: `vp test run apps/server/src/persistence/Errors.test.ts` passes.
   Then run every new test file and record which ones actually pass:
   `PiRpcConnection.test.ts`, `PiBridge.test.ts`, `PiChildIntegration.test.ts`,
   `PiAdapter.test.ts`, `ChildAgentControl.test.ts`,
   `ProjectionChildTranscripts.test.ts`, `ProviderRuntimeIngestion.test.ts`,
   `packages/client-runtime/src/state/childAgents.test.ts`.

## Phase 1: split the branch into two PRs

AGENTS.md: one concern per PR. The parent adapter is shippable on its own; the
child-agent feature has no producer yet (see Phase 5) and should not block it.

PR A, "feat(server): add Pi provider":

- `apps/server/src/provider/pi/PiRpcConnection.ts` and its test, `fakePiRpc.ts`
- `PiAdapter.ts` with the bridge wiring removed (Phase 1 step 2), and its test
- `PiProvider.ts`, `PiDriver.ts`, `PiTextGeneration.ts`, `builtInDrivers.ts`
- contracts: `PiSettings` in `settings.ts` only
- web: `Icons.tsx`, `providerIconUtils.ts`, `providerDriverMeta.ts`
- mobile: the two hardcoded provider lists (Phase 4)

PR B, "feat: surface Pi subagents in the Agents panel": everything else,
landed only once the extension side exists.

Steps:

1. Create a branch from `main` for PR A and cherry-pick or copy the files above.
2. In `PiAdapter.ts`, remove the bridge listener creation in `startSession`,
   the `bridgeFiber`, `handleBridgeEvent`, the `cancelChild` capability, and
   the quiesce calls in `interruptTurn` and `stopSessionInternal`. Keep a
   single `// Child bridge lands in PR B` marker where the env merge happens
   so the seam is obvious.
3. Acceptance: PR A typechecks and its tests pass with no reference to
   `PiBridge`, `task.transcript`, or `childAgent.*`.

## Phase 2: parent adapter fixes (PR A)

1. **Windows-safe discovery cwd.** `PiProvider.ts` line 266 spawns model
   discovery with `cwd: environment.TMPDIR ?? "/tmp"`. Use `os.tmpdir()` via
   the platform layer already in scope, or the server's `stateDir`.
2. **Prune `interruptedTurnIds`.** In `settleActiveTurn`, delete the settled
   turn id from the set. It currently grows for the life of the session.
3. **Text generation.** `PiTextGeneration.ts` refuses every operation, so Pi
   threads get no auto titles, branch names, or commit messages. Pi has a
   `--tools` allowlist and `--no-session`. Implement title and commit
   generation with `pi --mode rpc --no-session --tools ""` (or `-p` with the
   same flags), one short-lived process per call, using the existing
   `makePiRpcConnectionIn` helper. Keep the "never run a coding session"
   rationale in the module comment. If this is judged out of scope for PR A,
   leave the refusal in but change the user-facing detail to say what to do
   instead, and flip `supportsTextGeneration` in the snapshot so clients
   hide the actions rather than surfacing an error.
4. **Acceptance.** `PiAdapter.test.ts` and `PiRpcConnection.test.ts` pass. One
   integrated pass in a real client against the installed `pi` binary:
   start a thread, send a turn, steer mid-turn, interrupt, resume after a
   server restart. Ask before spinning up the browser.

## Phase 3: child-agent correctness (PR B)

### 3.1 Run id is a hardcoded literal

`resolveChildRunId` in `ProviderRuntimeIngestion.ts` returns `"current"`, and
startup reconciliation queries `"current"`. The bridge mints a real run id in
`PiBridge.ts` and never puts it on runtime events. Consequence: after a restart
a reused `sa-1` upserts over the previous row, and its transcript chunks
collide on `(…, child_id, seq)` and are dropped by `ON CONFLICT DO NOTHING`.

1. Add `runId` to the `task.*` event payloads that carry `taskType:
"subagent"`. The cleanest place is `taskAgentLinkageFields` in
   `packages/contracts/src/providerRuntime.ts`, as an optional
   `TrimmedNonEmptyString`.
2. In `PiAdapter.handleBridgeEvent`, stamp `runId: ctx.bridge.runId` on every
   emitted task event, including `task.transcript`.
3. Replace `resolveChildRunId` with a read of `payload.runId`. Drop the event
   on the floor if it is missing, with a `logWarning`; never fall back to a
   constant.
4. Startup reconciliation in `serverRuntimeStartup.ts`: mark interrupted for
   every `running` row of the thread regardless of run id, since any run that
   existed before boot is dead. Remove the `runId` parameter from
   `markUnfinishedInterrupted` or make it optional.
5. Update `ProjectionChildTranscripts.test.ts` so the "different run is a
   different generation" case is driven through ingestion, not by writing
   rows directly.

### 3.2 Status vocabularies do not line up

The bridge emits `task.updated` with `status: "done" | "error"`. Neither is in
`RuntimeTaskStatus`. It slips through because `emitEvent` casts. The child
projection's `ChildTranscriptStatus` then expects `done`/`error`, so a correct
adapter emitting `completed`/`failed` would fail decode and be swallowed by
`Effect.ignore`.

1. In `PiBridge.normalizeEvent` map the extension's `done` to `completed`,
   `error` to `failed`. The wire vocabulary is `RuntimeTaskStatus`, full stop.
2. In the ingestion child branch, map `RuntimeTaskStatus` to
   `ChildTranscriptStatus` explicitly: `completed → done`, `failed → error`,
   `cancelled → cancelled`, `interrupted → interrupted`, everything else →
   `running`. Delete the `as` cast.
3. Remove `Effect.ignore` from the child upserts and replace it with
   `Effect.catch` that logs at warning level with the event type and child id.
   Silent drops are how 3.1 went unnoticed.
4. Add an ingestion test that feeds a `task.updated` with `status:
"completed"` and asserts the projected row is `done`.

### 3.3 Children stick at "running" after stop, interrupt, or crash

Only startup reconciliation marks children interrupted. After
`stopSessionInternal`, `interruptTurn`, or `handleProcessExit`, the panel keeps
showing a running child with a Cancel button that always fails because the
session is gone.

1. In the adapter, after quiesce in `stopSessionInternal` and in
   `handleProcessExit`, emit a `task.completed` with `status: "stopped"` for
   every child the bridge has seen start and not seen settle. Track that set
   on `PiSessionContext` (`openChildIds`), populated in `handleBridgeEvent`.
2. `interruptTurn` is different: the parent turn stops but Pi's own children
   may already be settling. Send quiesce, wait for the ack, then emit
   `stopped` only for children still open after the ack.
3. Acceptance: a `PiAdapter.test.ts` case where the fake process exits with a
   child open, asserting a `task.completed` `stopped` event for it.

### 3.4 Each child renders twice in the Agents panel

The bridge emits `task.started` with `agentKind: "agent"`, so the existing
subagent fold in `packages/client-runtime/src/state/subagentRuntime.ts` puts
it in the native roster. The new "Provider children" section then lists it
again from `childAgent.list`.

Decide once, then apply everywhere:

Option 1 (recommended): there is one roster. Pi children ride the native
`task.*` fold like every other provider's subagents, and the only thing the
child-agent RPCs add is the transcript read and the cancel control. Delete
the "Provider children" section and `ProviderChildRow` from `AgentsPanel.tsx`.
On a native roster row whose payload carries `runId`, open
`ChildAgentTranscriptView` instead of the native detail. `childAgent.list`
becomes unnecessary; keep `transcript`, `cancel`, `subscribeChanges`.

Option 2: Pi children are a separate surface. Emit `agentKind: "background"`
so the native fold skips them. This loses the shared status pills, the
sidebar liveness dot, and every future roster feature. Not recommended.

Also remove `runHandles: { scriptPath: event.cwd }` from `handleBridgeEvent`.
`scriptPath` means a workflow script, and the panel offers a "show script"
button for it. If cwd is worth showing, add a `cwd` field to the linkage
fields.

### 3.5 Smaller server cleanups

- `ChildAgentControl.ts` still builds its own `ProviderSessionDirectory` layer
  at the bottom of the file. `server.ts` now provides the repository and the
  change hub at the top level; do the same for the directory and drop the
  local `Layer.provide` chain and the trailing imports.
- `052_ProjectionChildTranscripts.ts`: the secondary index on
  `projection_child_transcripts` duplicates the primary key. Remove it.
- `PiBridge.ts`: `Effect.runSync(Queue.offer(...))` from a socket callback
  works for an unbounded queue but reads as a foot-gun. Use `Queue.offerUnsafe`
  if effect-smol exposes it, otherwise leave a one-line comment on why sync
  is safe here.
- Delete `ChildAgentNotFoundError` and `ChildAgentControlUnavailableError`
  from the RPC error unions if the handlers never raise them (they currently
  return `{ cancelled: false }` instead). Either raise them or remove them.

## Phase 4: hit every surface

- **Mobile.** `apps/mobile/src/components/ProviderIcon.tsx` and
  `apps/mobile/src/features/threads/ThreadSettingsSheet.tsx` hardcode the
  provider list and have no `pi` entry. Add it in PR A. Mobile needs no
  child-agent UI in PR B, but confirm it ignores `task.transcript` and the
  new `runId` field without warnings.
- **Settings and command palette.** Confirm the Pi instance appears in
  Settings via `providerDriverMeta.ts` and that the "Early Access" badge
  matches how OpenCode is labelled.
- **Remote and tunnel.** The bridge binds `127.0.0.1` and clients only ever
  talk to T3, so remote browsers are fine. Verify once from a second device
  that the Agents panel and transcript view work over the relay.
- **Docs.** `docs/user/` needs a short Pi section: enable in Settings, binary
  path, optional agent directory, that auth is delegated to `pi auth`, and,
  in PR B, that subagents appear in the Agents panel and can be cancelled
  individually. Nothing in `docs/internals/` unless the bridge protocol is
  deemed a cross-component constraint, in which case one page with the
  handshake and the "T3 never restarts children" rule.

## Phase 5: the extension side (blocks PR B)

The bridge speaks `T3CODE_PI_BRIDGE_*` over loopback TCP, but the installed
subagents extension at `~/.pi/agent/extensions/subagents` has no client for
it. Until one exists, PR B ships two empty tables and unreachable RPCs.

1. Write the client inside the subagents extension: read the env vars, connect,
   send `hello`, then emit `child.started`, `child.status`,
   `child.transcript` with a per-child monotonic `seq`, `child.usage`,
   `child.result`, and answer `cancel`, `quiesce`, and `ping`. Reconnect is
   not needed; a dropped socket means T3 is gone.
2. Decide where the client lives. It cannot live in this repo unless the
   extension is vendored. The realistic options are a PR to the extension
   upstream, or a small T3-owned extension that the Pi adapter installs into
   the profile directory on first use. The second keeps the feature working
   for users who never touch their Pi setup and is the one to pursue.
3. `PiChildIntegration.test.ts` already drives the adapter with a fake
   extension. Keep the fake's envelope in lockstep with the real client, and
   add a comment pointing at the real implementation once it exists.

## Phase 6: verification before either PR

- `vp test run` on every file listed in Phase 0 plus
  `apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.test.ts`.
- Targeted typecheck: `vp run --filter t3 typecheck`,
  `--filter @t3tools/web`, `--filter @t3tools/contracts`,
  `--filter @t3tools/client-runtime`, `--filter @t3tools/mobile`.
- One real-client pass for PR A (Phase 2 step 4) and, for PR B, one pass with
  the real extension: spawn two subagents, open one transcript while it
  streams, cancel the other, restart the server, confirm the interrupted
  states and that the transcript survives.
- Before/after screenshots of the Agents panel for PR B.
- Remove this file.
