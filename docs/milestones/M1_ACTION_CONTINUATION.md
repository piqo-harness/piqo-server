# M1 — Action Continuation Protocol

State: `not_started`

## Outcome

A run paused by one or more provider tool calls can receive validated tool
results, persist them, reconstruct the provider transcript, and continue until
it completes, fails, is cancelled, or requests another action.

This milestone provides continuation mechanics only. It must not execute native
or MCP tools and must not treat client-submitted results as implicitly trusted
permission decisions.

## Current gap

The provider transport parses tool calls and the supervisor records
`tool_call_emitted` followed by `run_requires_action`. The assistant message and
session are then interrupted. No API accepts a result and no worker resumes the
run. Although a `tool_result` semantic event exists, it is not wired into this
path.

## Required design decisions

- Whether results are submitted one call at a time or as an atomic batch.
- Whether multiple calls emitted in one provider response may complete in any
  order.
- The exact lifecycle of the assistant message that emitted a tool call.
- How provider-specific call identifiers are preserved in Chat Completions and
  Responses transcripts.
- Idempotency semantics for duplicate client submissions.
- Whether a continuation is the same run with multiple turns or a child run.
  Prefer the same run unless event-log invariants require otherwise.
- Maximum tool/model turns before deterministic failure.

Record these decisions in `docs/CLIENT_PROTOCOL.md` before merging an externally
visible implementation.

## Implementation slices

### 1. Domain lifecycle

- Define valid transitions into and out of `requires_action`.
- Project outstanding calls, submitted results, and continuation readiness.
- Reject results for unknown, completed, cancelled, or mismatched calls.
- Make duplicate identical submissions idempotent and conflicting duplicates a
  stable conflict.
- Ensure replay produces the same continuation state after restart.

Primary crate: `piqo-core`.

### 2. Storage and events

- Persist tool results transactionally with any run-state transition.
- Add a migration only if existing event data cannot represent the required
  idempotency or lookup constraints.
- Preserve per-session monotonic event ordering under concurrent submissions.

Primary crate: `piqo-server` storage edge.

### 3. HTTP contract

- Add a versioned endpoint for submitting a result to an outstanding call.
- Return stable errors for unknown calls, wrong runs, duplicate conflicts,
  invalid result bodies, terminal runs, and shutdown.
- Extend OpenAPI and the normative client protocol.
- Define whether successful submission returns the updated run or `202`.

Primary crate: `piqo-server` HTTP edge.

### 4. Provider transcript reconstruction

- Render assistant tool calls and tool results in the correct protocol-specific
  shapes.
- Keep provider-owned raw request fields untouched.
- Preserve a caller-supplied `messages` or `input` body without overwriting it;
  document how continuation works when the caller owns the transcript.
- Continue the same execution snapshot across the action boundary or explicitly
  specify when the latest configuration is resolved.

Primary crates: `piqo-provider`, `piqo-server` supervisor.

### 5. Worker resumption

- Resume only when every required call has a valid result.
- Prevent two workers from continuing the same run.
- Support cancellation while waiting and while continuing.
- Recover a ready-to-continue run after server restart.
- Enforce a configurable or hard safety limit on model/tool turns.

Primary crate: `piqo-server` supervisor.

## Required tests

- One tool call, one result, then final assistant text.
- Multiple tool calls submitted in different orders.
- Duplicate identical and conflicting result submissions.
- Unknown or mismatched call ID.
- Cancellation while waiting for a result.
- Restart before and after the final required result arrives.
- SSE replay across the pause and continuation boundary.
- Chat Completions and Responses request-shape tests.
- Malformed tool arguments and arbitrary JSON results.
- Turn-limit exhaustion.

## Acceptance criteria

- A mock provider can drive a complete model → tool call → result → model flow.
- The flow survives process restart without losing or duplicating an action.
- Every observable state change is represented by durable semantic events.
- A repeated client request cannot execute or continue an action twice.
- OpenAPI, README, and the client protocol agree with the implementation.
- Text-only conversations remain backward compatible.

## Explicitly out of scope

- Native tool execution.
- Permission approval UI or endpoints.
- MCP subprocesses.
- Subagents.
- Context compaction.
