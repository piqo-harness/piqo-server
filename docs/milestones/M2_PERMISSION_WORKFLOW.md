# M2 — Permission Workflow

State: `not_started`

## Outcome

Every proposed tool invocation is evaluated before execution or acceptance. An
`allow` decision proceeds, a `deny` decision becomes a durable tool failure, and
an `ask` decision pauses until an authenticated client approves or denies it.

## Current gap

`piqo-core` contains a pure exact-match permission policy, configuration can
declare `read`, `write`, and `bash` settings, and permission semantic events are
projectable. The supervisor does not construct or invoke a policy, and the HTTP
API has no approval or denial routes.

## Required design decisions

- Default decision when an agent or tool has no rule. It should be `deny` for
  executable tools unless an explicit local development profile says otherwise.
- Precedence among global, project, agent, session, and one-time decisions.
- Whether denial is returned to the model as a tool result or terminates a run.
- Lifetime and scope of approvals: once, session, project, or configuration.
- How pending requests behave on cancellation, restart, project deletion, and
  shutdown.
- Which arguments are safe to expose to clients and logs.

## Implementation slices

### 1. Policy model

- Extend pure domain evaluation only as required by concrete policies.
- Compile agent configuration into an immutable policy snapshot for a run.
- Represent the reason and matching rule in an internal decision result without
  exposing secrets.
- Keep path and shell-specific parsing out of generic exact-match rules.

### 2. Durable requests and decisions

- Emit `permission_requested` before any `ask` action can proceed.
- Persist exactly one terminal resolution for each request.
- Project pending permission requests and their associated call IDs.
- Make duplicate identical resolutions idempotent and conflicting resolutions a
  stable conflict.

### 3. HTTP contract

- Add list/get support for pending permission requests as needed by clients.
- Add explicit approve and deny commands.
- Define stable errors for stale, unknown, terminal, or mismatched requests.
- Authenticate all sidecar permission operations.
- Update OpenAPI and `docs/CLIENT_PROTOCOL.md`.

### 4. Supervisor integration

- Evaluate before dispatching every native, MCP, plugin, or externally fulfilled
  tool call.
- Do not infer approval from receipt of a tool result.
- Resume only after a durable allow decision.
- Convert denial into deterministic event and transcript behavior.
- Cancel pending requests when their run becomes terminal.

## Required tests

- Allow, ask/approve, ask/deny, and immediate deny.
- Agent-specific override and safe default behavior.
- Duplicate and conflicting resolutions.
- Restart with a pending request.
- Cancellation and project deletion with a pending request.
- Attempted result submission before approval.
- SSE replay and projection reconstruction.
- Verification that the tool executor is never invoked for ask or deny.

## Acceptance criteria

- No invocation path can bypass permission evaluation.
- Pending decisions are durable and recover correctly after restart.
- Approval is explicit, scoped, auditable, and idempotent.
- Denied tools never execute.
- Configuration, API documentation, and runtime policy use the same semantics.

## Explicitly out of scope

- Detailed filesystem and shell authorization, which belongs to M3.
- Remote multi-user identity and authorization.
- A graphical approval interface.
