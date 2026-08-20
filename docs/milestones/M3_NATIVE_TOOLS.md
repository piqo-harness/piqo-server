# M3 — Native Tools

State: `not_started`

## Outcome

Piqo executes permission-gated `read`, `write`, `edit`, and `bash` tools within
an explicit project workspace, returns bounded structured results, and supports
cancellation and timeouts.

## Security boundary

This milestone crosses the remote-shell boundary. Tool correctness is secondary
to containment and explicit authorization. No native operation may start before
the permission pipeline from M2 returns `allow`.

## Required design decisions

- Whether sessions without a project may use native tools. Prefer no.
- Workspace and working-directory semantics for forked sessions.
- Symlink policy and canonicalization timing.
- Maximum read/write/output sizes and process duration.
- Supported `edit` preconditions and conflict behavior.
- Shell invocation model and supported platforms.
- Environment allowlist and provider credential isolation.
- Whether dangerous command structures require argument-aware permission rules.

## Implementation slices

### 1. Common execution contract

- Define typed tool arguments and structured results in `piqo-tools`.
- Define stable tool error categories without leaking sensitive content.
- Pass an explicit workspace root, cancellation token, timeout, and limits.
- Never rely on process-global current directory.
- Bound stdout, stderr, file content, and serialized event sizes.

### 2. Filesystem containment

- Canonicalize project roots.
- Reject traversal outside the root.
- Specify and test symlink behavior, including time-of-check/time-of-use risks.
- Use atomic replacement for writes where supported.
- Preserve existing user files on failed operations.

### 3. `read`

- Read regular files with explicit byte/line ranges.
- Detect binary or invalid UTF-8 content predictably.
- Return truncation metadata when limits apply.

### 4. `write` and `edit`

- Require clear create/overwrite semantics for `write`.
- Make `edit` conditional on expected content or another concurrency token.
- Return conflicts instead of silently applying to unexpected content.
- Avoid partial writes.

### 5. `bash`

- Execute with an explicit cwd under the project root.
- Apply timeout and cancellation to the complete process group.
- Use a controlled environment and never inject provider credentials by default.
- Capture bounded stdout/stderr and report exit/signal/timeout distinctly.
- Avoid glob-based permission matching for compound shell commands.

### 6. Supervisor integration

- Advertise only enabled tools to the provider.
- Route calls through permission evaluation and the native executor.
- Persist the result once, then use M1 continuation.
- Make retries safe: provider transport retries must never repeat a completed
  side effect.

## Required tests

- In-root reads/writes/edits and traversal attempts.
- Symlinks pointing in and out of the workspace.
- Edit conflict and atomic-write failure behavior.
- Binary, oversized, and missing files.
- Successful, failing, timed-out, cancelled, and high-output shell processes.
- Child-process cleanup after cancellation.
- Provider secrets absent from tool environments.
- Permission denial proves no filesystem or process side effect occurred.
- Restart after execution but before provider continuation does not re-execute.

## Acceptance criteria

- All four native tools complete end to end through the event log and provider
  continuation.
- Operations cannot escape the selected project root under the documented
  symlink policy.
- Outputs and execution duration are bounded.
- Cancellation terminates subprocess work and leaves durable terminal events.
- Side-effecting calls are at-most-once across retries and restarts.

## Explicitly out of scope

- Container or VM isolation.
- Remote access.
- Arbitrary native tools beyond the four core operations.
