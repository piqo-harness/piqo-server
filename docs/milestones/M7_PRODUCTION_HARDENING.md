# M7 — Production Hardening

State: `not_started`

## Outcome

The complete local harness has explicit resource limits, security controls,
failure recovery, diagnostics, and operational tests suitable for embedding in
a desktop application. This milestone does not by itself authorize public or
remote network exposure.

## Scope areas

### 1. Resource governance

- Bound HTTP bodies, SSE queues, event payloads, provider responses, tool output,
  subprocess counts, concurrent runs, and retained dump files.
- Define overload behavior with stable errors.
- Add configurable limits with safe defaults and validation.

### 2. Secret handling

- Audit logs, errors, request dumps, events, subprocess environments, and API
  responses for credentials and sensitive content.
- Keep request dumping opt-in with explicit warnings and restrictive file modes.
- Ensure provider headers and bearer tokens are never persisted accidentally.

### 3. Process and filesystem security

- Review sidecar directory ownership and file modes across supported platforms.
- Harden subprocess cleanup and process-group cancellation.
- Add adversarial tests for traversal, symlinks, command injection, malformed
  JSON-RPC, and oversized input.

### 4. Storage integrity and lifecycle

- Define backup, retention, deletion, and corruption diagnostics.
- Test migration from every supported schema version.
- Measure and document SQLite concurrency constraints.
- Add bounded cleanup policies without violating append-only session history.

### 5. Observability

- Add request/run/tool correlation identifiers to tracing spans.
- Add metrics for queue latency, provider latency, retries, tool duration,
  permission waits, compaction, and failures.
- Keep all telemetry content-safe by default.

### 6. Reliability validation

- Load-test concurrent sessions and SSE reconnects.
- Exercise shutdown during every run phase.
- Add fault injection at provider, storage, subprocess, and MCP boundaries.
- Verify no event loss or duplicate side effects after restart.

### 7. Remote-access design gate

Before any non-loopback binding is implemented, write and review a separate
security design covering:

- authenticated identities and authorization;
- TLS termination and certificate lifecycle;
- tenant/workspace isolation;
- rate limits and abuse controls;
- reduced permission profiles;
- audit and incident response.

Until that design is accepted, Piqo must reject non-loopback bind addresses.

## Required tests

- Limit enforcement for every externally controlled size and concurrency input.
- Secret-redaction regression suite.
- Sidecar shutdown and orphan-process checks under fault conditions.
- Database migration, corruption detection, and restart recovery.
- Sustained SSE replay/live handoff under concurrent load.
- Security-focused filesystem, shell, MCP, and HTTP cases.

## Acceptance criteria

- Resource exhaustion has bounded, documented behavior.
- Known secret-bearing values do not appear in logs, events, or public responses.
- Shutdown and restart do not lose semantic events or duplicate side effects.
- Operational failures are diagnosable without sensitive request content.
- Loopback-only posture remains enforced unless the remote-access gate is
  completed separately.

## Explicitly out of scope

- Claiming general production readiness for an internet-facing service.
- Implementing TLS or remote authentication without the design gate.
