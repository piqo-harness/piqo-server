# M8 — Interactive Client Readiness

State: `designing`

## Recorded decisions

- M8 is a capstone: it cannot move to `implementing` until M3 through M7 meet
  their acceptance criteria. In particular, incomplete MCP fixture and
  end-to-end coverage remains a gate.
- API v1 changes are additive. OpenAPI is the sole generated-model input; Piqo
  publishes versioned JSON workflow fixtures, not an SDK.
- `GET /sessions/{session_id}` is the typed client snapshot. Its
  `last_event_id` is an atomic SSE checkpoint for the included projection.
- Client recovery is defined by a normative HTTP status/error-code table;
  the error envelope is not extended with a category field.

## Outcome

The HTTP/SSE and sidecar contracts expose everything required for an independent
TUI or desktop client to operate complete agent workflows without calling server
internals or guessing state.

This repository remains headless. The milestone validates and completes the
server contract; it does not require implementing the client here.

## Required client workflows

A conforming client must be able to:

- start, validate, monitor, and stop the sidecar;
- manage providers, models, agents, projects, and sessions;
- queue, inspect, cancel, retry, and resume runs;
- render text, tool calls, tool results, retries, failures, and usage;
- list and resolve permission requests;
- reconnect SSE idempotently after sleep or network interruption;
- inspect parent/child agent activity;
- understand compaction and context-limit failures;
- distinguish recoverable, terminal, incompatible, and shutdown errors.

## Contract audit matrix

| Workflow | Read model / command | Stream or recovery signal |
| --- | --- | --- |
| Sidecar lifecycle | `ready`, `fatal`, health, OpenAPI | process exit and SSE EOF |
| Projects, providers, models, agents | collection and detail routes | refresh after configuration mutation |
| Sessions and runs | typed session snapshot, run inspection, queue commands | session SSE by event ID |
| Tool and permission actions | tool-result and permission routes | tool/permission/run events |
| Context and compaction | snapshot context projection | `context_*` events |
| Child agents | typed agent tree and child snapshots | one SSE cursor per session |

## Implementation slices

### 1. Contract audit

- Walk every client workflow against actual HTTP routes and SSE events.
- Add missing read models rather than requiring clients to replay undocumented
  internal assumptions.
- Ensure opaque cursors and identifiers are sufficient for pagination and
  reconnection.

### 2. Snapshot plus stream consistency

- Define the race-free sequence for loading a projection and joining SSE.
- Specify duplicate handling and event ordering across reconnects.
- Define lag behavior when a live broadcast receiver falls behind.
- Test late join during tool execution, permission wait, and subagent activity.

### 3. Error taxonomy

- Audit stable error codes for every command.
- Separate validation, conflict, unavailable, unauthorized, incompatible, and
  terminal errors.
- Ensure clients never need to branch on human-readable messages.

### 4. Sidecar compatibility

- Keep process and HTTP API versions independent.
- Document supported startup deadlines, shutdown sequencing, and exit statuses.
- Provide compatibility fixtures for client implementers.

### 5. SDK and fixtures

- Decide whether generated client models are published or OpenAPI remains the
  sole generation input.
- Provide deterministic HTTP/SSE fixture transcripts for the complete workflow.
- Include unknown additive fields and event types in compatibility fixtures.

### 6. CLI parity

- Extend the existing CLI where it provides useful contract validation, such as
  permission resolution and structured action inspection.
- Do not turn the CLI into a hidden privileged path; it must use public HTTP/SSE.

## Required tests

- End-to-end sidecar workflow from `ready` through clean exit.
- Projection/SSE race tests at every run phase.
- Reconnect with duplicates, gaps, lag, and unknown event types.
- Permission and tool-result client workflows.
- Parent/child agent navigation and cancellation.
- Compatibility behavior for unknown fields and error codes.

## Acceptance criteria

- An external client can implement every required workflow using only the
  published contracts.
- No workflow depends on polling human-readable logs or accessing SQLite.
- Snapshot and SSE handoff is documented and race-safe.
- OpenAPI, protocol examples, and server behavior agree.
- The CLI uses no private shortcut for operations exposed to users.

## Explicitly out of scope

- Building a TUI, desktop, mobile, or web UI in this repository.
- Choosing a client-side visual design system.
