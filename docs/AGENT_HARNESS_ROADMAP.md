# Agent Harness Implementation Roadmap

Status: planning document for the work required to turn the implemented text
conversation foundation into a complete local agent harness.

This roadmap is intentionally organized for both human contributors and coding
agents. It does not claim that planned capabilities already exist. The current
implemented surface remains documented by `README.md`, `ARCHITECTURE.md`, and
`docs/CLIENT_PROTOCOL.md`.

## Purpose

Piqo already provides durable sessions, a semantic event log, an HTTP/SSE API,
provider transports, exact request-body merging, run queues, configuration
reload, and a loopback-only sidecar. A provider tool call is currently recorded
and moves its run to `requires_action`; the server cannot execute the tool,
accept its result, or continue the model turn.

The milestones below close that gap in dependency order. Each milestone should
be independently reviewable and should leave the repository in a coherent,
tested state.

## Planning principles

All milestone work must preserve these repository invariants:

1. The event log is the durable source of truth. New state must be replayable.
2. Provider-owned JSON remains `serde_json::Value` and is merged verbatim.
3. Every tool invocation passes through permission evaluation before execution.
4. IO stays at crate edges; `piqo-core` remains synchronous and IO-free.
5. API v1, SSE events, error envelopes, and sidecar stdout are public contracts.
6. Existing migrations are immutable; schema changes use new ordered migrations.
7. Loopback remains the default and only supported binding until remote security
   has an explicit design.

## Milestone sequence

| ID | Milestone | Outcome | Depends on |
| --- | --- | --- | --- |
| M1 | [Action continuation protocol](milestones/M1_ACTION_CONTINUATION.md) | A client can submit tool results and resume a paused run safely. | Current foundation |
| M2 | [Permission workflow](milestones/M2_PERMISSION_WORKFLOW.md) | Every tool call is allowed, denied, or durably paused for approval. | M1 |
| M3 | [Native tools](milestones/M3_NATIVE_TOOLS.md) | Safe `read`, `write`, `edit`, and `bash` execution is wired into runs. | M2 |
| M4 | [MCP and plugins](milestones/M4_MCP_AND_PLUGINS.md) | External MCP tools use the same permission and continuation pipeline. | M2, preferably M3 |
| M5 | [Context management](milestones/M5_CONTEXT_MANAGEMENT.md) | Long sessions stay within model limits without losing auditability. | M1 |
| M6 | [Orchestration and subagents](milestones/M6_ORCHESTRATION.md) | Bounded parent/child agent workflows execute and remain replayable. | M2, M3, M5 |
| M7 | [Production hardening](milestones/M7_PRODUCTION_HARDENING.md) | Local execution has explicit limits, observability, and security hardening. | M1-M6 |
| M8 | [Interactive client readiness](milestones/M8_CLIENT_READINESS.md) | The API fully supports a TUI or desktop client for agent workflows. | M1-M7 |

M1 through M3 form the minimum complete local harness. M4 through M6 add the
tool ecosystem, long-running sessions, and multi-agent behavior. M7 and M8 make
those capabilities safe and usable as a product surface.

## Cross-milestone dependency map

```text
Current text-only foundation
            |
            v
 M1 action continuation
       |            \
       v             v
 M2 permissions   M5 context management
       |
       +----------+-----------+
       |          |           |
       v          v           v
 M3 native     M4 MCP     M6 orchestration
    tools       tools       (also needs M5)
       \          |           /
        +---------+----------+
                  v
          M7 production hardening
                  |
                  v
          M8 client readiness
```

## Milestone state model

Use exactly one of these states when tracking a milestone:

- `not_started`: no implementation work has been accepted.
- `designing`: contracts and decisions are being resolved.
- `implementing`: an approved design is being built.
- `validating`: implementation is complete and acceptance checks are running.
- `complete`: every acceptance criterion is satisfied and documentation agrees.
- `blocked`: progress requires a named decision or external dependency.

The table below is the canonical high-level tracker. Update it when a milestone
changes state; detailed task tracking belongs in an issue or pull request.

| Milestone | State | Tracking issue/PR | Last updated | Notes |
| --- | --- | --- | --- | --- |
| M1 | complete | — | 2026-08-21 | Action-result continuation merged. |
| M2 | complete | — | 2026-08-21 | Durable permission workflow merged. |
| M3 | implementing | — | 2026-08-21 | Project-scoped native tools under implementation. |
| M4 | not_started | — | — | — |
| M5 | not_started | — | — | — |
| M6 | not_started | — | — | — |
| M7 | not_started | — | — | — |
| M8 | not_started | — | — | — |

## Definition of ready

A milestone may move from `designing` to `implementing` only when:

- its open design questions have recorded answers;
- externally visible API and event changes are sketched in the milestone or a
  linked design document;
- storage changes and migration needs are identified;
- security boundaries and failure behavior are explicit;
- work is split into reviewable changes with dependency order;
- test scenarios include restart, cancellation, replay, and malformed input
  where applicable.

## Definition of done

A milestone is complete only when:

- all acceptance criteria in its document are satisfied;
- new behavior is replayable from the semantic event log;
- relevant unit and integration tests exist;
- `README.md` and `docs/CLIENT_PROTOCOL.md` describe the observable behavior;
- OpenAPI output includes all API changes;
- no planned capability is described as implemented before it is wired end to
  end;
- the following commands pass from the workspace root:

```sh
cargo fmt --all -- --check
cargo check --locked --workspace
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

## Instructions for planning agents

Before implementing a milestone, an agent must:

1. Read `AGENTS.md`, `ARCHITECTURE.md`, `README.md`, and
   `docs/CLIENT_PROTOCOL.md` in full.
2. Read this roadmap and the complete milestone document.
3. Inspect the current code instead of assuming this roadmap is current.
4. Identify overlapping uncommitted changes and preserve them.
5. Produce a plan that maps every task to a crate, contract, and test surface.
6. Resolve design questions before introducing public contracts or migrations.
7. Implement the smallest end-to-end slice first; avoid disconnected types that
   are not wired into the server.

Agents must not silently expand a milestone into remote access, a UI, or a new
abstraction layer. New traits require a genuine second implementation or runtime
heterogeneity, consistent with `ARCHITECTURE.md`.

## Required agent handoff

At the end of an implementation session, record:

```text
Milestone:
State:
Completed scope:
Remaining scope:
Decisions made:
API/event/storage changes:
Security considerations:
Validation run:
Known failures or blockers:
Recommended next task:
```

This handoff should appear in the pull request or tracking issue. Do not store
ephemeral session notes in a new repository file unless they are durable design
decisions.
