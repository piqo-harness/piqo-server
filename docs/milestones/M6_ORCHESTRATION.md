# M6 — Orchestration and Subagents

State: `not_started`

## Outcome

An orchestrator can spawn bounded child agents with explicit models,
instructions, permissions, context, and budgets; collect their results; and
remain fully replayable and cancellable.

## Current gap

Agent definitions and `agent_spawned`/`agent_finished` events exist, but runs are
executed by a single hard-coded assistant identity. There is no agent tree,
scheduler, result protocol, or resource budget.

## Required design decisions

- Workflow representation: model-driven spawn tool, declarative graph, or both.
- Agent identity and relationship to sessions and runs.
- Context inheritance and isolation.
- Result format returned to a parent.
- Per-agent and aggregate limits for depth, concurrency, turns, tokens, and time.
- Failure and cancellation propagation.
- Scheduling fairness and restart recovery.
- Whether child work shares a session log or uses linked child sessions.

Favor linked child sessions if that keeps event IDs, projections, forks, and
permissions simpler and more explicit. Decide through a written design before
implementation.

## Implementation slices

### 1. Domain model

- Define agent identity, parent relation, lifecycle, budgets, and terminal
  result.
- Represent spawn requests and results as semantic events.
- Make projections replayable and reject invalid trees or transitions.

### 2. Scheduling

- Add bounded global and per-session concurrency.
- Prevent unbounded recursive spawning.
- Persist queued work before starting it.
- Recover queued/running children predictably after restart.
- Make cancellation propagate without abandoning subprocesses or provider work.

### 3. Agent configuration

- Resolve provider, model, body layers, instructions, permissions, and limits per
  child.
- Define snapshot behavior across configuration reloads.
- Do not allow a parent to grant permissions it does not possess.

### 4. Context and results

- Pass only explicitly selected context to a child.
- Return a structured, bounded result to the parent.
- Preserve provenance so clients can inspect which agent produced each fact.
- Apply M5 budgets independently and in aggregate.

### 5. API and SSE

- Expose the agent tree and lifecycle needed by clients.
- Specify ordering when parent and child sessions emit concurrently.
- Preserve reconnect and late-join semantics.
- Extend OpenAPI and the normative client protocol.

## Required tests

- One child, multiple siblings, and bounded nesting.
- Concurrency and depth limit enforcement.
- Child permission isolation.
- Parent cancellation and child failure propagation.
- Restart with queued and active children.
- Configuration reload while a tree is active.
- SSE replay and late join across parent/child activity.
- Context and result size limits.

## Acceptance criteria

- A parent can delegate work and consume a child result end to end.
- All child work has explicit identity, permissions, budgets, and provenance.
- Concurrency and recursive depth are bounded.
- Cancellation and restart leave no invisible or orphaned work.
- The entire workflow can be understood from durable logs.

## Explicitly out of scope

- Distributed orchestration across multiple Piqo servers.
- Remote workers.
- Autonomous permission escalation.
