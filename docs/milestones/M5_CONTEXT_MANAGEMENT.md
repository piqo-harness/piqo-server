# M5 — Context Management and Compaction

State: `implementing`

## Outcome

Long-running sessions remain within provider context limits while preserving the
semantic facts needed for correctness, replay, audit, outstanding tool work, and
future forks.

## Current gap

The supervisor rebuilds the complete message transcript and sends it on every
run. There is no token accounting, model context metadata, truncation policy, or
compaction event.

## Required design decisions

- Source and trust model for model context-window metadata.
- Token estimation when a provider tokenizer is unavailable.
- Reserved budget for model output and tool definitions.
- Compaction trigger and target size.
- Summary generation provider/model and failure behavior.
- What must never be summarized or removed.
- Whether forks inherit compaction artifacts or recompute them.
- How caller-supplied raw transcripts interact with harness compaction.

## Recorded decisions

- Resolve limits from configured provider metadata JSON Pointers, then
  per-model configuration, then a configurable 128k context / 32k output
  fallback. Use `utf8_bytes_v1`, trigger at 80% of the input budget, and target
  60% after compaction.
- Default to a bounded LLM summary; `deterministic` marker compaction is
  configurable. A summary failure fails the run without replacing the active
  artifact.
- Caller-owned `messages` and `input` are never transformed. Forks inherit
  compaction artifacts already in their copied event prefix.

## Non-negotiable preservation rules

Compaction must retain:

- active system and agent instructions;
- unresolved tool calls and permission requests;
- tool call/result correlation identifiers;
- recent conversational turns required for continuity;
- facts explicitly marked as durable by the workflow;
- enough event metadata to explain what was summarized and why.

The source event log remains append-only. Compaction changes the provider view,
not historical truth.

## Implementation slices

### 1. Budget model

- Represent context and output budgets without strongly typing provider bodies.
- Account for transcript, instructions, tool schemas, raw request fields, and
  reserved output.
- Emit deterministic diagnostics when a request cannot fit even after allowed
  compaction.

### 2. Compaction plan

- Select a historical prefix using deterministic rules.
- Define a structured compacted representation and provenance.
- Record the source event range, strategy version, and generated artifact.
- Keep projection replay deterministic without calling a model during replay.

### 3. Summary generation

- Run summary generation as an explicit, cancellable operation.
- Bound retries, size, and cost.
- Validate that required structural facts remain present.
- Fail safely without corrupting the active projection.

### 4. Request construction

- Build protocol-specific transcripts from compacted views.
- Preserve raw caller-owned `messages` or `input` precedence.
- Ensure tool continuation remains correct across a compaction boundary.

### 5. API and observability

- Expose compaction status and failures if clients need them.
- Add semantic events and OpenAPI changes only after defining compatibility.
- Report estimated usage without claiming tokenizer-level accuracy when only an
  estimate is available.

## Required tests

- Sessions below and above the budget threshold.
- Instructions, pending calls, and tool results survive compaction.
- Replay and restart yield the same provider view.
- Fork before, within, and after a compacted range.
- Compaction model failure and cancellation.
- Oversized single message or tool schema.
- Both provider protocols and raw transcript overrides.

## Acceptance criteria

- Long sessions remain bounded by an explicit context budget.
- Compaction is durable, replayable, inspectable, and never mutates old events.
- Pending action semantics remain intact.
- Failure to compact cannot silently discard conversation state.
- The strategy and its limitations are documented.

## Explicitly out of scope

- Vector databases or semantic search unless a concrete need remains after the
  baseline compaction implementation.
- Provider-specific tokenizers for every model.
