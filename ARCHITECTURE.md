# piqo-server — objective and architecture

Status: implemented foundation. The workspace now contains the initial durable
session core, SQLite event log, versioned HTTP/SSE API, and loopback-only CLI
server. This document records what the server is for and which decisions are
already settled, so they do not get re-litigated.

## What this is

A headless agent harness server. Rust, cross-platform, single binary. It drives
LLM agents — orchestrator plus subagents — against local or remote model
servers, with three properties existing harnesses do not combine:

1. Exact, per-agent control over what goes into the request body.
2. Enforced per-agent permissions (an agent that cannot write files *cannot*,
   regardless of what the model decides).
3. A replayable event log, so any run can be resumed, joined late, forked, or
   debugged after the fact.

It has no user interface. Clients — TUI, desktop, mobile — are separate projects
speaking to its HTTP API.

## Why it exists

The design thesis, in one line: **the request body belongs to the user; the
harness fills gaps, it never normalizes.**

This came out of measured failures, not preference:

- **OpenCode** normalizes. Sampling params are hardcoded by substring match on
  model id (`if (id.includes("qwen")) return 1` forces `top_p: 1`, overriding the
  model card's 0.95). Agent-level `temperature` is silently dropped unless the
  model declares a capability flag. `top_k` has no config path at all. Output is
  capped at 32000 tokens regardless of declared limits. Body-level fields such as
  `chat_template_kwargs` are discarded before the wire, because options are
  routed into an AI SDK adapter that validates against its own schema and drops
  unknown keys. Verified by packet capture through a logging proxy.
- **Pi** gets this right — `samplingParams` is "merged verbatim into every
  request body, so its keys win" — but ships no subagents and no permission
  system by design ("No permission popups. Run in a container.").
- **Claude Code** exposes no sampling control at all, by design.

Nobody offers verbatim body control *and* enforced permissions *and* subagents.
That gap is the reason for this project.

## Non-goals

- Not competing with OpenCode on breadth. No LSP, no provider catalog, no OAuth
  flows for vendor subscriptions, until there is a concrete need.
- No UI in this repository.
- Not a general chat application. It runs structured agent workflows.

## Settled decisions

**Rust.** The core work is HTTP transport, JSON manipulation, process
supervision, and permission enforcement — all things Rust does well. More
importantly, the failure being designed against is an SDK abstraction that
decided which parameters were legitimate. Raw HTTP plus `serde_json` has no such
layer.

**Headless first.** The server and its API come before any interface. A TUI is
the first client, partly because it is useful and partly because it forces the
API to be honest — a client in the same process can cheat by calling internals.

**Daemon-capable, not embedded-only.** The server must be able to run
independently of any application. A desktop app may launch or attach to it, but
must not be the only thing that can host it. This is forced by the planned
mobile companion: if the server dies with the desktop UI, remote control is
impossible.

**SSE for events, plain POST for commands.** Not WebSocket. Traffic is heavily
asymmetric — a continuous server-to-client stream of tokens, tool events and
state changes, against occasional discrete client-to-server actions (send
prompt, cancel, approve a permission). SSE gives reconnection and resume for free
via `Last-Event-ID`, which matters because runs last minutes and laptops sleep.
It is also debuggable with `curl`, which WebSocket is not. A WebSocket channel
can be added later for anything genuinely interactive without disturbing this.

**MCP for the tool ecosystem.** Native implementations only for the
latency-critical, permission-integrated core: `read`, `write`, `edit`, `bash`.
Everything else comes from MCP servers via the official Rust SDK (`rmcp`).

**Plugins as subprocesses over JSON-RPC on stdio.** Same shape as MCP, so one
protocol serves two purposes, plugins can be written in any language, and no JS
engine gets embedded.

**axum + tower** for the HTTP layer. `Sse` is built in; the tower middleware
story is what pays off as auth, rate limiting, tracing and request IDs arrive —
all of which the mobile companion will require.

## Code architecture

**Functional core, imperative shell.** Domain logic — session state machine,
permission evaluation, event-log semantics — is pure: no IO, no async, no traits
required to test it. IO lives at the edges. This is the shape that fits Rust
best, and it conveniently puts the two security-critical components (permission
evaluator, state machine) in the part that is exhaustively testable without
mocks or fakes.

**Crates are the boundary.** Hexagonal / clean architecture applies here, but the
enforcement mechanism is the Cargo workspace, not folder conventions. If
`piqo-core` does not depend on `piqo-server`, the compiler enforces the
dependency direction. No review discipline needed, and no way to drift.

    piqo-core      pure domain: state machine, permission evaluator,
                   event-log types. No IO, no async, no tokio.
    piqo-provider  LLM transport: outbound HTTP, upstream SSE, body merge.  -> core
    piqo-tools     native tools + MCP client.                               -> core
    piqo-server    axum, HTTP API, SSE, session supervision.                -> all
    piqo-cli       binary: serve / attach / one-shot run.                   -> server

**No port before the second implementation.** Traits get introduced when a second
implementation actually exists, not in anticipation of one. Premature
`Box<dyn Trait>` layering and `Arc<Mutex<_>>` plumbing is the usual way clean
architecture goes wrong in Rust: cost with no benefit, plus lifetime friction.
The two places where abstraction is justified from the start are provider
transport and tool execution, because both will genuinely have several
implementations. Prefer generics (static dispatch) over trait objects unless the
collection is heterogeneous at runtime.

### Crates

| crate | role | note |
|---|---|---|
| `axum` | HTTP + SSE | `Sse` response type built in |
| `tower` / `tower-http` | middleware | auth, rate limiting, tracing, request IDs — the mobile companion needs all of these |
| `tokio` | async runtime | implied by axum |
| `reqwest` | outbound HTTP to providers | |
| `eventsource-stream` or `reqwest-eventsource` | consume upstream SSE | |
| `serde` / `serde_json` | body merge | the thesis lives here |
| `rmcp` | official MCP Rust SDK | client side |
| `tracing` | structured logs and spans | from day one, not retrofitted |
| `utoipa` | OpenAPI generated from types | optional, but the API is the product, so a published contract has value |

axum over actix-web deliberately. actix-web is roughly 10-15% faster under heavy
load, which is irrelevant here — this server handles a handful of local sessions,
not high throughput. What matters is built-in `Sse` and the tower middleware
story.

### One counter-intuitive rule

Do **not** strongly type the provider request body. It stays `serde_json::Value`,
merged verbatim, with strong types only for the fields the harness itself must
read back. Typing that body would reintroduce exactly the normalization layer
this project exists to avoid. Strong typing is correct almost everywhere else in
this codebase; this is the deliberate exception, and it is the point.

## The central piece: the event log

Everything else is arranged around it. It is not an implementation detail.

**Shape:** append-only, monotonically numbered, persisted per session.

**Granularity:** events are *semantic state changes*, not tokens. Message
started, tool call emitted, tool result, phase changed, permission requested,
permission resolved, agent spawned, agent finished. Token deltas stream over the
same channel but do not all need persisting. A log of tokens is enormous and
unreadable; a log of state changes reads like a transcript of what the run
actually did.

**What it buys, all from one mechanism:**

- SSE resume — client reconnects with `Last-Event-ID`, server replays from that
  offset.
- Late join — a client attaching mid-run gets the whole history, then live
  events, with no special path.
- Session fork — branch from any event index.
- Post-mortem debugging — the reason a run misbehaved is on disk. This is
  concretely motivated: a prior harness run reached its review phase with an
  empty gates section and there was no way to find out why, because nothing was
  recorded.

**Design rule:** if a component needs to know what happened, it reads the log.
Components do not gossip directly.

## What surrounds it

**Session engine.** Owns the agent state machine, turn loop, and subagent tree.
Emits events; holds no IO itself.

**Permission evaluator.** Pure function: (agent, tool, arguments) → allow / ask /
deny. No IO, exhaustively testable. Security-critical, so it stays free of
async and IO on purpose.

One trap to design against, inherited from studying prior art: naive glob
matching on shell commands is unsound. A rule allowing `git *` will happily
match `git status && rm -rf /`. Command patterns need real parsing, not globs.

**Provider transport.** Consumes upstream SSE, emits the request body. This is
where the thesis lives:

    defaults < model < agent < variant < request

merged verbatim, last writer wins, harness fills gaps only. No intermediate
schema, no allowlist, no key renaming. A `--dump-requests` flag writing the final
JSON body is a first-class feature, not a debug afterthought — reconstructing
what a harness actually sent previously required standing up a logging proxy.

Must degrade gracefully on malformed provider output. Small local models do emit
unterminated tool-call XML; the harness reports that as a fact rather than
crashing or inventing a call.

**Tool runtime.** Native core tools plus MCP clients. Every invocation passes the
permission evaluator first.

**Storage.** Sessions and event logs use SQLite through `sqlx`. The schema keeps
append and range reads cheap, with a transactional projection cache verified
against the append-only event log on startup.

**HTTP API.** The product surface, since every UI is a client. Versioned from
day one, streaming, with resume and fork as first-class operations rather than
retrofits.

## Security posture

The API exposes an agent that runs arbitrary shell commands. It is a remote
shell and gets treated as one: authentication required, TLS for anything
non-local, never bind to `0.0.0.0` by default, and a reduced permission profile
for remote sessions. This is the single highest-risk surface in the project.

## Open questions

- Event granularity — exact taxonomy, and which token-level data is persisted.
- Context compaction — when to summarize, what to preserve. The most
  underestimated part of any harness.
- Storage alternatives are intentionally deferred until a second implementation
  is needed; SQLite is the v1 concrete store.
- Whether the permission evaluator's command parsing needs a real shell grammar
  or a restricted, explicitly-supported subset.
