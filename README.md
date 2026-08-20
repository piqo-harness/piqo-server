# piqo-server

`piqo-server` is an experimental, headless agent harness written in Rust. It
runs model requests behind a versioned HTTP/SSE API, stores every semantic
session event in SQLite, and gives callers exact control over the JSON sent to
the model provider.

The project is built around one rule:

> The request body belongs to the user. The harness fills gaps; it does not
> normalize, rename, or discard provider-specific fields.

## Project status

This repository contains an **implemented foundation**, not yet a complete
autonomous coding agent or a production-ready remote service.

Available today:

- a local HTTP server and command-line client;
- OpenAI-compatible Chat Completions and Responses transports, with streaming
  and non-streaming response support;
- verbatim, layered JSON request-body merging;
- durable sessions and append-only semantic event logs in SQLite;
- live SSE events, history replay, and reconnection with `Last-Event-ID`;
- session listing, inspection, forking, run queues, cancellation, and retries;
- request dumping for inspecting the exact provider payload;
- a generated OpenAPI document at `/api/v1/openapi.json`.
- an authenticated macOS sidecar binary (`piqo-server`) with ephemeral-port
  discovery and graceful shutdown.

Designed or partially implemented, but **not wired into the server yet**:

- native `read`, `write`, `edit`, and `bash` tool execution;
- MCP server connections and plugin subprocesses;
- permission approval/denial endpoints and enforcement in the run loop;
- orchestrator/subagent execution;
- context compaction;
- TLS termination and remote access;
- a TUI, desktop UI, or other interactive client.

If a model emits a tool call today, piqo records it and changes the run to
`requires_action`. There is currently no API for submitting the tool result and
continuing that run. Text-only model conversations are the primary usable path.

Both server entry points are loopback-only. The dedicated sidecar additionally
requires a per-process bearer token; `piqo-cli serve` remains the simple,
unauthenticated development mode.

## How it works

1. A client creates a durable session.
2. The client queues a run with a provider, model, input, and optional raw JSON
   body.
3. piqo rebuilds the session transcript from its event log.
4. Configuration and request body layers are merged, from lowest to highest
   precedence.
5. Missing harness fields such as `model`, `stream`, and `messages`/`input` are
   added without replacing caller-supplied values.
6. The request is sent directly to the configured provider over HTTP.
7. Provider output becomes durable semantic events and is also published live
   over SSE.
8. A reconnecting or late client replays stored events, then continues with live
   events on the same stream.

For the rationale and design constraints, see [ARCHITECTURE.md](ARCHITECTURE.md).
Client implementers should follow the normative
[client communication contract](docs/CLIENT_PROTOCOL.md).
The work required to reach a complete local agent harness is organized in the
[agent harness implementation roadmap](docs/AGENT_HARNESS_ROADMAP.md).

## Requirements

- Rust 1.88 or newer;
- a model server implementing either the OpenAI Chat Completions API or the
  OpenAI Responses API;
- `curl` and `jq` for the API examples below (optional).

No external SQLite installation is required.

## Quick start

### 1. Build the workspace

```sh
git clone <repository-url>
cd piqo-server
cargo build --workspace
```

### 2. Configure a provider

Create `piqo.toml` in the repository root. This example assumes a local
OpenAI-compatible server on port 8000:

```toml
[providers.local]
base_url = "http://127.0.0.1:8000"
protocol = "chat_completions"
connect_timeout_seconds = 10

[defaults.body]
temperature = 0.7
```

For a provider requiring a bearer token, reference an environment variable
instead of placing the secret in the file:

```toml
[providers.hosted]
base_url = "https://provider.example/v1"
protocol = "responses"
api_key_env = "PIQO_PROVIDER_API_KEY"
```

Then export the variable before starting piqo:

```sh
export PIQO_PROVIDER_API_KEY="..."
```

### 3. Start the daemon

```sh
cargo run -p piqo-cli -- serve \
  --config ./piqo.toml \
  --database sqlite://piqo.db
```

The server listens on `127.0.0.1:8080` by default. Database migrations run
automatically and the database file is created if it does not exist.

Verify it from another terminal:

```sh
curl http://127.0.0.1:8080/api/v1/health
```

### 4. Run a prompt

Replace the model identifier with one served by your provider:

```sh
cargo run -p piqo-cli -- run \
  --provider local \
  --model Qwen/Qwen3-8B \
  "Explain why append-only logs are useful."
```

The CLI creates a session, queues the run, follows its event stream, and prints
assistant text. Add `--json` to print every event as newline-delimited JSON.

Create a project first to group its sessions in a TUI or desktop client, then
pass it when creating a new run:

```sh
cargo run -p piqo-cli -- project create --name piqo --path "$PWD"
cargo run -p piqo-cli -- run --project <project-id> --provider local --model Qwen/Qwen3-8B "Summarize this repository."
```

Use `piqo project list`, `get`, or `update` to manage groups. Project deletion
cascades to all of its sessions and event history, so the CLI requires an
explicit confirmation:

```sh
cargo run -p piqo-cli -- project delete <project-id> --yes
```

To continue an existing session:

```sh
cargo run -p piqo-cli -- run \
  --session <session-id> \
  --provider local \
  --model Qwen/Qwen3-8B \
  "Now give me a concrete example."
```

To replay a session and keep following new events:

```sh
cargo run -p piqo-cli -- attach <session-id>
```

Use `attach --json` to see the complete event objects instead of assistant text
only. Stop an attached client with Ctrl-C; this does not cancel the active run.

## Embedding the macOS sidecar

The `piqo-server` binary is intended to be bundled inside a macOS application
and launched with `Process`. It has no subcommands and uses a private profile at
`~/.config/piqo/`:

```text
~/.config/piqo/piqo.toml
~/.config/piqo/piqo.db
~/.config/piqo/piqo.lock
```

Start it without arguments:

```sh
piqo-server
```

stdout is reserved for a versioned, newline-delimited lifecycle protocol. The
first successful line is:

```json
{"type":"ready","protocol_version":1,"server_version":"0.1.0","api_version":"v1","pid":1234,"base_url":"http://127.0.0.1:54321","token":"..."}
```

The parent should parse this line, use `base_url` for HTTP/SSE requests, and
send the token as `Authorization: Bearer <token>` on **every** request,
including health and SSE. Logs go to stderr. Startup failures produce a
`fatal` JSON line and exit with status 2. A second process using the same
profile fails with `instance_already_running`.

The token is generated from 32 cryptographically random bytes for each launch;
it is not written to the database, lock file, or logs. The server binds to
`127.0.0.1:0`, so the actual port is only known from the `ready` message.

When the application exits, send SIGTERM (or Ctrl-C in a development shell) and
wait for the process to exit. The sidecar stops accepting mutations, cancels
provider work, marks every non-terminal run as `interrupted` with reason
`server_shutdown`, closes SSE streams, and exits cleanly within ten seconds.

The bundled executable is an unsigned arm64 Mach-O. The host application must
sign the nested executable with the same signing identity and hardened-runtime
settings as its app bundle before distribution. A Swift SDK is intentionally
not part of this repository; the lifecycle protocol is the integration
boundary.

Build the macOS arm64 distribution archive locally with:

```sh
./scripts/package-macos-arm64.sh
```

The script produces `dist/piqo-server-v<version>-macos-arm64.tar.gz`. The
archive contains the executable, its MIT license, and a machine-readable
compatibility manifest. GitHub records the SHA-256 digest when the artifact is
uploaded, so the repository does not publish a redundant checksum file.

To prepare a release, manually run the `Release` workflow and enter the Cargo
package version without a `v` prefix. The orchestrator validates the version,
calls the platform packaging workflows, downloads their artifacts, and creates
a draft in [GitHub Releases](https://github.com/piqo-harness/piqo-server/releases).
The macOS arm64 package is currently the only release artifact.

The package targets macOS 26.0 or newer and carries no distribution identity.
The host application must replace any linker-generated ad hoc signature with
its own signature before distribution.

## Configuration

The CLI reads `~/.config/piqo/piqo.toml` by default, or a path supplied with
`serve --config`. A missing configuration file is allowed, but no run can start
until its requested provider is configured.

### Providers

```toml
[providers.local]
base_url = "http://127.0.0.1:8000/v1"
protocol = "chat_completions" # or "responses"
connect_timeout_seconds = 10
api_key_env = "OPTIONAL_API_KEY_VARIABLE"
models = ["Qwen/Qwen3-8B"] # optional manual catalog override

[providers.local.headers]
x-custom-header = "value"
```

Provider options:

| Key | Meaning |
| --- | --- |
| `base_url` | Provider root, `/v1` root, or full protocol endpoint |
| `protocol` | `chat_completions` (default) or `responses` |
| `api_key` | Literal bearer token; supported but discouraged |
| `api_key_env` | Environment variable containing the bearer token |
| `headers` | Additional HTTP headers |
| `connect_timeout_seconds` | Connection timeout; defaults to 10 seconds |
| `models` | Optional manual model catalog; when present, automatic discovery is disabled |

`api_key` and `api_key_env` are mutually exclusive. piqo appends
`/v1/chat/completions` or `/v1/responses` when the configured URL does not
already contain the expected endpoint.

When `models` is absent, piqo queries the OpenAI-compatible `/v1/models`
endpoint after provider creation or updates and again during server startup.
Discovery failures do not invalidate the provider; the API reports the failure
and clients may retry it explicitly. A manual list is informational and does
not prevent a run from naming another model.

Providers can be managed without restarting the server through
`/api/v1/providers`. Mutations are written atomically to `piqo.toml`, preserving
comments and unrelated sections. Credential values and custom header values are
write-only API fields: responses expose only the credential kind and header
names. Prefer the `environment` credential form over storing a literal key:

```json
{
  "name": "local",
  "base_url": "http://127.0.0.1:8000/v1",
  "credentials": {"type": "environment", "variable": "LOCAL_API_KEY"}
}
```

The global `[models."model-id".body]` tables below remain request-body layers;
they are separate from each provider's optional model catalog.

Reload the complete file without restarting the process:

```sh
curl -X POST http://127.0.0.1:8080/api/v1/config/reload
```

The replacement is atomic. Active runs keep the configuration captured when
they started, while queued runs use the latest configuration when execution
begins. An invalid or missing file returns `422/config_invalid`, then causes a
graceful shutdown with a non-zero process exit so the host can surface the
configuration error.

### Request-body layers

Request bodies are shallow-merged in this order, with later layers winning:

```text
defaults < model < agent Markdown body < agent TOML body < variant < individual request
```

Example:

```toml
[defaults.body]
temperature = 0.2
max_tokens = 4096

[models."Qwen/Qwen3-8B".body]
top_p = 0.95
top_k = 40

[agents.reviewer.body]
temperature = 0.1

[variants.fast.body]
max_tokens = 1024

[variants.thinking.body.chat_template_kwargs]
enable_thinking = true
```

Select named layers through the API's `agent` and `variant` fields. The current
CLI exposes provider and model selection, but not agent, variant, or arbitrary
request-body flags; use the HTTP API for those.

### User-defined agents

Piqo loads top-level `*.md` agent files from an `agents/` directory alongside
`piqo.toml`; `agents/reviewer.md` defines the `reviewer` agent. Each file
starts with YAML front-matter and continues with the agent's system prompt:

```md
---
description: Review code without editing it
provider: local
model: Qwen/Qwen3-8B
permissions:
  read: allow
  write: deny
  bash: ask
body:
  temperature: 0.1
---
Review the supplied change for correctness, security, and missing tests.
```

`read`, `write`, and `bash` accept `allow`, `ask`, or `deny`. These
settings are retained in the resolved configuration, but are not enforced until
tool execution is connected to the server.

`piqo.toml` can override any file-defined agent field without replacing its
Markdown definition:

```toml
[agents.reviewer]
model = "Qwen/Qwen3-14B"
instructions = "Follow this repository's review checklist."

[agents.reviewer.permissions]
bash = "deny"
```

Explicit request `provider` and `model` values win. When an `agent`
supplies them, HTTP clients may omit those fields. A raw request `body` that
contains `messages` or `input` remains authoritative and bypasses automatic
system-prompt and transcript construction.

The merge is deliberately **shallow**. If two layers define the same key, the
entire later value replaces the earlier one. Unknown keys such as `top_k` or
`chat_template_kwargs` remain untouched.

After merging, piqo fills these fields only when absent:

- `model`;
- `stream` (defaults to `true`);
- `messages` for Chat Completions or `input` for Responses, built from the
  durable session transcript.

Consequently, an individual request body can override any of them.

## Inspect the exact provider request

Start the server with a dump directory:

```sh
cargo run -p piqo-cli -- serve \
  --config ./piqo.toml \
  --dump-requests ./request-dumps
```

Each attempt writes a JSON body and a small metadata file. Dumps can contain
prompts, conversation history, tool definitions, and other sensitive data. Keep
the directory private and do not commit it.

## HTTP API

The OpenAPI contract is available from a running server:

```sh
curl http://127.0.0.1:8080/api/v1/openapi.json
```

### Create a session and run

```sh
SESSION_ID=$(curl -sS \
  -X POST http://127.0.0.1:8080/api/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"title":"README demo"}' | jq -r .id)

curl -N \
  http://127.0.0.1:8080/api/v1/sessions/$SESSION_ID/events/stream
```

Leave the event stream open and, in another terminal, queue a run:

```sh
curl -sS \
  -X POST http://127.0.0.1:8080/api/v1/sessions/$SESSION_ID/runs \
  -H 'content-type: application/json' \
  -d '{
    "provider": "local",
    "model": "Qwen/Qwen3-8B",
    "input": "Hello from the API",
    "agent": "reviewer",
    "variant": "thinking",
    "body": {
      "temperature": 0.4,
      "vendor_specific_option": true
    }
  }'
```

### Replay events after a known event

For a finite JSON response:

```sh
curl "http://127.0.0.1:8080/api/v1/sessions/$SESSION_ID/events?after=10&limit=200"
```

For replay followed by live SSE events:

```sh
curl -N \
  -H 'Last-Event-ID: 10' \
  http://127.0.0.1:8080/api/v1/sessions/$SESSION_ID/events/stream
```

### Main routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/health` | Health check |
| `GET` | `/api/v1/openapi.json` | OpenAPI document |
| `POST`, `GET` | `/api/v1/projects` | Create or list project groups |
| `GET`, `PATCH`, `DELETE` | `/api/v1/projects/{id}` | Inspect, update, or delete a project and its sessions |
| `GET` | `/api/v1/projects/{id}/sessions` | Paginated sessions for one project |
| `POST`, `GET` | `/api/v1/sessions` | Create or list sessions |
| `GET` | `/api/v1/sessions?unassigned=true` | Sessions not attached to a project |
| `GET` | `/api/v1/sessions/{id}` | Session summary and projection |
| `GET` | `/api/v1/sessions/{id}/events` | Paginated event history |
| `GET` | `/api/v1/sessions/{id}/events/stream` | Replayable SSE stream |
| `POST` | `/api/v1/sessions/{id}/forks` | Fork at an event ID |
| `GET`, `POST` | `/api/v1/providers` | List or create providers |
| `GET` | `/api/v1/agents` | List resolved user-defined agents without their prompts |
| `GET`, `PATCH`, `DELETE` | `/api/v1/providers/{provider}` | Inspect, update, or immediately delete a provider |
| `GET`, `PUT`, `DELETE` | `/api/v1/providers/{provider}/models` | Read, replace, or clear the manual model catalog |
| `POST` | `/api/v1/providers/{provider}/models/refresh` | Refresh automatic discovery when no manual override exists |
| `POST` | `/api/v1/config/reload` | Atomically reload `piqo.toml` |
| `POST` | `/api/v1/sessions/{id}/runs` | Queue a run |
| `GET` | `/api/v1/sessions/{id}/runs/{run_id}` | Inspect a run |
| `POST` | `/api/v1/sessions/{id}/runs/{run_id}/cancel` | Cancel a run |
| `POST` | `/api/v1/sessions/{id}/runs/{run_id}/retries` | Retry a failed, interrupted, or cancelled run |
| `POST` | `/api/v1/sessions/{id}/queue/resume` | Resume a paused queue |

## Persistence and recovery

Sessions, projection metadata, and semantic events are stored in SQLite. The
full in-memory projection is rebuilt and checked against the append-only log.
Event IDs are monotonically increasing within a session, making history suitable
for replay and debugging.

On daemon restart, sessions that were running are marked as interrupted rather
than silently continued. Interrupted, failed, or cancelled runs can be retried
through the API.

Forking copies the selected historical prefix into a new session. Create a fork
by posting an event boundary:

```sh
curl -sS \
  -X POST http://127.0.0.1:8080/api/v1/sessions/$SESSION_ID/forks \
  -H 'content-type: application/json' \
  -d '{"at_event_id":10,"title":"Alternative path"}'
```

## Security

Treat any agent harness capable of invoking tools as a remote shell.

The sidecar authenticates every API route with its per-process bearer token and
enforces loopback-only binding. The development `piqo-cli serve` command remains
unauthenticated for compatibility, so it must never be proxied to an untrusted
network. Provider request dumps and the SQLite database may both contain
sensitive conversation data.

Remote operation should not be considered supported until TLS, rate limiting,
request identity, and permission-enforced tool execution are in place.

## Workspace layout

| Crate | Responsibility |
| --- | --- |
| `piqo-core` | Pure session state, permissions, projections, and event types |
| `piqo-provider` | Raw HTTP provider transport, JSON merging, and SSE parsing |
| `piqo-tools` | Permission/tool boundary and future MCP integration |
| `piqo-server` | axum API, supervision, and SQLite storage |
| `piqo-cli` | `serve`, `run`, and `attach` commands |

IO stays at crate edges; provider request bodies intentionally remain
`serde_json::Value` rather than a schema that could drop vendor-specific data.

## Development

Run the repository checks from the workspace root:

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Contributions should preserve the dependency direction documented in
[ARCHITECTURE.md](ARCHITECTURE.md), keep domain logic free of IO where possible,
and include tests for behavior affecting permissions, request-body preservation,
event replay, storage integrity, or HTTP/SSE compatibility.

## What is needed before a stable release?

The most important remaining work is:

1. connect every tool invocation to the permission evaluator and implement a
   safe approval/result continuation API;
2. implement native tools and real MCP client sessions;
3. implement the orchestrator/subagent run loop;
4. add TLS and a documented remote deployment model before allowing non-local
   binds;
5. define and test context compaction behavior;
6. exercise tagged release packaging in CI and add broader end-to-end tests
   against representative providers.

Until then, piqo is best used for local development, API experimentation,
provider request verification, macOS sidecar integration, and work on the
durable harness foundation.
