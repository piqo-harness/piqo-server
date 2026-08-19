# Piqo Client Communication Contract

Status: client contract for sidecar protocol version `1` and HTTP API `v1`.

This document is intended to be consumed by a code-generation agent implementing
a Piqo client. It defines the observable contract between a host application and
the private `piqo-server` sidecar. The words **MUST**, **MUST NOT**, **SHOULD**,
and **MAY** are normative.

The sidecar protocol and the HTTP API are versioned independently:

- `protocol_version` covers process startup and shutdown communication;
- `api_version` covers HTTP routes, JSON documents, and SSE events;
- `server_version` is informational and MUST NOT be used in place of either
  protocol version.

## 1. Client responsibilities

A conforming client MUST:

1. start the bundled `piqo-server` executable as a child process with no
   subcommand;
2. keep the child-process handle, stdout pipe, and stderr pipe for the entire
   lifetime of the integration;
3. read exactly one newline-delimited JSON startup message from stdout;
4. validate the startup message before using its URL or token;
5. authenticate every HTTP and SSE request with the announced bearer token;
6. use session event IDs to make event processing replayable and idempotent;
7. send `SIGTERM` to the exact child it started and keep consuming SSE until
   the streams close;
8. treat all identifiers as opaque strings and tolerate additive JSON fields
   and unknown event types.

A client MUST NOT discover a fixed port, read a token from disk, place the token
in a URL, or start a second sidecar for the same profile.

## 2. Process lifecycle protocol

### 2.1 Launch

Run:

```text
piqo-server
```

Normal operation accepts no arguments. `--help` and `--version` are intended
for packaging diagnostics, not application startup.

The child inherits `HOME` and any provider credential environment variables
referenced by `piqo.toml`. `HOME` MUST be set. The sidecar exclusively uses:

```text
$HOME/.config/piqo/piqo.toml
$HOME/.config/piqo/piqo.db
$HOME/.config/piqo/piqo.lock
```

The directory is created with Unix mode `0700` and the lock file with mode
`0600`. The lock is held for the lifetime of the process. Only one sidecar can
use this profile at a time.

stdout is a machine protocol and MUST NOT be shown as logs. stderr contains
human-readable logs and MUST be drained independently so its pipe cannot fill.
The client MUST redact the bearer token from all diagnostics.

### 2.2 Successful startup

After configuration loading, storage recovery, lock acquisition, migrations,
and binding to an ephemeral loopback port, stdout emits one UTF-8 NDJSON line:

```json
{"type":"ready","protocol_version":1,"server_version":"0.1.0","api_version":"v1","pid":1234,"base_url":"http://127.0.0.1:54321","token":"<base64url>"}
```

The client MUST validate all of the following before making a request:

- `type` is `ready`;
- `protocol_version` is exactly `1`;
- `api_version` is exactly `v1`;
- `pid` is a positive integer and matches the spawned child when the platform
  exposes both values;
- `base_url` uses `http`, has host exactly `127.0.0.1`, has a non-zero port,
  and contains no user info, query, or fragment;
- `token` is a non-empty string. In protocol version 1 it is 32 random bytes
  encoded as 43 base64url characters without padding.

The client MUST reject an unsupported protocol/API version and terminate the
child. It MUST build endpoint URLs relative to `base_url`; it MUST NOT replace
the host or port.

Binding has completed when `ready` is emitted, but the HTTP accept loop may not
have been polled yet. The client SHOULD verify readiness with authenticated
`GET /api/v1/health`, retrying transient connection failures with short bounded
backoff. A 15-second overall startup deadline is recommended. If the deadline
expires, terminate the child and report a startup timeout.

### 2.3 Startup failure

Failure before `ready` emits one UTF-8 NDJSON line and exits with status `2`:

```json
{"type":"fatal","protocol_version":1,"code":"config_invalid","message":"human-readable detail"}
```

Stable protocol version 1 codes are:

| Code | Meaning |
| --- | --- |
| `home_unavailable` | `HOME` is absent or unusable. |
| `instance_already_running` | Another process owns `piqo.lock`. |
| `config_invalid` | `piqo.toml` cannot be loaded or parsed. |
| `storage_unavailable` | The profile, token source, or SQLite storage is unavailable. |
| `bind_failed` | The loopback listener could not bind. |

The client MUST branch on `code`, not `message`. The message is diagnostic and
may change. In particular, `instance_already_running` is not an attach protocol:
the existing process token is intentionally not persisted, so the client MUST
surface the ownership conflict instead of attempting to connect to that process.

An EOF or process exit before a complete `ready` or `fatal` line is a malformed
startup. Unknown fields are additive and MUST be ignored. An unknown message
`type`, unknown fatal code, invalid JSON, or unsupported `protocol_version` MUST
be reported as an incompatible/malformed sidecar, preserving the raw line only
if secrets have been redacted.

### 2.4 Exit status

| Status | Meaning |
| --- | --- |
| `0` | Graceful shutdown completed. |
| `1` | Runtime failure or graceful-shutdown deadline exceeded. |
| `2` | Startup failed; a `fatal` line should be available. |

Signals can produce platform-specific statuses if the host force-kills the
process. Such a status is not a graceful exit.

## 3. HTTP transport and authentication

All routes are below `base_url` and `/api/v1`. The sidecar listens only on
loopback and does not provide TLS.

Every request, including health, OpenAPI, history, and SSE, MUST include:

```http
Authorization: Bearer <token>
```

The token MUST remain in memory only. It MUST NOT appear in query parameters,
URLs, persistent preferences, telemetry, crash reports, command-line arguments,
or application logs. A client MUST NOT forward it across redirects or to a host
other than the validated `127.0.0.1:<announced-port>` origin. Redirects SHOULD
be disabled.

JSON request bodies MUST use `Content-Type: application/json`. Success responses
with bodies are JSON unless the route is the SSE stream. Missing or incorrect
authentication returns:

```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
Content-Type: application/json

{"error":{"code":"unauthorized","message":"a valid bearer token is required"}}
```

All JSON API failures use this envelope:

```json
{"error":{"code":"stable_machine_code","message":"human-readable detail"}}
```

Clients MUST use the HTTP status and `error.code` for control flow, never the
message. A client SHOULD preserve unknown error codes as structured failures.
`503/server_shutting_down` means no new work should be submitted and MUST NOT be
retried against that process.

### 3.1 Compatibility check

Request:

```http
GET /api/v1/health
Authorization: Bearer <token>
```

Expected response:

```json
{"status":"ok","server_version":"0.1.0","api_version":"v1"}
```

The client MUST require `status == "ok"` and `api_version == "v1"`. The
authenticated OpenAPI document is available at `GET /api/v1/openapi.json` and
is the schema reference for generated request/response models.

## 4. Core API workflow

The normal client sequence is:

```text
spawn -> ready -> health -> create/get session -> open SSE -> queue run
      -> process events -> terminal run event -> SIGTERM -> SSE EOF -> exit
```

Opening SSE before queueing a run avoids missing live output. Event replay makes
the order safe even when requests race or the stream reconnects.

### 4.1 Projects

A project represents one existing local directory (including a repository root).
Its `path` is an absolute, canonical path and is unique. Create one with:

```http
POST /api/v1/projects
Content-Type: application/json

{"name":"Piqo","path":"/Users/example/src/piqo"}
```

The response is `201` with:

```json
{"id":"opaque-project-id","name":"Piqo","path":"/Users/example/src/piqo","created_at":"timestamp","updated_at":"timestamp"}
```

`GET /api/v1/projects?limit=50&cursor=<opaque>` lists projects. Use `GET`,
`PATCH`, and `DELETE /api/v1/projects/{project_id}` to inspect, update, and
delete a project. A patch accepts either or both of `name` and `path`; a path
is validated and canonicalized again.

Use `GET /api/v1/projects/{project_id}/sessions?limit=50&cursor=<opaque>` to
load the sessions for each project group. `GET /api/v1/sessions?unassigned=true`
returns the separate group of sessions with no project.

Deleting a project cancels its queued or active runs, then permanently deletes
the project, all of its sessions, and their event logs. A client MUST treat a
successful `204` as irreversible. While this work is in progress, mutations
for its sessions return `409/project_deleting`.

### 4.2 Sessions

Create a session:

```http
POST /api/v1/sessions
Content-Type: application/json

{"title":"Optional title","project_id":"opaque-project-id"}
```

`title` and `project_id` may be `null` or omitted. A non-null `project_id`
must identify an existing project. Success is `201` with a session summary:

```json
{
  "id": "opaque-session-id",
  "title": "Optional title",
  "project_id": "opaque-project-id",
  "parent_session_id": null,
  "forked_at_event_id": null,
  "created_at": "timestamp",
  "updated_at": "timestamp",
  "phase": "created",
  "revision": 1,
  "last_event_id": 1,
  "projection": null
}
```

`GET /api/v1/sessions/{session_id}` returns the same summary with a populated
`projection`. `GET /api/v1/sessions?limit=50&cursor=<opaque>` lists sessions;
`limit` is clamped to `1...200`, and `next_cursor: null` means the list is
complete. Cursors are opaque and MUST NOT be decoded or manufactured.

Fork a historical prefix with:

```http
POST /api/v1/sessions/{session_id}/forks
Content-Type: application/json

{"at_event_id":10,"title":"Optional fork title"}
```

Success is `201` with the new session summary. A fork is a new session; its ID
MUST replace the parent ID for subsequent operations on that branch. A fork
inherits its parent session's `project_id`.

### 4.3 Provider catalog

`GET /api/v1/providers` returns:

```json
{
  "providers": [
    {
      "name": "local",
      "protocol": "chat_completions",
      "streaming": true,
      "non_streaming": true,
      "models": ["model-id"]
    }
  ]
}
```

Provider and model identifiers are opaque, case-sensitive strings.

### 4.4 Queue a run

```http
POST /api/v1/sessions/{session_id}/runs
Content-Type: application/json

{
  "provider": "local",
  "model": "model-id",
  "input": "User prompt or any JSON value",
  "agent": null,
  "variant": null,
  "body": {}
}
```

`provider`, `model`, and `input` are required. `agent` and `variant` may be
`null`. `body` SHOULD be a JSON object and may contain arbitrary
provider-specific fields. The client MUST preserve unknown body fields and
MUST NOT normalize provider request bodies.

Success is `202`:

```json
{
  "session_id": "opaque-session-id",
  "run_id": "opaque-run-id",
  "status": "queued",
  "events_url": "/api/v1/sessions/opaque-session-id/events",
  "stream_url": "/api/v1/sessions/opaque-session-id/events/stream"
}
```

Returned URLs are origin-relative and MUST be resolved against the validated
`base_url`. A `202` acknowledges durable queueing, not completion.

Inspect a run with
`GET /api/v1/sessions/{session_id}/runs/{run_id}`. Run status is one of
`queued`, `running`, `requires_action`, `completed`, `failed`, `cancelled`, or
`interrupted`.

Cancel queued or active work with an empty-body request:

```http
POST /api/v1/sessions/{session_id}/runs/{run_id}/cancel
```

Success is `202` with an empty body. Cancellation is asynchronous; the client
MUST wait for the matching `run_cancelled` event or inspect the run.

Retry a `failed`, `cancelled`, or `interrupted` run with an empty-body request:

```http
POST /api/v1/sessions/{session_id}/runs/{run_id}/retries
```

Success is `202` with a new `run_id`. The original ID remains immutable.

If a tool call pauses the queue, `POST /api/v1/sessions/{session_id}/queue/resume`
returns `202` when resumption is valid. API v1 does not yet expose a way to
submit a tool result and continue a `requires_action` run. A client MUST surface
that state; it MUST NOT invent a result or assume the run completed.

## 5. Durable events and SSE

### 5.1 Finite history

Use:

```http
GET /api/v1/sessions/{session_id}/events?after=<event-id>&limit=200
```

`after` is exclusive: only events with a greater ID are returned. Omitting it
means `after=0`. IDs are unsigned decimal integers, monotonically increasing
within one session. A client MAY use this endpoint to resynchronize after an
SSE parsing or transport failure.

### 5.2 Live stream

Open:

```http
GET /api/v1/sessions/{session_id}/events/stream
Accept: text/event-stream
Authorization: Bearer <token>
Last-Event-ID: <last-processed-event-id>
```

The server first replays durable events after `Last-Event-ID`, then publishes
live events on the same connection. If the header is omitted, replay starts
after event `0`. An invalid header returns `400/invalid_cursor`.

Each semantic event is an SSE frame:

```text
id: 42
event: message_content_appended
data: {"id":42,"session_id":"...","schema_version":1,"occurred_at":"...","type":"message_content_appended","data":{"message_id":"...","block":{"kind":"text","value":"hello"}}}

```

The `event:` name and JSON `type` describe the same semantic event. The SSE
`id:` and JSON `id` describe the same durable position. A client MUST parse
standard SSE framing across arbitrary network chunk boundaries; one HTTP chunk
is not necessarily one frame. It MUST ignore comment/keep-alive frames. The
server currently emits a keep-alive comment every 15 seconds.

For each session, the client MUST:

1. initialize `last_processed_id` to `0` or a locally retained checkpoint;
2. ignore an event whose `id <= last_processed_id`;
3. apply an event completely before advancing the checkpoint;
4. reconnect after a non-terminal disconnect with bounded exponential backoff;
5. send the checkpoint as `Last-Event-ID` on every reconnect;
6. stop automatic reconnect when the owned sidecar is shutting down or has
   exited.

At-least-once delivery is possible across reconnection, so UI and storage
updates MUST be idempotent by `(session_id, id)`. Clients MUST tolerate unknown
event types, unknown `data` fields, and a higher event `schema_version` by
retaining or ignoring what they do not understand rather than terminating the
whole stream.

### 5.3 Event schema

Every event JSON object has this envelope:

```json
{
  "id": 42,
  "session_id": "opaque-session-id",
  "schema_version": 1,
  "occurred_at": "timestamp",
  "type": "snake_case_event_name",
  "data": {}
}
```

Current event names are:

```text
session_created                 session_phase_changed
session_interrupted             session_forked
message_started                 message_content_appended
message_completed               message_interrupted
run_queued                      run_started
run_attempt_started             run_attempt_failed
run_completed                   run_failed
run_cancelled                   run_interrupted
run_requires_action             queue_paused
queue_resumed                   tool_call_emitted
tool_result                     agent_phase_changed
permission_requested            permission_resolved
agent_spawned                   agent_finished
```

This list is additive. A client waiting for one run should correlate events by
`data.run_id`, not assume all events on the session belong to that run.

Terminal run events are `run_completed`, `run_failed`, `run_cancelled`, and
`run_interrupted`. `run_requires_action` is non-terminal but blocked. For text
rendering, track assistant messages from `message_started`, then append
`message_content_appended.data.block.value` only when `block.kind == "text"`
and the `message_id` matches. A JSON content block has `block.kind == "json"`
and its `value` is arbitrary JSON.

## 6. Graceful shutdown

The host application owns the sidecar lifetime. To stop it:

1. stop submitting new mutations;
2. send `SIGTERM` to the exact child-process handle;
3. continue processing already-open SSE streams;
4. wait for SSE EOF and child exit concurrently;
5. accept exit status `0` as graceful completion.

Once shutdown begins, new HTTP work may receive
`503/server_shutting_down`. The server cancels active provider requests and
changes every `queued`, `running`, or `requires_action` run to `interrupted`
with `data.reason == "server_shutdown"`. Those final durable events are sent
before SSE closes.

The server's graceful-shutdown deadline is 10 seconds. The host SHOULD wait at
least 12 seconds before using a force-kill as a last resort. A force-kill risks
losing final streamed notifications, although SQLite recovery will mark
unfinished work interrupted on the next start. After an unclean process exit,
the client SHOULD restart normally and rebuild UI state from the durable
session projection/event log instead of assuming in-memory state survived.

## 7. Client state machine

An implementation SHOULD model ownership explicitly:

```text
stopped
  -> starting (child exists; waiting for one NDJSON line)
  -> ready (validated origin/token; health is compatible)
  -> stopping (SIGTERM sent; drain SSE and wait)
  -> stopped (exit 0)

starting -> failed (fatal, timeout, malformed line, incompatible version)
ready    -> failed (unexpected EOF or non-zero exit)
stopping -> failed (deadline exceeded or non-zero exit)
```

The process handle, validated origin, token, and SSE tasks belong to the same
state-machine instance. They MUST NOT be reused across launches. Each new
`ready` line replaces the old origin and token.

## 8. Minimum conformance tests

A generated client is not conforming until automated tests cover:

- parsing valid `ready` and every stable `fatal` code;
- rejecting malformed JSON, unsupported versions, and a non-loopback
  `base_url`;
- authenticated health plus missing/incorrect token failures;
- token redaction from logs and persistence;
- session creation, SSE opened before a run, run correlation, and a terminal
  event;
- SSE frames split across arbitrary byte chunks, duplicate event suppression,
  keep-alives, and `Last-Event-ID` reconnection;
- cancellation and retry using the returned new run ID;
- graceful shutdown during active and queued work, including receipt of
  `run_interrupted` with reason `server_shutdown` before SSE EOF;
- startup timeout, unexpected child exit, second-instance failure, and
  force-kill fallback.

For integration tests, launch the real binary with a temporary `HOME` and a
simulated provider. Never point destructive tests at the user's actual
`~/.config/piqo` profile.
