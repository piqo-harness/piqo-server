# M4 — MCP and Plugin Integration

State: `implementing`

## Outcome

Piqo can launch configured MCP servers over stdio, discover their tools, expose
those tools to models, execute calls through the same permission pipeline as
native tools, and supervise subprocess lifecycle safely.

Plugin support should reuse MCP/JSON-RPC infrastructure. A separate plugin
protocol should be introduced only for requirements MCP cannot represent.

## Implementation status and remaining work

The first implementation slice is merged: configured stdio children are
started with a minimal environment, initialized through `rmcp`, discovered with
pagination, and exposed through the MCP permission and M1 continuation path.
The server also publishes sanitized diagnostics at `GET /api/v1/mcp/servers`.

M4 must remain `implementing` until the following gaps are closed and the
acceptance criteria below are demonstrated:

- Add a deterministic Rust MCP stdio fixture and `piqo-tools` integration tests
  covering initialization, paginated `tools/list`, continuous stderr draining,
  `tools/list_changed` refresh, `tools/call`, bounded shutdown, and absence of
  orphaned children.
- Exercise all failure paths against that fixture: invalid JSON Schema and
  JSON-RPC, incompatible names and collisions, timeout, child crash,
  oversized result, cancellation, bounded restart, and a side-effect counter
  proving an uncertain call is never replayed.
- Add API end-to-end tests in which a provider emits an MCP call and Piqo
  verifies allow, ask, and deny decisions; the allow path must persist exactly
  one started event and exactly one result, then resume the same run. Also
  verify that a client cannot submit a result for a server-managed MCP call.
- Add configuration-reload coverage for adding, modifying, and removing MCP
  servers. It must show that newly created turns use the new catalog while a
  previously emitted turn remains routable against its announced catalog
  generation.
- Close and test the catalog-generation boundary used during provider routing:
  a notification or reload after a turn is announced must not change the set
  of MCP tools or schemas used to interpret that existing turn.
- Run and record the locked workspace validation commands after the fixture and
  API coverage land; only then may the tracker move through `validating` to
  `complete`.

## Required design decisions

- Configuration schema and reload behavior for MCP servers.
- Stable tool naming and collision handling across native and MCP namespaces.
- Startup mode: eager, lazy, or hybrid.
- Capability refresh and behavior when a server changes its schemas.
- Permission mapping for dynamically discovered tools.
- Environment and secret injection policy.
- Crash restart policy and limits.
- Whether plugin metadata needs capabilities outside MCP.

## Implementation slices

### 1. Configuration and redaction

- Add MCP server definitions with command, arguments, cwd, selected environment,
  startup timeout, and enablement.
- Never return environment values or credentials from API responses.
- Atomically validate and replace MCP configuration on reload.

### 2. Client lifecycle

- Spawn subprocesses with piped stdio and independently drained stderr.
- Perform MCP initialization and capability negotiation.
- Track healthy, starting, failed, and stopped states.
- Shut down children gracefully, then force termination within a bound.
- Prevent orphan processes on server shutdown or failed startup.

### 3. Tool catalog

- Discover names, descriptions, and JSON schemas.
- Namespace tools deterministically and reject ambiguous collisions.
- Translate definitions to provider request bodies without normalizing unrelated
  provider fields.
- Expose diagnostics without exposing secrets or raw sensitive results.

### 4. Invocation

- Validate arguments against the advertised contract where practical.
- Evaluate permissions before sending JSON-RPC.
- Apply timeouts, cancellation, and response-size limits.
- Persist one durable result and continue through M1.
- Never automatically retry a side-effecting tool call after an uncertain
  transport failure.

### 5. Plugin compatibility

- Document which plugin needs are satisfied directly by MCP.
- If an extension is necessary, version it and keep lifecycle, permission, and
  cancellation behavior identical to MCP tools.

## Required tests

- Handshake, discovery, invocation, shutdown, and stderr draining.
- Invalid schemas, malformed JSON-RPC, timeouts, crashes, and oversized results.
- Tool name collisions.
- Permission allow/ask/deny.
- Configuration reload adding, changing, and removing servers.
- No child processes left after shutdown.
- No duplicate side effects after an uncertain call result.

## Acceptance criteria

- A fixture MCP server can complete an end-to-end model tool turn.
- MCP calls cannot bypass the permission evaluator.
- Child processes and IO are bounded and cleaned up deterministically.
- The tool catalog remains stable and collision-free.
- Configuration and protocol behavior are documented and tested.

## Explicitly out of scope

- Network MCP transports unless separately designed.
- A public plugin marketplace.
- Automatic installation of third-party binaries.
