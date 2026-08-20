# M4 — MCP and Plugin Integration

State: `not_started`

## Outcome

Piqo can launch configured MCP servers over stdio, discover their tools, expose
those tools to models, execute calls through the same permission pipeline as
native tools, and supervise subprocess lifecycle safely.

Plugin support should reuse MCP/JSON-RPC infrastructure. A separate plugin
protocol should be introduced only for requirements MCP cannot represent.

## Current gap

`piqo-tools` depends on the MCP SDK but contains only an `McpServerConfig` value
and a permission wrapper. There is no transport session, handshake, discovery,
execution, or subprocess supervision.

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
