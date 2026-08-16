//! Permission-gated native tools and MCP client integration.

use piqo_core::{PermissionDecision, PermissionPolicy, ToolRequest};

/// The runtime boundary through which every tool invocation is authorized.
#[derive(Debug, Clone)]
pub struct ToolRuntime {
    policy: PermissionPolicy,
}

impl ToolRuntime {
    pub fn new(policy: PermissionPolicy) -> Self {
        Self { policy }
    }

    pub fn authorize(&self, request: &ToolRequest) -> PermissionDecision {
        self.policy.evaluate(request)
    }
}

/// Configuration for an MCP server launched as a child process over stdio.
/// The actual rmcp client session is intentionally kept at this IO edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerConfig {
    pub command: String,
    pub args: Vec<String>,
}

impl McpServerConfig {
    pub fn new(
        command: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            command: command.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}
