use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The result of the pure permission decision function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionDecision {
    Allow,
    Ask,
    Deny,
}

/// A tool invocation presented to the permission evaluator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRequest {
    pub agent_id: String,
    pub tool_name: String,
    pub arguments: Value,
}

/// An exact-match rule. Command parsing and constrained shell policies belong
/// here later; glob matching is intentionally not used for shell commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRule {
    pub agent_id: Option<String>,
    pub tool_name: String,
    pub decision: PermissionDecision,
}

impl PermissionRule {
    pub fn new(
        agent_id: Option<impl Into<String>>,
        tool_name: impl Into<String>,
        decision: PermissionDecision,
    ) -> Self {
        Self {
            agent_id: agent_id.map(Into::into),
            tool_name: tool_name.into(),
            decision,
        }
    }

    fn matches(&self, request: &ToolRequest) -> bool {
        self.tool_name == request.tool_name
            && self
                .agent_id
                .as_deref()
                .is_none_or(|agent_id| agent_id == request.agent_id)
    }
}

/// Pure, ordered permission policy. The first matching exact rule wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionPolicy {
    rules: Vec<PermissionRule>,
    default_decision: PermissionDecision,
}

impl PermissionPolicy {
    pub fn new(default_decision: PermissionDecision) -> Self {
        Self {
            rules: Vec::new(),
            default_decision,
        }
    }

    pub fn add_rule(&mut self, rule: PermissionRule) {
        self.rules.push(rule);
    }

    pub fn evaluate(&self, request: &ToolRequest) -> PermissionDecision {
        self.rules
            .iter()
            .find(|rule| rule.matches(request))
            .map_or(self.default_decision, |rule| rule.decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_agent_specific_rules_before_the_default() {
        let mut policy = PermissionPolicy::new(PermissionDecision::Deny);
        policy.add_rule(PermissionRule::new(
            Some("reviewer"),
            "read",
            PermissionDecision::Allow,
        ));

        let request = ToolRequest {
            agent_id: "reviewer".into(),
            tool_name: "read".into(),
            arguments: serde_json::json!({"path": "README.md"}),
        };
        assert_eq!(policy.evaluate(&request), PermissionDecision::Allow);

        let other_agent = ToolRequest {
            agent_id: "worker".into(),
            ..request
        };
        assert_eq!(policy.evaluate(&other_agent), PermissionDecision::Deny);
    }

    #[test]
    fn preserves_ask_as_a_first_class_decision() {
        let mut policy = PermissionPolicy::new(PermissionDecision::Deny);
        policy.add_rule(PermissionRule::new(
            None::<String>,
            "bash",
            PermissionDecision::Ask,
        ));

        let request = ToolRequest {
            agent_id: "worker".into(),
            tool_name: "bash".into(),
            arguments: serde_json::json!({"command": "git status"}),
        };
        assert_eq!(policy.evaluate(&request), PermissionDecision::Ask);
    }
}
