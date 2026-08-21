use serde::{Deserialize, Serialize};

use crate::EventId;

pub const CONTEXT_ESTIMATOR_VERSION: &str = "utf8_bytes_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStrategy {
    Deterministic,
    Llm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextFact {
    pub fact_id: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCorrelation {
    pub call_id: String,
    pub tool_name: String,
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextArtifact {
    pub artifact_id: String,
    pub supersedes_artifact_id: Option<String>,
    pub strategy: CompactionStrategy,
    pub strategy_version: u16,
    pub source_start_event_id: EventId,
    pub source_end_event_id: EventId,
    pub context_window_tokens: u64,
    pub output_reserve_tokens: u64,
    pub estimated_input_tokens: u64,
    pub target_input_tokens: u64,
    pub estimator_version: String,
    pub summary: String,
    #[serde(default)]
    pub tool_correlations: Vec<ToolCorrelation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextProjection {
    #[serde(default)]
    pub durable_facts: Vec<ContextFact>,
    #[serde(default)]
    pub active_artifact: Option<ContextArtifact>,
    #[serde(default)]
    pub last_failure: Option<String>,
    #[serde(default)]
    pub last_bypass_reason: Option<String>,
}

pub fn estimate_tokens(
    serialized_bytes: usize,
    transcript_items: usize,
    tool_definitions: usize,
) -> u64 {
    let bytes = u64::try_from(serialized_bytes).unwrap_or(u64::MAX);
    bytes.saturating_add(2).saturating_div(3).saturating_add(
        u64::try_from(transcript_items.saturating_add(tool_definitions))
            .unwrap_or(u64::MAX)
            .saturating_mul(8),
    )
}

#[cfg(test)]
mod tests {
    use super::estimate_tokens;

    #[test]
    fn estimates_utf8_bytes_with_structural_overhead() {
        assert_eq!(estimate_tokens(10, 2, 1), 28);
    }
}
