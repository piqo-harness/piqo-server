use serde_json::{Map, Value};
use thiserror::Error;

/// Merge provider body layers from lowest to highest precedence.
///
/// The values remain untyped JSON: unknown provider-specific keys are retained
/// and later layers replace earlier values without normalization or renaming.
pub fn merge_request_bodies(
    layers: impl IntoIterator<Item = Value>,
) -> Result<Value, BodyMergeError> {
    let mut merged = Map::new();

    for (index, layer) in layers.into_iter().enumerate() {
        let object = layer
            .as_object()
            .ok_or(BodyMergeError::LayerIsNotAnObject { index })?;
        for (key, value) in object {
            merged.insert(key.clone(), value.clone());
        }
    }

    Ok(Value::Object(merged))
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BodyMergeError {
    #[error("provider body layer {index} is not a JSON object")]
    LayerIsNotAnObject { index: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lets_the_request_layer_win_and_preserves_unknown_keys() {
        let body = merge_request_bodies([
            serde_json::json!({"temperature": 0.2, "chat_template_kwargs": {"enable_thinking": true}}),
            serde_json::json!({"temperature": 0.7, "top_k": 40}),
            serde_json::json!({"temperature": 0.9, "vendor_flag": "untouched"}),
        ])
        .expect("all layers are objects");

        assert_eq!(body["temperature"], serde_json::json!(0.9));
        assert_eq!(body["top_k"], serde_json::json!(40));
        assert_eq!(body["vendor_flag"], serde_json::json!("untouched"));
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], true);
    }

    #[test]
    fn rejects_a_non_object_layer_instead_of_silently_dropping_it() {
        let result = merge_request_bodies([serde_json::json!({}), serde_json::json!(null)]);
        assert!(matches!(
            result,
            Err(BodyMergeError::LayerIsNotAnObject { index: 1 })
        ));
    }
}
