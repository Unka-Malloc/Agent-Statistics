//! Actual historical request options. Never reads a current Agent setting.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageVariant {
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub fast: Option<bool>,
}

impl UsageVariant {
    pub fn from_metadata(value: &Value) -> Self {
        let candidate = Value::Object(crate::normalize::request_usage_metadata(value));
        let effort = [
            "reasoning_effort",
            "reasoningEffort",
            "effort",
            "thinking_level",
            "thinkingLevel",
        ]
        .into_iter()
        .filter_map(|key| candidate.get(key).and_then(Value::as_str))
        .find_map(normalize_effort)
        .or_else(|| {
            value
                .get("variant")
                .and_then(Value::as_str)
                .and_then(selector_effort)
        })
        .or_else(|| {
            value
                .get("model")
                .and_then(Value::as_object)
                .and_then(|model| model.get("variant"))
                .and_then(Value::as_str)
                .and_then(selector_effort)
        });
        let fast = candidate.get("fast").and_then(Value::as_bool);
        Self { effort, fast }
    }

    pub fn with_fallback(&self, fallback: &Self) -> Self {
        Self {
            effort: self.effort.clone().or_else(|| fallback.effort.clone()),
            fast: self.fast.or(fallback.fast),
        }
    }

    pub fn label(&self) -> Option<String> {
        let effort = self.effort.as_deref().map(|effort| match effort {
            "low" => "Low",
            "medium" => "Medium",
            "high" => "High",
            "xhigh" => "Extra High",
            "max" => "Max",
            "ultra" => "Ultra",
            "none" => "None",
            "minimal" => "Minimal",
            "auto" => "Auto",
            "adaptive" => "Adaptive",
            "dynamic" => "Dynamic",
            other => other,
        });
        match (effort, self.fast) {
            (Some(effort), Some(true)) => Some(format!("{effort} Fast")),
            (Some(effort), _) => Some(effort.to_owned()),
            (None, Some(true)) => Some("Fast".to_owned()),
            (None, _) => None,
        }
    }

    pub fn from_label(label: &str) -> Self {
        let (effort, fast) = label
            .strip_suffix(" Fast")
            .map(|effort| (effort, Some(true)))
            .unwrap_or_else(|| {
                if label == "Fast" {
                    ("", Some(true))
                } else {
                    (label, None)
                }
            });
        Self {
            effort: if effort == "Unspecified" {
                None
            } else {
                normalize_effort(effort)
            },
            fast,
        }
    }
}

fn selector_effort(value: &str) -> Option<String> {
    normalize_effort(value).filter(|value| {
        matches!(
            value.as_str(),
            "none"
                | "minimal"
                | "low"
                | "medium"
                | "high"
                | "xhigh"
                | "max"
                | "ultra"
                | "auto"
                | "adaptive"
                | "dynamic"
        )
    })
}

fn normalize_effort(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "default" | "unknown" | "unspecified") {
        return None;
    }
    let normalized = match normalized.as_str() {
        "extra high" | "extra-high" | "extra_high" => "xhigh".to_owned(),
        _ => normalized,
    };
    (!normalized.is_empty()
        && normalized.len() <= 32
        && normalized
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')))
    .then_some(normalized)
}

/// Traverse only structured request/usage containers, never message content,
/// tool results, arbitrary strings, or arrays of conversation text.
fn metadata_objects(value: &Value) -> Vec<&Value> {
    let mut objects = vec![value];
    let mut start = 0;
    for _ in 0..3 {
        let end = objects.len();
        for index in start..end {
            for key in [
                "usage",
                "token_usage",
                "tokenUsage",
                "usageMetadata",
                "usage_metadata",
                "tokenCount",
                "token_count",
                "responseUsage",
                "response_usage",
                "gen_ai.usage",
                "tokens",
                "status",
                "message",
                "response",
                "modelInfo",
                "data",
                "payload",
                "request",
                "metadata",
                "options",
                "reasoning",
                "generationConfig",
                "output_config",
                "thinking",
            ] {
                if let Some(child) = objects[index].get(key).filter(|v| v.is_object()) {
                    objects.push(child);
                }
            }
        }
        start = end;
    }
    objects
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRequestContext {
    pub model: Option<String>,
    pub variant: UsageVariant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_explicit_options_without_using_thinking_text_or_token_counts() {
        let options = UsageVariant::from_metadata(
            &json!({"payload": {"reasoning": {"effort": "extra_high"}, "fast": false}}),
        );
        assert_eq!(options.effort.as_deref(), Some("xhigh"));
        assert_eq!(options.fast, Some(false));
        assert_eq!(options.label().as_deref(), Some("Extra High"));
        assert_eq!(
            UsageVariant::from_metadata(&json!({"thinking": "high", "reasoning_tokens": 900})),
            UsageVariant::default()
        );
        let conflict = UsageVariant::from_metadata(
            &json!({"effort":"high","service_tier":"default","collaboration_mode":{"settings":{"reasoning_effort":"xhigh","fast":true}}}),
        );
        assert_eq!(
            conflict,
            UsageVariant {
                effort: Some("high".into()),
                fast: Some(false)
            }
        );
        assert_eq!(UsageVariant::from_metadata(&json!({"collaboration_mode":{"settings":{"reasoning_effort":"xhigh"}},"service_tier":"priority"})).label().as_deref(),Some("Extra High Fast"));
        for missing in ["default", "Unspecified", "UNKNOWN"] {
            assert!(
                UsageVariant::from_metadata(&json!({"effort":missing}))
                    .effort
                    .is_none()
            );
        }
    }

    #[test]
    fn preserves_serving_provider_independently_from_slashes_in_model_id() {
        for metadata in [
            json!({"modelID":"lab/flash-latest","providerID":"relay-a","effort":"high"}),
            json!({"model":{"id":"lab/flash-latest","providerID":"relay-a","variant":"high"}}),
            json!({"model":{"id":"lab/flash-latest","variant":"high"},"providerID":"relay-a"}),
        ] {
            let raw = model_label(&metadata).unwrap();
            let selected = model_selection(&raw);
            assert_eq!(selected.id, "lab/flash-latest");
            assert_eq!(selected.provider_id.as_deref(), Some("relay-a"));
            assert_eq!(selected.unresolved_identity(), "relay-a/lab/flash-latest");
            assert_eq!(
                UsageVariant::from_metadata(&metadata)
                    .with_fallback(&selected.variant)
                    .label()
                    .as_deref(),
                Some("High")
            );
        }
        let nested = model_label(&json!({"model":{"id":"lab/flash-latest","providerID":"relay-b"},"providerID":"relay-a"})).unwrap();
        assert_eq!(
            model_selection(&nested).provider_id.as_deref(),
            Some("relay-b")
        );
        let request =
            json!({"id":"lab/flash-latest","providerID":"relay-a","variant":"high"}).to_string();
        let response = json!({"id":"lab/flash-latest","variant":"max"}).to_string();
        assert!(compatible_recorded_models(&request, &response));
        assert!(!compatible_recorded_models(&request, &nested));
        let completed = model_selection(&model_with_provider_fallback(&response, &request));
        assert_eq!(completed.provider_id.as_deref(), Some("relay-a"));
        assert_eq!(completed.variant.effort.as_deref(), Some("max"));
        assert_eq!(
            model_with_provider_fallback("different-model", &request),
            "different-model"
        );
    }

    #[test]
    fn decodes_complete_selectors_and_rejects_structural_fragments_as_model_ids() {
        let selector = json!({
            "modelID":"anthropic/claude-opus-4.6", "providerID":"synthetic-relay",
            "variant":"high", "fast":false
        });
        for encoded in [
            selector.to_string(),
            json!(selector.to_string()).to_string(),
        ] {
            let selection = model_selection(&encoded);
            assert_eq!(selection.id, "anthropic/claude-opus-4.6");
            assert_eq!(selection.provider_id.as_deref(), Some("synthetic-relay"));
            assert_eq!(selection.variant.label().as_deref(), Some("High"));
            assert_eq!(selection.variant.fast, Some(false));
        }
        let encoded = json!(
            json!({"id":"anthropic/claude-opus-4.6","variant":"high","fast":false}).to_string()
        )
        .to_string();
        let labeled =
            model_label(&json!({"model":encoded,"providerID":"synthetic-relay"})).unwrap();
        let selection = model_selection(&labeled);
        assert_eq!(selection.provider_id.as_deref(), Some("synthetic-relay"));
        assert_eq!(selection.variant.label().as_deref(), Some("High"));
        assert_eq!(selection.variant.fast, Some(false));
        for invalid in [
            r#"claude-opus-4.6","providerid":"synthetic-relay","variant":"high"}"#,
            "claude-opus-4.6\" ,\n  \"providerid\" : \"synthetic-relay\",\n  \"variant\" : \"high\"\n}",
            r#"synthetic/{"id":"anthropic/claude-opus-4.6"}"#,
            "synthetic/{\n  \"id\": \"anthropic/claude-opus-4.6\"\n}",
            r#"{"providerID":"synthetic-relay","variant":"high"}"#,
            r#"["claude-opus-4.6"]"#,
            r#"{"id":["claude-opus-4.6"]}"#,
        ] {
            let selection = model_selection(invalid);
            assert!(selection.id.is_empty());
            assert!(!compatible_recorded_models(invalid, invalid));
        }
        let missing = model_selection(r#"{"providerID":"synthetic-relay","variant":"high"}"#);
        assert_eq!(missing.variant.label().as_deref(), Some("High"));
        let fragment =
            model_selection(r#"claude-opus-4.6","providerid":"synthetic-relay","variant":"high"}"#);
        assert_eq!(fragment.variant, UsageVariant::default());
        assert_eq!(
            model_selection("custom/model-high[1m]").id,
            "custom/model-high[1m]"
        );
        assert_eq!(model_selection("123").id, "123");
        assert_eq!(
            model_selection("custom-{blue}, high:fast").id,
            "custom-{blue}, high:fast"
        );
    }
}

/// Structured selectors are request facts, including JSON-string selectors
/// written by OpenCode/Kilo. `variant: default` is not an observed effort.
#[derive(Default)]
pub struct ModelSelection {
    pub id: String,
    pub provider_id: Option<String>,
    pub variant: UsageVariant,
}

impl ModelSelection {
    /// Unknown selectors retain their recorded provider namespace. A slash in
    /// the selector itself does not establish which provider served it.
    pub fn unresolved_identity(&self) -> String {
        self.provider_id
            .as_ref()
            .map(|provider| format!("{provider}/{}", self.id))
            .unwrap_or_else(|| self.id.clone())
    }
}

pub fn model_selection(raw: &str) -> ModelSelection {
    let value = match serde_json::from_str::<Value>(raw) {
        Ok(Value::String(encoded)) => return model_selection(&encoded),
        Ok(value @ Value::Object(_)) => value,
        Ok(Value::Array(_)) => return ModelSelection::default(),
        Ok(_) | Err(_) => {
            return ModelSelection {
                id: if has_structured_selector_syntax(raw) {
                    String::new()
                } else {
                    raw.trim().to_owned()
                },
                ..Default::default()
            };
        }
    };
    let id = ["id", "modelID", "modelId", "model_id", "model", "name"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str));
    let mut selection = id.map(model_selection).unwrap_or_default();
    if selection.provider_id.is_none() {
        selection.provider_id = recorded_provider(&value).map(str::to_owned);
    }
    selection.variant = UsageVariant::from_metadata(&value).with_fallback(&selection.variant);
    selection
}

pub fn is_placeholder_id(id: &str) -> bool {
    matches!(
        id.rsplit('/')
            .next()
            .unwrap_or(id)
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "" | "default" | "auto" | "unknown" | "unspecified" | "others"
    )
}

fn has_concrete_model(raw: &str) -> bool {
    let selected = model_selection(raw);
    !selected.id.is_empty() && !is_placeholder_id(&selected.id)
}

/// Select only between facts the caller has already associated with one
/// record, request, response, or turn. Never searches session state or config.
pub fn prefer_recorded_model(primary: Option<String>, related: Option<String>) -> Option<String> {
    if primary.as_deref().is_some_and(has_concrete_model) {
        primary
    } else if related.as_deref().is_some_and(has_concrete_model) {
        related
    } else {
        primary.or(related)
    }
}

/// A serialized container or a remaining JSON field tail is not a model ID.
/// Keep the source elsewhere unchanged; never reconstruct missing fields from
/// fragments or pass request metadata through the display-name formatter.
fn has_structured_selector_syntax(raw: &str) -> bool {
    let raw = raw.trim();
    raw.starts_with(['{', '[', '"'])
        || raw
            .match_indices('{')
            .any(|(index, _)| raw[index + 1..].trim_start().starts_with('"'))
        || (raw.ends_with('}')
            && raw.match_indices('"').any(|(index, _)| {
                let Some(rest) = raw[index + 1..].trim_start().strip_prefix(',') else {
                    return false;
                };
                let Some(field) = rest.trim_start().strip_prefix('"') else {
                    return false;
                };
                field.split_once('"').is_some_and(|(key, rest)| {
                    !key.is_empty() && rest.trim_start().starts_with(':')
                })
            }))
}

/// A placeholder can be completed by evidence from the same associated request,
/// but cannot bridge two explicitly different serving providers.
pub fn compatible_recorded_models(left: &str, right: &str) -> bool {
    let left = model_selection(left);
    let right = model_selection(right);
    !left.id.is_empty()
        && !right.id.is_empty()
        && (left.id == right.id || is_placeholder_id(&left.id) || is_placeholder_id(&right.id))
        && match (&left.provider_id, &right.provider_id) {
            (Some(left), Some(right)) => left == right,
            _ => true,
        }
}

pub fn model_with_provider_fallback(raw: &str, request: &str) -> String {
    let current = model_selection(raw);
    let request = model_selection(request);
    if current.id.is_empty() || current.provider_id.is_some() || current.id != request.id {
        return raw.to_owned();
    }
    let Some(provider) = request.provider_id else {
        return raw.to_owned();
    };
    let mut selection =
        selector_object(raw).unwrap_or_else(|| serde_json::json!({"id":current.id}));
    selection["providerID"] = Value::String(provider);
    selection.to_string()
}

fn selector_object(raw: &str) -> Option<Value> {
    match serde_json::from_str(raw).ok()? {
        object @ Value::Object(_) => Some(object),
        Value::String(encoded) => selector_object(&encoded),
        _ => None,
    }
}

/// Preserve only the structured model selection, never the surrounding request.
pub fn model_label(value: &Value) -> Option<String> {
    let mut fallback = None;
    for candidate in metadata_objects(value) {
        for key in [
            "model",
            "modelId",
            "modelID",
            "model_id",
            "modelName",
            "model_name",
            "modelLabel",
            "model_label",
        ] {
            let Some(model) = candidate.get(key) else {
                continue;
            };
            if let Some(text) = model
                .as_str()
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                if let Some(provider) = recorded_provider(candidate) {
                    let selection = model_selection(text);
                    let request = serde_json::json!({"id":selection.id,"providerID":provider});
                    let labeled = model_with_provider_fallback(text, &request.to_string());
                    fallback = prefer_recorded_model(fallback, Some(labeled));
                } else {
                    fallback = prefer_recorded_model(fallback, Some(text.to_owned()));
                }
                if fallback.as_deref().is_some_and(has_concrete_model) {
                    return fallback;
                }
                continue;
            }
            if let Some(object) = model.as_object() {
                let mut selected = [
                    "id",
                    "modelID",
                    "modelId",
                    "model_id",
                    "name",
                    "providerID",
                    "providerId",
                    "provider_id",
                    "provider",
                    "variant",
                    "reasoning_effort",
                    "reasoningEffort",
                    "effort",
                    "fast",
                ]
                .into_iter()
                .filter_map(|key| {
                    object
                        .get(key)
                        .filter(|value| value.is_string() || value.is_boolean())
                        .map(|value| (key.to_owned(), value.clone()))
                })
                .collect::<serde_json::Map<_, _>>();
                if recorded_provider(model).is_none()
                    && let Some(provider) = recorded_provider(candidate)
                {
                    selected.insert("providerID".to_owned(), Value::String(provider.to_owned()));
                }
                if ["id", "modelID", "modelId", "model_id", "name"]
                    .into_iter()
                    .any(|key| selected.contains_key(key))
                {
                    fallback =
                        prefer_recorded_model(fallback, Some(Value::Object(selected).to_string()));
                    if fallback.as_deref().is_some_and(has_concrete_model) {
                        return fallback;
                    }
                }
            }
        }
    }
    fallback
}

fn recorded_provider(value: &Value) -> Option<&str> {
    ["providerID", "providerId", "provider_id", "provider"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
}
