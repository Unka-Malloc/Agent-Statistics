//! Private, bounded request counters for traffic actually handled by the
//! local LLM Gateway. This is intentionally separate from agent token usage.

use anyhow::Result;
use licoup_foundation::platform::file_security::{
    atomic_write_private_text, read_private_text_bounded,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use time::OffsetDateTime;

const SCHEMA: &str = "licoup.llm-gateway-usage.v1";
const MAX_USAGE_BYTES: usize = 512 * 1024;
const MAX_DAYS: usize = 90;
const MAX_MODELS_PER_DAY: usize = 64;
const MAX_MODEL_BYTES: usize = 128;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UsageDay {
    agents: BTreeMap<String, u64>,
    models: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UsageDocument {
    schema_version: String,
    days: BTreeMap<String, UsageDay>,
}

impl Default for UsageDocument {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA.to_owned(),
            days: BTreeMap::new(),
        }
    }
}

pub struct GatewayUsageRecorder {
    path: PathBuf,
    document: Mutex<UsageDocument>,
}

impl GatewayUsageRecorder {
    pub fn open(path: PathBuf) -> Result<Self> {
        let document = read_private_text_bounded(&path, MAX_USAGE_BYTES)?
            .and_then(|text| serde_json::from_str::<UsageDocument>(&text).ok())
            .filter(|document| document.schema_version == SCHEMA)
            .unwrap_or_default();
        Ok(Self {
            path,
            document: Mutex::new(document),
        })
    }

    pub fn record(&self, path: &str, user_agent: Option<&str>, body: &[u8]) {
        let Some(agent) = gateway_agent(path, user_agent) else {
            return;
        };
        let Some(model) = gateway_model(body) else {
            return;
        };
        let day = OffsetDateTime::now_utc().date().to_string();
        let Ok(mut document) = self.document.lock() else {
            return;
        };
        let usage = document.days.entry(day).or_default();
        increment(&mut usage.agents, agent);
        if usage.models.contains_key(&model) || usage.models.len() < MAX_MODELS_PER_DAY {
            increment(&mut usage.models, &model);
        } else {
            increment(&mut usage.models, "Other");
        }
        while document.days.len() > MAX_DAYS {
            let Some(oldest) = document.days.keys().next().cloned() else {
                break;
            };
            document.days.remove(&oldest);
        }
        if let Ok(encoded) = serde_json::to_string(&*document) {
            let _ = atomic_write_private_text(&self.path, &encoded);
        }
    }

    pub fn snapshot(&self) -> Value {
        let Ok(document) = self.document.lock() else {
            return json!({"ok": false, "schemaVersion": SCHEMA, "days": []});
        };
        json!({
            "ok": true,
            "schemaVersion": SCHEMA,
            "days": document.days.iter().map(|(date, usage)| json!({
                "date": date,
                "agents": usage.agents,
                "models": usage.models,
            })).collect::<Vec<_>>(),
        })
    }
}

pub fn read_usage(path: &Path) -> Result<Value> {
    Ok(GatewayUsageRecorder::open(path.to_path_buf())?.snapshot())
}

fn increment(values: &mut BTreeMap<String, u64>, key: &str) {
    let count = values.entry(key.to_owned()).or_default();
    *count = count.saturating_add(1);
}

fn gateway_agent(path: &str, user_agent: Option<&str>) -> Option<&'static str> {
    let agent = user_agent.unwrap_or_default().to_ascii_lowercase();
    if path == "/v1/messages" || agent.contains("claude") {
        return Some("claude-code");
    }
    if matches!(path, "/v1/responses" | "/v1/chat/completions") {
        return Some("codex");
    }
    None
}

fn gateway_model(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let raw = value.get("model")?.as_str()?.trim();
    if raw.is_empty()
        || raw.len() > MAX_MODEL_BYTES
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
    {
        return None;
    }
    Some(raw.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_request_attribution_is_path_and_model_bounded() {
        assert_eq!(gateway_agent("/v1/messages", None), Some("claude-code"));
        assert_eq!(gateway_agent("/v1/responses", None), Some("codex"));
        assert_eq!(gateway_agent("/v1/chat/completions", None), Some("codex"));
        assert_eq!(gateway_agent("/health", None), None);
        assert_eq!(
            gateway_model(br#"{"model":"deepseek-chat"}"#).as_deref(),
            Some("deepseek-chat")
        );
        assert!(gateway_model(br#"{"model":"bad model"}"#).is_none());
    }

    #[test]
    fn recorder_persists_separate_agent_and_model_request_counts() {
        let root =
            std::env::temp_dir().join(format!("licoup-gateway-usage-{}", uuid::Uuid::new_v4()));
        licoup_foundation::platform::file_security::ensure_private_dir(&root).unwrap();
        let path = root.join("usage.json");
        let recorder = GatewayUsageRecorder::open(path.clone()).unwrap();
        recorder.record(
            "/v1/responses",
            Some("codex-cli"),
            br#"{"model":"kimi-k2"}"#,
        );
        recorder.record(
            "/v1/messages",
            Some("claude-code"),
            br#"{"model":"deepseek-chat"}"#,
        );
        let reloaded = read_usage(&path).unwrap();
        let day = &reloaded["days"][0];
        assert_eq!(day["agents"]["codex"], json!(1));
        assert_eq!(day["agents"]["claude-code"], json!(1));
        assert_eq!(day["models"]["kimi-k2"], json!(1));
        assert_eq!(day["models"]["deepseek-chat"], json!(1));
        let _ = std::fs::remove_dir_all(root);
    }
}
