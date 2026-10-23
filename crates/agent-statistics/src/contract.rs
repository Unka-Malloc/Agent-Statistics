//! Stable local-token-usage contract and aggregation models.

use crate::model_identity::project_model_usage;
pub use crate::variant::UsageVariant;
pub const USAGE_PARSER_REVISION: &str = "request-variant-usage-v1";
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const AGENT_USAGE_SCHEMA_VERSION: u32 = 7;
pub const AGENT_USAGE_MODE: &str = "local-token-usage";
pub const AGENT_USAGE_TOKEN_SOURCE_MODE: &str = "native-metadata-first-incremental";
pub const REPORT_COLLECTION: &str = "agent-usage-reports";
pub const MAX_REPORTS: usize = 20;
pub const DEFAULT_USAGE_WINDOW_DAYS: u64 = 30;
pub const MAX_USAGE_WINDOW_DAYS: u64 = 90;
pub const UNATTRIBUTED_MODEL: &str = "Others";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AgentDef {
    pub id: &'static str,
    pub label: &'static str,
}

/// The usage-capable agents the host catalog publishes. A process without an
/// installed host answers with an empty list rather than inventing a catalog.
pub fn supported_agents() -> Vec<AgentDef> {
    crate::host::host()
        .map(|host| (host.supported_agents)())
        .unwrap_or_default()
}

#[derive(Clone, Debug, Default)]
pub struct HistoryUsageSummary {
    pub source: Option<&'static str>,
    pub session_count: u64,
    pub message_count: u64,
    pub explicit_prompt_tokens: u64,
    pub explicit_cached_input_tokens: u64,
    pub explicit_completion_tokens: u64,
    pub explicit_total_tokens: u64,
    pub estimated_prompt_tokens: u64,
    pub estimated_completion_tokens: u64,
    pub estimated_total_tokens: u64,
    pub explicit_records: u64,
    pub estimated_records: u64,
    /// Present usage events whose provider payload carried no token fields.
    /// They are counted as requests only: no token value is invented for them.
    pub token_unavailable_records: u64,
    pub source_paths: BTreeSet<String>,
    pub skipped: Vec<Value>,
    pub daily_usage: BTreeMap<String, DailyUsageSummary>,
    pub scan_cache: Option<Value>,
}

impl HistoryUsageSummary {
    pub fn prompt_tokens(&self) -> u64 {
        self.explicit_prompt_tokens
            .saturating_add(self.estimated_prompt_tokens)
    }

    pub fn completion_tokens(&self) -> u64 {
        self.explicit_completion_tokens
            .saturating_add(self.estimated_completion_tokens)
    }

    pub fn total_tokens(&self) -> u64 {
        self.explicit_total_tokens
            .saturating_add(self.estimated_total_tokens)
    }

    pub fn confidence(&self) -> &'static str {
        if self.token_unavailable_records > 0 {
            // A source that omits token fields for part of its requests can
            // never be high confidence: the missing consumption is real and
            // unbounded, so the totals stay explicitly partial.
            return if self.explicit_records > 0 {
                "medium"
            } else {
                "low"
            };
        }
        if self.explicit_records > 0 && self.estimated_records == 0 {
            "high"
        } else if self.explicit_records > 0 && self.estimated_records > 0 {
            "medium"
        } else if self.estimated_records > 0 {
            "low"
        } else {
            "unavailable"
        }
    }

    #[cfg(test)]
    pub fn to_json(&self) -> Value {
        self.to_json_for_agent(None, &crate::registry::snapshot())
    }

    pub fn to_json_for_agent(
        &self,
        source_agent: Option<&str>,
        registry: &crate::registry::RegistrySnapshot,
    ) -> Value {
        json!({
            "sessionCount": self.session_count,
            "messageCount": self.message_count,
            "promptTokens": self.prompt_tokens(),
            "cachedInputTokens": self.explicit_cached_input_tokens,
            "completionTokens": self.completion_tokens(),
            "totalTokens": self.total_tokens(),
            "explicitCoverage": if self.total_tokens() > 0 {
                self.explicit_total_tokens as f64 / self.total_tokens() as f64
            } else { 0.0 },
            "tokenSourceBreakdown": {
                "explicitRecords": self.explicit_records,
                "estimatedRecords": self.estimated_records,
                "tokenUnavailableRequests": self.token_unavailable_records,
                "explicitPromptTokens": self.explicit_prompt_tokens,
                "explicitCachedInputTokens": self.explicit_cached_input_tokens,
                "explicitCompletionTokens": self.explicit_completion_tokens,
                "explicitTotalTokens": self.explicit_total_tokens,
                "estimatedPromptTokens": self.estimated_prompt_tokens,
                "estimatedCompletionTokens": self.estimated_completion_tokens,
                "estimatedTotalTokens": self.estimated_total_tokens
            },
            "dailyUsage": self.daily_usage_json(source_agent, registry),
            "source": self.source.unwrap_or("native-history-adapters"),
            "confidence": self.confidence(),
            "tokenUnavailableRequests": self.token_unavailable_records,
            "scanCache": self.scan_cache
        })
    }

    pub fn add(&mut self, usage: MessageUsage, date_key: Option<String>) {
        if usage.total_tokens == 0 {
            return;
        }
        match usage.accuracy {
            UsageAccuracy::Exact => {
                self.explicit_prompt_tokens = self
                    .explicit_prompt_tokens
                    .saturating_add(usage.prompt_tokens);
                self.explicit_cached_input_tokens = self
                    .explicit_cached_input_tokens
                    .saturating_add(usage.cached_input_tokens.min(usage.prompt_tokens));
                self.explicit_completion_tokens = self
                    .explicit_completion_tokens
                    .saturating_add(usage.completion_tokens);
                self.explicit_total_tokens = self
                    .explicit_total_tokens
                    .saturating_add(usage.total_tokens);
                self.explicit_records = self.explicit_records.saturating_add(1);
            }
            UsageAccuracy::Estimated => {
                self.estimated_prompt_tokens = self
                    .estimated_prompt_tokens
                    .saturating_add(usage.prompt_tokens);
                self.estimated_completion_tokens = self
                    .estimated_completion_tokens
                    .saturating_add(usage.completion_tokens);
                self.estimated_total_tokens = self
                    .estimated_total_tokens
                    .saturating_add(usage.total_tokens);
                self.estimated_records = self.estimated_records.saturating_add(1);
            }
        }
        if let Some(date_key) = date_key.filter(|value| !value.trim().is_empty()) {
            self.daily_usage.entry(date_key).or_default().add(usage);
        }
    }

    /// Record one present-but-tokenless usage event: a real request whose
    /// provider payload carried no token fields. Nothing is estimated for it;
    /// it only raises the request counters and the day's visibility.
    pub fn add_token_unavailable_request_with_variant(
        &mut self,
        date_key: Option<String>,
        model: Option<String>,
        variant: UsageVariant,
    ) {
        self.token_unavailable_records = self.token_unavailable_records.saturating_add(1);
        if let Some(date_key) = date_key.filter(|value| !value.trim().is_empty()) {
            self.daily_usage
                .entry(date_key)
                .or_default()
                .add_token_unavailable_request(model, variant);
        }
    }

    /// Re-adopt one already-published local day verbatim (hosted incremental
    /// baseline). The day's counters are trusted as previously published; no
    /// token value is recomputed and no estimate is introduced.
    pub fn adopt_daily_summary(&mut self, date: String, usage: DailyUsageSummary) {
        self.message_count = self.message_count.saturating_add(usage.message_count);
        self.explicit_prompt_tokens = self
            .explicit_prompt_tokens
            .saturating_add(usage.prompt_tokens);
        self.explicit_cached_input_tokens = self
            .explicit_cached_input_tokens
            .saturating_add(usage.cached_input_tokens);
        self.explicit_completion_tokens = self
            .explicit_completion_tokens
            .saturating_add(usage.completion_tokens);
        self.explicit_total_tokens = self
            .explicit_total_tokens
            .saturating_add(usage.total_tokens);
        self.explicit_records = self.explicit_records.saturating_add(usage.explicit_records);
        self.token_unavailable_records = self
            .token_unavailable_records
            .saturating_add(usage.token_unavailable_requests);
        self.daily_usage.insert(date, usage);
    }

    pub fn merge(&mut self, other: &Self) {
        self.session_count = self.session_count.saturating_add(other.session_count);
        self.message_count = self.message_count.saturating_add(other.message_count);
        self.explicit_prompt_tokens = self
            .explicit_prompt_tokens
            .saturating_add(other.explicit_prompt_tokens);
        self.explicit_cached_input_tokens = self
            .explicit_cached_input_tokens
            .saturating_add(other.explicit_cached_input_tokens);
        self.explicit_completion_tokens = self
            .explicit_completion_tokens
            .saturating_add(other.explicit_completion_tokens);
        self.explicit_total_tokens = self
            .explicit_total_tokens
            .saturating_add(other.explicit_total_tokens);
        self.estimated_prompt_tokens = self
            .estimated_prompt_tokens
            .saturating_add(other.estimated_prompt_tokens);
        self.estimated_completion_tokens = self
            .estimated_completion_tokens
            .saturating_add(other.estimated_completion_tokens);
        self.estimated_total_tokens = self
            .estimated_total_tokens
            .saturating_add(other.estimated_total_tokens);
        self.explicit_records = self.explicit_records.saturating_add(other.explicit_records);
        self.estimated_records = self
            .estimated_records
            .saturating_add(other.estimated_records);
        self.token_unavailable_records = self
            .token_unavailable_records
            .saturating_add(other.token_unavailable_records);
        for (date, usage) in &other.daily_usage {
            self.daily_usage
                .entry(date.clone())
                .or_default()
                .merge(usage);
        }
    }

    fn daily_usage_json(
        &self,
        source_agent: Option<&str>,
        registry: &crate::registry::RegistrySnapshot,
    ) -> Vec<Value> {
        self.daily_usage
            .iter()
            .filter(|(_, usage)| usage.total_tokens > 0 || usage.request_count > 0)
            .map(|(date, usage)| {
                let models = project_model_usage(&usage.model_variants, source_agent, registry);
                let totals = models
                    .iter()
                    .map(|(name, usage)| (name, &usage["totalTokens"]))
                    .collect::<BTreeMap<_, _>>();
                json!({
                    "date": date,
                    "promptTokens": usage.prompt_tokens,
                    "cachedInputTokens": usage.cached_input_tokens,
                    "completionTokens": usage.completion_tokens,
                    "totalTokens": usage.total_tokens,
                    "messageCount": usage.message_count,
                    "requestCount": usage.request_count,
                    "tokenUnavailableRequests": usage.token_unavailable_requests,
                    "modelUsage": totals,
                    "modelTokenUsage": models,
                    "rawModelUsage": crate::model_identity::raw_usage_json(&usage.model_variants),
                    "explicitRecords": usage.explicit_records,
                    "estimatedRecords": usage.estimated_records
                })
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MessageUsage {
    pub prompt_tokens: u64,
    pub cached_input_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub model: Option<String>,
    pub variant: UsageVariant,
    pub accuracy: UsageAccuracy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UsageAccuracy {
    #[default]
    Exact,
    Estimated,
}

#[derive(Clone, Debug, Default)]
pub struct DailyUsageSummary {
    pub prompt_tokens: u64,
    pub cached_input_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub message_count: u64,
    pub explicit_records: u64,
    pub estimated_records: u64,
    pub estimated_prompt_tokens: u64,
    pub estimated_completion_tokens: u64,
    /// Event-level request counters, populated by sources that ledger
    /// individual requests (the hosted Cursor ledger). Aggregate-only sources
    /// leave them at zero.
    pub request_count: u64,
    pub token_unavailable_requests: u64,
    pub model_usage: BTreeMap<String, ModelTokenUsageSummary>,
    pub model_variants: BTreeMap<(String, UsageVariant), ModelTokenUsageSummary>,
}

impl DailyUsageSummary {
    fn add(&mut self, usage: MessageUsage) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(usage.prompt_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens.min(usage.prompt_tokens));
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(usage.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
        self.message_count = self.message_count.saturating_add(1);
        self.request_count = self.request_count.saturating_add(1);
        match usage.accuracy {
            UsageAccuracy::Exact => {
                self.explicit_records = self.explicit_records.saturating_add(1);
            }
            UsageAccuracy::Estimated => {
                self.estimated_records = self.estimated_records.saturating_add(1);
                self.estimated_prompt_tokens = self
                    .estimated_prompt_tokens
                    .saturating_add(usage.prompt_tokens);
                self.estimated_completion_tokens = self
                    .estimated_completion_tokens
                    .saturating_add(usage.completion_tokens);
            }
        }
        let model = usage
            .model
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| UNATTRIBUTED_MODEL.to_owned());
        let mut entry = ModelTokenUsageSummary::default();
        entry.record_request();
        entry.add(
            usage.prompt_tokens,
            usage.cached_input_tokens,
            usage.completion_tokens,
            usage.total_tokens,
            usage.accuracy,
        );
        self.add_model_variant_totals(model, usage.variant, entry);
    }

    /// One tokenless request for a model: counted, never converted to tokens.
    fn add_token_unavailable_request(&mut self, model: Option<String>, variant: UsageVariant) {
        let model = model
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| UNATTRIBUTED_MODEL.to_owned());
        self.message_count = self.message_count.saturating_add(1);
        self.request_count = self.request_count.saturating_add(1);
        self.token_unavailable_requests = self.token_unavailable_requests.saturating_add(1);
        let mut usage = ModelTokenUsageSummary::default();
        usage.record_token_unavailable_request();
        self.add_model_variant_totals(model, variant, usage);
    }

    pub fn add_model_usage_with_variant(
        &mut self,
        model: String,
        variant: UsageVariant,
        prompt_tokens: u64,
        cached_input_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
    ) {
        self.add_model_variant_totals(
            model,
            variant,
            ModelTokenUsageSummary {
                prompt_tokens,
                cached_input_tokens: cached_input_tokens.min(prompt_tokens),
                completion_tokens,
                total_tokens,
                ..Default::default()
            },
        );
    }

    pub fn add_model_variant_totals(
        &mut self,
        model: String,
        variant: UsageVariant,
        usage: ModelTokenUsageSummary,
    ) {
        self.model_usage
            .entry(model.clone())
            .or_default()
            .merge(usage);
        self.model_variants
            .entry((model, variant))
            .or_default()
            .merge(usage);
    }

    fn merge(&mut self, other: &Self) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(other.prompt_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(other.cached_input_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(other.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.message_count = self.message_count.saturating_add(other.message_count);
        self.request_count = self.request_count.saturating_add(other.request_count);
        self.token_unavailable_requests = self
            .token_unavailable_requests
            .saturating_add(other.token_unavailable_requests);
        self.explicit_records = self.explicit_records.saturating_add(other.explicit_records);
        self.estimated_records = self
            .estimated_records
            .saturating_add(other.estimated_records);
        self.estimated_prompt_tokens = self
            .estimated_prompt_tokens
            .saturating_add(other.estimated_prompt_tokens);
        self.estimated_completion_tokens = self
            .estimated_completion_tokens
            .saturating_add(other.estimated_completion_tokens);
        for ((model, variant), usage) in &other.model_variants {
            self.add_model_variant_totals(model.clone(), variant.clone(), *usage);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelTokenUsageSummary {
    pub prompt_tokens: u64,
    pub cached_input_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub estimated_prompt_tokens: u64,
    pub estimated_completion_tokens: u64,
    pub request_count: u64,
    pub token_unavailable_requests: u64,
}

impl ModelTokenUsageSummary {
    /// Count one event-level request for this model without adding tokens.
    fn record_request(&mut self) {
        self.request_count = self.request_count.saturating_add(1);
    }

    fn record_token_unavailable_request(&mut self) {
        self.request_count = self.request_count.saturating_add(1);
        self.token_unavailable_requests = self.token_unavailable_requests.saturating_add(1);
    }

    fn add(
        &mut self,
        prompt_tokens: u64,
        cached_input_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        accuracy: UsageAccuracy,
    ) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(prompt_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(cached_input_tokens.min(prompt_tokens));
        self.completion_tokens = self.completion_tokens.saturating_add(completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(total_tokens);
        if accuracy == UsageAccuracy::Estimated {
            self.estimated_prompt_tokens =
                self.estimated_prompt_tokens.saturating_add(prompt_tokens);
            self.estimated_completion_tokens = self
                .estimated_completion_tokens
                .saturating_add(completion_tokens);
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.add(
            other.prompt_tokens,
            other.cached_input_tokens,
            other.completion_tokens,
            other.total_tokens,
            UsageAccuracy::Exact,
        );
        self.estimated_prompt_tokens = self
            .estimated_prompt_tokens
            .saturating_add(other.estimated_prompt_tokens);
        self.estimated_completion_tokens = self
            .estimated_completion_tokens
            .saturating_add(other.estimated_completion_tokens);
        self.request_count = self.request_count.saturating_add(other.request_count);
        self.token_unavailable_requests = self
            .token_unavailable_requests
            .saturating_add(other.token_unavailable_requests);
    }

    pub fn has_usage(&self) -> bool {
        self.total_tokens > 0 || self.request_count > 0 || self.token_unavailable_requests > 0
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            prompt_tokens: self.prompt_tokens.saturating_sub(other.prompt_tokens),
            cached_input_tokens: self
                .cached_input_tokens
                .saturating_sub(other.cached_input_tokens),
            completion_tokens: self
                .completion_tokens
                .saturating_sub(other.completion_tokens),
            total_tokens: self.total_tokens.saturating_sub(other.total_tokens),
            estimated_prompt_tokens: self
                .estimated_prompt_tokens
                .saturating_sub(other.estimated_prompt_tokens),
            estimated_completion_tokens: self
                .estimated_completion_tokens
                .saturating_sub(other.estimated_completion_tokens),
            request_count: self.request_count.saturating_sub(other.request_count),
            token_unavailable_requests: self
                .token_unavailable_requests
                .saturating_sub(other.token_unavailable_requests),
        }
    }

    pub fn from_json(value: &Value) -> Self {
        Self {
            prompt_tokens: number_field(value, &["promptTokens"]).unwrap_or(0),
            cached_input_tokens: number_field(value, &["cachedInputTokens"]).unwrap_or(0),
            completion_tokens: number_field(value, &["completionTokens"]).unwrap_or(0),
            total_tokens: number_field(value, &["totalTokens"]).unwrap_or(0),
            request_count: number_field(value, &["requestCount"]).unwrap_or(0),
            token_unavailable_requests: number_field(value, &["tokenUnavailableRequests"])
                .unwrap_or(0),
            ..Self::default()
        }
    }

    pub fn to_json(self) -> Value {
        json!({
            "promptTokens": self.prompt_tokens,
            "cachedInputTokens": self.cached_input_tokens,
            "completionTokens": self.completion_tokens,
            "totalTokens": self.total_tokens,
            "requestCount": self.request_count,
            "tokenUnavailableRequests": self.token_unavailable_requests,
        })
    }
}

pub fn text_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub fn number_field(value: &Value, keys: &[&str]) -> Option<u64> {
    let object = value.as_object()?;
    keys.iter()
        .find_map(|key| object.get(*key).and_then(number_value))
}

pub fn number_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_i64()
                .filter(|number| *number >= 0)
                .map(|v| v as u64)
        })
        .or_else(|| value.as_str()?.trim().parse::<u64>().ok())
}

pub fn normalize_agent_id(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "claude" => "claude-code".to_owned(),
        "github-copilot" => "copilot".to_owned(),
        "vscode" | "vs-code" => "code".to_owned(),
        "kilo" => "kilo-code".to_owned(),
        "hermes-agent" => "hermes".to_owned(),
        "pi-agent" | "pi-coding-agent" => "pi".to_owned(),
        other => other.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregation_preserves_agent_and_model_dimensions() {
        let mut first = HistoryUsageSummary::default();
        first.add(
            MessageUsage {
                prompt_tokens: 8,
                cached_input_tokens: 3,
                total_tokens: 8,
                model: Some("model-a".to_owned()),
                ..MessageUsage::default()
            },
            Some("2026-07-01".to_owned()),
        );
        let mut second = HistoryUsageSummary::default();
        second.add(
            MessageUsage {
                completion_tokens: 5,
                total_tokens: 5,
                model: Some("model-b".to_owned()),
                accuracy: UsageAccuracy::Estimated,
                ..MessageUsage::default()
            },
            Some("2026-07-01".to_owned()),
        );
        first.merge(&second);

        let contract = first.to_json();
        assert_eq!(contract["totalTokens"], 13);
        assert_eq!(contract["dailyUsage"][0]["modelUsage"]["model-a"], 8);
        assert_eq!(contract["dailyUsage"][0]["modelUsage"]["model-b"], 5);
        assert_eq!(contract["confidence"], "medium");
        assert_eq!(contract["tokenSourceBreakdown"]["estimatedRecords"], 1);
    }

    #[test]
    fn contract_identity_is_schema_seven_metadata_first_incremental() {
        assert_eq!(AGENT_USAGE_SCHEMA_VERSION, 7);
        assert_eq!(AGENT_USAGE_MODE, "local-token-usage");
        assert_eq!(
            AGENT_USAGE_TOKEN_SOURCE_MODE,
            "native-metadata-first-incremental"
        );
        assert_eq!(DEFAULT_USAGE_WINDOW_DAYS, 30);
    }

    #[test]
    fn tokenless_requests_are_counted_without_inventing_tokens() {
        let mut summary = HistoryUsageSummary::default();
        summary.add(
            MessageUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                model: Some("composer-2.5".to_owned()),
                ..MessageUsage::default()
            },
            Some("2026-07-15".to_owned()),
        );
        summary.add_token_unavailable_request_with_variant(
            Some("2026-07-15".to_owned()),
            Some("cursor-auto".to_owned()),
            UsageVariant::default(),
        );

        // The tokenless request never becomes a token total, and it keeps the
        // source at explicit-partial confidence instead of high.
        assert_eq!(summary.total_tokens(), 15);
        assert_eq!(summary.confidence(), "medium");
        assert_eq!(summary.token_unavailable_records, 1);
        let contract = summary.to_json();
        assert_eq!(contract["totalTokens"], 15);
        assert_eq!(contract["tokenUnavailableRequests"], 1);
        assert_eq!(
            contract["tokenSourceBreakdown"]["tokenUnavailableRequests"],
            1
        );
        assert_eq!(contract["dailyUsage"][0]["requestCount"], 2);
        assert_eq!(contract["dailyUsage"][0]["tokenUnavailableRequests"], 1);
        let model = &contract["dailyUsage"][0]["modelTokenUsage"]["cursor-auto"];
        assert_eq!(model["totalTokens"], 0);
        assert_eq!(model["requestCount"], 1);
        assert_eq!(model["tokenUnavailableRequests"], 1);
    }
}
