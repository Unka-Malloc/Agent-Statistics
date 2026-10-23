//! Product capabilities the statistics engine consumes through an installed port.
//!
//! The generic engine owns measurement, aggregation, persistence-independent
//! windowing and the usage journal. Which agents carry usage capability, how one
//! agent's history summarizes, and what target readiness looks like are product
//! decisions the embedding host answers once at process start.

use crate::contract::{AgentDef, HistoryUsageSummary};
use crate::window::UsageWindow;
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::OnceLock;

/// The host answers. `summarize_agent_history` returns one agent's usage
/// summary; returning `Err` reports the agent as skipped rather than failing
/// the scan. `target_status` answers the readiness map for the optional
/// target-status report section. `model_registry_snapshot` answers the current
/// model catalogue for the given state root.
pub struct StatisticsHost {
    pub supported_agents: fn() -> Vec<AgentDef>,
    pub summarize_agent_history:
        fn(&AgentDef, &Value, &UsageWindow, &mut Vec<Value>) -> Result<HistoryUsageSummary>,
    pub target_status: fn(&Value, &mut Vec<Value>) -> BTreeMap<String, String>,
    pub model_registry_snapshot:
        fn(Option<&Path>) -> std::sync::Arc<licoup_model_catalog::identity::RegistrySnapshot>,
}

static HOST: OnceLock<StatisticsHost> = OnceLock::new();

/// Install the host answers once per process. A process that never installs
/// refuses every scan rather than inventing a product catalog.
pub fn install_host(host: StatisticsHost) -> Result<(), &'static str> {
    HOST.set(host)
        .map_err(|_| "statistics_host_already_installed")
}

pub fn host() -> Result<&'static StatisticsHost, &'static str> {
    HOST.get().ok_or("statistics_host_not_installed")
}

/// The stub answers this crate's own tests compose: a small catalog and a
/// deterministic reading, so engine and journal assertions stay independent of
/// any product reader.
#[cfg(test)]
pub(crate) fn install_test_host() {
    let _ = install_host(StatisticsHost {
        supported_agents: || {
            vec![
                AgentDef {
                    id: "codex",
                    label: "Codex",
                },
                AgentDef {
                    id: "kimi-code",
                    label: "Kimi Code",
                },
                AgentDef {
                    id: "cursor",
                    label: "Cursor",
                },
                AgentDef {
                    id: "opencode",
                    label: "OpenCode",
                },
                AgentDef {
                    id: "lico-agent",
                    label: "Lico Agent",
                },
                AgentDef {
                    id: "deepseek-harness",
                    label: "DeepSeek Harness",
                },
            ]
        },
        summarize_agent_history: |_, _, _, _| {
            Ok(HistoryUsageSummary {
                source: Some("test-stub"),
                session_count: 1,
                message_count: 1,
                explicit_prompt_tokens: 120,
                explicit_cached_input_tokens: 20,
                explicit_completion_tokens: 30,
                explicit_total_tokens: 150,
                explicit_records: 1,
                daily_usage: BTreeMap::from([(
                    "2026-07-08".to_owned(),
                    crate::contract::DailyUsageSummary {
                        model_usage: BTreeMap::from([(
                            "local-model".to_owned(),
                            crate::contract::ModelTokenUsageSummary {
                                prompt_tokens: 120,
                                cached_input_tokens: 20,
                                completion_tokens: 30,
                                total_tokens: 150,
                                request_count: 1,
                                ..Default::default()
                            },
                        )]),
                        ..Default::default()
                    },
                )]),
                ..HistoryUsageSummary::default()
            })
        },
        target_status: |_, _| BTreeMap::new(),
        model_registry_snapshot: |_| licoup_model_catalog::identity::snapshot(),
    });
}
