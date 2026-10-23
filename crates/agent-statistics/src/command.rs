//! Public scan/report command orchestration.

use crate::contract::{
    AGENT_USAGE_MODE, AGENT_USAGE_SCHEMA_VERSION, AGENT_USAGE_TOKEN_SOURCE_MODE, AgentDef,
    HistoryUsageSummary, MAX_REPORTS, REPORT_COLLECTION, normalize_agent_id, supported_agents,
};
use crate::params::{number_param, text_param};
use crate::persistence::{persist_report, read_retained_reports};
use crate::window::UsageWindow;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub fn scan(params: &Value) -> Result<Value> {
    let state_root = text_param(params, &["stateRoot"]).filter(|root| !root.trim().is_empty());
    let registry = model_registry_snapshot(state_root.as_deref().map(Path::new));
    let generated_at = timestamp_rfc3339();
    let agent_filter = text_param(params, &["agent", "target"]);
    let include_target_status = bool_param(params, "includeTargetStatus").unwrap_or(false);
    let usage_window = UsageWindow::from_params(params);
    let mut warnings = Vec::<Value>::new();
    // The base metering journal this scan settles into is composed before any
    // reading settles, over the data home these parameters resolve to. A store
    // this client cannot open is a warning, not a failed scan: the readings are
    // still reported, they simply have no ledger to settle into.
    if let Err(error) = crate::source_host::install(params) {
        warnings.push(json!({
            "code": "usage_journal_unavailable",
            "reason": error.code
        }));
    }
    let target_status = if include_target_status {
        target_status_map(params, &mut warnings)
    } else {
        BTreeMap::new()
    };
    let mut agents = Vec::<Value>::new();
    let mut summary = HistoryUsageSummary::default();

    for def in supported_agents() {
        if agent_filter
            .as_ref()
            .is_some_and(|filter| normalize_agent_id(filter) != def.id)
        {
            continue;
        }
        let history = summarize_agent_history(&def, params, &usage_window, &mut warnings);
        summary.merge(&history);
        let confidence = history.confidence();
        // Every reading this production scan took settles into the base
        // ledger: the same numbers read again replay instead of settling
        // twice. A store that cannot be opened is reported, never fatal to the
        // scan or to the other agents.
        let metering = match crate::source_host::settle_agent_readings(params, def.id, &history) {
            Ok(settled) => settled,
            Err(_) => {
                warnings.push(json!({"code": "usage_journal_unsettled", "agentId": def.id}));
                Value::Null
            }
        };
        agents.push(json!({
            "agentId": def.id,
            "label": def.label,
            "status": target_status
                .get(def.id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_owned()),
            "history": history.to_json_for_agent(Some(def.id), &registry),
            "confidence": confidence,
            "metering": metering,
            "sources": {
                "historyRoots": history.source_paths.into_iter().collect::<Vec<_>>(),
                "skipped": history.skipped
            }
        }));
    }

    let workflow_report_value = acativa::usage_ledger::workflow_report(params)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let report = json!({
        "ok": true,
        "schemaVersion": AGENT_USAGE_SCHEMA_VERSION,
        "mode": AGENT_USAGE_MODE,
        "generatedAt": generated_at,
        "window": {
            "start": usage_window.start,
            "end": usage_window.end,
            "days": usage_window.days,
            "timezoneOffsetMinutes": usage_window.timezone_offset_minutes,
            "timezoneTransitionCount": usage_window.timezone_transitions.len()
        },
        "tokenSourceMode": AGENT_USAGE_TOKEN_SOURCE_MODE,
        "usageParserRevision": crate::contract::USAGE_PARSER_REVISION,
        "modelRegistryRevision": registry.revision(),
        "summary": {
            "agentCount": agents.len(),
            "sessionCount": summary.session_count,
            "messageCount": summary.message_count,
            "promptTokens": summary.prompt_tokens(),
            "completionTokens": summary.completion_tokens(),
            "totalTokens": summary.total_tokens(),
            "windowStart": usage_window.start,
            "windowEnd": usage_window.end,
            "windowDays": usage_window.days,
            "confidence": summary.confidence()
        },
        "agents": agents,
        "sources": {
            "history": "native-history-adapters",
            "retention": {
                "collection": REPORT_COLLECTION,
                "maxReports": MAX_REPORTS
            }
        },
        "workflow": workflow_report_value,
        "warnings": warnings
    });
    persist_report(params, &report)?;
    Ok(report)
}

pub fn report(params: &Value) -> Result<Value> {
    // Reading composes the journal over this home so the projection names the
    // facts earlier runs settled. A store this client cannot open is reported
    // as unavailable below, which is the documented answer for an unreadable
    // store, so the failed install is not fatal here.
    let _ = crate::source_host::install(params);
    let state_root = text_param(params, &["stateRoot"]).filter(|root| !root.trim().is_empty());
    let registry = model_registry_snapshot(state_root.as_deref().map(Path::new));
    let agent_filter =
        text_param(params, &["agent", "target"]).map(|value| normalize_agent_id(&value));
    let limit = number_param(params, "limit").unwrap_or(10) as usize;
    let reports = read_retained_reports(params, agent_filter.as_deref(), limit, &registry)?;
    let workflow_report_value = acativa::usage_ledger::workflow_report(params)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    // What the base ledger holds right now: the facts the scans settled and the
    // obligations the workflow ledger still leaves open. A store this client
    // cannot read is reported as unavailable rather than as an empty ledger.
    let metering = crate::source_host::held_metering(params).unwrap_or_else(
        |_| json!({"journal": crate::source_host::USAGE_JOURNAL_SCHEMA, "available": false}),
    );
    Ok(json!({
        "ok": true,
        "schemaVersion": AGENT_USAGE_SCHEMA_VERSION,
        "mode": AGENT_USAGE_MODE,
        "tokenSourceMode": AGENT_USAGE_TOKEN_SOURCE_MODE,
        "usageParserRevision": crate::contract::USAGE_PARSER_REVISION,
        "modelRegistryRevision": registry.revision(),
        "resultKind": "retained-reports",
        "reports": reports,
        "metering": metering,
        "workflow": workflow_report_value
    }))
}

fn model_registry_snapshot(
    state_root: Option<&Path>,
) -> std::sync::Arc<licoup_model_catalog::identity::RegistrySnapshot> {
    crate::host::host().map_or_else(
        |_| licoup_model_catalog::identity::refresh_cached_snapshot_for_state_root(state_root),
        |host| (host.model_registry_snapshot)(state_root),
    )
}

fn summarize_agent_history(
    def: &AgentDef,
    params: &Value,
    window: &UsageWindow,
    warnings: &mut Vec<Value>,
) -> HistoryUsageSummary {
    match crate::host::host() {
        Ok(host) => {
            (host.summarize_agent_history)(def, params, window, warnings).unwrap_or_default()
        }
        Err(_) => HistoryUsageSummary {
            source: Some("native-usage-source-unavailable"),
            skipped: vec![json!({"code": "native_usage_source_unavailable", "agentId": def.id})],
            ..HistoryUsageSummary::default()
        },
    }
}

fn target_status_map(params: &Value, warnings: &mut Vec<Value>) -> BTreeMap<String, String> {
    match crate::host::host() {
        Ok(host) => (host.target_status)(params, warnings),
        Err(_) => {
            warnings.push(json!({"code": "target_scan_failed"}));
            BTreeMap::new()
        }
    }
}

fn timestamp_rfc3339() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    OffsetDateTime::from_unix_timestamp(duration.as_secs() as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn bool_param(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|value| {
        value.as_bool().or_else(|| {
            value
                .as_str()
                .and_then(|text| match text.trim().to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => Some(true),
                    "0" | "false" | "no" | "off" => Some(false),
                    _ => None,
                })
        })
    })
}
