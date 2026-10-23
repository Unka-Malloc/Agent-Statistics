//! Local-only report retention and retrieval.

use crate::contract::{
    AGENT_USAGE_MODE, AGENT_USAGE_SCHEMA_VERSION, AGENT_USAGE_TOKEN_SOURCE_MODE,
    HistoryUsageSummary, MAX_REPORTS, REPORT_COLLECTION, number_field, supported_agents,
};
use crate::ledger::{
    WORKFLOW_LEDGER_REPORT_SCHEMA, WORKFLOW_LEDGER_RESULT_KIND, WORKFLOW_LEDGER_SCHEMA_VERSION,
};
use crate::model_identity::normalize_retained_report;
use crate::params::text_param;
use anyhow::{Context, Result};
use licoup_client_state::ClientStateStore;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::PathBuf;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub fn persist_report(params: &Value, report: &Value) -> Result<()> {
    let store =
        client_state_store(params).context("usage report snapshot storage could not be opened")?;
    let mut collection = store
        .read_collection(REPORT_COLLECTION)
        .context("usage report snapshots could not be read")?;
    let mut items = collection
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(is_current_report)
        .collect::<Vec<_>>();
    items.push(report.clone());
    sort_reports_by_generated_at(&mut items);
    if let Some(object) = collection.as_object_mut() {
        object.insert("items".to_owned(), Value::Array(items));
    }
    store
        .write_collection_retaining_latest_items(REPORT_COLLECTION, collection, MAX_REPORTS)
        .context("usage report snapshots could not be saved")
        .map(|_| ())
}

pub fn read_retained_reports(
    params: &Value,
    agent_filter: Option<&str>,
    limit: usize,
    registry: &crate::registry::RegistrySnapshot,
) -> Result<Vec<Value>> {
    let store =
        client_state_store(params).context("usage report snapshot storage could not be opened")?;
    let mut collection = store
        .read_collection(REPORT_COLLECTION)
        .context("usage report snapshots could not be read")?;
    let stored_items = collection
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut retained_items = stored_items
        .iter()
        .filter(|report| is_current_report(report))
        .cloned()
        .collect::<Vec<_>>();
    for report in &mut retained_items {
        normalize_retained_report(report, registry);
    }
    sort_reports_by_generated_at(&mut retained_items);
    if retained_items != stored_items || retained_items.len() > MAX_REPORTS {
        if let Some(object) = collection.as_object_mut() {
            object.insert("items".to_owned(), Value::Array(retained_items));
        }
        let mut saved = store
            .write_collection_retaining_latest_items(REPORT_COLLECTION, collection, MAX_REPORTS)
            .context("normalized usage report snapshots could not be saved")?;
        retained_items = saved
            .get_mut("items")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
    }
    let supported = supported_agents()
        .into_iter()
        .map(|agent| agent.id)
        .collect::<BTreeSet<_>>();
    let mut reports = retained_items
        .into_iter()
        .map(|mut report| {
            project_supported_agents(&mut report, &supported);
            report
        })
        .filter(|report| {
            agent_filter
                .map(|agent_id| report_has_agent(report, agent_id))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if reports.len() > limit {
        reports = reports[reports.len() - limit..].to_vec();
    }
    reports.reverse();
    Ok(reports)
}

/// Restrict the returned view to current adapters without rewriting retained
/// accounting records when an adapter is removed.
fn project_supported_agents(report: &mut Value, supported: &BTreeSet<&str>) {
    let Some(agents) = report.get_mut("agents").and_then(Value::as_array_mut) else {
        return;
    };
    let previous_count = agents.len();
    agents.retain(|agent| {
        agent
            .get("agentId")
            .and_then(Value::as_str)
            .is_some_and(|id| supported.contains(id))
    });
    if agents.len() == previous_count {
        return;
    }
    let mut totals = serde_json::Map::new();
    totals.insert("agentCount".to_owned(), Value::from(agents.len()));
    for field in [
        "sessionCount",
        "messageCount",
        "promptTokens",
        "cachedInputTokens",
        "completionTokens",
        "totalTokens",
    ] {
        let total = agents.iter().fold(0_u64, |total, agent| {
            total.saturating_add(number_field(&agent["history"], &[field]).unwrap_or(0))
        });
        totals.insert(field.to_owned(), Value::from(total));
    }
    let mut coverage = HistoryUsageSummary::default();
    for agent in agents {
        let history = &agent["history"];
        let sources = &history["tokenSourceBreakdown"];
        coverage.explicit_records = coverage
            .explicit_records
            .saturating_add(number_field(sources, &["explicitRecords"]).unwrap_or(0));
        coverage.estimated_records = coverage
            .estimated_records
            .saturating_add(number_field(sources, &["estimatedRecords"]).unwrap_or(0));
        coverage.token_unavailable_records = coverage
            .token_unavailable_records
            .saturating_add(number_field(history, &["tokenUnavailableRequests"]).unwrap_or(0));
    }
    totals.insert("confidence".to_owned(), Value::from(coverage.confidence()));
    if !report["summary"].is_object() {
        report["summary"] = Value::Object(serde_json::Map::new());
    }
    report["summary"].as_object_mut().unwrap().extend(totals);
}

pub fn client_state_store(params: &Value) -> Result<ClientStateStore> {
    if let Some(path) = text_param(params, &["stateRoot"])
        && !path.trim().is_empty()
    {
        return ClientStateStore::new(PathBuf::from(path));
    }
    ClientStateStore::portable()
}

fn is_current_report(report: &Value) -> bool {
    report.get("schemaVersion").and_then(Value::as_u64) == Some(AGENT_USAGE_SCHEMA_VERSION as u64)
        && report.get("mode").and_then(Value::as_str) == Some(AGENT_USAGE_MODE)
        && report.get("tokenSourceMode").and_then(Value::as_str)
            == Some(AGENT_USAGE_TOKEN_SOURCE_MODE)
        && report_generated_at(report).is_some()
        && report_workflow_is_current(report)
}

/// The embedded Graph usage projection must carry the schema the client
/// parses. A retained report from an older workflow generation is dropped and
/// purged from the local collection instead of failing the whole list.
fn report_workflow_is_current(report: &Value) -> bool {
    match report.get("workflow") {
        None => true,
        Some(workflow) => {
            workflow.get("schemaVersion").and_then(Value::as_str)
                == Some(WORKFLOW_LEDGER_REPORT_SCHEMA)
                && workflow.get("ledgerSchemaVersion").and_then(Value::as_i64)
                    == Some(WORKFLOW_LEDGER_SCHEMA_VERSION)
                && workflow.get("resultKind").and_then(Value::as_str)
                    == Some(WORKFLOW_LEDGER_RESULT_KIND)
        }
    }
}

fn report_generated_at(report: &Value) -> Option<i128> {
    report
        .get("generatedAt")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .map(|value| value.unix_timestamp_nanos())
}

fn sort_reports_by_generated_at(reports: &mut [Value]) {
    reports.sort_by_key(|report| report_generated_at(report).unwrap_or(i128::MIN));
}

fn report_has_agent(report: &Value, agent_id: &str) -> bool {
    report
        .get("agents")
        .and_then(Value::as_array)
        .is_some_and(|agents| {
            agents
                .iter()
                .any(|agent| agent.get("agentId").and_then(Value::as_str) == Some(agent_id))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("lico-agent-usage-persistence-{nonce}"))
    }

    fn report(index: usize, agent_id: &str) -> Value {
        json!({
            "schemaVersion": AGENT_USAGE_SCHEMA_VERSION,
            "mode": AGENT_USAGE_MODE,
            "tokenSourceMode": AGENT_USAGE_TOKEN_SOURCE_MODE,
            "generatedAt": format!("2026-07-{:02}T00:00:00Z", index + 1),
            "agents": [{"agentId": agent_id}]
        })
    }

    #[test]
    fn report_projection_uses_current_membership_without_deleting_accounting() {
        crate::host::install_test_host();
        let root = temp_root();
        let params = json!({"stateRoot": root});
        let mut stored = report(1, "unsupported-agent");
        stored["summary"] = json!({"agentCount":2,"totalTokens":30});
        stored["agents"] = json!([
            {"agentId":"unsupported-agent","history":{"totalTokens":10}},
            {"agentId":"kimi-code","history":{
                "totalTokens":20,"promptTokens":12,"completionTokens":8,
                "tokenSourceBreakdown":{"explicitRecords":1},
                "modelTokenUsage":{"moonshotai/kimi-k3":{"totalTokens":20,"requestCount":1}}
            }}
        ]);
        persist_report(&params, &stored).unwrap();
        let registry = crate::registry::RegistrySnapshot::from_catalog(json!({
            "models":{"moonshotai/kimi-k3":{"name":"Kimi K3"}},"providers":{}
        }))
        .unwrap();

        let reports = read_retained_reports(&params, None, 10, &registry).unwrap();
        assert_eq!(reports[0]["agents"].as_array().unwrap().len(), 1);
        assert_eq!(reports[0]["agents"][0]["agentId"], "kimi-code");
        assert_eq!(reports[0]["summary"]["totalTokens"], 20);
        assert_eq!(reports[0]["summary"]["agentCount"], 1);
        assert_eq!(reports[0]["summary"]["confidence"], "high");
        assert_eq!(
            reports[0]["agents"][0]["history"]["modelTokenUsage"]["moonshotai/kimi-k3"]["totalTokens"],
            20
        );
        assert!(
            read_retained_reports(&params, Some("unsupported-agent"), 10, &registry)
                .unwrap()
                .is_empty()
        );
        let retained = client_state_store(&params)
            .unwrap()
            .read_collection(REPORT_COLLECTION)
            .unwrap();
        assert_eq!(retained["items"][0]["agents"].as_array().unwrap().len(), 2);
        assert_eq!(
            retained["items"][0]["agents"][0]["history"]["totalTokens"],
            10
        );
        assert_eq!(retained["items"][0]["summary"]["totalTokens"], 30);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retention_is_local_bounded_current_schema_and_filterable() {
        crate::host::install_test_host();
        let root = temp_root();
        let params = json!({"stateRoot": root.to_string_lossy()});
        for index in 0..(MAX_REPORTS + 2) {
            let agent = if index % 2 == 0 { "codex" } else { "cursor" };
            persist_report(&params, &report(index, agent)).unwrap();
        }
        let store = client_state_store(&params).unwrap();
        let retained = store.read_collection(REPORT_COLLECTION).unwrap();
        assert_eq!(retained["items"].as_array().unwrap().len(), MAX_REPORTS);
        let codex =
            read_retained_reports(&params, Some("codex"), 3, &crate::registry::snapshot()).unwrap();
        assert_eq!(codex.len(), 3);
        assert!(codex.iter().all(|item| report_has_agent(item, "codex")));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn byte_retention_keeps_complete_latest_reports_and_rejects_an_oversized_latest() {
        let root = temp_root();
        let params = json!({"stateRoot": root.to_string_lossy()});
        let padding = "x".repeat(6 * 1024 * 1024);
        let mut latest = Value::Null;
        for index in 0..4 {
            latest = report(index, "codex");
            latest["syntheticPadding"] = json!(padding);
            latest["agents"][0]["history"] = json!({
                "totalTokens": 73,
                "modelTokenUsage": {"synthetic-model": {"totalTokens": 73}},
                "dailyUsage": [{"date": "2026-07-01", "totalTokens": 73}]
            });
            persist_report(&params, &latest).unwrap();
        }
        let store = client_state_store(&params).unwrap();
        let saved = store.read_collection(REPORT_COLLECTION).unwrap();
        let items = saved["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["generatedAt"], report(2, "codex")["generatedAt"]);
        assert!(items.last() == Some(&latest));
        let path = store.collection_path(REPORT_COLLECTION).unwrap();
        let before = fs::read(&path).unwrap();
        assert_eq!(
            before.len(),
            serde_json::to_vec_pretty(&saved).unwrap().len() + 1
        );
        assert!(before.len() < 16 * 1024 * 1024);

        let mut oversized = report(4, "codex");
        oversized["syntheticPadding"] = json!("x".repeat(16 * 1024 * 1024));
        let error = persist_report(&params, &oversized).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("usage report snapshots could not be saved"));
        assert!(message.contains("complete latest collection item exceeds its bounded size"));
        assert!(fs::read(&path).unwrap() == before);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retrieval_applies_byte_retention_when_normalization_grows_reports() {
        crate::host::install_test_host();
        let root = temp_root();
        let params = json!({"stateRoot": root.to_string_lossy()});
        let store = client_state_store(&params).unwrap();
        let mut collection = store
            .write_collection(
                REPORT_COLLECTION,
                json!({"items": [report(0, "codex"), report(1, "codex")]}),
            )
            .unwrap();
        for item in collection["items"].as_array_mut().unwrap() {
            item["syntheticPadding"] = json!("");
            item["agents"][0]["history"] = json!({
                "modelUsage": {"synthetic-model": 73}, "totalTokens": 73
            });
        }
        // Fill the original representation exactly to the existing 16 MiB
        // policy. Registry normalization adds fields without changing usage.
        let budget = 16 * 1024 * 1024;
        let base_bytes = serde_json::to_vec_pretty(&collection).unwrap().len() + 1;
        let padding = budget - base_bytes;
        collection["items"][0]["syntheticPadding"] = json!("x".repeat(padding / 2));
        collection["items"][1]["syntheticPadding"] = json!("x".repeat(padding - padding / 2));
        let registry = crate::registry::snapshot();
        let mut expected = collection["items"][1].clone();
        normalize_retained_report(&mut expected, &registry);
        assert!(
            serde_json::to_vec_pretty(&expected).unwrap().len()
                > serde_json::to_vec_pretty(&collection["items"][1])
                    .unwrap()
                    .len()
        );
        store
            .write_collection(REPORT_COLLECTION, collection)
            .unwrap();
        let path = store.collection_path(REPORT_COLLECTION).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), budget as u64);

        let reports = read_retained_reports(&params, None, MAX_REPORTS, &registry).unwrap();
        assert_eq!(reports.len(), 1);
        assert!(reports[0] == expected);
        let saved = store.read_collection(REPORT_COLLECTION).unwrap();
        assert!(saved["items"] == json!([expected]));
        assert!(fs::metadata(&path).unwrap().len() < budget as u64);
        let again = read_retained_reports(&params, None, MAX_REPORTS, &registry).unwrap();
        assert!(again == reports);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retrieval_removes_noncurrent_report_contracts() {
        let root = temp_root();
        let params = json!({"stateRoot": root.to_string_lossy()});
        let store = client_state_store(&params).unwrap();
        store
            .write_collection(
                REPORT_COLLECTION,
                json!({
                    "items": [
                        {
                            "schemaVersion": 999,
                            "mode": "invalid-contract",
                            "generatedAt": "2026-07-01T00:00:00Z"
                        },
                        report(1, "codex")
                    ]
                }),
            )
            .unwrap();
        let reports =
            read_retained_reports(&params, None, 10, &crate::registry::snapshot()).unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0]["schemaVersion"], AGENT_USAGE_SCHEMA_VERSION);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retrieval_removes_legacy_workflow_projection_contracts() {
        let root = temp_root();
        let params = json!({"stateRoot": root.to_string_lossy()});
        let store = client_state_store(&params).unwrap();
        let mut legacy = report(1, "codex");
        legacy["workflow"] = json!({
            "schemaVersion": "licoup.workflow-token-report.v1",
            "ledgerSchemaVersion": 1,
            "resultKind": "workflow-token-usage"
        });
        let mut current = report(2, "codex");
        current["workflow"] = json!({
            "schemaVersion": WORKFLOW_LEDGER_REPORT_SCHEMA,
            "ledgerSchemaVersion": WORKFLOW_LEDGER_SCHEMA_VERSION,
            "resultKind": WORKFLOW_LEDGER_RESULT_KIND,
            "runs": [],
            "summary": {}
        });
        store
            .write_collection(REPORT_COLLECTION, json!({ "items": [legacy, current] }))
            .unwrap();
        let reports =
            read_retained_reports(&params, None, 10, &crate::registry::snapshot()).unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0]["workflow"]["schemaVersion"],
            WORKFLOW_LEDGER_REPORT_SCHEMA
        );
        // The legacy entry is purged from the stored collection too.
        let retained = store.read_collection(REPORT_COLLECTION).unwrap();
        assert_eq!(retained["items"].as_array().unwrap().len(), 1);
        fs::remove_dir_all(root).unwrap();
    }
}
