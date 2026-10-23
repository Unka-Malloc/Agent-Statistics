//! Local, numeric-only usage accounting for Adaptive Flywheel Graph runs.
//!
//! The ledger owns only opaque run, command, state and Membership identities
//! plus checked integer counters. It never reads or stores a transcript,
//! prompt, reply, tool payload, credential, native path or runtime endpoint.
//! Admission reservations, actual settlement and unresolved reconciliation
//! balances are separate facts; a reservation is never rewritten as usage.

use crate::persistence::client_state_store;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::fmt::{Display, Formatter};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(test)]
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const WORKFLOW_LEDGER_SCHEMA_VERSION: i64 = 2;
pub const WORKFLOW_LEDGER_FILE_NAME: &str = "graph-usage-ledger-v2.sqlite3";
pub const WORKFLOW_LEDGER_REPORT_SCHEMA: &str = "licoup.graph-usage-report.v2";
pub const WORKFLOW_LEDGER_RESULT_KIND: &str = "graph-run-usage";
pub const WORKFLOW_LEDGER_MAX_RUNS: usize = 256;

/// A first-time WAL transition does not honor busy_timeout when two
/// connections open a new ledger concurrently, so bound a retry to keep
/// parallel admissions on one budget pool serialized instead of failing.
const LEDGER_OPEN_BUSY_RETRIES: u32 = 50;
const LEDGER_OPEN_BUSY_DELAY: Duration = Duration::from_millis(10);

const WORKFLOW_LEDGER_INIT: &str = "PRAGMA busy_timeout=5000;
             PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS graph_usage_schema (
               singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO graph_usage_schema(singleton,version) VALUES(1,2);
             CREATE TABLE IF NOT EXISTS graph_runs (
               run_id TEXT PRIMARY KEY,
               revision_digest TEXT NOT NULL,
               conversation_id TEXT,
               assistant_membership_id TEXT,
               status TEXT NOT NULL,
               created_at_ms INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS graph_runs_updated
               ON graph_runs(updated_at_ms DESC,run_id DESC);
             CREATE TABLE IF NOT EXISTS graph_commands (
               command_id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES graph_runs(run_id) ON DELETE CASCADE,
               state_id TEXT NOT NULL,
               membership_id TEXT,
               kind TEXT NOT NULL,
               status TEXT NOT NULL,
               attempt INTEGER NOT NULL,
               agent_id TEXT,
               model TEXT,
               accuracy TEXT NOT NULL,
               prompt_tokens INTEGER NOT NULL,
               cached_input_tokens INTEGER NOT NULL,
               completion_tokens INTEGER NOT NULL,
               total_tokens INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS graph_commands_run
               ON graph_commands(run_id,command_id);
             CREATE TABLE IF NOT EXISTS graph_usage_budget_pools (
               budget_id TEXT PRIMARY KEY,
               limit_tokens INTEGER,
               observed_used_tokens INTEGER,
               observed_remaining_tokens INTEGER,
               actual_tokens INTEGER NOT NULL DEFAULT 0,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS graph_usage_reservations (
               invocation_id TEXT PRIMARY KEY,
               budget_id TEXT NOT NULL,
               run_id TEXT,
               command_id TEXT,
               estimate_accuracy TEXT NOT NULL,
               estimated_tokens INTEGER,
               reserved_tokens INTEGER NOT NULL,
               state TEXT NOT NULL,
               settlement_status TEXT,
               prompt_tokens INTEGER,
               cached_input_tokens INTEGER,
               completion_tokens INTEGER,
               total_tokens INTEGER,
               usage_accuracy TEXT,
               overage_tokens INTEGER NOT NULL DEFAULT 0,
               created_at_ms INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS graph_usage_reservations_budget
               ON graph_usage_reservations(budget_id,state,invocation_id);
             CREATE INDEX IF NOT EXISTS graph_usage_reservations_run
               ON graph_usage_reservations(run_id,updated_at_ms,invocation_id);
             CREATE TABLE IF NOT EXISTS usage_source_events (
               source_ref TEXT NOT NULL,
               source_epoch TEXT NOT NULL,
               observation_id TEXT NOT NULL,
               revision INTEGER NOT NULL,
               scope_ref TEXT NOT NULL,
               measurement_ref TEXT,
               observed_at TEXT NOT NULL,
               content_digest TEXT NOT NULL,
               retracted INTEGER NOT NULL DEFAULT 0,
               settled_at_ms INTEGER NOT NULL,
               PRIMARY KEY(source_ref,source_epoch,observation_id)
             );
             CREATE TABLE IF NOT EXISTS usage_source_facts (
               fact_id TEXT PRIMARY KEY,
               scope_ref TEXT NOT NULL,
               metric TEXT NOT NULL,
               value TEXT,
               unit TEXT NOT NULL,
               quality TEXT NOT NULL,
               eligibility TEXT NOT NULL,
               source_ref TEXT NOT NULL,
               source_epoch TEXT NOT NULL,
               observation_id TEXT NOT NULL,
               observed_at TEXT NOT NULL,
               revision INTEGER,
               retracted INTEGER NOT NULL DEFAULT 0,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS usage_source_facts_scope
               ON usage_source_facts(scope_ref,metric,fact_id);
             CREATE INDEX IF NOT EXISTS usage_source_facts_event
               ON usage_source_facts(source_ref,source_epoch,observation_id);
             CREATE TABLE IF NOT EXISTS usage_source_cursors (
               source_ref TEXT NOT NULL,
               source_epoch TEXT NOT NULL,
               scope_ref TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               cursor TEXT NOT NULL,
               updated_at_ms INTEGER NOT NULL,
               PRIMARY KEY(source_ref,source_epoch)
             );";

fn is_database_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if inner.code == rusqlite::ErrorCode::DatabaseBusy
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerError {
    pub code: String,
    pub stage: String,
    pub retryable: bool,
    pub recovery: String,
}

impl LedgerError {
    pub fn storage() -> Self {
        Self {
            code: "usage_ledger_store_unavailable".into(),
            stage: "graph-usage-ledger".into(),
            retryable: true,
            recovery: "retry_after_store_recovers".into(),
        }
    }

    fn invalid(code: &'static str) -> Self {
        Self {
            code: code.into(),
            stage: "graph-usage-ledger".into(),
            retryable: false,
            recovery: "correct_request_and_retry".into(),
        }
    }

    /// A refusal raised by another owner of the same base store, so the failure
    /// names the boundary that refused it instead of the graph ledger.
    pub fn refused(stage: &'static str, code: &'static str) -> Self {
        Self {
            code: code.into(),
            stage: stage.into(),
            retryable: false,
            recovery: "correct_request_and_retry".into(),
        }
    }
}

impl Display for LedgerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.code)
    }
}

impl std::error::Error for LedgerError {}

pub type LedgerResult<T> = std::result::Result<T, LedgerError>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CheckedUsage {
    prompt: u64,
    cached: u64,
    completion: u64,
    total: u64,
    accuracy: UsageAccuracy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum UsageAccuracy {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

impl UsageAccuracy {
    fn parse(value: Option<&str>, has_usage: bool) -> LedgerResult<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            Some("exact") => Ok(Self::Exact),
            Some("estimated") => Ok(Self::Estimated),
            Some("unknown") | None if !has_usage => Ok(Self::Unknown),
            Some("unknown") | None => Ok(Self::Exact),
            _ => Err(LedgerError::invalid("usage_ledger_accuracy_invalid")),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Unknown => "unknown",
        }
    }
}

impl CheckedUsage {
    fn from_value(value: Option<&Value>) -> LedgerResult<Self> {
        let Some(object) = value.and_then(Value::as_object) else {
            return Ok(Self::default());
        };
        let prompt = numeric_field(
            object,
            &[
                "promptTokens",
                "prompt_tokens",
                "inputTokens",
                "input_tokens",
            ],
        )
        .unwrap_or(0);
        let cached = numeric_field(
            object,
            &[
                "cachedInputTokens",
                "cached_input_tokens",
                "cacheReadInputTokens",
                "cache_read_input_tokens",
            ],
        )
        .unwrap_or(0);
        let completion = numeric_field(
            object,
            &[
                "completionTokens",
                "completion_tokens",
                "outputTokens",
                "output_tokens",
            ],
        )
        .unwrap_or(0);
        let total = prompt
            .checked_add(completion)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        if cached > prompt || total > i64::MAX as u64 {
            return Err(LedgerError::invalid("usage_ledger_counter_invalid"));
        }
        if let Some(submitted_total) = numeric_field(object, &["totalTokens", "total_tokens"])
            && submitted_total != total
        {
            return Err(LedgerError::invalid("usage_ledger_total_mismatch"));
        }
        let has_usage = prompt > 0 || cached > 0 || completion > 0;
        let accuracy = UsageAccuracy::parse(
            object
                .get("accuracy")
                .or_else(|| object.get("usageAccuracy"))
                .and_then(Value::as_str),
            has_usage,
        )?;
        Ok(Self {
            prompt,
            cached,
            completion,
            total,
            accuracy,
        })
    }

    fn to_value(self) -> Value {
        json!({
            "promptTokens": self.prompt,
            "cachedInputTokens": self.cached,
            "completionTokens": self.completion,
            "totalTokens": self.total,
        })
    }

    fn to_value_with_counts(self, exact_count: u64, estimated_count: u64) -> Value {
        json!({
            "promptTokens": self.prompt,
            "cachedInputTokens": self.cached,
            "completionTokens": self.completion,
            "totalTokens": self.total,
            "exactCount": exact_count,
            "estimatedCount": estimated_count,
        })
    }

    fn checked_add(self, other: Self) -> LedgerResult<Self> {
        let prompt = self
            .prompt
            .checked_add(other.prompt)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        let cached = self
            .cached
            .checked_add(other.cached)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        let completion = self
            .completion
            .checked_add(other.completion)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        let total = prompt
            .checked_add(completion)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        Ok(Self {
            prompt,
            cached,
            completion,
            total,
            accuracy: UsageAccuracy::Unknown,
        })
    }
}

pub struct Ledger {
    pub connection: Connection,
}

#[derive(Clone, Copy, Debug, Default)]
struct BudgetSnapshot {
    limit_tokens: Option<u64>,
    used_tokens: Option<u64>,
    remaining_tokens: Option<u64>,
    supplied: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct UsageEstimate {
    total_tokens: Option<u64>,
    accuracy: UsageAccuracy,
}

#[derive(Clone, Debug)]
struct BudgetPool {
    budget_id: String,
    limit_tokens: Option<u64>,
    observed_used_tokens: Option<u64>,
    observed_remaining_tokens: Option<u64>,
    actual_tokens: u64,
}

#[derive(Clone, Debug)]
struct ReservationRow {
    invocation_id: String,
    budget_id: String,
    run_id: Option<String>,
    command_id: Option<String>,
    estimate_accuracy: UsageAccuracy,
    estimated_tokens: Option<u64>,
    reserved_tokens: u64,
    state: String,
    settlement_status: Option<String>,
    usage: Option<CheckedUsage>,
    overage_tokens: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ActiveReservationStats {
    reserved_tokens: u64,
    in_flight_count: u64,
    unknown_count: u64,
}

/// Admit one immutable Graph run identity. Replays may update lifecycle state
/// but cannot change the revision, Conversation or designated Assistant.
pub fn begin_graph_run(params: &Value) -> LedgerResult<Value> {
    let run_id = required_id(params, "runId")?;
    let revision_digest = required_id(params, "revisionDigest")?;
    let conversation_id = optional_id(params, "conversationId")?;
    let assistant_membership_id = optional_id(params, "assistantMembershipId")?;
    let status = lifecycle(
        params
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("pending"),
    )?;
    let mut ledger = open_ledger(params)?;
    let transaction = ledger
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| LedgerError::storage())?;
    let now = now_ms();
    transaction
        .execute(
            "INSERT INTO graph_runs(
               run_id,revision_digest,conversation_id,assistant_membership_id,
               status,created_at_ms,updated_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6,?6)
             ON CONFLICT(run_id) DO NOTHING",
            params![
                run_id,
                revision_digest,
                conversation_id,
                assistant_membership_id,
                status,
                now
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    let persisted = load_run_identity(&transaction, &run_id)?.ok_or_else(LedgerError::storage)?;
    if persisted.0 != revision_digest
        || persisted.1.as_deref() != conversation_id.as_deref()
        || persisted.2.as_deref() != assistant_membership_id.as_deref()
    {
        return Err(LedgerError::invalid("usage_ledger_run_identity_conflict"));
    }
    transaction
        .execute(
            "UPDATE graph_runs SET status=?2,updated_at_ms=?3 WHERE run_id=?1",
            params![run_id, status, now],
        )
        .map_err(|_| LedgerError::storage())?;
    prune_terminal_runs(&transaction)?;
    transaction.commit().map_err(|_| LedgerError::storage())?;
    Ok(json!({
        "runId": run_id,
        "revisionDigest": revision_digest,
        "status": status,
    }))
}

/// Record one durable Graph command and an optional normalized usage sample.
/// A later sample may fill an empty row, but conflicting non-zero samples fail.
pub fn record_graph_command(params: &Value) -> LedgerResult<Value> {
    let run_id = required_id(params, "runId")?;
    let command_id = required_id(params, "commandId")?;
    let state_id = required_id(params, "stateId")?;
    let membership_id = optional_id(params, "membershipId")?;
    let kind = command_kind(required_text(params, "kind")?)?;
    let status = command_status(required_text(params, "status")?)?;
    let attempt = params
        .get("attempt")
        .and_then(Value::as_u64)
        .filter(|value| *value <= u8::MAX as u64)
        .unwrap_or(0);
    let agent_id = optional_id(params, "agentId")?;
    let model = optional_label(params, "model")?;
    let usage = CheckedUsage::from_value(params.get("usage"))?;
    let mut ledger = open_ledger(params)?;
    let transaction = ledger
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| LedgerError::storage())?;
    if load_run_identity(&transaction, &run_id)?.is_none() {
        return Err(LedgerError::invalid("usage_ledger_run_not_found"));
    }
    let now = now_ms();
    transaction
        .execute(
            "INSERT INTO graph_commands(
               command_id,run_id,state_id,membership_id,kind,status,attempt,
               agent_id,model,accuracy,prompt_tokens,cached_input_tokens,
               completion_tokens,total_tokens,updated_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
             ON CONFLICT(command_id) DO NOTHING",
            params![
                command_id,
                run_id,
                state_id,
                membership_id,
                kind,
                status,
                attempt,
                agent_id,
                model,
                usage.accuracy.as_str(),
                usage.prompt,
                usage.cached,
                usage.completion,
                usage.total,
                now,
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    let current = load_command(&transaction, &command_id)?.ok_or_else(LedgerError::storage)?;
    if current.run_id != run_id
        || current.state_id != state_id
        || current.membership_id.as_deref() != membership_id.as_deref()
        || current.kind != kind
        || current.agent_id.as_deref() != agent_id.as_deref()
        || current.model.as_deref() != model.as_deref()
    {
        return Err(LedgerError::invalid(
            "usage_ledger_command_identity_conflict",
        ));
    }
    if current.usage.total > 0 && current.usage != usage && usage.total > 0 {
        return Err(LedgerError::invalid("usage_ledger_command_usage_conflict"));
    }
    let settled = if current.usage.total == 0 && usage.total > 0 {
        usage
    } else {
        current.usage
    };
    transaction
        .execute(
            "UPDATE graph_commands SET status=?2,attempt=?3,accuracy=?4,
               prompt_tokens=?5,cached_input_tokens=?6,completion_tokens=?7,
               total_tokens=?8,updated_at_ms=?9 WHERE command_id=?1",
            params![
                command_id,
                status,
                attempt,
                settled.accuracy.as_str(),
                settled.prompt,
                settled.cached,
                settled.completion,
                settled.total,
                now,
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    transaction
        .execute(
            "UPDATE graph_runs SET updated_at_ms=?2 WHERE run_id=?1",
            params![run_id, now],
        )
        .map_err(|_| LedgerError::storage())?;
    transaction.commit().map_err(|_| LedgerError::storage())?;
    if let Some(settlement_status) = graph_settlement_status(status) {
        let mut settlement = json!({
            "stateRoot": params.get("stateRoot").cloned().unwrap_or(Value::Null),
            "invocationId": command_id,
            "commandId": command_id,
            "settlementStatus": settlement_status,
            "usage": params.get("usage").cloned().unwrap_or(Value::Null),
        });
        if settlement["stateRoot"].is_null() {
            settlement
                .as_object_mut()
                .expect("settlement object")
                .remove("stateRoot");
        }
        // The Graph usage row remains authoritative even when an older caller
        // has no admission reservation. A missing reservation is expected for
        // pre-admission history and is deliberately ignored here.
        let _ = settle_graph_command(&settlement);
    }
    Ok(json!({
        "runId": run_id,
        "commandId": command_id,
        "usage": settled.to_value(),
    }))
}

fn graph_settlement_status(status: &str) -> Option<&'static str> {
    match status {
        "succeeded" => Some("completed"),
        "failed" => Some("failed"),
        "cancelled" => Some("cancelled"),
        "in-doubt" => Some("unknown"),
        _ => None,
    }
}

/// Atomically reserve one locally admitted, chargeable invocation from a
/// budget pool. The pool snapshot is an admission input, not a provider
/// billing guarantee: estimates can protect LicoUp's own concurrent work but
/// cannot cap calls made internally by an external Agent.
pub fn reserve_graph_command(params: &Value) -> LedgerResult<Value> {
    let invocation_id = required_invocation_id(params)?;
    let run_id = optional_id(params, "runId")?;
    let command_id = optional_id(params, "commandId")?;
    let (budget_id, budget_snapshot) = parse_budget(params)?;
    let estimate = parse_usage_estimate(params)?;
    let chargeable = chargeable(params)?;
    let mut ledger = open_ledger(params)?;
    let transaction = ledger
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| LedgerError::storage())?;

    if let Some(existing) = load_reservation(&transaction, &invocation_id)? {
        ensure_reservation_identity(
            &existing,
            &budget_id,
            run_id.as_deref(),
            command_id.as_deref(),
        )?;
        if existing.estimated_tokens != estimate.total_tokens
            || existing.estimate_accuracy != estimate.accuracy
        {
            return Err(LedgerError::invalid(
                "usage_ledger_reservation_request_conflict",
            ));
        }
        let pool = load_or_update_budget_pool(&transaction, &budget_id, budget_snapshot)?;
        let active = active_reservation_stats(&transaction, &budget_id)?;
        let available = available_tokens(&pool, active);
        transaction.commit().map_err(|_| LedgerError::storage())?;
        return Ok(reservation_admission_value(
            &existing,
            &pool,
            active,
            available,
            true,
            existing.state == "reserved" || existing.state == "unknown",
        ));
    }

    let pool = load_or_update_budget_pool(&transaction, &budget_id, budget_snapshot)?;
    let active = active_reservation_stats(&transaction, &budget_id)?;
    let available = available_tokens(&pool, active);

    if !chargeable {
        transaction.commit().map_err(|_| LedgerError::storage())?;
        return Ok(json!({
            "ok": true,
            "invocationId": invocation_id,
            "budgetId": budget_id,
            "admitted": true,
            "state": "free",
            "admissionMode": "free-read",
            "hardCapGuaranteed": false,
            "budget": budget_value(&pool, active, available),
        }));
    }

    let exhausted = available.is_some_and(|available| available == 0);
    let exceeds_remaining = available.is_some_and(|available| {
        estimate
            .total_tokens
            .is_some_and(|estimate| estimate > available)
    });
    let denied = exhausted || exceeds_remaining;
    if denied {
        let code = if exhausted {
            "budget_exhausted"
        } else {
            "budget_reservation_exceeds_remaining"
        };
        transaction.commit().map_err(|_| LedgerError::storage())?;
        return Ok(json!({
            "ok": true,
            "invocationId": invocation_id,
            "budgetId": budget_id,
            "admitted": false,
            "state": "waiting-budget",
            "code": code,
            "admissionMode": if estimate.total_tokens.is_some() { "estimate-only" } else { "unknown-estimate" },
            "hardCapGuaranteed": false,
            "prompt": {
                "code": "budget_adjustment_required",
                "recovery": "adjust_budget_and_resume",
                "keepsInFlight": true,
                "stopsNewDispatch": exhausted,
            },
            "estimate": estimate_value(estimate),
            "budget": budget_value(&pool, active, available),
        }));
    }

    let reserved_tokens = estimate.total_tokens.unwrap_or(0);
    let now = now_ms();
    transaction
        .execute(
            "INSERT INTO graph_usage_reservations(
               invocation_id,budget_id,run_id,command_id,estimate_accuracy,
               estimated_tokens,reserved_tokens,state,created_at_ms,updated_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,'reserved',?8,?8)",
            params![
                &invocation_id,
                &budget_id,
                run_id.as_deref(),
                command_id.as_deref(),
                estimate.accuracy.as_str(),
                sqlite_optional_u64(estimate.total_tokens, "usage_ledger_counter_overflow",)?,
                sqlite_u64(reserved_tokens, "usage_ledger_counter_overflow")?,
                now,
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    let reservation =
        load_reservation(&transaction, &invocation_id)?.ok_or_else(LedgerError::storage)?;
    let active = active_reservation_stats(&transaction, &budget_id)?;
    let available = available_tokens(&pool, active);
    transaction.commit().map_err(|_| LedgerError::storage())?;
    Ok(reservation_admission_value(
        &reservation,
        &pool,
        active,
        available,
        false,
        true,
    ))
}

/// Settle one reservation against its original invocation identity. A result
/// without usage is retained as unknown and continues to occupy its numeric
/// estimate until a later reconciliation supplies usage for the same id.
pub fn settle_graph_command(params: &Value) -> LedgerResult<Value> {
    let invocation_id = required_invocation_id(params)?;
    let run_id = optional_id(params, "runId")?;
    let command_id = optional_id(params, "commandId")?;
    let requested_budget_id = optional_budget_id(params)?;
    let usage = parse_settlement_usage(params)?;
    let settlement_status = parse_settlement_status(params, usage.is_some())?;
    let mut ledger = open_ledger(params)?;
    let transaction = ledger
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| LedgerError::storage())?;
    let existing = load_reservation(&transaction, &invocation_id)?
        .ok_or_else(|| LedgerError::invalid("usage_ledger_reservation_not_found"))?;
    ensure_reservation_identity(
        &existing,
        requested_budget_id
            .as_deref()
            .unwrap_or(&existing.budget_id),
        run_id.as_deref(),
        command_id.as_deref(),
    )?;

    if existing.state == "released" {
        return Err(LedgerError::invalid("usage_ledger_reservation_released"));
    }
    if existing.state == "settled" {
        if let Some(usage) = usage
            && existing.usage != Some(usage)
        {
            return Err(LedgerError::invalid("usage_ledger_settlement_conflict"));
        }
        let active = active_reservation_stats(&transaction, &existing.budget_id)?;
        let pool = load_or_update_budget_pool(
            &transaction,
            &existing.budget_id,
            BudgetSnapshot::default(),
        )?;
        let available = available_tokens(&pool, active);
        transaction.commit().map_err(|_| LedgerError::storage())?;
        return Ok(settlement_value(
            &existing, &pool, active, available, false, true,
        ));
    }

    let was_unknown = existing.state == "unknown";
    if let Some(usage) = usage {
        let overage = usage.total.saturating_sub(existing.reserved_tokens);
        let pool = load_or_update_budget_pool(
            &transaction,
            &existing.budget_id,
            BudgetSnapshot::default(),
        )?;
        let actual_tokens = pool
            .actual_tokens
            .checked_add(usage.total)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        let now = now_ms();
        transaction
            .execute(
                "UPDATE graph_usage_reservations SET
                   state='settled',settlement_status=?2,prompt_tokens=?3,
                   cached_input_tokens=?4,completion_tokens=?5,total_tokens=?6,
                   usage_accuracy=?7,overage_tokens=?8,updated_at_ms=?9
                 WHERE invocation_id=?1",
                params![
                    invocation_id,
                    settlement_status,
                    sqlite_u64(usage.prompt, "usage_ledger_counter_overflow")?,
                    sqlite_u64(usage.cached, "usage_ledger_counter_overflow")?,
                    sqlite_u64(usage.completion, "usage_ledger_counter_overflow")?,
                    sqlite_u64(usage.total, "usage_ledger_counter_overflow")?,
                    usage.accuracy.as_str(),
                    sqlite_u64(overage, "usage_ledger_counter_overflow")?,
                    now,
                ],
            )
            .map_err(|_| LedgerError::storage())?;
        transaction
            .execute(
                "UPDATE graph_usage_budget_pools SET actual_tokens=?2,updated_at_ms=?3
                 WHERE budget_id=?1",
                params![
                    existing.budget_id,
                    sqlite_u64(actual_tokens, "usage_ledger_counter_overflow")?,
                    now,
                ],
            )
            .map_err(|_| LedgerError::storage())?;
    } else {
        let now = now_ms();
        transaction
            .execute(
                "UPDATE graph_usage_reservations SET
                   state='unknown',settlement_status=COALESCE(settlement_status,?2),
                   updated_at_ms=?3 WHERE invocation_id=?1",
                params![invocation_id, settlement_status, now],
            )
            .map_err(|_| LedgerError::storage())?;
    }

    let settled =
        load_reservation(&transaction, &invocation_id)?.ok_or_else(LedgerError::storage)?;
    let pool =
        load_or_update_budget_pool(&transaction, &settled.budget_id, BudgetSnapshot::default())?;
    let active = active_reservation_stats(&transaction, &settled.budget_id)?;
    let available = available_tokens(&pool, active);
    transaction.commit().map_err(|_| LedgerError::storage())?;
    Ok(settlement_value(
        &settled,
        &pool,
        active,
        available,
        was_unknown,
        false,
    ))
}

/// Release a reservation that was admitted but never started. An unknown
/// external outcome is deliberately not releasable: it must settle or remain
/// in the reconciliation balance.
pub fn release_graph_command(params: &Value) -> LedgerResult<Value> {
    let invocation_id = required_invocation_id(params)?;
    let requested_budget_id = optional_budget_id(params)?;
    let mut ledger = open_ledger(params)?;
    let transaction = ledger
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| LedgerError::storage())?;
    let existing = load_reservation(&transaction, &invocation_id)?
        .ok_or_else(|| LedgerError::invalid("usage_ledger_reservation_not_found"))?;
    if requested_budget_id
        .as_deref()
        .is_some_and(|budget_id| budget_id != existing.budget_id)
    {
        return Err(LedgerError::invalid(
            "usage_ledger_reservation_identity_conflict",
        ));
    }
    match existing.state.as_str() {
        "reserved" => {
            transaction
                .execute(
                    "UPDATE graph_usage_reservations SET state='released',
                     settlement_status='not-started',updated_at_ms=?2 WHERE invocation_id=?1",
                    params![invocation_id, now_ms()],
                )
                .map_err(|_| LedgerError::storage())?;
        }
        "unknown" => {
            return Err(LedgerError::invalid("usage_ledger_unknown_not_releasable"));
        }
        "settled" | "released" => {}
        _ => return Err(LedgerError::invalid("usage_ledger_state_invalid")),
    }
    let released =
        load_reservation(&transaction, &invocation_id)?.ok_or_else(LedgerError::storage)?;
    let pool =
        load_or_update_budget_pool(&transaction, &released.budget_id, BudgetSnapshot::default())?;
    let active = active_reservation_stats(&transaction, &released.budget_id)?;
    let available = available_tokens(&pool, active);
    transaction.commit().map_err(|_| LedgerError::storage())?;
    Ok(json!({
        "ok": true,
        "invocationId": released.invocation_id,
        "budgetId": released.budget_id,
        "state": released.state,
        "released": true,
        "budget": budget_value(&pool, active, available),
    }))
}

/// Return the numeric admission state without exposing any external payload.
pub fn graph_admission_report(params: &Value) -> LedgerResult<Value> {
    let requested_run = optional_id(params, "runId")?;
    let ledger = open_ledger(params)?;
    admission_report(&ledger.connection, requested_run.as_deref())
}

fn required_invocation_id(params: &Value) -> LedgerResult<String> {
    let primary = params.get("invocationId");
    let alias = params.get("reservationId");
    let value = match (primary, alias) {
        (Some(primary), Some(alias)) => {
            let primary = primary
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
            let alias = alias
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
            if primary != alias {
                return Err(LedgerError::invalid(
                    "usage_ledger_reservation_identity_conflict",
                ));
            }
            primary
        }
        (Some(value), None) | (None, Some(value)) => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?,
        (None, None) => {
            return Err(LedgerError::invalid("usage_ledger_request_invalid"));
        }
    };
    validate_id(value)?;
    Ok(value.to_owned())
}

fn parse_budget(params: &Value) -> LedgerResult<(String, BudgetSnapshot)> {
    let nested = match params.get("budget") {
        None | Some(Value::Null) => None,
        Some(Value::Object(object)) => Some(object),
        Some(_) => return Err(LedgerError::invalid("usage_ledger_budget_invalid")),
    };
    let top = params
        .as_object()
        .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
    let nested_id = nested
        .map(|object| optional_map_id(object, &["budgetId", "poolId", "id"]))
        .transpose()?
        .flatten();
    let top_id = optional_map_id(top, &["budgetId", "poolId"])?;
    if let (Some(nested_id), Some(top_id)) = (&nested_id, &top_id)
        && nested_id != top_id
    {
        return Err(LedgerError::invalid(
            "usage_ledger_budget_identity_conflict",
        ));
    }
    let budget_id = nested_id.or(top_id).unwrap_or_else(|| "default".to_owned());
    validate_id(&budget_id)?;

    let limit_tokens = budget_number(nested, top, &["limitTokens", "limit"])?;
    let used_tokens = budget_number(nested, top, &["usedTokens", "used"])?;
    let remaining_tokens = budget_number(
        nested,
        top,
        &["remainingTokens", "remaining", "availableTokens"],
    )?;
    let supplied = limit_tokens.is_some() || used_tokens.is_some() || remaining_tokens.is_some();
    let snapshot = normalize_budget(BudgetSnapshot {
        limit_tokens,
        used_tokens,
        remaining_tokens,
        supplied,
    })?;
    Ok((budget_id, snapshot))
}

fn optional_budget_id(params: &Value) -> LedgerResult<Option<String>> {
    let top = params
        .as_object()
        .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
    let top_id = optional_map_id(top, &["budgetId", "poolId"])?;
    let nested = match params.get("budget") {
        None | Some(Value::Null) => None,
        Some(Value::Object(object)) => Some(object),
        Some(_) => return Err(LedgerError::invalid("usage_ledger_budget_invalid")),
    };
    let nested_id = nested
        .map(|object| optional_map_id(object, &["budgetId", "poolId", "id"]))
        .transpose()?
        .flatten();
    if let (Some(nested_id), Some(top_id)) = (&nested_id, &top_id)
        && nested_id != top_id
    {
        return Err(LedgerError::invalid(
            "usage_ledger_budget_identity_conflict",
        ));
    }
    Ok(nested_id.or(top_id))
}

fn optional_map_id(object: &Map<String, Value>, keys: &[&str]) -> LedgerResult<Option<String>> {
    let Some(value) = keys.iter().find_map(|key| object.get(*key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LedgerError::invalid("usage_ledger_budget_invalid"))?;
    validate_id(value)?;
    Ok(Some(value.to_owned()))
}

fn budget_number(
    nested: Option<&Map<String, Value>>,
    top: &Map<String, Value>,
    keys: &[&str],
) -> LedgerResult<Option<u64>> {
    let value = nested
        .and_then(|object| keys.iter().find_map(|key| object.get(*key)))
        .or_else(|| keys.iter().find_map(|key| top.get(*key)));
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| LedgerError::invalid("usage_ledger_budget_invalid"))
}

fn normalize_budget(mut snapshot: BudgetSnapshot) -> LedgerResult<BudgetSnapshot> {
    if let (Some(limit), Some(remaining)) = (snapshot.limit_tokens, snapshot.remaining_tokens)
        && remaining > limit
    {
        return Err(LedgerError::invalid("usage_ledger_budget_snapshot_invalid"));
    }
    if let (Some(limit), Some(used), Some(remaining)) = (
        snapshot.limit_tokens,
        snapshot.used_tokens,
        snapshot.remaining_tokens,
    ) && limit.saturating_sub(used) != remaining
    {
        return Err(LedgerError::invalid("usage_ledger_budget_snapshot_invalid"));
    }
    if let (Some(limit), Some(used)) = (snapshot.limit_tokens, snapshot.used_tokens) {
        snapshot.remaining_tokens = Some(limit.saturating_sub(used));
    } else if let (Some(limit), Some(remaining)) =
        (snapshot.limit_tokens, snapshot.remaining_tokens)
    {
        snapshot.used_tokens = Some(limit.saturating_sub(remaining));
    } else if let (Some(used), Some(remaining)) = (snapshot.used_tokens, snapshot.remaining_tokens)
    {
        snapshot.limit_tokens = Some(
            used.checked_add(remaining)
                .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?,
        );
    }
    Ok(snapshot)
}

fn parse_usage_estimate(params: &Value) -> LedgerResult<UsageEstimate> {
    let value = params
        .get("estimate")
        .or_else(|| params.get("usageEstimate"))
        .or_else(|| params.get("estimatedUsage"));
    let Some(value) = value else {
        return Ok(UsageEstimate::default());
    };
    if value.is_null() {
        return Ok(UsageEstimate::default());
    }
    if let Some(total_tokens) = value.as_u64() {
        return Ok(UsageEstimate {
            total_tokens: Some(total_tokens),
            accuracy: UsageAccuracy::Estimated,
        });
    }
    let object = value
        .as_object()
        .ok_or_else(|| LedgerError::invalid("usage_ledger_estimate_invalid"))?;
    let prompt = optional_object_number(
        object,
        &[
            "promptTokens",
            "prompt_tokens",
            "inputTokens",
            "input_tokens",
        ],
        "usage_ledger_estimate_invalid",
    )?;
    let completion = optional_object_number(
        object,
        &[
            "completionTokens",
            "completion_tokens",
            "outputTokens",
            "output_tokens",
        ],
        "usage_ledger_estimate_invalid",
    )?;
    let explicit_total = optional_object_number(
        object,
        &["totalTokens", "total_tokens", "tokens", "estimatedTokens"],
        "usage_ledger_estimate_invalid",
    )?;
    let total_tokens = match (explicit_total, prompt, completion) {
        (Some(total), _, _) => Some(total),
        (None, Some(prompt), Some(completion)) => Some(
            prompt
                .checked_add(completion)
                .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?,
        ),
        (None, Some(prompt), None) | (None, None, Some(prompt)) => Some(prompt),
        (None, None, None) => None,
    };
    let accuracy = match object
        .get("accuracy")
        .or_else(|| object.get("usageAccuracy"))
    {
        None => {
            if total_tokens.is_some() {
                UsageAccuracy::Estimated
            } else {
                UsageAccuracy::Unknown
            }
        }
        Some(Value::String(value)) => parse_accuracy_text(value, total_tokens.is_some())?,
        Some(_) => return Err(LedgerError::invalid("usage_ledger_estimate_invalid")),
    };
    Ok(UsageEstimate {
        total_tokens,
        accuracy,
    })
}

fn optional_object_number(
    object: &Map<String, Value>,
    keys: &[&str],
    error_code: &'static str,
) -> LedgerResult<Option<u64>> {
    let Some(value) = keys.iter().find_map(|key| object.get(*key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| LedgerError::invalid(error_code))
}

fn parse_accuracy_text(value: &str, has_usage: bool) -> LedgerResult<UsageAccuracy> {
    UsageAccuracy::parse(Some(value), has_usage)
}

fn chargeable(params: &Value) -> LedgerResult<bool> {
    match params.get("chargeable") {
        None => Ok(true),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(LedgerError::invalid("usage_ledger_request_invalid")),
    }
}

fn parse_settlement_usage(params: &Value) -> LedgerResult<Option<CheckedUsage>> {
    let Some(value) = params.get("usage") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let object = value
        .as_object()
        .ok_or_else(|| LedgerError::invalid("usage_ledger_usage_invalid"))?;
    let has_numeric_fields = [
        "promptTokens",
        "prompt_tokens",
        "inputTokens",
        "input_tokens",
        "completionTokens",
        "completion_tokens",
        "outputTokens",
        "output_tokens",
        "totalTokens",
        "total_tokens",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    let explicitly_exact = object
        .get("accuracy")
        .or_else(|| object.get("usageAccuracy"))
        .and_then(Value::as_str)
        == Some("exact");
    if !has_numeric_fields && !explicitly_exact {
        return Ok(None);
    }
    let mut normalized = object.clone();
    let has_prompt = [
        "promptTokens",
        "prompt_tokens",
        "inputTokens",
        "input_tokens",
    ]
    .iter()
    .any(|key| normalized.contains_key(*key));
    let has_completion = [
        "completionTokens",
        "completion_tokens",
        "outputTokens",
        "output_tokens",
    ]
    .iter()
    .any(|key| normalized.contains_key(*key));
    if !has_prompt
        && !has_completion
        && let Some(total) = optional_object_number(
            &normalized,
            &["totalTokens", "total_tokens"],
            "usage_ledger_usage_invalid",
        )?
    {
        normalized.insert("promptTokens".to_owned(), json!(total));
    }
    CheckedUsage::from_value(Some(&Value::Object(normalized))).map(Some)
}

fn parse_settlement_status(params: &Value, has_usage: bool) -> LedgerResult<&'static str> {
    let Some(value) = params
        .get("settlementStatus")
        .or_else(|| params.get("status"))
    else {
        return Ok(if has_usage { "completed" } else { "unknown" });
    };
    let value = value
        .as_str()
        .map(str::trim)
        .ok_or_else(|| LedgerError::invalid("usage_ledger_settlement_status_invalid"))?;
    match value {
        "completed" | "succeeded" => Ok("completed"),
        "failed" => Ok("failed"),
        "cancelled" | "canceled" => Ok("cancelled"),
        "unknown" | "in-doubt" => Ok("unknown"),
        _ => Err(LedgerError::invalid(
            "usage_ledger_settlement_status_invalid",
        )),
    }
}

fn sqlite_u64(value: u64, error_code: &'static str) -> LedgerResult<i64> {
    i64::try_from(value).map_err(|_| LedgerError::invalid(error_code))
}

fn sqlite_optional_u64(value: Option<u64>, error_code: &'static str) -> LedgerResult<Option<i64>> {
    value.map(|value| sqlite_u64(value, error_code)).transpose()
}

fn from_sqlite_u64(value: Option<i64>) -> LedgerResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))
        })
        .transpose()
}

fn load_or_update_budget_pool(
    connection: &Connection,
    budget_id: &str,
    snapshot: BudgetSnapshot,
) -> LedgerResult<BudgetPool> {
    connection
        .execute(
            "INSERT OR IGNORE INTO graph_usage_budget_pools(
               budget_id,limit_tokens,observed_used_tokens,observed_remaining_tokens,
               actual_tokens,updated_at_ms
             ) VALUES(?1,NULL,NULL,NULL,0,?2)",
            params![budget_id, now_ms()],
        )
        .map_err(|_| LedgerError::storage())?;
    if snapshot.supplied {
        connection
            .execute(
                "UPDATE graph_usage_budget_pools SET
                   limit_tokens=COALESCE(?2,limit_tokens),
                   observed_used_tokens=COALESCE(?3,observed_used_tokens),
                   observed_remaining_tokens=COALESCE(?4,observed_remaining_tokens),
                   updated_at_ms=?5 WHERE budget_id=?1",
                params![
                    budget_id,
                    sqlite_optional_u64(snapshot.limit_tokens, "usage_ledger_counter_overflow")?,
                    sqlite_optional_u64(snapshot.used_tokens, "usage_ledger_counter_overflow")?,
                    sqlite_optional_u64(
                        snapshot.remaining_tokens,
                        "usage_ledger_counter_overflow"
                    )?,
                    now_ms(),
                ],
            )
            .map_err(|_| LedgerError::storage())?;
    }
    let raw = connection
        .query_row(
            "SELECT budget_id,limit_tokens,observed_used_tokens,
                    observed_remaining_tokens,actual_tokens
             FROM graph_usage_budget_pools WHERE budget_id=?1",
            params![budget_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .map_err(|_| LedgerError::storage())?;
    let actual_tokens =
        u64::try_from(raw.4).map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?;
    Ok(BudgetPool {
        budget_id: raw.0,
        limit_tokens: from_sqlite_u64(raw.1)?,
        observed_used_tokens: from_sqlite_u64(raw.2)?,
        observed_remaining_tokens: from_sqlite_u64(raw.3)?,
        actual_tokens,
    })
}

fn active_reservation_stats(
    connection: &Connection,
    budget_id: &str,
) -> LedgerResult<ActiveReservationStats> {
    connection
        .query_row(
            "SELECT COALESCE(SUM(reserved_tokens),0),
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN state='unknown' THEN 1 ELSE 0 END),0)
             FROM graph_usage_reservations
             WHERE budget_id=?1 AND state IN ('reserved','unknown')",
            params![budget_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|_| LedgerError::storage())
        .and_then(|(reserved_tokens, in_flight_count, unknown_count)| {
            Ok(ActiveReservationStats {
                reserved_tokens: u64::try_from(reserved_tokens)
                    .map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?,
                in_flight_count: u64::try_from(in_flight_count)
                    .map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?,
                unknown_count: u64::try_from(unknown_count)
                    .map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?,
            })
        })
}

fn available_tokens(pool: &BudgetPool, active: ActiveReservationStats) -> Option<u64> {
    let base = if let Some(limit) = pool.limit_tokens {
        let used = pool
            .observed_used_tokens
            .unwrap_or(0)
            .max(pool.actual_tokens);
        Some(limit.saturating_sub(used))
    } else {
        pool.observed_remaining_tokens
            .map(|remaining| remaining.saturating_sub(pool.actual_tokens))
    };
    base.map(|available| available.saturating_sub(active.reserved_tokens))
}

fn estimate_value(estimate: UsageEstimate) -> Value {
    json!({
        "totalTokens": estimate.total_tokens,
        "accuracy": estimate.accuracy.as_str(),
    })
}

fn budget_value(
    pool: &BudgetPool,
    active: ActiveReservationStats,
    available: Option<u64>,
) -> Value {
    let status = match available {
        Some(0) => "exhausted",
        Some(_) => "available",
        None => "unknown",
    };
    json!({
        "budgetId": pool.budget_id,
        "limitTokens": pool.limit_tokens,
        "observedUsedTokens": pool.observed_used_tokens,
        "observedRemainingTokens": pool.observed_remaining_tokens,
        "actualTokens": pool.actual_tokens,
        "reservedTokens": active.reserved_tokens,
        "inFlightCount": active.in_flight_count,
        "unknownInFlightCount": active.unknown_count,
        "availableTokens": available,
        "status": status,
        "hardCapGuaranteed": false,
    })
}

fn reservation_admission_value(
    reservation: &ReservationRow,
    pool: &BudgetPool,
    active: ActiveReservationStats,
    available: Option<u64>,
    reused: bool,
    admitted: bool,
) -> Value {
    json!({
        "ok": true,
        "invocationId": reservation.invocation_id,
        "budgetId": reservation.budget_id,
        "admitted": admitted,
        "state": reservation.state,
        "reused": reused,
        "admissionMode": if reservation.estimated_tokens.is_some() { "estimate-only" } else { "unknown-estimate" },
        "hardCapGuaranteed": false,
        "estimate": {
            "totalTokens": reservation.estimated_tokens,
            "accuracy": reservation.estimate_accuracy.as_str(),
        },
        "reservedTokens": reservation.reserved_tokens,
        "budget": budget_value(pool, active, available),
    })
}

fn settlement_value(
    reservation: &ReservationRow,
    pool: &BudgetPool,
    active: ActiveReservationStats,
    available: Option<u64>,
    late: bool,
    idempotent: bool,
) -> Value {
    json!({
        "ok": true,
        "invocationId": reservation.invocation_id,
        "budgetId": reservation.budget_id,
        "state": reservation.state,
        "settled": reservation.state == "settled",
        "reconciliationRequired": reservation.state == "unknown",
        "late": late,
        "idempotent": idempotent,
        "settlementStatus": reservation.settlement_status,
        "usage": reservation.usage.map(CheckedUsage::to_value),
        "usageAccuracy": reservation.usage.map(|usage| usage.accuracy.as_str()),
        "reservedTokens": reservation.reserved_tokens,
        "overageTokens": reservation.overage_tokens,
        "budget": budget_value(pool, active, available),
    })
}

fn ensure_reservation_identity(
    reservation: &ReservationRow,
    budget_id: &str,
    run_id: Option<&str>,
    command_id: Option<&str>,
) -> LedgerResult<()> {
    if reservation.budget_id != budget_id
        || run_id.is_some_and(|run_id| reservation.run_id.as_deref() != Some(run_id))
        || command_id
            .is_some_and(|command_id| reservation.command_id.as_deref() != Some(command_id))
    {
        return Err(LedgerError::invalid(
            "usage_ledger_reservation_identity_conflict",
        ));
    }
    Ok(())
}

fn admission_report(connection: &Connection, requested_run: Option<&str>) -> LedgerResult<Value> {
    let mut pools_statement = connection
        .prepare("SELECT budget_id FROM graph_usage_budget_pools ORDER BY budget_id")
        .map_err(|_| LedgerError::storage())?;
    let pool_ids = pools_statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| LedgerError::storage())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|_| LedgerError::storage())?;
    let mut pools = Vec::with_capacity(pool_ids.len());
    let mut waiting = false;
    for budget_id in pool_ids {
        let pool = load_or_update_budget_pool(connection, &budget_id, BudgetSnapshot::default())?;
        let active = active_reservation_stats(connection, &budget_id)?;
        let available = available_tokens(&pool, active);
        if available == Some(0) {
            waiting = true;
        }
        pools.push(budget_value(&pool, active, available));
    }
    let reservations = load_reservations(connection, requested_run)?
        .into_iter()
        .map(|reservation| reservation_value(&reservation))
        .collect::<Vec<_>>();
    let in_flight_count = reservations
        .iter()
        .filter(|reservation| {
            matches!(
                reservation.get("state").and_then(Value::as_str),
                Some("reserved" | "unknown")
            )
        })
        .count();
    let unknown_count = reservations
        .iter()
        .filter(|reservation| reservation.get("state") == Some(&json!("unknown")))
        .count();
    let mut report = json!({
        "schemaVersion": 1,
        "status": if waiting { "waiting-budget" } else if pools.is_empty() { "unknown" } else { "ready" },
        "stopsNewDispatch": waiting,
        "keepsInFlight": waiting || in_flight_count > 0,
        "inFlightCount": in_flight_count,
        "unknownInFlightCount": unknown_count,
        "pools": pools,
        "reservations": reservations,
    });
    if waiting {
        report["prompt"] = json!({
            "code": "budget_adjustment_required",
            "recovery": "adjust_budget_and_resume",
        });
    }
    Ok(report)
}

fn reservation_value(reservation: &ReservationRow) -> Value {
    json!({
        "invocationId": reservation.invocation_id,
        "budgetId": reservation.budget_id,
        "runId": reservation.run_id,
        "commandId": reservation.command_id,
        "state": reservation.state,
        "estimateAccuracy": reservation.estimate_accuracy.as_str(),
        "estimatedTokens": reservation.estimated_tokens,
        "reservedTokens": reservation.reserved_tokens,
        "settlementStatus": reservation.settlement_status,
        "usage": reservation.usage.map(CheckedUsage::to_value),
        "overageTokens": reservation.overage_tokens,
    })
}

/// Project a bounded Graph run/command usage report. Every public string is
/// an allowlisted opaque identity or lifecycle enum.
pub fn workflow_report(params: &Value) -> LedgerResult<Value> {
    let ledger = open_ledger(params)?;
    let requested_run = optional_id(params, "runId")?;
    let admission = admission_report(&ledger.connection, requested_run.as_deref())?;
    let mut statement = ledger
        .connection
        .prepare(
            "SELECT run_id,revision_digest,conversation_id,assistant_membership_id,status
             FROM graph_runs
             WHERE (?1 IS NULL OR run_id=?1)
             ORDER BY updated_at_ms DESC,run_id DESC LIMIT ?2",
        )
        .map_err(|_| LedgerError::storage())?;
    let rows = statement
        .query_map(params![requested_run, WORKFLOW_LEDGER_MAX_RUNS], |row| {
            Ok(RunRow {
                run_id: row.get(0)?,
                revision_digest: row.get(1)?,
                conversation_id: row.get(2)?,
                assistant_membership_id: row.get(3)?,
                status: row.get(4)?,
            })
        })
        .map_err(|_| LedgerError::storage())?;
    let mut runs = Vec::new();
    let mut summary = CheckedUsage::default();
    let mut exact_count = 0u64;
    let mut estimated_count = 0u64;
    for row in rows {
        let row = row.map_err(|_| LedgerError::storage())?;
        let commands = load_commands_for_run(&ledger.connection, &row.run_id)?;
        let mut totals = CheckedUsage::default();
        let mut run_exact = 0u64;
        let mut run_estimated = 0u64;
        let command_values = commands
            .into_iter()
            .map(|command| {
                totals = totals.checked_add(command.usage)?;
                match command.usage.accuracy {
                    UsageAccuracy::Exact if command.usage.total > 0 => run_exact += 1,
                    UsageAccuracy::Estimated if command.usage.total > 0 => run_estimated += 1,
                    _ => {}
                }
                Ok(command.to_value())
            })
            .collect::<LedgerResult<Vec<_>>>()?;
        summary = summary.checked_add(totals)?;
        exact_count = exact_count
            .checked_add(run_exact)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        estimated_count = estimated_count
            .checked_add(run_estimated)
            .ok_or_else(|| LedgerError::invalid("usage_ledger_counter_overflow"))?;
        runs.push(json!({
            "runId": row.run_id,
            "revisionDigest": row.revision_digest,
            "conversationId": row.conversation_id,
            "assistantMembershipId": row.assistant_membership_id,
            "status": row.status,
            "totals": totals.to_value_with_counts(run_exact, run_estimated),
            "commands": command_values,
        }));
    }
    Ok(json!({
        "ok": true,
        "schemaVersion": WORKFLOW_LEDGER_REPORT_SCHEMA,
        "ledgerSchemaVersion": WORKFLOW_LEDGER_SCHEMA_VERSION,
        "resultKind": WORKFLOW_LEDGER_RESULT_KIND,
        "summary": summary.to_value_with_counts(exact_count, estimated_count),
        "admission": admission,
        "runs": runs,
    }))
}

#[derive(Debug)]
struct RunRow {
    run_id: String,
    revision_digest: String,
    conversation_id: Option<String>,
    assistant_membership_id: Option<String>,
    status: String,
}

#[derive(Debug)]
struct CommandRow {
    command_id: String,
    run_id: String,
    state_id: String,
    membership_id: Option<String>,
    kind: String,
    status: String,
    attempt: u64,
    agent_id: Option<String>,
    model: Option<String>,
    usage: CheckedUsage,
}

struct RawReservationRow {
    invocation_id: String,
    budget_id: String,
    run_id: Option<String>,
    command_id: Option<String>,
    estimate_accuracy: String,
    estimated_tokens: Option<i64>,
    reserved_tokens: i64,
    state: String,
    settlement_status: Option<String>,
    prompt_tokens: Option<i64>,
    cached_input_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
    usage_accuracy: Option<String>,
    overage_tokens: i64,
}

fn load_reservation(
    connection: &Connection,
    invocation_id: &str,
) -> LedgerResult<Option<ReservationRow>> {
    let raw = connection
        .query_row(
            "SELECT invocation_id,budget_id,run_id,command_id,estimate_accuracy,
                    estimated_tokens,reserved_tokens,state,settlement_status,
                    prompt_tokens,cached_input_tokens,completion_tokens,total_tokens,
                    usage_accuracy,overage_tokens
             FROM graph_usage_reservations WHERE invocation_id=?1",
            params![invocation_id],
            raw_reservation_from_row,
        )
        .optional()
        .map_err(|_| LedgerError::storage())?;
    raw.map(decode_reservation).transpose()
}

fn load_reservations(
    connection: &Connection,
    run_id: Option<&str>,
) -> LedgerResult<Vec<ReservationRow>> {
    let mut statement = connection
        .prepare(
            "SELECT invocation_id,budget_id,run_id,command_id,estimate_accuracy,
                    estimated_tokens,reserved_tokens,state,settlement_status,
                    prompt_tokens,cached_input_tokens,completion_tokens,total_tokens,
                    usage_accuracy,overage_tokens
             FROM graph_usage_reservations
             WHERE (?1 IS NULL OR run_id=?1)
             ORDER BY updated_at_ms DESC,invocation_id DESC",
        )
        .map_err(|_| LedgerError::storage())?;
    let rows = statement
        .query_map(params![run_id], raw_reservation_from_row)
        .map_err(|_| LedgerError::storage())?;
    let mut reservations = Vec::new();
    for row in rows {
        let raw = row.map_err(|_| LedgerError::storage())?;
        reservations.push(decode_reservation(raw)?);
    }
    Ok(reservations)
}

fn raw_reservation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawReservationRow> {
    Ok(RawReservationRow {
        invocation_id: row.get(0)?,
        budget_id: row.get(1)?,
        run_id: row.get(2)?,
        command_id: row.get(3)?,
        estimate_accuracy: row.get(4)?,
        estimated_tokens: row.get(5)?,
        reserved_tokens: row.get(6)?,
        state: row.get(7)?,
        settlement_status: row.get(8)?,
        prompt_tokens: row.get(9)?,
        cached_input_tokens: row.get(10)?,
        completion_tokens: row.get(11)?,
        total_tokens: row.get(12)?,
        usage_accuracy: row.get(13)?,
        overage_tokens: row.get(14)?,
    })
}

fn decode_reservation(raw: RawReservationRow) -> LedgerResult<ReservationRow> {
    let estimate_accuracy = stored_accuracy(&raw.estimate_accuracy)?;
    let estimated_tokens = from_sqlite_u64(raw.estimated_tokens)?;
    let reserved_tokens = u64::try_from(raw.reserved_tokens)
        .map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?;
    let overage_tokens = u64::try_from(raw.overage_tokens)
        .map_err(|_| LedgerError::invalid("usage_ledger_storage_invalid"))?;
    let usage = match raw.total_tokens {
        None => None,
        Some(_) => Some(CheckedUsage {
            prompt: from_sqlite_u64(raw.prompt_tokens)?.unwrap_or(0),
            cached: from_sqlite_u64(raw.cached_input_tokens)?.unwrap_or(0),
            completion: from_sqlite_u64(raw.completion_tokens)?.unwrap_or(0),
            total: from_sqlite_u64(raw.total_tokens)?.unwrap_or(0),
            accuracy: stored_accuracy(raw.usage_accuracy.as_deref().unwrap_or("unknown"))?,
        }),
    };
    Ok(ReservationRow {
        invocation_id: raw.invocation_id,
        budget_id: raw.budget_id,
        run_id: raw.run_id,
        command_id: raw.command_id,
        estimate_accuracy,
        estimated_tokens,
        reserved_tokens,
        state: raw.state,
        settlement_status: raw.settlement_status,
        usage,
        overage_tokens,
    })
}

fn stored_accuracy(value: &str) -> LedgerResult<UsageAccuracy> {
    match value {
        "exact" => Ok(UsageAccuracy::Exact),
        "estimated" => Ok(UsageAccuracy::Estimated),
        "unknown" => Ok(UsageAccuracy::Unknown),
        _ => Err(LedgerError::invalid("usage_ledger_storage_invalid")),
    }
}

impl CommandRow {
    fn to_value(&self) -> Value {
        json!({
            "commandId": self.command_id,
            "stateId": self.state_id,
            "membershipId": self.membership_id,
            "kind": self.kind,
            "status": self.status,
            "attempt": self.attempt,
            "agentId": self.agent_id,
            "model": self.model,
            "accuracy": self.usage.accuracy.as_str(),
            "usage": self.usage.to_value(),
        })
    }
}

pub fn open_ledger(params: &Value) -> LedgerResult<Ledger> {
    let store = client_state_store(params).map_err(|_| LedgerError::storage())?;
    let root = store.root().join("agent-usage");
    fs::create_dir_all(&root).map_err(|_| LedgerError::storage())?;
    #[cfg(unix)]
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
        .map_err(|_| LedgerError::storage())?;
    let mut attempt = 0;
    let connection = loop {
        let opened =
            Connection::open(root.join(WORKFLOW_LEDGER_FILE_NAME)).and_then(|connection| {
                connection
                    .execute_batch(WORKFLOW_LEDGER_INIT)
                    .map(|_| connection)
            });
        match opened {
            Ok(connection) => break connection,
            Err(error) if attempt < LEDGER_OPEN_BUSY_RETRIES && is_database_busy(&error) => {
                attempt += 1;
                std::thread::sleep(LEDGER_OPEN_BUSY_DELAY);
            }
            Err(_) => return Err(LedgerError::storage()),
        }
    };
    let version: i64 = connection
        .query_row(
            "SELECT version FROM graph_usage_schema WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| LedgerError::storage())?;
    if version != WORKFLOW_LEDGER_SCHEMA_VERSION {
        return Err(LedgerError::invalid("usage_ledger_schema_unsupported"));
    }
    Ok(Ledger { connection })
}

type RunIdentity = (String, Option<String>, Option<String>);

fn load_run_identity(connection: &Connection, run_id: &str) -> LedgerResult<Option<RunIdentity>> {
    connection
        .query_row(
            "SELECT revision_digest,conversation_id,assistant_membership_id
             FROM graph_runs WHERE run_id=?1",
            params![run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|_| LedgerError::storage())
}

fn load_command(connection: &Connection, command_id: &str) -> LedgerResult<Option<CommandRow>> {
    connection
        .query_row(
            "SELECT command_id,run_id,state_id,membership_id,kind,status,attempt,
                    agent_id,model,accuracy,prompt_tokens,cached_input_tokens,
                    completion_tokens,total_tokens
             FROM graph_commands WHERE command_id=?1",
            params![command_id],
            command_from_row,
        )
        .optional()
        .map_err(|_| LedgerError::storage())
}

fn load_commands_for_run(connection: &Connection, run_id: &str) -> LedgerResult<Vec<CommandRow>> {
    let mut statement = connection
        .prepare(
            "SELECT command_id,run_id,state_id,membership_id,kind,status,attempt,
                    agent_id,model,accuracy,prompt_tokens,cached_input_tokens,
                    completion_tokens,total_tokens
             FROM graph_commands WHERE run_id=?1 ORDER BY command_id",
        )
        .map_err(|_| LedgerError::storage())?;
    statement
        .query_map(params![run_id], command_from_row)
        .map_err(|_| LedgerError::storage())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|_| LedgerError::storage())
}

fn command_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CommandRow> {
    let accuracy: String = row.get(9)?;
    Ok(CommandRow {
        command_id: row.get(0)?,
        run_id: row.get(1)?,
        state_id: row.get(2)?,
        membership_id: row.get(3)?,
        kind: row.get(4)?,
        status: row.get(5)?,
        attempt: row.get(6)?,
        agent_id: row.get(7)?,
        model: row.get(8)?,
        usage: CheckedUsage {
            accuracy: match accuracy.as_str() {
                "exact" => UsageAccuracy::Exact,
                "estimated" => UsageAccuracy::Estimated,
                _ => UsageAccuracy::Unknown,
            },
            prompt: row.get(10)?,
            cached: row.get(11)?,
            completion: row.get(12)?,
            total: row.get(13)?,
        },
    })
}

fn prune_terminal_runs(connection: &Connection) -> LedgerResult<()> {
    connection
        .execute(
            "DELETE FROM graph_runs
             WHERE status IN ('blocked','cancelled','failed','completed')
               AND run_id NOT IN (
                 SELECT run_id FROM graph_runs
                 WHERE status IN ('blocked','cancelled','failed','completed')
                 ORDER BY updated_at_ms DESC,run_id DESC LIMIT ?1
               )",
            params![WORKFLOW_LEDGER_MAX_RUNS],
        )
        .map_err(|_| LedgerError::storage())?;
    Ok(())
}

fn required_text<'a>(params: &'a Value, key: &str) -> LedgerResult<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))
}

fn required_id(params: &Value, key: &str) -> LedgerResult<String> {
    let value = required_text(params, key)?;
    validate_id(value)?;
    Ok(value.to_owned())
}

fn optional_id(params: &Value, key: &str) -> LedgerResult<Option<String>> {
    let Some(value) = params.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
    validate_id(value)?;
    Ok(Some(value.to_owned()))
}

fn optional_label(params: &Value, key: &str) -> LedgerResult<Option<String>> {
    let Some(value) = params.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LedgerError::invalid("usage_ledger_request_invalid"))?;
    if value.len() > 256 || value.chars().any(|character| character.is_control()) {
        return Err(LedgerError::invalid("usage_ledger_label_invalid"));
    }
    Ok(Some(value.to_owned()))
}

fn validate_id(value: &str) -> LedgerResult<()> {
    if value.len() > 256
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'+' | b'-')
        })
    {
        return Err(LedgerError::invalid("usage_ledger_identity_invalid"));
    }
    Ok(())
}

fn lifecycle(value: &str) -> LedgerResult<&str> {
    if matches!(
        value,
        "pending"
            | "authorization-required"
            | "runtime-missing"
            | "running"
            | "waiting"
            | "retryable"
            | "cancel-requested"
            | "cancel-in-doubt"
            | "blocked"
            | "cancelled"
            | "failed"
            | "completed"
    ) {
        Ok(value)
    } else {
        Err(LedgerError::invalid("usage_ledger_status_invalid"))
    }
}

fn command_kind(value: &str) -> LedgerResult<&str> {
    if matches!(value, "authorization" | "actor" | "script" | "workset-item") {
        Ok(value)
    } else {
        Err(LedgerError::invalid("usage_ledger_command_kind_invalid"))
    }
}

fn command_status(value: &str) -> LedgerResult<&str> {
    if matches!(
        value,
        "pending"
            | "claimed"
            | "running"
            | "succeeded"
            | "failed"
            | "retryable"
            | "cancel-requested"
            | "cancelled"
            | "in-doubt"
    ) {
        Ok(value)
    } else {
        Err(LedgerError::invalid("usage_ledger_command_status_invalid"))
    }
}

fn numeric_field(object: &Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| object.get(*key)?.as_u64())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        std::env::temp_dir().join(format!("lico-graph-usage-{}", uuid::Uuid::new_v4()))
    }

    fn begin(root: &std::path::Path) -> Value {
        json!({
            "stateRoot": root.to_string_lossy(),
            "runId": "run:one",
            "revisionDigest": "revision:one",
            "conversationId": "conversation:one",
            "assistantMembershipId": "membership:assistant",
            "status": "running",
        })
    }

    #[test]
    fn graph_command_usage_is_checked_and_idempotent() {
        let root = root();
        begin_graph_run(&begin(&root)).unwrap();
        let command = json!({
            "stateRoot": root.to_string_lossy(),
            "runId": "run:one",
            "commandId": "command:one",
            "stateId": "state:actor",
            "membershipId": "membership:worker",
            "kind": "actor",
            "status": "succeeded",
            "attempt": 1,
            "agentId": "codex",
            "model": "gpt-model",
            "usage": {
                "promptTokens": 11,
                "cachedInputTokens": 3,
                "completionTokens": 5,
                "totalTokens": 16,
                "accuracy": "exact",
                "prompt": "not persisted",
            }
        });
        record_graph_command(&command).unwrap();
        record_graph_command(&command).unwrap();
        let report = workflow_report(&json!({"stateRoot": root.to_string_lossy()})).unwrap();
        assert_eq!(report["summary"]["totalTokens"], 16);
        assert_eq!(report["summary"]["exactCount"], 1);
        assert_eq!(report["runs"][0]["commands"].as_array().unwrap().len(), 1);
        let serialized = report.to_string();
        assert!(!serialized.contains("not persisted"));
        assert!(!serialized.contains("\"prompt\":"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn conflicting_identity_and_invalid_totals_fail_closed() {
        let root = root();
        begin_graph_run(&begin(&root)).unwrap();
        let conflict = begin_graph_run(&json!({
            "stateRoot": root.to_string_lossy(),
            "runId": "run:one",
            "revisionDigest": "revision:other",
            "status": "running",
        }))
        .unwrap_err();
        assert_eq!(conflict.code, "usage_ledger_run_identity_conflict");
        let invalid = record_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "runId": "run:one",
            "commandId": "command:one",
            "stateId": "state:actor",
            "kind": "actor",
            "status": "succeeded",
            "usage": {"promptTokens": 2, "completionTokens": 3, "totalTokens": 6},
        }))
        .unwrap_err();
        assert_eq!(invalid.code, "usage_ledger_total_mismatch");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reservation_denies_new_work_when_pool_is_exhausted_and_keeps_in_flight() {
        let root = root();
        let first = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:one",
            "invocationId": "invocation:one",
            "runId": "run:one",
            "commandId": "command:one",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 10, "accuracy": "estimated"}
        }))
        .unwrap();
        assert_eq!(first["admitted"], true);
        assert_eq!(first["budget"]["availableTokens"], 0);

        let denied = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:one",
            "invocationId": "invocation:two",
            "runId": "run:one",
            "commandId": "command:two",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 1, "accuracy": "estimated"}
        }))
        .unwrap();
        assert_eq!(denied["admitted"], false);
        assert_eq!(denied["state"], "waiting-budget");
        assert_eq!(denied["code"], "budget_exhausted");
        assert_eq!(denied["prompt"]["keepsInFlight"], true);
        assert_eq!(denied["prompt"]["stopsNewDispatch"], true);

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["status"], "waiting-budget");
        assert_eq!(report["stopsNewDispatch"], true);
        assert_eq!(report["inFlightCount"], 1);

        let settled = settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:one",
            "settlementStatus": "completed",
            "usage": {
                "promptTokens": 3,
                "completionTokens": 1,
                "totalTokens": 4,
                "accuracy": "exact"
            }
        }))
        .unwrap();
        assert_eq!(settled["state"], "settled");
        assert_eq!(settled["budget"]["actualTokens"], 4);
        assert_eq!(settled["budget"]["availableTokens"], 6);

        let admitted_after_settlement = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:one",
            "invocationId": "invocation:three",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 5, "accuracy": "estimated"}
        }))
        .unwrap();
        assert_eq!(admitted_after_settlement["admitted"], true);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_settlement_holds_reservation_until_late_usage_and_records_overage() {
        let root = root();
        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:late",
            "invocationId": "invocation:late",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 4, "accuracy": "estimated"}
        }))
        .unwrap();

        let unknown = settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:late",
            "settlementStatus": "failed"
        }))
        .unwrap();
        assert_eq!(unknown["state"], "unknown");
        assert_eq!(unknown["reconciliationRequired"], true);
        assert_eq!(unknown["budget"]["reservedTokens"], 4);

        let denied = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:late",
            "invocationId": "invocation:blocked",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 7, "accuracy": "estimated"}
        }))
        .unwrap();
        assert_eq!(denied["admitted"], false);

        let late = settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:late",
            "settlementStatus": "completed",
            "usage": {
                "promptTokens": 5,
                "completionTokens": 3,
                "totalTokens": 8,
                "accuracy": "exact"
            }
        }))
        .unwrap();
        assert_eq!(late["state"], "settled");
        assert_eq!(late["late"], true);
        assert_eq!(late["overageTokens"], 4);
        assert_eq!(late["budget"]["actualTokens"], 8);

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["unknownInFlightCount"], 0);
        assert_eq!(report["reservations"][0]["overageTokens"], 4);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_estimate_and_unknown_result_are_not_counted_as_zero_usage() {
        let root = root();
        let admitted = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:unknown",
            "invocationId": "invocation:unknown",
            "budget": {"limitTokens": 10, "usedTokens": 0}
        }))
        .unwrap();
        assert_eq!(admitted["admitted"], true);
        assert_eq!(admitted["admissionMode"], "unknown-estimate");
        assert_eq!(admitted["hardCapGuaranteed"], false);

        let unknown = settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:unknown",
            "settlementStatus": "unknown"
        }))
        .unwrap();
        assert_eq!(unknown["state"], "unknown");
        assert_eq!(unknown["budget"]["inFlightCount"], 1);

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["unknownInFlightCount"], 1);
        assert_eq!(report["reservations"][0]["estimatedTokens"], Value::Null);
        assert_eq!(report["reservations"][0]["reservedTokens"], 0);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exhausted_budget_stops_paid_dispatch_but_allows_free_reads() {
        let root = root();
        let denied = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:empty",
            "invocationId": "invocation:zero",
            "budget": {"limitTokens": 0, "usedTokens": 0},
            "estimate": {"totalTokens": 0, "accuracy": "estimated"}
        }))
        .unwrap();
        assert_eq!(denied["admitted"], false);
        assert_eq!(denied["prompt"]["stopsNewDispatch"], true);

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["status"], "waiting-budget");
        assert_eq!(report["stopsNewDispatch"], true);
        assert_eq!(report["inFlightCount"], 0);

        let free_read = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:empty",
            "invocationId": "invocation:free",
            "chargeable": false,
            "budget": {"limitTokens": 0, "usedTokens": 0}
        }))
        .unwrap();
        assert_eq!(free_read["admitted"], true);
        assert_eq!(free_read["admissionMode"], "free-read");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_reservations_share_one_atomic_budget_pool() {
        let root = root();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let first_root = root.clone();
        let first_barrier = std::sync::Arc::clone(&barrier);
        let second_root = root.clone();
        let second_barrier = std::sync::Arc::clone(&barrier);
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(move || {
                first_barrier.wait();
                reserve_graph_command(&json!({
                    "stateRoot": first_root.to_string_lossy(),
                    "budgetId": "pool:atomic",
                    "invocationId": "invocation:atomic-one",
                    "budget": {"limitTokens": 10, "usedTokens": 0},
                    "estimate": {"totalTokens": 6, "accuracy": "estimated"}
                }))
            });
            let second = scope.spawn(move || {
                second_barrier.wait();
                reserve_graph_command(&json!({
                    "stateRoot": second_root.to_string_lossy(),
                    "budgetId": "pool:atomic",
                    "invocationId": "invocation:atomic-two",
                    "budget": {"limitTokens": 10, "usedTokens": 0},
                    "estimate": {"totalTokens": 6, "accuracy": "estimated"}
                }))
            });
            (first.join().unwrap(), second.join().unwrap())
        });
        let admitted = [first.unwrap(), second.unwrap()]
            .iter()
            .filter(|value| value["admitted"] == true)
            .count();
        assert_eq!(admitted, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn existing_graph_command_settlement_closes_matching_reservation() {
        let root = root();
        begin_graph_run(&begin(&root)).unwrap();
        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:record",
            "invocationId": "command:record",
            "runId": "run:one",
            "commandId": "command:record",
            "budget": {"limitTokens": 20, "usedTokens": 0},
            "estimate": {"totalTokens": 6, "accuracy": "estimated"}
        }))
        .unwrap();
        record_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "runId": "run:one",
            "commandId": "command:record",
            "stateId": "state:actor",
            "kind": "actor",
            "status": "succeeded",
            "usage": {
                "promptTokens": 2,
                "completionTokens": 1,
                "totalTokens": 3,
                "accuracy": "exact"
            }
        }))
        .unwrap();
        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["reservations"][0]["state"], "settled");
        assert_eq!(report["pools"][0]["actualTokens"], 3);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn release_returns_reserved_budget_and_unknown_is_not_releasable() {
        let root = root();
        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:release",
            "invocationId": "invocation:release",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 4, "accuracy": "estimated"}
        }))
        .unwrap();

        let released = release_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:release"
        }))
        .unwrap();
        assert_eq!(released["state"], "released");
        assert_eq!(released["budget"]["availableTokens"], 10);

        let replay = release_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:release"
        }))
        .unwrap();
        assert_eq!(replay["state"], "released");

        let missing = release_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:missing"
        }))
        .unwrap_err();
        assert_eq!(missing.code, "usage_ledger_reservation_not_found");

        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:release",
            "invocationId": "invocation:held",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 4, "accuracy": "estimated"}
        }))
        .unwrap();
        let unknown = settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:held",
            "settlementStatus": "unknown"
        }))
        .unwrap();
        assert_eq!(unknown["state"], "unknown");

        let not_releasable = release_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:held"
        }))
        .unwrap_err();
        assert_eq!(not_releasable.code, "usage_ledger_unknown_not_releasable");

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["unknownInFlightCount"], 1);
        assert_eq!(report["pools"][0]["reservedTokens"], 4);
        assert_eq!(report["pools"][0]["availableTokens"], 6);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn settlement_replay_is_idempotent_and_conflicting_usage_fails_closed() {
        let root = root();
        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:idempotent",
            "invocationId": "invocation:idempotent",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 6, "accuracy": "estimated"}
        }))
        .unwrap();
        let settle = |usage_total: u64| {
            settle_graph_command(&json!({
                "stateRoot": root.to_string_lossy(),
                "invocationId": "invocation:idempotent",
                "settlementStatus": "completed",
                "usage": {
                    "promptTokens": usage_total,
                    "completionTokens": 0,
                    "totalTokens": usage_total,
                    "accuracy": "exact"
                }
            }))
        };
        let first = settle(4).unwrap();
        assert_eq!(first["settled"], true);
        assert_eq!(first["idempotent"], false);

        let replay = settle(4).unwrap();
        assert_eq!(replay["idempotent"], true);
        assert_eq!(replay["budget"]["actualTokens"], 4);

        let conflict = settle(5).unwrap_err();
        assert_eq!(conflict.code, "usage_ledger_settlement_conflict");

        let report = graph_admission_report(&json!({
            "stateRoot": root.to_string_lossy()
        }))
        .unwrap();
        assert_eq!(report["pools"][0]["actualTokens"], 4);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reservation_replay_requires_the_identical_request() {
        let root = root();
        let request = json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:replay",
            "invocationId": "invocation:replay",
            "runId": "run:replay",
            "budget": {"limitTokens": 10, "usedTokens": 0},
            "estimate": {"totalTokens": 4, "accuracy": "estimated"}
        });
        let first = reserve_graph_command(&request).unwrap();
        assert_eq!(first["admitted"], true);
        assert_eq!(first["reused"], false);

        let replay = reserve_graph_command(&request).unwrap();
        assert_eq!(replay["reused"], true);
        assert_eq!(replay["admitted"], true);

        let mut changed_estimate = request.clone();
        changed_estimate["estimate"] = json!({"totalTokens": 5, "accuracy": "estimated"});
        let conflict = reserve_graph_command(&changed_estimate).unwrap_err();
        assert_eq!(conflict.code, "usage_ledger_reservation_request_conflict");

        let mut changed_budget = request.clone();
        changed_budget["budgetId"] = json!("pool:other");
        changed_budget["budget"] =
            json!({"budgetId": "pool:other", "limitTokens": 10, "usedTokens": 0});
        let conflict = reserve_graph_command(&changed_budget).unwrap_err();
        assert_eq!(conflict.code, "usage_ledger_reservation_identity_conflict");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exhausted_pool_denies_unknown_estimates_without_claiming_an_estimate_mode() {
        let root = root();
        reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:full",
            "invocationId": "invocation:full",
            "budget": {"limitTokens": 2, "usedTokens": 0},
            "estimate": {"totalTokens": 2, "accuracy": "estimated"}
        }))
        .unwrap();
        let denied = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:full",
            "invocationId": "invocation:unestimated",
            "budget": {"limitTokens": 2, "usedTokens": 0}
        }))
        .unwrap();
        assert_eq!(denied["admitted"], false);
        assert_eq!(denied["code"], "budget_exhausted");
        assert_eq!(denied["admissionMode"], "unknown-estimate");
        assert_eq!(denied["hardCapGuaranteed"], false);
        let _ = fs::remove_dir_all(root);
    }
}
