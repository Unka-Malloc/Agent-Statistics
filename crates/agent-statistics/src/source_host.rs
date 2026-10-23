//! The production owner of the base usage journal.
//!
//! [`UsageJournal`] is the kernel's settlement seam: it turns what a source
//! observed into durable facts in the ledger store the graph-run reservations
//! already live in. A journal nobody constructs is not a seam, so this module
//! is the composition that owns it on the client's real paths:
//!
//! - [`UsageSourceHost::open`] resolves the client's data home — the explicit
//!   `stateRoot`, or the portable client-state root — and opens the journal over
//!   that home's existing `graph-usage-ledger-v2.sqlite3`. There is one store,
//!   never a second one: facts, source events and source cursors are additive
//!   tables of the ledger the workflow owner already owns. [`install`] is how a
//!   verb puts that host in the process registry, and [`source_host`] only ever
//!   reads it back: a run that skips the install has no metering surface, which
//!   is visible as an unsettled reading and an unavailable projection rather
//!   than as a silently created second journal.
//! - The `agent-usage` verbs drive it. Every reading a production scan takes
//!   from the client's own histories settles as a base fact through
//!   [`MeteringFactPort::record_all`], and the retained report projects what the
//!   ledger holds through [`MeteringFactPort::read`] and
//!   [`MeteringFactPort::pending`]. A scan that runs again replays: the same
//!   reading settles as `Unchanged` instead of a second fact.
//! - The transports of third-party sources deliver through
//!   [`UsageSourceHost::publish`], [`UsageSourceHost::resume_request`] and
//!   [`UsageSourceHost::admit_page`] — the [`UsageSourcePort`] on the same
//!   journal, so a pushed batch, a pulled page and a resumed cursor all land on
//!   one durable record with one settlement authority.
//!
//! A reading is a fact about what a source observed, never about what it may
//! charge: the host grants [`SettlementEligibility`], and a reading whose
//! producer counted requests without tokens settles as `unknown` with no
//! number, because a zero would be a claim that nothing happened.

use crate::contract::HistoryUsageSummary;
use crate::journal::{
    FactOutcome, FactPage, FactReceipt, IngestReport, MAX_FACT_PAGE, MeteringFact,
    MeteringFactPort, PendingObligation, SettlementEligibility, SourceAdmission, UsageJournal,
    UsageSourcePort,
};
use crate::ledger::{
    LedgerError, LedgerResult, WORKFLOW_LEDGER_FILE_NAME, WORKFLOW_LEDGER_SCHEMA_VERSION,
};
use crate::persistence::client_state_store;
use licoup_extension_contracts::usage::Quality;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use token_usage_detector::binding::{BoundObservation, SourceBinding};
use token_usage_detector::collection::{PublishBatch, QueryPage, QueryRequest};
use token_usage_detector::metrics as general;

/// The journal generation these facts belong to, as the report names it.
pub const USAGE_JOURNAL_SCHEMA: &str = "usage-journal.v1";

/// The unit every general token metric is read in.
const TOKEN_UNIT: &str = "tokens";

/// What one production settle of a scan's readings did.
///
/// It is the receipt vocabulary of a replay: a first delivery inserts, a
/// corrected reading updates, and a scan that reads the same numbers again
/// reports them `replayed` without writing a row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadingSettlement {
    /// Facts this settle wrote.
    pub facts: usize,
    pub inserted: usize,
    pub corrected: usize,
    pub replayed: usize,
    pub superseded: usize,
    /// Readings recorded as unknown because their producer counted requests
    /// without tokens.
    pub unknown_readings: usize,
}

impl ReadingSettlement {
    pub fn to_json(&self) -> Value {
        json!({
            "facts": self.facts,
            "inserted": self.inserted,
            "corrected": self.corrected,
            "replayed": self.replayed,
            "superseded": self.superseded,
            "unknownReadings": self.unknown_readings
        })
    }

    fn observe(&mut self, receipt: &FactReceipt) {
        match receipt.outcome {
            FactOutcome::Inserted => self.inserted += 1,
            FactOutcome::Updated => self.corrected += 1,
            FactOutcome::Unchanged => self.replayed += 1,
            _ => self.superseded += 1,
        }
    }
}

/// The host's composition of the base journal over one resolved data home.
pub struct UsageSourceHost {
    journal: UsageJournal,
    data_home: PathBuf,
}

impl std::fmt::Debug for UsageSourceHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UsageSourceHost")
            .field("data_home", &self.data_home)
            .field("ledger", &self.ledger_path())
            .finish()
    }
}

impl UsageSourceHost {
    /// The production entry: resolve the data home, then open the base journal
    /// in the ledger store that home already holds.
    pub fn open(params: &Value) -> LedgerResult<Self> {
        let data_home = data_home(params)?;
        let resolved = json!({"stateRoot": data_home.to_string_lossy()});
        Ok(Self {
            journal: UsageJournal::open(&resolved)?,
            data_home,
        })
    }

    /// The data home this host resolved.
    pub fn data_home(&self) -> &Path {
        &self.data_home
    }

    /// The ledger store the journal shares with the workflow owner.
    pub fn ledger_path(&self) -> PathBuf {
        self.data_home
            .join("agent-usage")
            .join(WORKFLOW_LEDGER_FILE_NAME)
    }

    /// Settle one bounded batch a source pushed (`usage.publish`).
    pub fn publish(
        &mut self,
        admission: &SourceAdmission,
        batch: &PublishBatch,
    ) -> LedgerResult<IngestReport> {
        UsageSourcePort::publish(&mut self.journal, admission, batch)
    }

    /// Settle one observation the host already bound.
    pub fn admit_observation(
        &mut self,
        admission: &SourceAdmission,
        bound: &BoundObservation,
    ) -> LedgerResult<crate::journal::EventSettlement> {
        UsageSourcePort::admit_observation(&mut self.journal, admission, bound)
    }

    /// The request that reads a source: no cursor for the first page, the
    /// durable cursor to resume.
    pub fn resume_request(
        &self,
        admission: &SourceAdmission,
        scope_ref: &str,
        limit: Option<u32>,
    ) -> LedgerResult<QueryRequest> {
        UsageSourcePort::resume_request(&self.journal, admission, scope_ref, limit)
    }

    /// Settle one bounded page the host pulled in answer to `request`, then
    /// advance the durable cursor.
    pub fn admit_page(
        &mut self,
        admission: &SourceAdmission,
        request: &QueryRequest,
        page: &QueryPage,
    ) -> LedgerResult<IngestReport> {
        UsageSourcePort::admit_page(&mut self.journal, admission, request, page)
    }

    /// The durable position for this source and its epoch.
    pub fn durable_cursor(&self, binding: &SourceBinding) -> LedgerResult<Option<String>> {
        UsageSourcePort::durable_cursor(&self.journal, binding)
    }

    /// Record one fact, keyed by `fact_id`.
    pub fn record(&mut self, fact: MeteringFact) -> LedgerResult<FactReceipt> {
        MeteringFactPort::record(&mut self.journal, fact)
    }

    /// Record a bounded set of facts as one delivery.
    pub fn record_all(&mut self, facts: Vec<MeteringFact>) -> LedgerResult<Vec<FactReceipt>> {
        MeteringFactPort::record_all(&mut self.journal, facts)
    }

    /// Withdraw one fact by identity.
    pub fn retract(&mut self, fact_id: &str) -> LedgerResult<FactReceipt> {
        MeteringFactPort::retract(&mut self.journal, fact_id)
    }

    /// Read facts, by cursor.
    pub fn read(&self, cursor: Option<&str>, limit: usize) -> LedgerResult<FactPage> {
        MeteringFactPort::read(&self.journal, cursor, limit)
    }

    /// The obligations the ledger still holds unsettled.
    pub fn pending(&self) -> LedgerResult<Vec<PendingObligation>> {
        MeteringFactPort::pending(&self.journal)
    }

    /// The exact total of one scope and metric over the facts that may settle.
    pub fn settled_total(&self, scope_ref: &str, metric: &str) -> LedgerResult<Option<String>> {
        self.journal.settled_total(scope_ref, metric)
    }

    /// The scope one agent's readings settle into: its whole retained window.
    pub fn agent_scope(agent_id: &str) -> String {
        format!("agent:{agent_id}")
    }

    /// Settle the readings one production scan took from an agent's histories.
    ///
    /// Each counted reading of one day and model settles as one fact per
    /// general token metric; the same reading read again is a replay. A reading
    /// that mixes counted and estimated records settles as `estimated`, because
    /// the conservative claim is the honest one. A model-day whose requests
    /// carried no token fields settles as `unknown` with no number at all: the
    /// host never turns a tokenless producer into a zero.
    pub fn settle_readings(
        &mut self,
        agent_id: &str,
        summary: &HistoryUsageSummary,
    ) -> LedgerResult<ReadingSettlement> {
        let mut facts = Vec::new();
        let mut unknown_readings = 0usize;
        for (day, daily) in &summary.daily_usage {
            for (model, usage) in &daily.model_usage {
                let has_tokens = usage.prompt_tokens > 0
                    || usage.cached_input_tokens > 0
                    || usage.completion_tokens > 0
                    || usage.total_tokens > 0;
                if has_tokens {
                    let quality = if usage.estimated_prompt_tokens > 0
                        || usage.estimated_completion_tokens > 0
                    {
                        Quality::Estimated
                    } else {
                        Quality::Reported
                    };
                    let identity = format!("reading:{day}:{model}");
                    for (metric, value) in [
                        (general::TOKENS_INPUT, usage.prompt_tokens),
                        (general::TOKENS_CACHED_INPUT, usage.cached_input_tokens),
                        (general::TOKENS_OUTPUT, usage.completion_tokens),
                        (general::TOKENS_TOTAL, usage.total_tokens),
                    ] {
                        facts.push(reading_fact(
                            agent_id,
                            &identity,
                            metric,
                            Some(value.to_string()),
                            quality,
                            day,
                        ));
                    }
                }
                if usage.token_unavailable_requests > 0 {
                    // Requests were counted and their tokens were not reported:
                    // the reading is unknown, and an unknown reading has no
                    // number rather than a zero. A partly reported model-day
                    // keeps both facts: what was counted, and that the reading
                    // is not the whole story.
                    facts.push(reading_fact(
                        agent_id,
                        &format!("reading:{day}:{model}:unknown"),
                        general::TOKENS_TOTAL,
                        None,
                        Quality::Unknown,
                        day,
                    ));
                    unknown_readings += 1;
                }
            }
        }
        if facts.is_empty() {
            return Ok(ReadingSettlement::default());
        }
        let receipts = self.record_all(facts)?;
        let mut settlement = ReadingSettlement {
            facts: receipts.len(),
            unknown_readings,
            ..ReadingSettlement::default()
        };
        for receipt in &receipts {
            settlement.observe(receipt);
        }
        Ok(settlement)
    }
}

/// One reading of one day and model, as a base fact the host may settle.
fn reading_fact(
    agent_id: &str,
    identity: &str,
    metric: &str,
    value: Option<String>,
    quality: Quality,
    day: &str,
) -> MeteringFact {
    let scope_ref = UsageSourceHost::agent_scope(agent_id);
    MeteringFact {
        fact_id: format!("{scope_ref}#{identity}#{metric}"),
        scope_ref,
        metric: metric.to_owned(),
        value,
        unit: TOKEN_UNIT.to_owned(),
        quality,
        // The host's own history readers are admitted to settle: this is the
        // host's grant, and a producer's own quality never grants it.
        eligibility: SettlementEligibility::SettlementEligible,
        source_ref: format!("native:{agent_id}"),
        observed_at: format!("{day}T00:00:00Z"),
    }
}

/// The one journal per data home this process composes.
///
/// Two verbs of one client run — a scan and the source transport that answered
/// it — settle into the same journal instead of opening the store twice.
fn hosts() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<UsageSourceHost>>>> {
    static HOSTS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<UsageSourceHost>>>>> = OnceLock::new();
    HOSTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The resolved data home of these parameters.
fn data_home(params: &Value) -> LedgerResult<PathBuf> {
    let store = client_state_store(params).map_err(|_| LedgerError::storage())?;
    Ok(store.root().to_path_buf())
}

/// The port's production entry: compose this run's host over the data home the
/// parameters resolve to, and keep it for the verbs that settle and project.
///
/// A client run installs this before it settles or projects anything, so the
/// data home is resolved the same way the rest of the store is and the base
/// journal opens in the ledger that home already holds. Installing twice for
/// one home is the same as installing once.
pub fn install(params: &Value) -> LedgerResult<()> {
    let home = data_home(params)?;
    let mut open = hosts().lock().map_err(|_| LedgerError::storage())?;
    if open.contains_key(&home) {
        return Ok(());
    }
    let host = Arc::new(Mutex::new(UsageSourceHost::open(params)?));
    open.insert(home, host);
    Ok(())
}

/// The composed host for these parameters.
///
/// A verb that has not installed the journal has no metering surface: the
/// caller reports the store unavailable rather than this lookup opening a
/// second journal behind the composition's back.
pub fn source_host(params: &Value) -> LedgerResult<Arc<Mutex<UsageSourceHost>>> {
    let home = data_home(params)?;
    let open = hosts().lock().map_err(|_| LedgerError::storage())?;
    open.get(&home)
        .map(Arc::clone)
        .ok_or_else(LedgerError::storage)
}

/// Settle one production scan's readings for one agent through the composed host.
pub fn settle_agent_readings(
    params: &Value,
    agent_id: &str,
    summary: &HistoryUsageSummary,
) -> LedgerResult<Value> {
    if summary.daily_usage.is_empty() {
        return Ok(ReadingSettlement::default().to_json());
    }
    let host = source_host(params)?;
    let mut host = host.lock().map_err(|_| LedgerError::storage())?;
    Ok(host.settle_readings(agent_id, summary)?.to_json())
}

/// What the base ledger holds right now, as the retained report names it.
pub fn held_metering(params: &Value) -> LedgerResult<Value> {
    let host = source_host(params)?;
    let host = host.lock().map_err(|_| LedgerError::storage())?;
    let page = host.read(None, MAX_FACT_PAGE)?;
    let obligations = host.pending()?;
    Ok(json!({
        "journal": USAGE_JOURNAL_SCHEMA,
        "ledgerSchemaVersion": WORKFLOW_LEDGER_SCHEMA_VERSION,
        "store": WORKFLOW_LEDGER_FILE_NAME,
        "facts": page.facts.len(),
        "truncated": page.next_cursor.is_some(),
        "obligations": obligations
            .iter()
            .map(|obligation| json!({
                "scopeRef": obligation.scope_ref,
                "metric": obligation.metric,
                "reason": obligation.reason
            }))
            .collect::<Vec<_>>()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use licoup_extension_contracts::usage::{
        MetricValue, Temporality, UsageObservation, UsageOperation,
    };
    use licoup_extension_contracts::wire;
    use rusqlite::Connection;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use token_usage_detector::collection::Cursor;

    const SOURCE: &str = "source:example.analytics#1";
    const EXTENSION: &str = "example.analytics";
    const SCOPE: &str = "scope-1";
    const METRIC: &str = "licoup.tokens.input";

    fn root(label: &str) -> PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lico-usage-source-host-{label}-{nonce}-{}",
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("synthetic root");
        path
    }

    fn params(root: &Path) -> Value {
        json!({"stateRoot": root.to_string_lossy()})
    }

    fn binding(epoch: &str) -> SourceBinding {
        SourceBinding::new(SOURCE, EXTENSION, "instance-1", 1, epoch, [SCOPE]).expect("binding")
    }

    fn admission(epoch: &str) -> SourceAdmission {
        SourceAdmission::new(binding(epoch), SettlementEligibility::SettlementEligible)
    }

    fn upsert(observation_id: &str, revision: u32, value: &str) -> UsageObservation {
        UsageObservation {
            schema: wire::USAGE.to_owned(),
            observation_id: observation_id.to_owned(),
            revision,
            operation: UsageOperation::Upsert,
            source_epoch: "epoch-1".to_owned(),
            scope_ref: SCOPE.to_owned(),
            observed_at: "2026-09-21T00:00:00Z".to_owned(),
            interval_start: None,
            metrics: BTreeMap::from([(
                METRIC.to_owned(),
                MetricValue::reported(value, "tokens", Temporality::Delta),
            )]),
            cost: None,
        }
    }

    fn batch(observations: &[UsageObservation]) -> PublishBatch {
        PublishBatch::new(
            observations
                .iter()
                .map(|observation| serde_json::to_value(observation).expect("wire observation"))
                .collect(),
        )
        .expect("bounded batch")
    }

    fn pull() -> QueryRequest {
        QueryRequest::new(SCOPE, None, None).expect("request")
    }

    fn page(observations: &[UsageObservation], sequence: u64) -> QueryPage {
        QueryPage {
            source_epoch: "epoch-1".to_owned(),
            scope_ref: SCOPE.to_owned(),
            observations: observations.to_vec(),
            next_cursor: Some(Cursor::new("epoch-1", sequence).expect("cursor").encode()),
            has_more: true,
        }
    }

    fn fact_rows(host: &UsageSourceHost) -> i64 {
        let connection = Connection::open(host.ledger_path()).expect("the ledger store");
        connection
            .query_row("SELECT COUNT(*) FROM usage_source_facts", [], |row| {
                row.get(0)
            })
            .expect("fact rows")
    }

    fn codex_history(root: &Path) {
        let tokens = |timestamp: &str, total: (u64, u64, u64), last: (u64, u64, u64)| {
            json!({
                "timestamp": timestamp,
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {
                            "input_tokens": total.0,
                            "cached_input_tokens": total.1,
                            "output_tokens": total.2,
                            "reasoning_output_tokens": total.2 / 2,
                            "total_tokens": total.0 + total.2
                        },
                        "last_token_usage": {
                            "input_tokens": last.0,
                            "cached_input_tokens": last.1,
                            "output_tokens": last.2,
                            "reasoning_output_tokens": last.2 / 2,
                            "total_tokens": last.0 + last.2
                        }
                    }
                }
            })
            .to_string()
        };
        fs::write(
            root.join("rollout.jsonl"),
            [
                json!({
                    "timestamp": "2026-07-08T10:00:00Z",
                    "type": "session_meta",
                    "payload": {"id": "session-production-wiring"}
                })
                .to_string(),
                json!({
                    "timestamp": "2026-07-08T10:00:01Z",
                    "type": "turn_context",
                    "payload": {"model": "gpt-synthetic"}
                })
                .to_string(),
                tokens("2026-07-08T10:00:02Z", (120, 20, 30), (120, 20, 30)),
            ]
            .join("\n"),
        )
        .expect("synthetic history");
    }

    fn scan_params(history: &Path, state: &Path) -> Value {
        json!({
            "agent": "codex",
            "root": history.to_string_lossy(),
            "stateRoot": state.to_string_lossy(),
            "forceRefresh": true,
            "historyDays": 30,
            "now": "2026-07-10T12:00:00Z"
        })
    }

    /// The production verb settles its readings into the base ledger, and the
    /// second scan of the same history is a replay instead of a second fact.
    #[test]
    fn a_production_scan_settles_once_and_a_rescan_replays() {
        crate::host::install_test_host();
        let history = root("scan-history");
        let state = root("scan-state");
        codex_history(&history);

        let first = crate::scan(&scan_params(&history, &state)).expect("scan");
        let settled = &first["agents"][0]["metering"];
        assert_eq!(
            settled["inserted"].as_u64(),
            Some(4),
            "one counted reading settles one fact per token metric: {settled}"
        );
        assert_eq!(settled["replayed"].as_u64(), Some(0));
        assert_eq!(first["agents"][0]["history"]["totalTokens"], 150);

        // The host the verbs composed, reopened from the same resolved data
        // home: the facts are the store's, and the reading is the one the scan
        // reported.
        let host = UsageSourceHost::open(&params(&state)).expect("production owner");
        assert_eq!(
            host.ledger_path(),
            state.join("agent-usage").join(WORKFLOW_LEDGER_FILE_NAME),
            "the journal keeps the existing ledger store"
        );
        assert_eq!(fact_rows(&host), 4, "one fact row per token metric");
        assert_eq!(
            host.settled_total(
                &UsageSourceHost::agent_scope("codex"),
                general::TOKENS_TOTAL
            )
            .expect("total")
            .as_deref(),
            Some("150")
        );

        let second = crate::scan(&scan_params(&history, &state)).expect("rescan");
        let replayed = &second["agents"][0]["metering"];
        assert_eq!(
            replayed["replayed"].as_u64(),
            Some(4),
            "a rescan of the same history replays: {replayed}"
        );
        assert_eq!(replayed["inserted"].as_u64(), Some(0));
        assert_eq!(replayed["corrected"].as_u64(), Some(0));
        assert_eq!(fact_rows(&host), 4, "a replay writes no second fact row");
        assert_eq!(
            host.settled_total(
                &UsageSourceHost::agent_scope("codex"),
                general::TOKENS_TOTAL
            )
            .expect("total")
            .as_deref(),
            Some("150"),
            "a replay never double-counts"
        );

        let _ = fs::remove_dir_all(history);
        let _ = fs::remove_dir_all(state);
    }

    /// A pushed batch settles once through the production owner, and the same
    /// batch delivered again is a no-op replay.
    #[test]
    fn a_pushed_batch_settles_once_through_the_production_owner() {
        let state = root("push");
        let mut host = UsageSourceHost::open(&params(&state)).expect("production owner");
        let admission = admission("epoch-1");
        let batch = batch(&[upsert("obs-1", 1, "120"), upsert("obs-2", 1, "80")]);

        let settled = host.publish(&admission, &batch).expect("delivered");
        assert_eq!(settled.accepted, 2);
        assert!(
            settled
                .events
                .iter()
                .all(|event| event.outcome == crate::journal::EventOutcome::Settled)
        );
        assert_eq!(fact_rows(&host), 2, "one fact per settled observation");

        let replayed = host.publish(&admission, &batch).expect("replayed");
        assert!(
            replayed
                .events
                .iter()
                .all(|event| !event.wrote()
                    && event.outcome == crate::journal::EventOutcome::Replayed),
            "a redelivered batch writes nothing"
        );
        assert_eq!(fact_rows(&host), 2, "a replay adds no fact row");
        assert_eq!(
            host.settled_total(SCOPE, METRIC).expect("total").as_deref(),
            Some("200")
        );

        let _ = fs::remove_dir_all(state);
    }

    /// A restart on the same synthetic root preserves the facts and the durable
    /// cursor: both are the store's, not the process's.
    #[test]
    fn a_restart_preserves_facts_and_the_durable_cursor() {
        let state = root("restart");
        let admission = admission("epoch-1");
        let page = page(&[upsert("obs-1", 1, "120")], 1);
        {
            let mut host = UsageSourceHost::open(&params(&state)).expect("production owner");
            assert_eq!(
                host.resume_request(&admission, SCOPE, None)
                    .expect("first read")
                    .mode(),
                token_usage_detector::collection::CollectionMode::Pull,
                "a source with no durable position is read from the start"
            );
            let settled = host
                .admit_page(&admission, &pull(), &page)
                .expect("settled");
            assert_eq!(settled.cursor.as_deref(), Some("1@epoch-1"));
        }

        let mut restarted = UsageSourceHost::open(&params(&state)).expect("restarted owner");
        assert_eq!(
            restarted
                .durable_cursor(&admission.binding)
                .expect("cursor")
                .as_deref(),
            Some("1@epoch-1"),
            "a restart resumes where the journal stopped"
        );
        let resumed = restarted
            .resume_request(&admission, SCOPE, Some(10))
            .expect("resume");
        assert_eq!(
            resumed.mode(),
            token_usage_detector::collection::CollectionMode::Cursor
        );
        assert_eq!(resumed.cursor.as_deref(), Some("1@epoch-1"));
        assert_eq!(fact_rows(&restarted), 1, "the facts survived the restart");

        // A duplicate page neither double-counts nor rewinds the cursor.
        let replayed = restarted
            .admit_page(&admission, &pull(), &page)
            .expect("replayed");
        assert_eq!(replayed.cursor.as_deref(), Some("1@epoch-1"));
        assert_eq!(fact_rows(&restarted), 1);
        assert_eq!(
            restarted
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("120")
        );

        let _ = fs::remove_dir_all(state);
    }

    /// A conflicting redelivery at one revision is refused, exactly as the
    /// journal's own rules say, and the settled fact is not rewritten.
    #[test]
    fn a_conflicting_redelivery_is_refused() {
        let state = root("conflict");
        let mut host = UsageSourceHost::open(&params(&state)).expect("production owner");
        let admission = admission("epoch-1");
        host.publish(&admission, &batch(&[upsert("obs-1", 1, "120")]))
            .expect("settled");

        let failure = host
            .publish(&admission, &batch(&[upsert("obs-1", 1, "130")]))
            .expect_err("a same-revision conflict is refused");
        assert_eq!(failure.code, "usage_journal_event_conflict");
        assert_eq!(failure.stage, "usage-journal");
        assert!(!failure.retryable);
        assert_eq!(
            host.settled_total(SCOPE, METRIC).expect("total").as_deref(),
            Some("120"),
            "the refusal never rewrites what was settled"
        );

        let _ = fs::remove_dir_all(state);
    }

    /// A reading whose requests carried no tokens settles as unknown with no
    /// number, so a tokenless producer never becomes a zero on the production
    /// path.
    #[test]
    fn a_tokenless_reading_settles_as_unknown_never_as_zero() {
        let state = root("unknown");
        let mut host = UsageSourceHost::open(&params(&state)).expect("production owner");
        let summary = HistoryUsageSummary {
            daily_usage: BTreeMap::from([(
                "2026-07-08".to_owned(),
                crate::contract::DailyUsageSummary {
                    token_unavailable_requests: 3,
                    model_usage: BTreeMap::from([(
                        crate::contract::UNATTRIBUTED_MODEL.to_owned(),
                        crate::contract::ModelTokenUsageSummary {
                            request_count: 3,
                            token_unavailable_requests: 3,
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                },
            )]),
            ..HistoryUsageSummary::default()
        };

        let settled = host.settle_readings("cursor", &summary).expect("settled");
        assert_eq!(settled.unknown_readings, 1);
        assert_eq!(settled.inserted, 1);
        let facts = host.read(None, 10).expect("readable").facts;
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].metric, general::TOKENS_TOTAL);
        assert_eq!(facts[0].value, None, "an unknown reading carries no number");
        assert_eq!(facts[0].quality, Quality::Unknown);
        assert_eq!(
            host.settled_total(
                &UsageSourceHost::agent_scope("cursor"),
                general::TOKENS_TOTAL
            )
            .expect("total"),
            None,
            "an unknown total is not zero"
        );

        let _ = fs::remove_dir_all(state);
    }

    /// The owner's facts live in the ledger the workflow owner already owns,
    /// with the graph-run tables, so the port is never a second store.
    #[test]
    fn the_owner_keeps_the_existing_ledger_store() {
        crate::host::install_test_host();
        let history = root("store-history");
        let state = root("store-state");
        codex_history(&history);
        crate::scan(&scan_params(&history, &state)).expect("scan");

        let ledger = state.join("agent-usage").join(WORKFLOW_LEDGER_FILE_NAME);
        assert!(ledger.is_file(), "the scan settles into the existing store");
        let connection = Connection::open(&ledger).expect("ledger");
        let tables: Vec<String> = {
            let mut statement = connection
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .expect("schema");
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("tables");
            rows.map(|row| row.expect("table name")).collect()
        };
        for table in [
            "graph_usage_reservations",
            "usage_source_events",
            "usage_source_facts",
            "usage_source_cursors",
        ] {
            assert!(
                tables.iter().any(|name| name == table),
                "the ledger must hold {table}: {tables:?}"
            );
        }
        assert_eq!(
            tables
                .iter()
                .filter(|name| name.as_str() == "usage_source_facts")
                .count(),
            1,
            "there is exactly one base fact table"
        );

        let retained = crate::report(&params(&state)).expect("report");
        assert_eq!(retained["metering"]["journal"], USAGE_JOURNAL_SCHEMA);
        assert_eq!(retained["metering"]["facts"], 4);
        assert_eq!(retained["metering"]["truncated"], false);
        assert_eq!(
            retained["metering"]["obligations"]
                .as_array()
                .expect("obligations")
                .len(),
            0
        );

        let _ = fs::remove_dir_all(history);
        let _ = fs::remove_dir_all(state);
    }

    /// The production verbs must settle and project through this owner.
    ///
    /// The source is read at run time rather than embedded, so the failure this
    /// assertion exists for is observable: remove either call from the verbs
    /// and the same test binary fails, which is what an audit of "the port has
    /// no production consumer" needs to see.
    #[test]
    fn the_agent_usage_verbs_settle_and_project_through_this_owner() {
        let verbs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/command.rs");
        let verbs = fs::read_to_string(&verbs)
            .unwrap_or_else(|error| panic!("read {}: {error}", verbs.display()));
        assert!(
            verbs.contains("crate::source_host::settle_agent_readings("),
            "the scan verb must settle its readings through the production owner"
        );
        assert!(
            verbs.contains("crate::source_host::held_metering("),
            "the report verb must project the base ledger through the production owner"
        );
    }
}
