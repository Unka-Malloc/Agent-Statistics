//! The base usage journal: source events, their cursors, and the facts they
//! settle into.
//!
//! The kernel owns the base usage facts (`usage-journal.v1`). Third-party
//! sources — through the C11 usage-source SDK — produce *observations*, and this
//! module is the seam where an observation becomes a durable fact in the same
//! base ledger store the graph-run reservations live in. Three rules are
//! structural here rather than conventional:
//!
//! - **A delivered event settles once.** An event is keyed by the host-bound
//!   source, epoch and observation id, and its revision and content digest are
//!   recorded with it. A replay at the same revision changes nothing; an older
//!   revision is superseded; a same-revision payload with different content is
//!   refused instead of silently rewriting a settled fact. A duplicate cursor
//!   therefore cannot double-count and cannot rewrite what it already settled.
//! - **A cursor is durable and monotonic.** The position a page returned is
//!   stored with the source and its epoch, advanced only forward and only after
//!   the page's observations settled, so a restart resumes where the journal
//!   stopped. A cursor belongs to one scope: resuming it for another is refused
//!   rather than answered with someone else's position.
//! - **Unknown stays unknown.** A reading the producer does not know is stored
//!   with no number at all, and a metric the producer did not send has no fact.
//!   Neither becomes zero, because zero is a claim that nothing happened.
//!
//! The facts are the kernel's and stay here. [`MeteringFactPort`] is the narrow
//! slice an optional package borrows; the host composes it, and a client without
//! that package records, reads and settles exactly the same facts.
//! [`UsageSourcePort`] is the host's collection seam: push, bounded pull and the
//! cursor that resumes. Deduplication across several sources of one call is the
//! host's fact too: when it issues a measurement, every report of that
//! measurement settles into one fact instead of one per source.

use acativa::usage_ledger::{Ledger, LedgerError, LedgerResult, now_ms, open_ledger};
use licoup_extension_contracts::usage::{ExactNumber, Quality, UsageObservation, UsageOperation};
use licoup_extension_contracts::{ApplicationFailure, is_namespaced};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use token_usage_detector::binding::{
    BatchRefusal, BoundObservation, BoundObservationKey, SourceBinding,
};
use token_usage_detector::collection::{Cursor, PublishBatch, QueryPage, QueryRequest};
use token_usage_detector::metrics as general;

/// The stage every refusal from this journal reports.
const STAGE: &str = "usage-journal";

/// The most facts one read returns.
pub const MAX_FACT_PAGE: usize = 500;

/// The metric an unsettled obligation is reported under: the general total the
/// SDK publishes, because an obligation is about tokens until it is settled.
const PENDING_METRIC: &str = general::TOKENS_TOTAL;

/// Whether a fact may take part in settlement, or is display material only.
///
/// The host composes this with the source binding; a producer's own `quality`
/// never grants it. Keeping the decision in the fact — rather than in the
/// optional package that reads the fact — is what makes a client without that
/// package settle the same way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementEligibility {
    /// Shown with its provenance; never recorded as a settlement.
    DisplayOnly,
    /// The host's source policy allows this fact to settle.
    SettlementEligible,
}

impl SettlementEligibility {
    pub const fn id(self) -> &'static str {
        match self {
            Self::DisplayOnly => "display-only",
            Self::SettlementEligible => "settlement-eligible",
        }
    }

    pub const fn is_settlement(self) -> bool {
        matches!(self, Self::SettlementEligible)
    }

    fn parse(text: &str) -> LedgerResult<Self> {
        match text {
            "display-only" => Ok(Self::DisplayOnly),
            "settlement-eligible" => Ok(Self::SettlementEligible),
            _ => Err(LedgerError::refused(STAGE, "usage_journal_fact_invalid")),
        }
    }
}

/// One base metering fact: the kernel's own record of one measured value.
///
/// It is deliberately small: an identity, the scope it belongs to, one metric,
/// an exact decimal or an explicit unknown, and the two facts about its origin —
/// which host-bound source reported it, and whether the host lets it settle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeteringFact {
    /// Stable identity. Recording the same id again is a correction or a
    /// no-op, never a second charge.
    pub fact_id: String,
    /// The invocation or authorized aggregate range this fact belongs to.
    pub scope_ref: String,
    pub metric: String,
    /// Exact decimal text, or `None` when the value is unknown.
    pub value: Option<String>,
    pub unit: String,
    pub quality: Quality,
    pub eligibility: SettlementEligibility,
    /// The host-bound source the reading came from.
    pub source_ref: String,
    pub observed_at: String,
}

impl MeteringFact {
    /// Structural validation: a namespaced metric, a unit, a value present
    /// exactly when the quality claims one, and exact decimal text when it is.
    pub fn validate(&self) -> LedgerResult<()> {
        let invalid = || LedgerError::refused(STAGE, "usage_journal_fact_invalid");
        if self.fact_id.is_empty()
            || self.scope_ref.is_empty()
            || !is_namespaced(&self.metric)
            || self.unit.is_empty()
            || self.source_ref.is_empty()
            || self.observed_at.is_empty()
        {
            return Err(invalid());
        }
        if self.quality.has_value() != self.value.is_some() {
            return Err(invalid());
        }
        if let Some(value) = &self.value
            && ExactNumber::parse(value).is_none()
        {
            return Err(invalid());
        }
        Ok(())
    }

    /// The exact number, when there is one.
    pub fn exact(&self) -> Option<ExactNumber> {
        self.value.as_deref().and_then(ExactNumber::parse)
    }
}

/// What recording or retracting one fact did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FactOutcome {
    /// A new fact.
    Inserted,
    /// The same identity with a corrected value.
    Updated,
    /// The same identity with the same value: a replay.
    Unchanged,
    /// An older revision than the recorded one: it changes nothing.
    Superseded,
    /// The fact was withdrawn by a retraction.
    Withdrawn,
    /// A retraction named an identity the ledger does not hold.
    UnknownFact,
}

/// The result of recording or retracting one fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FactReceipt {
    pub fact_id: String,
    pub outcome: FactOutcome,
}

impl FactReceipt {
    pub fn recorded(&self) -> bool {
        matches!(
            self.outcome,
            FactOutcome::Inserted | FactOutcome::Updated | FactOutcome::Unchanged
        )
    }
}

/// What settling one source event did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventOutcome {
    /// The event was not recorded before and its facts are now recorded.
    Settled,
    /// The same revision with the same content: nothing was written.
    Replayed,
    /// A higher revision replaced the event's facts.
    Corrected,
    /// An older revision than the recorded one: it changed nothing.
    Superseded,
    /// A retraction withdrew this event's facts.
    Withdrawn,
    /// A retraction named an event the journal does not hold.
    UnknownEvent,
}

/// The result of settling one source event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSettlement {
    pub observation_id: String,
    pub revision: u32,
    pub outcome: EventOutcome,
    /// What settling each fact this event owns did.
    pub facts: Vec<FactReceipt>,
}

impl EventSettlement {
    /// Whether this delivery wrote anything to the base facts.
    pub fn wrote(&self) -> bool {
        matches!(
            self.outcome,
            EventOutcome::Settled | EventOutcome::Corrected | EventOutcome::Withdrawn
        )
    }

    /// The facts this event owns, whichever way the delivery ended.
    pub fn fact_ids(&self) -> impl Iterator<Item = &str> {
        self.facts.iter().map(|fact| fact.fact_id.as_str())
    }
}

/// What one delivery did.
#[derive(Clone, Debug, PartialEq)]
pub struct IngestReport {
    pub source_ref: String,
    /// Observations the transport binding accepted.
    pub accepted: usize,
    /// Observations the binding refused, each with the payload position.
    pub refused: Vec<BatchRefusal>,
    pub events: Vec<EventSettlement>,
    /// The durable position of this source after the delivery.
    pub cursor: Option<String>,
}

/// An obligation the base ledger still holds unsettled for a scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingObligation {
    pub scope_ref: String,
    pub metric: String,
    /// Why it is still pending, in a form the client can show.
    pub reason: String,
}

/// One page of recorded facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FactPage {
    pub facts: Vec<MeteringFact>,
    pub next_cursor: Option<String>,
}

impl FactPage {
    pub fn empty() -> Self {
        Self {
            facts: Vec::new(),
            next_cursor: None,
        }
    }
}

/// The host's composition of one source: the binding it issued at the transport
/// and the settlement authority it grants that source.
///
/// The binding carries the source identity, the instance generation, the
/// current epoch and the authorized scopes; the grant is the host's decision,
/// never the producer's. A source the host composes cannot settle anything by
/// reporting `reported`.
///
/// The admission is a capability: its fields are sealed so only the crate's own
/// composition can mint one, and [`crate::source_host::UsageSourceHost::admit_source`]
/// is the public issuance path a host drives. A caller that never passes
/// through the host cannot fabricate an admission, and an issued one cannot be
/// reshaped — every journal entry point requires one by reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceAdmission {
    binding: SourceBinding,
    eligibility: SettlementEligibility,
}

impl SourceAdmission {
    pub(crate) fn new(binding: SourceBinding, eligibility: SettlementEligibility) -> Self {
        Self {
            binding,
            eligibility,
        }
    }

    /// The transport binding the host issued for this source.
    pub fn binding(&self) -> &SourceBinding {
        &self.binding
    }

    /// The settlement authority the host grants this source.
    pub fn eligibility(&self) -> SettlementEligibility {
        self.eligibility
    }
}

/// The port the host drives to collect one source: push, bounded pull and the
/// durable cursor that resumes.
pub trait UsageSourcePort {
    /// Settle one bounded batch a source pushed (`usage.publish`).
    fn publish(
        &mut self,
        admission: &SourceAdmission,
        batch: &PublishBatch,
    ) -> LedgerResult<IngestReport>;

    /// Settle one observation the host already bound, carrying the logical
    /// measurement it knows several sources are reporting.
    fn admit_observation(
        &mut self,
        admission: &SourceAdmission,
        bound: &BoundObservation,
    ) -> LedgerResult<EventSettlement>;

    /// The request that reads this source: no cursor for the first page, the
    /// durable cursor to resume.
    fn resume_request(
        &self,
        admission: &SourceAdmission,
        scope_ref: &str,
        limit: Option<u32>,
    ) -> LedgerResult<QueryRequest>;

    /// Settle one bounded page the host pulled in answer to `request`, then
    /// advance the durable cursor to the position the page returned.
    ///
    /// The position advances over the page the host actually read, and only
    /// forward: an observation the binding refuses is reported in
    /// [`IngestReport::refused`] and not settled, but the read did happen, so
    /// the journal never re-reads a page it already consumed. The page must
    /// answer the request it was read for: a page stamped with another scope
    /// is refused rather than admitted under the wrong question.
    fn admit_page(
        &mut self,
        admission: &SourceAdmission,
        request: &QueryRequest,
        page: &QueryPage,
    ) -> LedgerResult<IngestReport>;

    /// The durable position for this source and its epoch.
    fn durable_cursor(&self, binding: &SourceBinding) -> LedgerResult<Option<String>>;
}

/// The port over the base facts that the host composes for consumers.
///
/// It is read-mostly: a consumer may record an admitted fact and retract one,
/// and read the facts and the obligations the ledger still holds. It cannot
/// rewrite a fact it does not own and it cannot compute a settlement.
///
/// Every mutation names its `owner` — the consumer identity the host composed
/// for the caller. The journal enforces it: a fact records only under the
/// owner its `source_ref` names, and an existing fact answers only to the
/// owner it was recorded under. A fact an observation event settled is never
/// touched by this port at all — the event's own settlement path corrects or
/// withdraws it. Settlement eligibility is the host's grant at record time
/// and stays immutable on a consumer's update.
pub trait MeteringFactPort {
    /// Record one fact under `owner`, keyed by `fact_id`.
    fn record(&mut self, owner: &str, fact: MeteringFact) -> LedgerResult<FactReceipt>;
    /// Record a bounded set of facts under `owner` as one delivery, each keyed
    /// by its own `fact_id`.
    ///
    /// The default records them one at a time, which is all a port without a
    /// transactional store can promise. A port over a store that has one
    /// commits the delivery together, so settling a whole reading costs one
    /// transaction instead of one per fact.
    fn record_all(
        &mut self,
        owner: &str,
        facts: Vec<MeteringFact>,
    ) -> LedgerResult<Vec<FactReceipt>> {
        facts
            .into_iter()
            .map(|fact| self.record(owner, fact))
            .collect()
    }
    /// Withdraw one fact the caller owns, by identity. It cancels that fact
    /// and nothing else.
    fn retract(&mut self, owner: &str, fact_id: &str) -> LedgerResult<FactReceipt>;
    /// Read facts, by cursor.
    fn read(&self, cursor: Option<&str>, limit: usize) -> LedgerResult<FactPage>;
    /// The obligations the ledger still holds unsettled.
    fn pending(&self) -> LedgerResult<Vec<PendingObligation>>;
}

/// The journal's own schema inside the shared usage store. The Graph-run
/// tables belong to `acativa::usage_ledger`; these are additive tables of the
/// same durable file, created idempotently whichever owner opens first.
const USAGE_JOURNAL_INIT: &str = "CREATE TABLE IF NOT EXISTS usage_source_events (
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

/// The base usage journal, in the kernel's ledger store.
pub struct UsageJournal {
    ledger: Ledger,
}

impl UsageJournal {
    /// Open the base ledger the journal shares with the graph-run reservations.
    pub fn open(params: &Value) -> LedgerResult<Self> {
        let ledger = open_ledger(params)?;
        ledger
            .connection
            .execute_batch(USAGE_JOURNAL_INIT)
            .map_err(|_| LedgerError::storage())?;
        Ok(Self { ledger })
    }

    /// The exact total of one scope and metric over the facts that may settle.
    ///
    /// A scope whose readings are all unknown has no total at all: `None` is not
    /// zero, and reporting one would turn "nobody knows" into "nothing was
    /// used".
    pub fn settled_total(&self, scope_ref: &str, metric: &str) -> LedgerResult<Option<String>> {
        let mut total: Option<ExactNumber> = None;
        let mut statement = self
            .ledger
            .connection
            .prepare(
                "SELECT value,eligibility FROM usage_source_facts
                 WHERE scope_ref=?1 AND metric=?2 AND retracted=0",
            )
            .map_err(|_| LedgerError::storage())?;
        let rows = statement
            .query_map(params![scope_ref, metric], |row| {
                Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| LedgerError::storage())?;
        for row in rows {
            let (value, eligibility) = row.map_err(|_| LedgerError::storage())?;
            if !SettlementEligibility::parse(&eligibility)?.is_settlement() {
                continue;
            }
            let Some(value) = value.and_then(|value| ExactNumber::parse(&value)) else {
                continue;
            };
            total = Some(match total {
                None => value,
                Some(previous) => checked_sum(previous, value)
                    .ok_or_else(|| LedgerError::refused(STAGE, "usage_journal_decimal_overflow"))?,
            });
        }
        Ok(total.map(|total| total.to_canonical_string()))
    }
}

impl UsageSourcePort for UsageJournal {
    fn publish(
        &mut self,
        admission: &SourceAdmission,
        batch: &PublishBatch,
    ) -> LedgerResult<IngestReport> {
        let admitted = admission.binding.admit_batch(batch);
        let transaction = self
            .ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| LedgerError::storage())?;
        let mut events = Vec::with_capacity(admitted.accepted.len());
        let accepted_count = admitted.accepted_count();
        let mut refused = admitted.refused;
        for (index, bound) in admitted.accepted.iter().enumerate() {
            match settle_event(&transaction, admission, bound) {
                Ok(settlement) => events.push(settlement),
                // A settlement refusal isolates to its own observation: the
                // neighbours the batch carried still commit. A storage failure
                // is different — nothing commits over a broken store.
                Err(error) if error.retryable => return Err(error),
                Err(error) => refused.push(BatchRefusal {
                    index,
                    failure: failure_from(error),
                }),
            }
        }
        let cursor = stored_cursor(
            &transaction,
            &admission.binding.source_ref,
            &admission.binding.source_epoch,
        )?
        .map(|stored| stored.cursor);
        transaction.commit().map_err(|_| LedgerError::storage())?;
        Ok(IngestReport {
            source_ref: admission.binding.source_ref.clone(),
            accepted: accepted_count,
            refused,
            events,
            cursor,
        })
    }

    fn admit_observation(
        &mut self,
        admission: &SourceAdmission,
        bound: &BoundObservation,
    ) -> LedgerResult<EventSettlement> {
        require_admitted(admission, bound)?;
        let transaction = self
            .ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| LedgerError::storage())?;
        let settlement = settle_event(&transaction, admission, bound)?;
        transaction.commit().map_err(|_| LedgerError::storage())?;
        Ok(settlement)
    }

    fn resume_request(
        &self,
        admission: &SourceAdmission,
        scope_ref: &str,
        limit: Option<u32>,
    ) -> LedgerResult<QueryRequest> {
        if !admission.binding.authorizes_scope(scope_ref) {
            return Err(LedgerError::refused(
                STAGE,
                "usage_journal_scope_not_authorized",
            ));
        }
        let cursor = match stored_cursor(
            &self.ledger.connection,
            &admission.binding.source_ref,
            &admission.binding.source_epoch,
        )? {
            None => None,
            Some(stored) => {
                if stored.scope_ref != scope_ref {
                    return Err(LedgerError::refused(
                        STAGE,
                        "usage_journal_cursor_scope_mismatch",
                    ));
                }
                Some(stored.cursor)
            }
        };
        QueryRequest::new(scope_ref, cursor, limit).map_err(from_refusal)
    }

    fn admit_page(
        &mut self,
        admission: &SourceAdmission,
        request: &QueryRequest,
        page: &QueryPage,
    ) -> LedgerResult<IngestReport> {
        page.validate().map_err(from_refusal)?;
        page.check_request(request).map_err(from_refusal)?;
        if page.source_epoch != admission.binding.source_epoch {
            return Err(LedgerError::refused(STAGE, "usage_source_epoch_mismatch"));
        }
        if !admission.binding.authorizes_scope(&page.scope_ref) {
            return Err(LedgerError::refused(
                STAGE,
                "usage_journal_scope_not_authorized",
            ));
        }
        let next = match &page.next_cursor {
            None => None,
            Some(cursor) => {
                let cursor = Cursor::parse(cursor).map_err(from_refusal)?;
                cursor
                    .check_epoch(&admission.binding.source_epoch)
                    .map_err(from_refusal)?;
                Some(cursor)
            }
        };

        let mut events = Vec::with_capacity(page.observations.len());
        let mut refused = Vec::new();
        let transaction = self
            .ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| LedgerError::storage())?;
        for (index, observation) in page.observations.iter().enumerate() {
            match admission
                .binding
                .bind_observation(observation.clone(), None)
            {
                Ok(bound) => match settle_event(&transaction, admission, &bound) {
                    Ok(settlement) => events.push(settlement),
                    // Same isolation as publish: a refused observation reports
                    // itself and does not pin the page's cursor.
                    Err(error) if error.retryable => return Err(error),
                    Err(error) => refused.push(BatchRefusal {
                        index,
                        failure: failure_from(error),
                    }),
                },
                Err(failure) => refused.push(BatchRefusal { index, failure }),
            }
        }
        // The cursor advances only over what settled, and only forward. A
        // replayed page carries an older position; the journal keeps the
        // furthest one it has actually read.
        let cursor = match next {
            None => stored_cursor(
                &transaction,
                &admission.binding.source_ref,
                &admission.binding.source_epoch,
            )?
            .map(|stored| stored.cursor),
            Some(next) => {
                let stored = stored_cursor(
                    &transaction,
                    &admission.binding.source_ref,
                    &admission.binding.source_epoch,
                )?;
                match stored {
                    Some(stored) if next.sequence() <= stored.sequence => Some(stored.cursor),
                    _ => {
                        transaction
                            .execute(
                                "INSERT INTO usage_source_cursors(
                                   source_ref,source_epoch,scope_ref,sequence,cursor,updated_at_ms
                                 ) VALUES(?1,?2,?3,?4,?5,?6)
                                 ON CONFLICT(source_ref,source_epoch) DO UPDATE SET
                                   scope_ref=excluded.scope_ref,sequence=excluded.sequence,
                                   cursor=excluded.cursor,updated_at_ms=excluded.updated_at_ms",
                                params![
                                    &admission.binding.source_ref,
                                    &admission.binding.source_epoch,
                                    &page.scope_ref,
                                    sqlite_counter(next.sequence())?,
                                    next.encode(),
                                    now_ms(),
                                ],
                            )
                            .map_err(|_| LedgerError::storage())?;
                        Some(next.encode())
                    }
                }
            }
        };
        transaction.commit().map_err(|_| LedgerError::storage())?;
        Ok(IngestReport {
            source_ref: admission.binding.source_ref.clone(),
            accepted: events.len(),
            refused,
            events,
            cursor,
        })
    }

    fn durable_cursor(&self, binding: &SourceBinding) -> LedgerResult<Option<String>> {
        stored_cursor(
            &self.ledger.connection,
            &binding.source_ref,
            &binding.source_epoch,
        )
        .map(|stored| stored.map(|stored| stored.cursor))
    }
}

/// The owner a consumer names must name itself: non-empty, bounded, and equal
/// to the `source_ref` every fact it records carries. An owner that names
/// another consumer's facts is refused, not trusted.
fn require_owner(owner: &str) -> LedgerResult<()> {
    if owner.trim().is_empty() || owner.len() > 256 {
        return Err(LedgerError::refused(STAGE, "usage_journal_owner_invalid"));
    }
    Ok(())
}

impl MeteringFactPort for UsageJournal {
    fn record(&mut self, owner: &str, fact: MeteringFact) -> LedgerResult<FactReceipt> {
        let mut receipts = self.record_all(owner, vec![fact])?;
        Ok(receipts.remove(0))
    }

    /// One delivery, one transaction: a host settling a whole reading commits
    /// its facts together instead of paying a commit per fact.
    fn record_all(
        &mut self,
        owner: &str,
        facts: Vec<MeteringFact>,
    ) -> LedgerResult<Vec<FactReceipt>> {
        require_owner(owner)?;
        for fact in &facts {
            fact.validate()?;
            if fact.source_ref != owner {
                return Err(LedgerError::refused(STAGE, "usage_journal_owner_mismatch"));
            }
        }
        let transaction = self
            .ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| LedgerError::storage())?;
        let mut receipts = Vec::with_capacity(facts.len());
        for fact in &facts {
            receipts.push(FactReceipt {
                fact_id: fact.fact_id.clone(),
                outcome: write_consumer_fact(&transaction, owner, fact)?,
            });
        }
        transaction.commit().map_err(|_| LedgerError::storage())?;
        Ok(receipts)
    }

    fn retract(&mut self, owner: &str, fact_id: &str) -> LedgerResult<FactReceipt> {
        require_owner(owner)?;
        let transaction = self
            .ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| LedgerError::storage())?;
        // A consumer withdraws only its own fact: never an event-owned one and
        // never another consumer's. The owner and ownership clauses in the
        // statement are the check itself, not a pre-read a race could outrun.
        let updated = transaction
            .execute(
                "UPDATE usage_source_facts SET retracted=1,updated_at_ms=?3
                 WHERE fact_id=?1 AND source_ref=?2 AND observation_id='' AND retracted=0",
                params![fact_id, owner, now_ms()],
            )
            .map_err(|_| LedgerError::storage())?;
        let outcome = if updated == 1 {
            FactOutcome::Withdrawn
        } else {
            match load_fact(&transaction, fact_id)? {
                None => FactOutcome::UnknownFact,
                Some(_) => {
                    return Err(LedgerError::refused(STAGE, "usage_journal_fact_not_owned"));
                }
            }
        };
        transaction.commit().map_err(|_| LedgerError::storage())?;
        Ok(FactReceipt {
            fact_id: fact_id.to_owned(),
            outcome,
        })
    }

    fn read(&self, cursor: Option<&str>, limit: usize) -> LedgerResult<FactPage> {
        if limit == 0 {
            return Ok(FactPage::empty());
        }
        let limit = limit.min(MAX_FACT_PAGE);
        let mut statement = self
            .ledger
            .connection
            .prepare(
                "SELECT fact_id,scope_ref,metric,value,unit,quality,eligibility,
                        source_ref,observed_at
                 FROM usage_source_facts
                 WHERE retracted=0 AND (?1 IS NULL OR fact_id>?1)
                 ORDER BY fact_id LIMIT ?2",
            )
            .map_err(|_| LedgerError::storage())?;
        let rows = statement
            .query_map(
                params![cursor, sqlite_counter((limit + 1) as u64)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .map_err(|_| LedgerError::storage())?;
        let mut facts = Vec::new();
        for row in rows {
            let (fact_id, scope_ref, metric, value, unit, quality, eligibility, source_ref, at) =
                row.map_err(|_| LedgerError::storage())?;
            facts.push(MeteringFact {
                fact_id,
                scope_ref,
                metric,
                value,
                unit,
                quality: quality_from(&quality)?,
                eligibility: SettlementEligibility::parse(&eligibility)?,
                source_ref,
                observed_at: at,
            });
        }
        let next_cursor = if facts.len() > limit {
            facts.truncate(limit);
            facts.last().map(|fact| fact.fact_id.clone())
        } else {
            None
        };
        Ok(FactPage { facts, next_cursor })
    }

    fn pending(&self) -> LedgerResult<Vec<PendingObligation>> {
        // The journal reports what the shared store still holds unsettled. The
        // reservation rows belong to the Graph ledger (`acativa::usage_ledger`)
        // — this read goes through that owner's API rather than naming its
        // tables, so a home the Graph owner never opened reports no
        // obligations instead of failing on a missing table.
        Ok(
            acativa::usage_ledger::pending_graph_obligations(&self.ledger.connection)?
                .into_iter()
                .map(|obligation| PendingObligation {
                    scope_ref: obligation.scope_ref,
                    metric: PENDING_METRIC.to_owned(),
                    reason: match obligation.state.as_str() {
                        "reserved" => "in-flight",
                        _ => "unknown-usage",
                    }
                    .to_owned(),
                })
                .collect(),
        )
    }
}

/// One stored event, as the journal recorded it.
struct StoredEvent {
    revision: u32,
    content_digest: String,
    measurement_ref: Option<String>,
    retracted: bool,
}

/// The event that owns a fact it just settled.
struct FactOwner<'a> {
    source_ref: &'a str,
    source_epoch: &'a str,
    observation_id: &'a str,
    revision: u32,
}

/// Settle one bound observation: record the event, then its facts.
fn settle_event(
    transaction: &rusqlite::Transaction<'_>,
    admission: &SourceAdmission,
    bound: &BoundObservation,
) -> LedgerResult<EventSettlement> {
    let key = bound.key();
    let stored: Option<StoredEvent> = transaction
        .query_row(
            "SELECT revision,content_digest,measurement_ref,retracted
             FROM usage_source_events
             WHERE source_ref=?1 AND source_epoch=?2 AND observation_id=?3",
            params![&key.source_ref, &key.source_epoch, &key.observation_id],
            |row| {
                Ok(StoredEvent {
                    revision: row.get(0)?,
                    content_digest: row.get(1)?,
                    measurement_ref: row.get(2)?,
                    retracted: row.get::<_, i64>(3)? != 0,
                })
            },
        )
        .optional()
        .map_err(|_| LedgerError::storage())?;
    let incoming_digest = content_digest(&bound.observation)?;
    let measurement_ref = match (&stored, bound.measurement_ref.as_deref()) {
        (Some(stored), Some(incoming)) if stored.measurement_ref.as_deref() != Some(incoming) => {
            return Err(LedgerError::refused(
                STAGE,
                "usage_journal_event_identity_conflict",
            ));
        }
        (Some(stored), None) => stored.measurement_ref.clone(),
        (_, incoming) => incoming.map(str::to_owned),
    };
    let identity = identity_of(
        measurement_ref.as_deref(),
        &key.source_ref,
        &key.observation_id,
    );
    let facts = observation_facts(bound, admission, &identity);
    let revision = bound.revision();
    let owner = FactOwner {
        source_ref: &key.source_ref,
        source_epoch: &key.source_epoch,
        observation_id: &key.observation_id,
        revision,
    };
    let receipts = |outcome: FactOutcome| -> Vec<FactReceipt> {
        facts
            .iter()
            .map(|fact| FactReceipt {
                fact_id: fact.fact_id.clone(),
                outcome,
            })
            .collect()
    };
    let settlement = |outcome: EventOutcome, facts: Vec<FactReceipt>| EventSettlement {
        observation_id: key.observation_id.clone(),
        revision,
        outcome,
        facts,
    };

    let Some(stored) = stored else {
        if bound.observation.operation == UsageOperation::Retract {
            return Ok(settlement(EventOutcome::UnknownEvent, Vec::new()));
        }
        insert_event(
            transaction,
            &key,
            revision,
            &bound.observation,
            &incoming_digest,
            measurement_ref.as_deref(),
        )?;
        let mut settled = Vec::with_capacity(facts.len());
        for fact in &facts {
            let outcome = write_fact(transaction, fact, Some(&owner))?;
            settled.push(FactReceipt {
                fact_id: fact.fact_id.clone(),
                outcome,
            });
        }
        return Ok(settlement(EventOutcome::Settled, settled));
    };

    match bound.observation.operation {
        UsageOperation::Retract => {
            if stored.retracted {
                return Ok(settlement(
                    EventOutcome::Replayed,
                    receipts(FactOutcome::Withdrawn),
                ));
            }
            if revision < stored.revision {
                return Ok(settlement(
                    EventOutcome::Superseded,
                    receipts(FactOutcome::Superseded),
                ));
            }
            transaction
                .execute(
                    "UPDATE usage_source_events SET revision=?4,retracted=1,settled_at_ms=?5
                     WHERE source_ref=?1 AND source_epoch=?2 AND observation_id=?3",
                    params![
                        &key.source_ref,
                        &key.source_epoch,
                        &key.observation_id,
                        revision,
                        now_ms(),
                    ],
                )
                .map_err(|_| LedgerError::storage())?;
            withdraw_facts(transaction, &owner)?;
            Ok(settlement(
                EventOutcome::Withdrawn,
                receipts(FactOutcome::Withdrawn),
            ))
        }
        UsageOperation::Upsert => {
            if revision < stored.revision || (stored.retracted && revision == stored.revision) {
                return Ok(settlement(
                    EventOutcome::Superseded,
                    receipts(FactOutcome::Superseded),
                ));
            }
            if !stored.retracted && revision == stored.revision {
                if stored.content_digest != incoming_digest {
                    return Err(LedgerError::refused(STAGE, "usage_journal_event_conflict"));
                }
                return Ok(settlement(
                    EventOutcome::Replayed,
                    receipts(FactOutcome::Unchanged),
                ));
            }
            transaction
                .execute(
                    "UPDATE usage_source_events SET
                       revision=?4,content_digest=?5,measurement_ref=?6,retracted=0,
                       scope_ref=?8,observed_at=?9,settled_at_ms=?7
                     WHERE source_ref=?1 AND source_epoch=?2 AND observation_id=?3",
                    params![
                        &key.source_ref,
                        &key.source_epoch,
                        &key.observation_id,
                        revision,
                        &incoming_digest,
                        measurement_ref.as_deref(),
                        now_ms(),
                        &bound.observation.scope_ref,
                        &bound.observation.observed_at,
                    ],
                )
                .map_err(|_| LedgerError::storage())?;
            reconcile_event_facts(transaction, &owner, &facts)?;
            let mut corrected = Vec::with_capacity(facts.len());
            for fact in &facts {
                let outcome = write_fact(transaction, fact, Some(&owner))?;
                corrected.push(FactReceipt {
                    fact_id: fact.fact_id.clone(),
                    outcome,
                });
            }
            Ok(settlement(EventOutcome::Corrected, corrected))
        }
    }
}

/// The facts one bound observation claims.
fn observation_facts(
    bound: &BoundObservation,
    admission: &SourceAdmission,
    identity: &str,
) -> Vec<MeteringFact> {
    let scope_ref = &bound.observation.scope_ref;
    let mut facts = Vec::with_capacity(bound.observation.metrics.len() + 1);
    for (metric, reading) in &bound.observation.metrics {
        facts.push(MeteringFact {
            fact_id: fact_id(scope_ref, identity, metric),
            scope_ref: scope_ref.clone(),
            metric: metric.clone(),
            value: reading.value.clone(),
            unit: reading.unit.clone(),
            quality: reading.quality,
            eligibility: admission.eligibility,
            source_ref: bound.source_ref().to_owned(),
            observed_at: bound.observation.observed_at.clone(),
        });
    }
    if let Some(cost) = &bound.observation.cost {
        facts.push(MeteringFact {
            fact_id: fact_id(scope_ref, identity, general::COST),
            scope_ref: scope_ref.clone(),
            metric: general::COST.to_owned(),
            value: cost.amount.clone(),
            unit: cost.currency.clone(),
            quality: cost.quality,
            eligibility: admission.eligibility,
            source_ref: bound.source_ref().to_owned(),
            observed_at: bound.observation.observed_at.clone(),
        });
    }
    facts
}

/// Write one fact, applying the base rules:
///
/// - a withdrawn fact is not restored by an older or equal revision;
/// - identical content is a no-op for a replay;
/// - a strictly higher revision corrects it, so an event's own correction
///   replaces while a second source reporting one measurement does not
///   overwrite the settlement already recorded;
/// - a consumer recording by identity corrects it without a revision, because
///   the identity is the only correction key that port has;
/// - only an event owns a fact. A fact a consumer records carries the empty
///   ownership, and recording over a fact an event already settled leaves that
///   event's ownership in place, so the event can still withdraw its own fact.
fn write_fact(
    transaction: &rusqlite::Transaction<'_>,
    fact: &MeteringFact,
    owner: Option<&FactOwner<'_>>,
) -> LedgerResult<FactOutcome> {
    let existing = load_fact(transaction, &fact.fact_id)?;
    let outcome = match &existing {
        None => FactOutcome::Inserted,
        Some(existing) => {
            let newer_revision =
                owner.is_none_or(|owner| existing.revision.is_none_or(|r| owner.revision > r));
            if existing.retracted {
                if newer_revision {
                    FactOutcome::Updated
                } else {
                    return Ok(FactOutcome::Superseded);
                }
            } else if existing.same_content(fact) {
                FactOutcome::Unchanged
            } else if newer_revision {
                FactOutcome::Updated
            } else {
                return Ok(FactOutcome::Superseded);
            }
        }
    };
    if matches!(outcome, FactOutcome::Inserted | FactOutcome::Updated) {
        let revision = owner.map(|owner| i64::from(owner.revision));
        // The event-ownership columns are never null in the store: a fact with
        // no owning event carries the empty ownership, and the update keeps the
        // stored one instead of taking it over.
        let owned = i64::from(owner.is_some());
        let (source_epoch, observation_id) = match owner {
            Some(owner) => (owner.source_epoch, owner.observation_id),
            None => ("", ""),
        };
        transaction
            .execute(
                "INSERT INTO usage_source_facts(
                   fact_id,scope_ref,metric,value,unit,quality,eligibility,source_ref,
                   source_epoch,observation_id,observed_at,revision,retracted,updated_at_ms
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,0,?13)
                 ON CONFLICT(fact_id) DO UPDATE SET
                   scope_ref=excluded.scope_ref,metric=excluded.metric,value=excluded.value,
                   unit=excluded.unit,quality=excluded.quality,
                   eligibility=CASE WHEN ?14=0 THEN usage_source_facts.eligibility
                     ELSE excluded.eligibility END,
                   source_ref=excluded.source_ref,
                   source_epoch=CASE WHEN ?14=0 THEN usage_source_facts.source_epoch
                     ELSE excluded.source_epoch END,
                   observation_id=CASE WHEN ?14=0 THEN usage_source_facts.observation_id
                     ELSE excluded.observation_id END,
                   observed_at=excluded.observed_at,
                   revision=COALESCE(excluded.revision,usage_source_facts.revision),
                   retracted=0,updated_at_ms=excluded.updated_at_ms",
                params![
                    &fact.fact_id,
                    &fact.scope_ref,
                    &fact.metric,
                    fact.value.as_deref(),
                    &fact.unit,
                    quality_id(fact.quality),
                    fact.eligibility.id(),
                    &fact.source_ref,
                    source_epoch,
                    observation_id,
                    &fact.observed_at,
                    revision,
                    now_ms(),
                    owned,
                ],
            )
            .map_err(|_| LedgerError::storage())?;
    }
    Ok(outcome)
}

/// Reconcile one event's owned fact set against what a correction reports:
/// anything the event still owns that this revision no longer names is
/// withdrawn, so a removed metric or a moved scope cannot keep counting.
/// Facts another event owns under the same identity are untouched — the
/// ownership columns, not the fact id, decide what the event may retract.
fn reconcile_event_facts(
    transaction: &rusqlite::Transaction<'_>,
    owner: &FactOwner<'_>,
    facts: &[MeteringFact],
) -> LedgerResult<()> {
    let keep: std::collections::BTreeSet<&str> =
        facts.iter().map(|fact| fact.fact_id.as_str()).collect();
    let mut statement = transaction
        .prepare(
            "SELECT fact_id FROM usage_source_facts
             WHERE source_ref=?1 AND source_epoch=?2 AND observation_id=?3 AND retracted=0",
        )
        .map_err(|_| LedgerError::storage())?;
    let owned = statement
        .query_map(
            params![owner.source_ref, owner.source_epoch, owner.observation_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(|_| LedgerError::storage())?;
    let mut stale = Vec::new();
    for fact_id in owned {
        let fact_id = fact_id.map_err(|_| LedgerError::storage())?;
        if !keep.contains(fact_id.as_str()) {
            stale.push(fact_id);
        }
    }
    drop(statement);
    for fact_id in stale {
        transaction
            .execute(
                "UPDATE usage_source_facts SET retracted=1,revision=?2,updated_at_ms=?3
                 WHERE fact_id=?1 AND retracted=0",
                params![fact_id, i64::from(owner.revision), now_ms()],
            )
            .map_err(|_| LedgerError::storage())?;
    }
    Ok(())
}

/// Mark every fact of one event withdrawn, without touching another event's
/// facts that happen to share an identity.
fn withdraw_facts(
    transaction: &rusqlite::Transaction<'_>,
    owner: &FactOwner<'_>,
) -> LedgerResult<()> {
    transaction
        .execute(
            "UPDATE usage_source_facts SET retracted=1,revision=?4,updated_at_ms=?5
             WHERE source_ref=?1 AND source_epoch=?2 AND observation_id=?3 AND retracted=0",
            params![
                owner.source_ref,
                owner.source_epoch,
                owner.observation_id,
                i64::from(owner.revision),
                now_ms(),
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    Ok(())
}

fn insert_event(
    transaction: &rusqlite::Transaction<'_>,
    key: &BoundObservationKey,
    revision: u32,
    observation: &UsageObservation,
    content_digest: &str,
    measurement_ref: Option<&str>,
) -> LedgerResult<()> {
    transaction
        .execute(
            "INSERT INTO usage_source_events(
               source_ref,source_epoch,observation_id,revision,scope_ref,measurement_ref,
               observed_at,content_digest,retracted,settled_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,0,?9)",
            params![
                &key.source_ref,
                &key.source_epoch,
                &key.observation_id,
                revision,
                &observation.scope_ref,
                measurement_ref,
                &observation.observed_at,
                content_digest,
                now_ms(),
            ],
        )
        .map_err(|_| LedgerError::storage())?;
    Ok(())
}

/// One fact's stored columns.
struct FactRow {
    scope_ref: String,
    metric: String,
    value: Option<String>,
    unit: String,
    quality: Quality,
    eligibility: SettlementEligibility,
    /// The consumer or source the fact was recorded under.
    source_ref: String,
    /// Non-empty marks a fact an observation event settled; empty marks one a
    /// consumer recorded directly.
    observation_id: String,
    revision: Option<u32>,
    retracted: bool,
}

impl FactRow {
    fn same_content(&self, fact: &MeteringFact) -> bool {
        self.scope_ref == fact.scope_ref
            && self.metric == fact.metric
            && self.value == fact.value
            && self.unit == fact.unit
            && self.quality == fact.quality
            && self.eligibility == fact.eligibility
    }

    /// A fact an observation event settled answers only to that event's own
    /// settlement path — a consumer record or retract never touches it.
    fn is_event_owned(&self) -> bool {
        !self.observation_id.is_empty()
    }
}

fn load_fact(
    transaction: &rusqlite::Transaction<'_>,
    fact_id: &str,
) -> LedgerResult<Option<FactRow>> {
    transaction
        .query_row(
            "SELECT scope_ref,metric,value,unit,quality,eligibility,source_ref,
                    observation_id,revision,retracted
             FROM usage_source_facts WHERE fact_id=?1",
            params![fact_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)? != 0,
                ))
            },
        )
        .optional()
        .map_err(|_| LedgerError::storage())
        .and_then(|row| match row {
            None => Ok(None),
            Some((
                scope_ref,
                metric,
                value,
                unit,
                quality,
                eligibility,
                source_ref,
                observation_id,
                revision,
                retracted,
            )) => Ok(Some(FactRow {
                scope_ref,
                metric,
                value,
                unit,
                quality: quality_from(&quality)?,
                eligibility: SettlementEligibility::parse(&eligibility)?,
                source_ref,
                observation_id,
                revision: revision.and_then(|revision| u32::try_from(revision).ok()),
                retracted,
            })),
        })
}

/// Write one fact through the consumer port: the row answers only to the owner
/// it was recorded under, and an event-owned fact is refused outright — the
/// event's settlement path is the only writer of what it owns.
fn write_consumer_fact(
    transaction: &rusqlite::Transaction<'_>,
    owner: &str,
    fact: &MeteringFact,
) -> LedgerResult<FactOutcome> {
    if let Some(existing) = load_fact(transaction, &fact.fact_id)?
        && (existing.is_event_owned() || existing.source_ref != owner)
    {
        return Err(LedgerError::refused(STAGE, "usage_journal_fact_not_owned"));
    }
    write_fact(transaction, fact, None)
}

/// Whether the host composed this observation under this admission: the same
/// source and epoch, and a scope the binding authorized.
fn require_admitted(admission: &SourceAdmission, bound: &BoundObservation) -> LedgerResult<()> {
    if bound.source_ref() != admission.binding.source_ref
        || bound.binding.source_epoch != admission.binding.source_epoch
        || !admission.binding.authorizes_scope(bound.scope_ref())
    {
        return Err(LedgerError::refused(
            STAGE,
            "usage_journal_observation_not_admitted",
        ));
    }
    Ok(())
}

fn identity_of(measurement_ref: Option<&str>, source_ref: &str, observation_id: &str) -> String {
    match measurement_ref {
        Some(measurement) => format!("measurement:{measurement}"),
        None => format!("observation:{source_ref}:{observation_id}"),
    }
}

fn fact_id(scope_ref: &str, identity: &str, metric: &str) -> String {
    format!("{scope_ref}#{identity}#{metric}")
}

/// A digest of everything a producer claimed — including the scope and the
/// producer's own timestamp, which decide which facts the observation owns —
/// so a replay at one revision with different content is refused instead of
/// silently rewriting a settled fact or silently keeping a stale-scope one.
fn content_digest(observation: &UsageObservation) -> LedgerResult<String> {
    let encoded = serde_json::to_vec(&(
        &observation.scope_ref,
        &observation.observed_at,
        &observation.operation,
        &observation.interval_start,
        &observation.metrics,
        &observation.cost,
    ))
    .map_err(|_| LedgerError::refused(STAGE, "usage_journal_event_unencodable"))?;
    let mut hasher = Sha256::new();
    hasher.update(&encoded);
    let mut digest = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(digest, "{byte:02x}");
    }
    Ok(digest)
}

/// The stored cursor of one source epoch.
struct StoredCursor {
    scope_ref: String,
    cursor: String,
    sequence: u64,
}

fn stored_cursor(
    connection: &rusqlite::Connection,
    source_ref: &str,
    source_epoch: &str,
) -> LedgerResult<Option<StoredCursor>> {
    connection
        .query_row(
            "SELECT scope_ref,cursor,sequence FROM usage_source_cursors
             WHERE source_ref=?1 AND source_epoch=?2",
            params![source_ref, source_epoch],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|_| LedgerError::storage())
        .and_then(|row| match row {
            None => Ok(None),
            Some((scope_ref, cursor, sequence)) => Ok(Some(StoredCursor {
                scope_ref,
                cursor,
                sequence: u64::try_from(sequence)
                    .map_err(|_| LedgerError::refused(STAGE, "usage_journal_cursor_invalid"))?,
            })),
        })
}

/// Exact addition over the published decimal.
///
/// The contract publishes exact subtraction — `0.1` and `0.3` have no exact
/// binary form, so a total is never a float — and an addition through it is a
/// subtraction of the negation, which rounds nothing.
fn checked_sum(previous: ExactNumber, value: ExactNumber) -> Option<ExactNumber> {
    let canonical = value.to_canonical_string();
    let negated = match canonical.strip_prefix('-') {
        Some(magnitude) => magnitude.to_owned(),
        None => format!("-{canonical}"),
    };
    previous.checked_sub(ExactNumber::parse(&negated)?)
}

fn quality_id(quality: Quality) -> &'static str {
    match quality {
        Quality::Reported => "reported",
        Quality::Estimated => "estimated",
        Quality::Unknown => "unknown",
    }
}

fn quality_from(text: &str) -> LedgerResult<Quality> {
    match text {
        "reported" => Ok(Quality::Reported),
        "estimated" => Ok(Quality::Estimated),
        "unknown" => Ok(Quality::Unknown),
        _ => Err(LedgerError::refused(STAGE, "usage_journal_fact_invalid")),
    }
}

fn sqlite_counter(value: u64) -> LedgerResult<i64> {
    i64::try_from(value).map_err(|_| LedgerError::refused(STAGE, "usage_journal_counter_overflow"))
}

/// Carry a refusal from the published SDK through the base ledger's error shape
/// without losing the code the caller reconciles on.
fn from_refusal(failure: ApplicationFailure) -> LedgerError {
    LedgerError {
        code: failure.code,
        stage: failure.stage,
        retryable: failure.retryable,
        recovery: failure.recovery.mcp_wire().to_owned(),
    }
}

/// Carry a journal refusal into the report's refusal vocabulary, preserving the
/// code the caller reconciles on. Storage failures never reach this shape —
/// they abort the batch instead.
fn failure_from(error: LedgerError) -> ApplicationFailure {
    if error.retryable {
        ApplicationFailure::retryable(error.code, error.stage)
    } else {
        ApplicationFailure::permanent(error.code, error.stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acativa::usage_ledger::{reserve_graph_command, settle_graph_command};
    use licoup_extension_contracts::usage::{
        CostObservation, MetricValue, Temporality, UsageObservation,
    };
    use licoup_extension_contracts::wire;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use token_usage_detector::normalize::OtlpSumNormalizer;

    /// The synthetic OTLP-shaped series the accepted component fixtures publish,
    /// reused here so the base journal is exercised against the same producer
    /// shapes the analytics package reads.
    const OTLP_CUMULATIVE: &str = include_str!("../tests/fixtures/otlp-cumulative.json");
    const OTLP_RESTARTED: &str = include_str!("../tests/fixtures/otlp-restarted.json");
    const OTLP_REGRESSED: &str = include_str!("../tests/fixtures/otlp-regressed.json");

    const SOURCE: &str = "source:example.analytics#1";
    const EXTENSION: &str = "example.analytics";
    const SCOPE: &str = "scope-1";
    const METRIC: &str = "licoup.tokens.input";
    const OTLP_METRIC: &str = "example.analytics/tokens.input";

    fn root(label: &str) -> PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lico-usage-journal-{label}-{nonce}-{}",
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn params(root: &Path) -> Value {
        json!({"stateRoot": root.to_string_lossy()})
    }

    fn binding(epoch: &str) -> SourceBinding {
        SourceBinding::new(SOURCE, EXTENSION, "instance-1", 1, epoch, [SCOPE]).expect("binding")
    }

    fn source_admission(epoch: &str, eligibility: SettlementEligibility) -> SourceAdmission {
        SourceAdmission::new(binding(epoch), eligibility)
    }

    fn reading(value: &str) -> MetricValue {
        MetricValue::reported(value, "tokens", Temporality::Delta)
    }

    fn upsert(
        observation_id: &str,
        revision: u32,
        metrics: Vec<(&str, MetricValue)>,
    ) -> UsageObservation {
        UsageObservation {
            schema: wire::USAGE.to_owned(),
            observation_id: observation_id.to_owned(),
            revision,
            operation: UsageOperation::Upsert,
            source_epoch: "epoch-1".to_owned(),
            scope_ref: SCOPE.to_owned(),
            observed_at: "2026-09-21T00:00:00Z".to_owned(),
            interval_start: None,
            metrics: metrics
                .into_iter()
                .map(|(metric, reading)| (metric.to_owned(), reading))
                .collect::<BTreeMap<_, _>>(),
            cost: None,
        }
    }

    fn metric_upsert(observation_id: &str, revision: u32, value: &str) -> UsageObservation {
        upsert(observation_id, revision, vec![(METRIC, reading(value))])
    }

    fn retraction(observation_id: &str, revision: u32) -> UsageObservation {
        UsageObservation {
            operation: UsageOperation::Retract,
            metrics: BTreeMap::new(),
            ..metric_upsert(observation_id, revision, "0")
        }
    }

    fn payload(observation: &UsageObservation) -> Value {
        serde_json::to_value(observation).expect("wire observation")
    }

    fn batch(observations: &[UsageObservation]) -> PublishBatch {
        PublishBatch::new(observations.iter().map(payload).collect()).expect("bounded batch")
    }

    /// One bounded page of the synthetic OTLP-shaped series, as a source that
    /// was pulled would answer it.
    fn pull() -> QueryRequest {
        QueryRequest::new(SCOPE, None, None).expect("request")
    }

    fn otlp_page(record: &str, epoch: &str, sequence: u64) -> QueryPage {
        let record: Value = serde_json::from_str(record).expect("fixture");
        let observations = OtlpSumNormalizer::new(EXTENSION, Quality::Reported)
            .expect("normalizer")
            .normalize(&record, &binding(epoch))
            .expect("mapped")
            .into_iter()
            .map(|bound| bound.observation)
            .collect();
        QueryPage {
            source_epoch: epoch.to_owned(),
            scope_ref: SCOPE.to_owned(),
            observations,
            next_cursor: Some(Cursor::new(epoch, sequence).expect("cursor").encode()),
            has_more: true,
        }
    }

    fn facts(journal: &UsageJournal) -> Vec<MeteringFact> {
        journal.read(None, 100).expect("readable").facts
    }

    #[test]
    fn a_pushed_batch_settles_once_and_a_replay_writes_nothing() {
        let root = root("push");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let batch = batch(&[
            metric_upsert("obs-1", 1, "120"),
            metric_upsert("obs-2", 1, "80"),
        ]);

        let settled = journal.publish(&admission, &batch).expect("settled");
        assert_eq!(settled.accepted, 2);
        assert!(
            settled
                .events
                .iter()
                .all(|event| event.outcome == EventOutcome::Settled)
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("200")
        );

        let replayed = journal.publish(&admission, &batch).expect("replayed");
        assert_eq!(replayed.accepted, 2);
        assert!(
            replayed
                .events
                .iter()
                .all(|event| event.outcome == EventOutcome::Replayed && !event.wrote()),
            "a replay must not write"
        );
        assert_eq!(
            replayed.events[0].facts[0].outcome,
            FactOutcome::Unchanged,
            "a replayed event reports its facts unchanged"
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("200"),
            "a replay does not add a second settlement"
        );
        assert_eq!(facts(&journal).len(), 2, "one fact per observation");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_page_for_another_scope_is_refused_at_admission() {
        let root = root("page-scope");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let mut grants = binding("epoch-1");
        grants.scope_grants.push("scope-2".to_owned());
        let admission = SourceAdmission::new(grants, SettlementEligibility::SettlementEligible);
        let mut page = otlp_page(OTLP_CUMULATIVE, "epoch-1", 1);
        page.scope_ref = "scope-2".to_owned();
        for observation in &mut page.observations {
            observation.scope_ref = "scope-2".to_owned();
        }
        let failure = journal
            .admit_page(&admission, &pull(), &page)
            .expect_err("the page did not answer the request");
        assert_eq!(failure.code, "usage_page_scope_mismatch");
    }

    #[test]
    fn a_duplicate_cursor_page_does_not_double_count_or_move_the_cursor_back() {
        let root = root("cursor");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let page = otlp_page(OTLP_CUMULATIVE, "epoch-1", 1);

        let first = journal
            .admit_page(&admission, &pull(), &page)
            .expect("settled");
        assert_eq!(first.accepted, 2);
        assert_eq!(first.cursor.as_deref(), Some("1@epoch-1"));
        assert_eq!(
            journal
                .settled_total(SCOPE, OTLP_METRIC)
                .expect("total")
                .as_deref(),
            Some("275"),
            "100 then 175 cumulative points are two readings, not a sum of totals"
        );

        let replayed = journal
            .admit_page(&admission, &pull(), &page)
            .expect("replayed");
        assert!(
            replayed
                .events
                .iter()
                .all(|event| event.outcome == EventOutcome::Replayed),
            "the same page delivered again settles nothing"
        );
        assert_eq!(
            replayed.cursor.as_deref(),
            Some("1@epoch-1"),
            "a replayed page does not rewind the durable cursor"
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, OTLP_METRIC)
                .expect("total")
                .as_deref(),
            Some("275"),
            "a duplicate cursor does not double-count"
        );
        assert_eq!(facts(&journal).len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_restart_resumes_from_the_durable_cursor_and_a_reset_starts_a_new_epoch() {
        let root = root("restart");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let first_page = otlp_page(OTLP_CUMULATIVE, "epoch-1", 1);
        {
            let mut journal = UsageJournal::open(&params(&root)).expect("journal");
            assert_eq!(
                journal
                    .resume_request(&admission, SCOPE, None)
                    .expect("first read")
                    .mode(),
                token_usage_detector::collection::CollectionMode::Pull,
                "a source with no durable position is read from the start"
            );
            journal
                .admit_page(&admission, &pull(), &first_page)
                .expect("settled");
        }

        // The same ledger is reopened: the cursor and the facts are the store's,
        // not the process's.
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        assert_eq!(
            journal
                .durable_cursor(&admission.binding)
                .expect("cursor")
                .as_deref(),
            Some("1@epoch-1")
        );
        let resumed = journal
            .resume_request(&admission, SCOPE, Some(10))
            .expect("resume");
        assert_eq!(
            resumed.mode(),
            token_usage_detector::collection::CollectionMode::Cursor
        );
        assert_eq!(resumed.cursor.as_deref(), Some("1@epoch-1"));
        assert_eq!(
            journal
                .settled_total(SCOPE, OTLP_METRIC)
                .expect("total")
                .as_deref(),
            Some("275"),
            "a restart preserves the facts"
        );

        // A producer restart is a new epoch: it is read from the start, and the
        // old epoch's position is not resumed into the new series.
        let reset = source_admission("epoch-2", SettlementEligibility::SettlementEligible);
        let fresh = journal.resume_request(&reset, SCOPE, None).expect("read");
        assert!(
            fresh.cursor.is_none(),
            "a reset does not resume an old cursor"
        );
        let restarted_page = otlp_page(OTLP_RESTARTED, "epoch-2", 1);
        let settled = journal
            .admit_page(&reset, &pull(), &restarted_page)
            .expect("settled");
        assert_eq!(settled.events[0].outcome, EventOutcome::Settled);
        assert_eq!(settled.cursor.as_deref(), Some("1@epoch-2"));
        assert_eq!(
            journal
                .settled_total(SCOPE, OTLP_METRIC)
                .expect("total")
                .as_deref(),
            Some("280"),
            "a new origin is its own reading, never a negative delta"
        );

        // Within one epoch a regressed cumulative value is still a new reading:
        // differencing is the consumer's rule, and the base never invents one.
        let regressed = otlp_page(OTLP_REGRESSED, "epoch-2", 2);
        let settled = journal
            .admit_page(&reset, &pull(), &regressed)
            .expect("settled");
        assert_eq!(settled.events[0].outcome, EventOutcome::Settled);
        assert_eq!(settled.cursor.as_deref(), Some("2@epoch-2"));
        assert_eq!(
            journal
                .settled_total(SCOPE, OTLP_METRIC)
                .expect("total")
                .as_deref(),
            Some("320")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn an_unknown_reading_is_stored_without_a_number_and_an_absent_metric_has_no_fact() {
        let root = root("unknown");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let observation = upsert(
            "obs-unknown",
            1,
            vec![
                (METRIC, reading("120")),
                (
                    general::TOKENS_TOTAL,
                    MetricValue::unknown("tokens", Temporality::Delta),
                ),
            ],
        );
        journal
            .publish(&admission, &batch(&[observation]))
            .expect("settled");

        let unknown = facts(&journal)
            .into_iter()
            .find(|fact| fact.metric == general::TOKENS_TOTAL)
            .expect("the call is known even though its total is not");
        assert_eq!(unknown.value, None, "an unknown count carries no number");
        assert_eq!(unknown.quality, Quality::Unknown);
        assert_eq!(
            journal
                .settled_total(SCOPE, general::TOKENS_TOTAL)
                .expect("total"),
            None,
            "an unknown total is not zero"
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("120")
        );
        assert!(
            !facts(&journal)
                .iter()
                .any(|fact| fact.metric == general::TOKENS_OUTPUT),
            "a metric the producer did not send is absent, not zero"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn the_base_facts_settle_without_the_optional_analytics_package() {
        // The base ledger is the kernel's. The optional analytics package reads
        // it through the composed port; nothing here depends on that package
        // existing, and the kernel manifest must not name it.
        let manifest = include_str!("../Cargo.toml");
        assert!(!manifest.contains("licoup-analytics"));
        assert!(!manifest.contains("org.licoland.feature.analytics"));

        let root = root("analytics-absent");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "42")]))
            .expect("settled");
        let page = journal.read(None, 10).expect("readable");
        assert_eq!(page.facts.len(), 1, "the base port reads its own facts");
        assert_eq!(page.facts[0].value.as_deref(), Some("42"));
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("42")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn the_detector_dependency_points_downward_only() {
        // The kernel takes the usage detector so its own journal admits the
        // source shapes; the detector maps inputs onto the published contract
        // and never names the kernel or this package back. The detector's own
        // repository enforces its side of the direction.
        let kernel = include_str!("../Cargo.toml");
        assert!(kernel.contains("token-usage-detector"));
    }

    #[test]
    fn a_refused_observation_does_not_take_its_neighbours_with_it() {
        let root = root("refused");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let mut asserted = payload(&metric_upsert("obs-2", 1, "80"));
        asserted["extensionId"] = Value::String("someone.else".to_owned());
        let batch = PublishBatch::new(vec![
            payload(&metric_upsert("obs-1", 1, "120")),
            asserted,
            payload(&metric_upsert("obs-3", 1, "1")),
        ])
        .expect("batch");

        let report = journal.publish(&admission, &batch).expect("delivered");
        assert_eq!(report.accepted, 2);
        assert_eq!(report.refused.len(), 1);
        assert_eq!(report.refused[0].index, 1);
        assert_eq!(report.refused[0].failure.code, "usage_source_self_asserted");
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("121"),
            "the accepted neighbours settled and the refusal did not"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_retraction_withdraws_its_own_observation_and_an_older_replay_cannot_restore_it() {
        let root = root("retract");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(
                &admission,
                &batch(&[
                    metric_upsert("obs-1", 1, "120"),
                    metric_upsert("obs-2", 1, "80"),
                ]),
            )
            .expect("settled");

        let withdrawn = journal
            .publish(&admission, &batch(&[retraction("obs-1", 2)]))
            .expect("withdrawn");
        assert_eq!(withdrawn.events[0].outcome, EventOutcome::Withdrawn);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("80"),
            "a retraction cancels its own observation and nothing else"
        );

        let resurrect = journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("superseded");
        assert_eq!(resurrect.events[0].outcome, EventOutcome::Superseded);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("80"),
            "an older replay cannot restore a withdrawn fact"
        );

        let corrected = journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 3, "150")]))
            .expect("corrected");
        assert_eq!(corrected.events[0].outcome, EventOutcome::Corrected);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("230"),
            "a strictly newer revision is a correction, not a second charge"
        );
        assert_eq!(facts(&journal).len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_same_revision_with_other_content_is_refused_instead_of_rewriting() {
        let root = root("conflict");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("settled");

        let report = journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "7")]))
            .expect("a refused observation lands in the report");
        assert!(report.events.is_empty(), "the conflict settled nothing");
        assert_eq!(report.refused.len(), 1);
        assert_eq!(
            report.refused[0].failure.code,
            "usage_journal_event_conflict"
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("120"),
            "the settled fact stands"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_correction_that_drops_a_metric_withdraws_the_fact_it_owned() {
        let root = root("reconcile");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(
                &admission,
                &batch(&[upsert(
                    "obs-1",
                    1,
                    vec![
                        (METRIC, reading("120")),
                        ("gen.tokens.output", reading("30")),
                    ],
                )]),
            )
            .expect("settled");
        assert_eq!(facts(&journal).len(), 2);

        // The correction reports only one metric: the dropped one was this
        // event's fact, so it leaves the ledger instead of counting a stale
        // value forever.
        let corrected = journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 2, "140")]))
            .expect("corrected");
        assert_eq!(corrected.events[0].outcome, EventOutcome::Corrected);
        let remaining = facts(&journal);
        assert_eq!(remaining.len(), 1, "the dropped metric's fact is gone");
        assert_eq!(remaining[0].metric, METRIC);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("140")
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, "gen.tokens.output")
                .expect("total"),
            None
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_correction_that_moves_scope_reconciles_the_old_scope_facts() {
        let root = root("rescope");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let binding = SourceBinding::new(
            SOURCE,
            EXTENSION,
            "instance-1",
            1,
            "epoch-1",
            [SCOPE, "scope:other"],
        )
        .expect("binding");
        let admission = SourceAdmission::new(binding, SettlementEligibility::SettlementEligible);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("settled");

        // A higher revision re-keyed to another scope: the old-scope facts the
        // event owned are reconciled away, not left counting under a scope the
        // producer no longer claims.
        let mut moved = metric_upsert("obs-1", 2, "130");
        moved.scope_ref = "scope:other".to_owned();
        let corrected = journal
            .publish(&admission, &batch(&[moved]))
            .expect("corrected");
        assert_eq!(corrected.events[0].outcome, EventOutcome::Corrected);
        assert_eq!(journal.settled_total(SCOPE, METRIC).expect("total"), None);
        assert_eq!(
            journal
                .settled_total("scope:other", METRIC)
                .expect("total")
                .as_deref(),
            Some("130")
        );
        assert_eq!(
            facts(&journal).len(),
            1,
            "one fact, under the new scope only"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_mixed_page_advances_the_cursor_over_what_settled() {
        let root = root("mixed-page");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("settled");

        // A page that validates as an envelope but carries one observation
        // settlement refuses: the neighbours still commit and the cursor still
        // advances over the page the source delivered.
        let page = QueryPage {
            source_epoch: "epoch-1".to_owned(),
            scope_ref: SCOPE.to_owned(),
            observations: vec![
                metric_upsert("obs-1", 1, "999"),
                metric_upsert("obs-2", 1, "80"),
            ],
            next_cursor: Some(Cursor::new("epoch-1", 7).expect("cursor").encode()),
            has_more: true,
        };

        let report = journal
            .admit_page(&admission, &pull(), &page)
            .expect("the page settles what it may");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.refused.len(), 1);
        assert_eq!(report.refused[0].index, 0);
        assert_eq!(
            report.refused[0].failure.code,
            "usage_journal_event_conflict"
        );
        assert_eq!(
            report.cursor.as_deref(),
            Some("7@epoch-1"),
            "the cursor moved over the processed page, invalid item included"
        );
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("200"),
            "the conflicting redelivery rewrote nothing and the valid neighbour settled"
        );

        // The durable cursor the next pull resumes from is the page's position,
        // not the refused observation's.
        let resumed = journal
            .resume_request(&admission, SCOPE, None)
            .expect("resumed");
        assert_eq!(resumed.cursor.as_deref(), Some("7@epoch-1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn one_host_issued_measurement_settles_once_across_two_sources() {
        let root = root("measurement");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let first = SourceAdmission::new(
            SourceBinding::new(
                "source:agent#1",
                "example.agent",
                "instance-1",
                1,
                "epoch-1",
                [SCOPE],
            )
            .expect("binding"),
            SettlementEligibility::SettlementEligible,
        );
        let second = SourceAdmission::new(
            SourceBinding::new(
                "source:gateway#1",
                "example.gateway",
                "instance-1",
                1,
                "epoch-1",
                [SCOPE],
            )
            .expect("binding"),
            SettlementEligibility::SettlementEligible,
        );
        let observation = metric_upsert("obs-agent", 1, "120");
        let bound = first
            .binding
            .bind_observation(observation.clone(), Some("call-1"))
            .expect("bound");
        let settled = journal.admit_observation(&first, &bound).expect("settled");
        assert_eq!(settled.outcome, EventOutcome::Settled);

        let same_call = second
            .binding
            .bind_observation(observation, Some("call-1"))
            .expect("bound");
        let settled = journal
            .admit_observation(&second, &same_call)
            .expect("settled");
        assert_eq!(
            settled.facts[0].outcome,
            FactOutcome::Unchanged,
            "the same host-issued measurement is one fact, whichever source reports it"
        );
        assert_eq!(facts(&journal).len(), 1);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("120"),
            "two reports of one measurement are one settlement"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_display_only_source_is_readable_but_never_settles() {
        let root = root("display");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::DisplayOnly);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("settled");

        let page = journal.read(None, 10).expect("readable");
        assert_eq!(page.facts.len(), 1, "the fact and its provenance are kept");
        assert_eq!(
            page.facts[0].eligibility,
            SettlementEligibility::DisplayOnly
        );
        assert_eq!(
            journal.settled_total(SCOPE, METRIC).expect("total"),
            None,
            "a producer's claim is displayed, never settled"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_cursor_belongs_to_one_scope_and_one_epoch() {
        let root = root("cursor-scope");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .admit_page(
                &admission,
                &pull(),
                &otlp_page(OTLP_CUMULATIVE, "epoch-1", 4),
            )
            .expect("settled");

        let other = SourceBinding::new(SOURCE, EXTENSION, "instance-1", 1, "epoch-1", ["scope-2"])
            .expect("binding");
        let other = SourceAdmission::new(other, SettlementEligibility::SettlementEligible);
        let mismatch = journal
            .resume_request(&other, "scope-2", None)
            .expect_err("another scope");
        assert_eq!(
            mismatch.code, "usage_journal_cursor_scope_mismatch",
            "a cursor belongs to the scope it was read under"
        );
        let ungranted = journal
            .resume_request(&other, "scope-3", None)
            .expect_err("no grant");
        assert_eq!(ungranted.code, "usage_journal_scope_not_authorized");

        let page = QueryPage {
            source_epoch: "epoch-2".to_owned(),
            scope_ref: SCOPE.to_owned(),
            observations: Vec::new(),
            next_cursor: None,
            has_more: false,
        };
        let stale = journal
            .admit_page(&admission, &pull(), &page)
            .expect_err("another epoch");
        assert_eq!(stale.code, "usage_source_epoch_mismatch");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cursors_are_per_source_and_a_source_that_never_delivered_has_none() {
        let root = root("cursor-source");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let first = SourceAdmission::new(
            SourceBinding::new(
                "source:first#1",
                "example.analytics",
                "instance-1",
                1,
                "epoch-1",
                [SCOPE],
            )
            .expect("binding"),
            SettlementEligibility::SettlementEligible,
        );
        let second = SourceAdmission::new(
            SourceBinding::new(
                "source:second#1",
                "example.gateway",
                "instance-1",
                1,
                "epoch-1",
                [SCOPE],
            )
            .expect("binding"),
            SettlementEligibility::SettlementEligible,
        );

        journal
            .admit_page(&first, &pull(), &otlp_page(OTLP_CUMULATIVE, "epoch-1", 7))
            .expect("settled");
        assert_eq!(
            journal
                .durable_cursor(&first.binding)
                .expect("cursor")
                .as_deref(),
            Some("7@epoch-1")
        );
        assert_eq!(
            journal.durable_cursor(&second.binding).expect("cursor"),
            None,
            "a source that never delivered has no position, not another source's"
        );

        journal
            .admit_page(&second, &pull(), &otlp_page(OTLP_CUMULATIVE, "epoch-1", 2))
            .expect("settled");
        assert_eq!(
            journal
                .durable_cursor(&second.binding)
                .expect("cursor")
                .as_deref(),
            Some("2@epoch-1")
        );
        assert_eq!(
            journal
                .durable_cursor(&first.binding)
                .expect("cursor")
                .as_deref(),
            Some("7@epoch-1"),
            "one source's read never moves another source's position"
        );
        assert_eq!(
            journal
                .resume_request(&first, SCOPE, None)
                .expect("resume")
                .cursor
                .as_deref(),
            Some("7@epoch-1")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_obligations_report_what_the_base_ledger_still_holds() {
        let root = root("pending");
        let journal = UsageJournal::open(&params(&root)).expect("journal");
        assert!(journal.pending().expect("pending").is_empty());

        let reserved = reserve_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "budgetId": "pool:one",
            "invocationId": "invocation:one",
            "runId": "run:one",
            "commandId": "command:one",
            "budget": {"limitTokens": 100, "usedTokens": 0},
            "estimate": {"totalTokens": 10, "accuracy": "estimated"}
        }))
        .expect("reserved");
        assert_eq!(reserved["admitted"], true);
        let pending = journal.pending().expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].scope_ref, "run:one");
        assert_eq!(pending[0].reason, "in-flight");
        assert_eq!(pending[0].metric, general::TOKENS_TOTAL);

        settle_graph_command(&json!({
            "stateRoot": root.to_string_lossy(),
            "invocationId": "invocation:one",
            "status": "failed"
        }))
        .expect("settled as unknown");
        let pending = journal.pending().expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].reason, "unknown-usage",
            "a settlement without usage is retained as an obligation"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_consumer_records_only_facts_it_owns() {
        let root = root("consumer");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        journal
            .publish(&admission, &batch(&[metric_upsert("obs-1", 1, "120")]))
            .expect("settled");
        let settled = facts(&journal);
        assert_eq!(settled.len(), 1);

        // A consumer correcting the event's fact by identity is refused: the
        // fact names its owner and the consumer is not it.
        let mut stolen = settled[0].clone();
        stolen.value = Some("130".to_owned());
        let error = journal
            .record("native:agent-1", stolen)
            .expect_err("a consumer cannot rewrite an event's fact");
        assert!(!error.retryable);
        assert_eq!(
            journal
                .settled_total(SCOPE, METRIC)
                .expect("total")
                .as_deref(),
            Some("120"),
            "the event's fact keeps its settled value"
        );

        // The consumer's own fact it may record and correct in place.
        let owned = MeteringFact {
            fact_id: format!("{SCOPE}#consumer:native:agent-1#{METRIC}"),
            scope_ref: SCOPE.to_owned(),
            metric: METRIC.to_owned(),
            value: Some("7".to_owned()),
            unit: "tokens".to_owned(),
            quality: Quality::Reported,
            eligibility: SettlementEligibility::SettlementEligible,
            source_ref: "native:agent-1".to_owned(),
            observed_at: "2026-09-21T00:00:00Z".to_owned(),
        };
        let receipt = journal
            .record("native:agent-1", owned.clone())
            .expect("recorded");
        assert_eq!(receipt.outcome, FactOutcome::Inserted);
        let mut corrected = owned.clone();
        corrected.value = Some("8".to_owned());
        let receipt = journal
            .record("native:agent-1", corrected)
            .expect("corrected");
        assert_eq!(receipt.outcome, FactOutcome::Updated);
        assert_eq!(facts(&journal).len(), 2);

        // And a fact that names another owner never reaches the store.
        let mut foreign = owned.clone();
        foreign.fact_id = format!("{SCOPE}#consumer:native:agent-1#foreign");
        foreign.source_ref = "native:agent-2".to_owned();
        let error = journal
            .record("native:agent-1", foreign)
            .expect_err("an owner cannot write under another's name");
        assert!(!error.retryable);

        // Retract follows the same ownership: the consumer withdraws only its
        // own fact, never the event's.
        let error = journal
            .retract("native:agent-1", &settled[0].fact_id)
            .expect_err("a consumer cannot retract an event's fact");
        assert!(!error.retryable);
        let receipt = journal
            .retract(
                "native:agent-1",
                &format!("{SCOPE}#consumer:native:agent-1#{METRIC}"),
            )
            .expect("withdrawn");
        assert_eq!(receipt.outcome, FactOutcome::Withdrawn);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_cost_and_a_metric_of_one_observation_are_separate_facts() {
        let root = root("cost");
        let mut journal = UsageJournal::open(&params(&root)).expect("journal");
        let admission = source_admission("epoch-1", SettlementEligibility::SettlementEligible);
        let mut first = metric_upsert("obs-cost-1", 1, "120");
        first.cost = Some(CostObservation {
            amount: Some("0.1".to_owned()),
            currency: "USD".to_owned(),
            quality: Quality::Estimated,
        });
        let mut second = metric_upsert("obs-cost-2", 1, "80");
        second.cost = Some(CostObservation {
            amount: Some("0.2".to_owned()),
            currency: "USD".to_owned(),
            quality: Quality::Estimated,
        });
        journal
            .publish(&admission, &batch(&[first, second]))
            .expect("settled");

        let page = journal.read(None, 10).expect("readable");
        assert_eq!(
            page.facts.len(),
            4,
            "a metric and a cost are separate facts"
        );
        let cost = page
            .facts
            .iter()
            .find(|fact| fact.metric == general::COST)
            .expect("cost fact");
        assert_eq!(cost.value.as_deref(), Some("0.1"));
        assert_eq!(cost.unit, "USD");
        assert_eq!(cost.quality, Quality::Estimated);
        assert_eq!(
            journal
                .settled_total(SCOPE, general::COST)
                .expect("total")
                .as_deref(),
            Some("0.3"),
            "exact decimals are summed exactly, never through a float"
        );

        let _ = fs::remove_dir_all(root);
    }
}
