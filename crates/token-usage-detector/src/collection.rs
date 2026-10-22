//! The three collection directions and their bounds.
//!
//! C11 gives a source exactly two methods to be collected through —
//! `usage.publish` and `usage.query(cursor, limit, scopeRef)` — and the task's
//! three collection shapes are the ways they are used:
//!
//! | Shape | Method | State |
//! |:---|:---|:---|
//! | [`CollectionMode::Push`] | `usage.publish` | none; the source sends a bounded batch |
//! | [`CollectionMode::Pull`] | `usage.query` | none; the host asks for the first bounded page |
//! | [`CollectionMode::Cursor`] | `usage.query` | the position the previous page returned |
//!
//! A cursor is opaque text the *host* issues and reads back. It names one
//! monotonic sequence within one source epoch, so a page can never be resumed
//! across a reset: an epoch change is a new series, and resuming an old cursor
//! into it would either skip work or count it twice. That is why
//! [`Cursor::check_epoch`] exists and why a stale cursor is a refusal the caller
//! reconciles rather than an empty page.

use licoup_extension_contracts::transport::Framing;
use licoup_extension_contracts::usage::UsageObservation;
use licoup_extension_contracts::{ApplicationFailure, RecoveryAction};
use serde_json::Value;

use crate::{refusal, refusal_with};

/// The most observations one push batch may carry.
pub const MAX_BATCH_OBSERVATIONS: usize = 256;

/// The most observations one query page may carry, and therefore the largest
/// `limit` a caller may ask for.
pub const MAX_QUERY_LIMIT: u32 = 500;

/// The limit a caller gets when it does not ask for one.
pub const DEFAULT_QUERY_LIMIT: u32 = 100;

/// The longest cursor text accepted.
pub const MAX_CURSOR_BYTES: usize = 256;

/// Bytes reserved for the JSON-RPC envelope (`jsonrpc`, `id`, `method`) when a
/// batch is checked against a negotiated frame bound.
///
/// A carrier that wants the exact figure measures the encoded frame; this is the
/// conservative constant that keeps "the payload fits" from being false by the
/// size of its own envelope.
pub const ENVELOPE_ALLOWANCE_BYTES: usize = 512;

/// How a source's observations are collected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionMode {
    /// The source sends bounded batches on its own schedule.
    Push,
    /// The host asks for one bounded page with no position.
    Pull,
    /// The host asks for one bounded page from a position a previous page
    /// returned.
    Cursor,
}

impl CollectionMode {
    pub const ALL: [Self; 3] = [Self::Push, Self::Pull, Self::Cursor];

    pub const fn id(self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Pull => "pull",
            Self::Cursor => "cursor",
        }
    }

    /// The profile method this shape is carried by.
    pub const fn method(self) -> &'static str {
        match self {
            Self::Push => licoup_extension_contracts::profile::METHOD_USAGE_PUBLISH,
            Self::Pull | Self::Cursor => licoup_extension_contracts::profile::METHOD_USAGE_QUERY,
        }
    }

    /// Whether this shape requires a cursor to be present.
    pub const fn requires_cursor(self) -> bool {
        matches!(self, Self::Cursor)
    }

    /// Whether this shape forbids a cursor.
    pub const fn forbids_cursor(self) -> bool {
        matches!(self, Self::Pull)
    }
}

/// A push batch: a bounded, non-empty list of observation payloads.
///
/// The payloads are still wire values, not parsed observations: they are bound
/// and validated by [`crate::binding::SourceBinding`], which is where the
/// transport-bound refusals live. A batch that is over the count bound is
/// refused here, before anything parses it.
#[derive(Clone, Debug, PartialEq)]
pub struct PublishBatch {
    observations: Vec<Value>,
}

impl PublishBatch {
    /// Build a batch, refusing an empty one and one over the count bound.
    ///
    /// An empty batch is refused rather than accepted as a no-op: a source that
    /// sends nothing is a source that should not have been asked, and accepting
    /// it would make "no observations" indistinguishable from "the batch was
    /// dropped".
    pub fn new(observations: Vec<Value>) -> Result<Self, ApplicationFailure> {
        if observations.is_empty() {
            return Err(refusal("usage_batch_empty").with_field("observations"));
        }
        if observations.len() > MAX_BATCH_OBSERVATIONS {
            return Err(refusal("usage_batch_oversize")
                .with_field("observations")
                .with_presentation_arg("limit", &MAX_BATCH_OBSERVATIONS.to_string()));
        }
        Ok(Self { observations })
    }

    pub fn observations(&self) -> &[Value] {
        &self.observations
    }

    pub fn len(&self) -> usize {
        self.observations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }

    /// The encoded size of the batch as sent.
    pub fn encoded_len(&self) -> Result<usize, ApplicationFailure> {
        serde_json::to_vec(&self.observations)
            .map(|encoded| encoded.len())
            .map_err(|_| refusal("usage_batch_unencodable").with_field("observations"))
    }

    /// Refuse a batch whose encoded frame would exceed the negotiated bound.
    ///
    /// The check includes [`ENVELOPE_ALLOWANCE_BYTES`], so a batch that passes
    /// here fits the carrier rather than merely fitting the carrier minus its
    /// own envelope.
    pub fn check_frame(&self, framing: &Framing) -> Result<usize, ApplicationFailure> {
        let encoded = self.encoded_len()?;
        let frame = encoded.saturating_add(ENVELOPE_ALLOWANCE_BYTES);
        framing.check_frame(frame)?;
        Ok(encoded)
    }
}

/// One bounded pull request: `usage.query(cursor, limit, scopeRef)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRequest {
    pub scope_ref: String,
    /// `None` for the first page ([`CollectionMode::Pull`]); `Some` to resume
    /// ([`CollectionMode::Cursor`]).
    pub cursor: Option<String>,
    pub limit: u32,
}

impl QueryRequest {
    /// Build a request with a default limit.
    pub fn new(
        scope_ref: impl Into<String>,
        cursor: Option<String>,
        limit: Option<u32>,
    ) -> Result<Self, ApplicationFailure> {
        let request = Self {
            scope_ref: scope_ref.into(),
            cursor,
            limit: limit.unwrap_or(DEFAULT_QUERY_LIMIT),
        };
        request.validate()?;
        Ok(request)
    }

    /// The collection shape this request uses.
    pub fn mode(&self) -> CollectionMode {
        match &self.cursor {
            Some(_) => CollectionMode::Cursor,
            None => CollectionMode::Pull,
        }
    }

    /// Structural validation: a scope, a limit within bounds, and a cursor that
    /// parses.
    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if self.scope_ref.is_empty() {
            return Err(refusal("usage_query_invalid").with_field("scopeRef"));
        }
        if self.limit == 0 || self.limit > MAX_QUERY_LIMIT {
            return Err(refusal("usage_query_invalid")
                .with_field("limit")
                .with_presentation_arg("limit", &MAX_QUERY_LIMIT.to_string()));
        }
        if let Some(cursor) = &self.cursor {
            Cursor::parse(cursor)?;
        }
        Ok(())
    }
}

/// One bounded page of observations, with the position of the next one.
///
/// A page states whether more remain and, exactly when it does, the cursor that
/// resumes after the last observation. `has_more` without a cursor is refused:
/// "there is more and I will not tell you where" is not a resumable answer.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryPage {
    /// The epoch the page was read from, as bound by the transport.
    pub source_epoch: String,
    /// The authorized scope the read was made under.
    pub scope_ref: String,
    pub observations: Vec<UsageObservation>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

impl QueryPage {
    pub fn empty(source_epoch: impl Into<String>, scope_ref: impl Into<String>) -> Self {
        Self {
            source_epoch: source_epoch.into(),
            scope_ref: scope_ref.into(),
            observations: Vec::new(),
            next_cursor: None,
            has_more: false,
        }
    }

    /// Structural validation, including the rule that a page may not carry an
    /// observation from another epoch or another scope.
    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if self.source_epoch.is_empty() {
            return Err(refusal("usage_page_invalid").with_field("sourceEpoch"));
        }
        if self.scope_ref.is_empty() {
            return Err(refusal("usage_page_invalid").with_field("scopeRef"));
        }
        if self.observations.len() > MAX_QUERY_LIMIT as usize {
            return Err(refusal("usage_page_invalid").with_field("observations"));
        }
        if self.has_more != self.next_cursor.is_some() {
            return Err(refusal("usage_page_invalid").with_field("nextCursor"));
        }
        for observation in &self.observations {
            if observation.source_epoch != self.source_epoch {
                return Err(refusal("usage_page_invalid").with_field("sourceEpoch"));
            }
            if observation.scope_ref != self.scope_ref {
                return Err(refusal("usage_page_invalid").with_field("scopeRef"));
            }
        }
        if let Some(cursor) = &self.next_cursor {
            let cursor = Cursor::parse(cursor)?;
            cursor.check_epoch(&self.source_epoch)?;
        }
        Ok(())
    }
}

/// An opaque resume position inside one source epoch.
///
/// The encoding is text the host issues and reads back; callers must treat it as
/// opaque and must not construct positions of their own. The sequence is
/// monotonic within the epoch, so a page read at a position never overlaps the
/// page before it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cursor {
    sequence: u64,
    source_epoch: String,
}

impl Cursor {
    /// Build a cursor. The epoch must be non-empty and the encoded text bounded.
    pub fn new(source_epoch: impl Into<String>, sequence: u64) -> Result<Self, ApplicationFailure> {
        let cursor = Self {
            sequence,
            source_epoch: source_epoch.into(),
        };
        cursor.check_encoded_len()?;
        Ok(cursor)
    }

    /// Parse a cursor that was issued by a host.
    pub fn parse(text: &str) -> Result<Self, ApplicationFailure> {
        if text.is_empty() || text.len() > MAX_CURSOR_BYTES {
            return Err(refusal("usage_cursor_invalid").with_field("cursor"));
        }
        let Some((sequence, source_epoch)) = text.split_once('@') else {
            return Err(refusal("usage_cursor_invalid").with_field("cursor"));
        };
        let sequence: u64 = sequence
            .parse()
            .map_err(|_| refusal("usage_cursor_invalid").with_field("cursor"))?;
        if source_epoch.is_empty() {
            return Err(refusal("usage_cursor_invalid").with_field("cursor"));
        }
        Ok(Self {
            sequence,
            source_epoch: source_epoch.to_owned(),
        })
    }

    pub fn encode(&self) -> String {
        format!("{}@{}", self.sequence, self.source_epoch)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn source_epoch(&self) -> &str {
        &self.source_epoch
    }

    /// The position after this one, for the page that ends here.
    pub fn next(&self) -> Result<Self, ApplicationFailure> {
        Self::new(
            self.source_epoch.clone(),
            self.sequence
                .checked_add(1)
                .ok_or_else(|| refusal("usage_cursor_exhausted").with_field("cursor"))?,
        )
    }

    /// Refuse a cursor whose epoch is not the current one.
    ///
    /// A source that reset starts a new epoch with new series. Resuming an old
    /// cursor there is not an empty read: it is a read of a series that no
    /// longer exists, and the caller reconciles instead of treating it as
    /// "nothing new".
    pub fn check_epoch(&self, current_epoch: &str) -> Result<(), ApplicationFailure> {
        if self.source_epoch == current_epoch {
            return Ok(());
        }
        Err(refusal_with(
            "usage_cursor_epoch_stale",
            RecoveryAction::ReconcileBeforeRetry,
        )
        .with_field("cursor")
        .with_presentation_arg("cursorEpoch", &self.source_epoch)
        .with_presentation_arg("currentEpoch", current_epoch))
    }

    fn check_encoded_len(&self) -> Result<(), ApplicationFailure> {
        if self.source_epoch.is_empty() || self.encode().len() > MAX_CURSOR_BYTES {
            return Err(refusal("usage_cursor_invalid").with_field("cursor"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(id: &str) -> Value {
        serde_json::json!({
            "schema": licoup_extension_contracts::wire::USAGE,
            "observationId": id,
            "revision": 1,
            "operation": "upsert",
            "sourceEpoch": "epoch-1",
            "scopeRef": "scope-1",
            "observedAt": "2026-09-21T00:00:00Z",
            "metrics": { "licoup.tokens.input": {
                "value": "1", "unit": "tokens", "temporality": "delta", "quality": "reported"
            }}
        })
    }

    #[test]
    fn a_batch_is_bounded_and_non_empty() {
        assert!(PublishBatch::new(Vec::new()).is_err());
        let full = PublishBatch::new(
            (0..MAX_BATCH_OBSERVATIONS)
                .map(|i| payload(&i.to_string()))
                .collect(),
        )
        .expect("at the bound");
        assert_eq!(full.len(), MAX_BATCH_OBSERVATIONS);
        let over = PublishBatch::new(
            (0..=MAX_BATCH_OBSERVATIONS)
                .map(|i| payload(&i.to_string()))
                .collect(),
        )
        .expect_err("over the bound");
        assert_eq!(over.code, "usage_batch_oversize");
    }

    #[test]
    fn a_batch_is_checked_against_the_negotiated_frame() {
        let batch = PublishBatch::new(
            (0..64)
                .map(|index| payload(&format!("obs-{index}-{}", "x".repeat(120))))
                .collect(),
        )
        .expect("batch");
        let encoded = batch.encoded_len().expect("encoded");
        assert!(
            encoded + ENVELOPE_ALLOWANCE_BYTES
                > Framing::new(4 * 1024, 4 * 1024).negotiated_max_frame_bytes(),
            "the batch must be large enough to exceed the smallest negotiated bound"
        );
        let roomy = Framing::new(64 * 1024, 64 * 1024);
        assert!(batch.check_frame(&roomy).is_ok());
        let tight = Framing::new(4 * 1024, 4 * 1024);
        assert_eq!(
            batch.check_frame(&tight).expect_err("over bound").code,
            "transport_frame_oversize"
        );
    }

    #[test]
    fn pull_and_cursor_differ_only_by_the_position() {
        let first = QueryRequest::new("scope-1", None, None).expect("first page");
        assert_eq!(first.mode(), CollectionMode::Pull);
        assert_eq!(first.limit, DEFAULT_QUERY_LIMIT);
        assert_eq!(first.mode().method(), "usage.query");

        let position = Cursor::new("epoch-1", 7).expect("cursor").encode();
        let resumed =
            QueryRequest::new("scope-1", Some(position.clone()), Some(10)).expect("resume");
        assert_eq!(resumed.mode(), CollectionMode::Cursor);
        assert!(CollectionMode::Cursor.requires_cursor());
        assert!(CollectionMode::Pull.forbids_cursor());
        assert_eq!(Cursor::parse(&position).expect("parsed").sequence(), 7);
        assert_eq!(
            Cursor::parse(&position).expect("parsed").source_epoch(),
            "epoch-1"
        );
    }

    #[test]
    fn a_cursor_is_opaque_text_and_is_refused_across_epochs() {
        for bad in [
            "",
            "7",
            "@epoch",
            "x@epoch",
            "7@",
            "7@epoch@".repeat(40).as_str(),
        ] {
            assert!(
                Cursor::parse(bad).is_err(),
                "{bad} is not a cursor the host issued"
            );
        }
        let cursor = Cursor::new("epoch-2", 3).expect("cursor");
        assert!(cursor.check_epoch("epoch-2").is_ok());
        let failure = cursor.check_epoch("epoch-3").expect_err("stale epoch");
        assert_eq!(failure.code, "usage_cursor_epoch_stale");
        assert_eq!(failure.recovery, RecoveryAction::ReconcileBeforeRetry);
    }

    #[test]
    fn a_page_that_has_more_must_say_where_to_resume() {
        let mut page = QueryPage::empty("epoch-1", "scope-1");
        assert!(page.validate().is_ok());

        page.has_more = true;
        assert_eq!(
            page.validate().expect_err("no cursor").field.as_deref(),
            Some("nextCursor")
        );

        page.next_cursor = Some(Cursor::new("epoch-2", 1).expect("cursor").encode());
        assert_eq!(
            page.validate().expect_err("other epoch").code,
            "usage_cursor_epoch_stale"
        );

        page.next_cursor = Some(Cursor::new("epoch-1", 1).expect("cursor").encode());
        assert!(page.validate().is_ok());
    }

    #[test]
    fn a_page_may_not_smuggle_another_scope_or_epoch() {
        let observation =
            licoup_extension_contracts::usage::UsageObservation::from_value(payload("obs-1"))
                .expect("observation");
        let mut page = QueryPage::empty("epoch-1", "scope-1");
        page.observations = vec![observation.clone()];
        assert!(page.validate().is_ok());

        page.scope_ref = "scope-2".to_owned();
        assert_eq!(
            page.validate().expect_err("other scope").field.as_deref(),
            Some("scopeRef")
        );

        page.scope_ref = "scope-1".to_owned();
        page.source_epoch = "epoch-2".to_owned();
        assert_eq!(
            page.validate().expect_err("other epoch").field.as_deref(),
            Some("sourceEpoch")
        );
    }

    #[test]
    fn a_query_limit_is_bounded() {
        assert!(QueryRequest::new("scope-1", None, Some(0)).is_err());
        assert!(QueryRequest::new("scope-1", None, Some(MAX_QUERY_LIMIT + 1)).is_err());
        assert!(QueryRequest::new("", None, None).is_err());
    }
}
