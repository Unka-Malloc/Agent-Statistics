//! The host-side binding: where a payload stops being a claim about itself.
//!
//! C11 says the source is bound by the transport and never self-declared. This
//! module is that rule made concrete: a [`SourceBinding`] is the set of facts the
//! host already knows when it reads from one instance of one extension — the
//! source identity, the extension and instance, the generation, the current
//! source epoch, and the scopes it has authorized — and every input shape
//! (pushed batch, pulled page, normalized vendor record) is mapped through it to
//! exactly one [`BoundObservation`].
//!
//! The refusals are the point:
//!
//! - A payload carrying `source`, `extensionId`, `instanceId`, a generation, a
//!   registry epoch or an authority field is refused before it is parsed, so no
//!   producer can attach itself to another user's run.
//! - A payload carrying a logical measurement identity is refused too: whether
//!   two reports are two observations of one call or two calls is the host's
//!   fact, and letting the producer choose it would let it merge or split its own
//!   charges.
//! - A payload whose `sourceEpoch` is not the epoch bound at the transport is
//!   refused: a producer that could name an old epoch would evade deduplication
//!   by replaying into a series that is no longer current.
//! - A payload whose `scopeRef` is not one the host issued is refused: an
//!   observation may only be attached to an invocation or an aggregate range the
//!   user's work is already under.

use licoup_extension_contracts::usage::{UsageObservation, self_asserted_binding_field};
use licoup_extension_contracts::{ApplicationFailure, is_namespaced};
use serde_json::Value;

use crate::collection::PublishBatch;
use crate::{refusal, refusal_with};

/// The longest host-issued source identity accepted.
pub const MAX_SOURCE_REF_BYTES: usize = 160;

/// The longest host-issued measurement identity accepted.
pub const MAX_MEASUREMENT_REF_BYTES: usize = 160;

/// Payload fields that assert the host's correlation fact.
///
/// They are refused by name rather than ignored, because ignoring one would
/// silently merge or split a measurement without telling anyone which rule was
/// applied.
pub const MEASUREMENT_BOUND_FIELDS: &[&str] =
    &["measurement", "measurementref", "logicalmeasurement"];

/// The host's facts about one source instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceBinding {
    /// The host-issued source identity. This is the identity observations are
    /// deduplicated by; it is not the extension id, because one extension may
    /// run several instances with different grants.
    pub source_ref: String,
    /// The namespaced extension the transport is attached to.
    pub extension_id: String,
    /// The instance the read comes from.
    pub instance_id: String,
    /// The generation of that instance, so a replaced instance is a new source.
    pub generation: u64,
    /// The current epoch of this source. A reset starts a new one.
    pub source_epoch: String,
    /// The scopes the host issued to this source: invocations and authorized
    /// aggregate ranges.
    pub scope_grants: Vec<String>,
}

impl SourceBinding {
    pub fn new(
        source_ref: impl Into<String>,
        extension_id: impl Into<String>,
        instance_id: impl Into<String>,
        generation: u64,
        source_epoch: impl Into<String>,
        scope_grants: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, ApplicationFailure> {
        let binding = Self {
            source_ref: source_ref.into(),
            extension_id: extension_id.into(),
            instance_id: instance_id.into(),
            generation,
            source_epoch: source_epoch.into(),
            scope_grants: scope_grants.into_iter().map(Into::into).collect(),
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if self.source_ref.is_empty() || self.source_ref.len() > MAX_SOURCE_REF_BYTES {
            return Err(refusal("usage_binding_invalid").with_field("sourceRef"));
        }
        if !is_namespaced(&self.extension_id) {
            return Err(refusal("usage_binding_invalid").with_field("extensionId"));
        }
        if self.instance_id.is_empty() || self.generation < 1 || self.source_epoch.is_empty() {
            return Err(refusal("usage_binding_invalid").with_field("instanceId"));
        }
        if self.scope_grants.is_empty() {
            return Err(refusal("usage_binding_invalid").with_field("scopeGrants"));
        }
        Ok(())
    }

    /// Whether the host issued this scope to this source.
    pub fn authorizes_scope(&self, scope_ref: &str) -> bool {
        self.scope_grants.iter().any(|grant| grant == scope_ref)
    }

    /// Bind a pushed or pulled wire payload.
    ///
    /// This is the single mapping every collection direction goes through: the
    /// payload is checked for self-asserted binding facts, parsed into the C11
    /// observation, and then checked against the transport-bound epoch and the
    /// authorized scopes.
    pub fn bind_value(
        &self,
        value: Value,
        measurement_ref: Option<&str>,
    ) -> Result<BoundObservation, ApplicationFailure> {
        if let Some(field) = self_asserted_binding_field(&value) {
            return Err(refusal("usage_source_self_asserted").with_field(&field));
        }
        if let Some(field) = measurement_bound_field(&value) {
            return Err(refusal("usage_source_self_asserted").with_field(&field));
        }
        let observation = UsageObservation::from_value(value)?;
        self.bind_observation(observation, measurement_ref)
    }

    /// Bind an observation a boundary adapter already constructed.
    ///
    /// The adapter owns the vendor-to-C11 mapping; the host still owns the epoch
    /// and the scope, so both are checked here exactly as for a pushed payload,
    /// and the observation's own structure is validated on the same path.
    pub fn bind_observation(
        &self,
        observation: UsageObservation,
        measurement_ref: Option<&str>,
    ) -> Result<BoundObservation, ApplicationFailure> {
        observation.validate()?;
        if observation.source_epoch != self.source_epoch {
            return Err(refusal_with(
                "usage_source_epoch_mismatch",
                licoup_extension_contracts::RecoveryAction::ReconcileBeforeRetry,
            )
            .with_field("sourceEpoch")
            .with_presentation_arg("boundEpoch", &self.source_epoch));
        }
        if !self.authorizes_scope(&observation.scope_ref) {
            return Err(refusal("usage_scope_not_authorized").with_field("scopeRef"));
        }
        if let Some(measurement_ref) = measurement_ref
            && (measurement_ref.is_empty() || measurement_ref.len() > MAX_MEASUREMENT_REF_BYTES)
        {
            return Err(refusal("usage_binding_invalid").with_field("measurementRef"));
        }
        Ok(BoundObservation {
            binding: self.clone(),
            measurement_ref: measurement_ref.map(str::to_owned),
            observation,
        })
    }

    /// Bind every payload in a bounded batch.
    ///
    /// One malformed observation refuses itself and not its neighbours: a batch
    /// is a delivery unit, and losing twenty good observations because the
    /// twenty-first is wrong would make a producer's partial failure a data
    /// loss.
    pub fn admit_batch(&self, batch: &PublishBatch) -> BatchAdmission {
        let mut admission = BatchAdmission::default();
        for (index, value) in batch.observations().iter().enumerate() {
            match self.bind_value(value.clone(), None) {
                Ok(bound) => admission.accepted.push(bound),
                Err(failure) => admission.refused.push(BatchRefusal { index, failure }),
            }
        }
        admission
    }
}

/// One refused observation inside an otherwise accepted batch.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchRefusal {
    /// The position in the batch, so the producer can point at the record.
    pub index: usize,
    pub failure: ApplicationFailure,
}

/// The result of binding a batch: what was accepted and what was refused.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchAdmission {
    pub accepted: Vec<BoundObservation>,
    pub refused: Vec<BatchRefusal>,
}

impl BatchAdmission {
    pub fn is_complete(&self) -> bool {
        self.refused.is_empty()
    }

    pub fn accepted_count(&self) -> usize {
        self.accepted.len()
    }
}

/// The identity a bound observation is deduplicated by on the consumer's side.
///
/// The source is the host-bound one, not anything the payload said, so the same
/// observation id from two sources stays two observations and a replayed payload
/// from the same source stays one.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BoundObservationKey {
    pub source_ref: String,
    pub source_epoch: String,
    pub observation_id: String,
}

/// One observation with the host's binding attached.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundObservation {
    pub binding: SourceBinding,
    /// The host-issued logical measurement, when the host knows that several
    /// sources are reporting one call. `None` means the host does not know, and
    /// the consumer must not guess.
    pub measurement_ref: Option<String>,
    pub observation: UsageObservation,
}

impl BoundObservation {
    /// The deduplication key: host-bound source, host-confirmed epoch,
    /// observation id.
    pub fn key(&self) -> BoundObservationKey {
        BoundObservationKey {
            source_ref: self.binding.source_ref.clone(),
            source_epoch: self.binding.source_epoch.clone(),
            observation_id: self.observation.observation_id.clone(),
        }
    }

    pub fn revision(&self) -> u32 {
        self.observation.revision
    }

    pub fn scope_ref(&self) -> &str {
        &self.observation.scope_ref
    }

    pub fn source_ref(&self) -> &str {
        &self.binding.source_ref
    }
}

/// The dotted path of the first payload field that asserts a correlation fact,
/// if any.
pub fn measurement_bound_field(value: &Value) -> Option<String> {
    fn walk(value: &Value, path: &str, found: &mut Option<String>) {
        if found.is_some() {
            return;
        }
        match value {
            Value::Object(map) => {
                for (key, nested) in map {
                    let normalized: String = key
                        .chars()
                        .filter(|character| *character != '_' && *character != '-')
                        .flat_map(char::to_lowercase)
                        .collect();
                    if MEASUREMENT_BOUND_FIELDS.contains(&normalized.as_str()) {
                        *found = Some(format!("{path}{key}"));
                        return;
                    }
                    walk(nested, &format!("{path}{key}."), found);
                }
            }
            Value::Array(items) => {
                for (index, nested) in items.iter().enumerate() {
                    walk(nested, &format!("{path}{index}."), found);
                }
            }
            _ => {}
        }
    }

    let mut found = None;
    walk(value, "", &mut found);
    found
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
                "value": "120", "unit": "tokens", "temporality": "delta", "quality": "reported"
            }}
        })
    }

    fn binding() -> SourceBinding {
        SourceBinding::new(
            "source:example.analytics#1",
            "example.analytics",
            "instance-1",
            3,
            "epoch-1",
            ["scope-1"],
        )
        .expect("binding")
    }

    #[test]
    fn every_direction_converges_on_one_bound_value() {
        let bound = binding().bind_value(payload("obs-1"), None).expect("bound");
        assert_eq!(bound.key().source_ref, "source:example.analytics#1");
        assert_eq!(bound.key().source_epoch, "epoch-1");
        assert_eq!(bound.key().observation_id, "obs-1");
        assert_eq!(bound.source_ref(), bound.binding.source_ref);
        assert_eq!(bound.revision(), 1);
        assert!(bound.measurement_ref.is_none());
    }

    #[test]
    fn a_payload_cannot_assert_the_binding_or_the_measurement() {
        for field in [
            "extensionId",
            "instanceId",
            "source",
            "generation",
            "registryEpoch",
        ] {
            let mut wire = payload("obs-1");
            wire[field] = Value::String("someone.else".to_owned());
            let failure = binding()
                .bind_value(wire, None)
                .expect_err("self-asserted binding");
            assert_eq!(failure.code, "usage_source_self_asserted");
        }
        for field in ["measurementRef", "measurement", "logicalMeasurement"] {
            let mut wire = payload("obs-1");
            wire[field] = Value::String("call-1".to_owned());
            let failure = binding()
                .bind_value(wire, None)
                .expect_err("self-asserted measurement");
            assert_eq!(failure.code, "usage_source_self_asserted");
            assert_eq!(failure.field.as_deref(), Some(field));
        }
    }

    #[test]
    fn a_payload_cannot_choose_its_epoch_or_its_scope() {
        let mut wire = payload("obs-1");
        wire["sourceEpoch"] = Value::String("epoch-0".to_owned());
        assert_eq!(
            binding()
                .bind_value(wire, None)
                .expect_err("other epoch")
                .code,
            "usage_source_epoch_mismatch"
        );

        let mut wire = payload("obs-1");
        wire["scopeRef"] = Value::String("scope-other".to_owned());
        assert_eq!(
            binding()
                .bind_value(wire, None)
                .expect_err("other scope")
                .code,
            "usage_scope_not_authorized"
        );
    }

    #[test]
    fn a_batch_refuses_one_observation_without_losing_its_neighbours() {
        let batch = PublishBatch::new(vec![
            payload("obs-1"),
            {
                let mut bad = payload("obs-2");
                bad["extensionId"] = Value::String("someone.else".to_owned());
                bad
            },
            payload("obs-3"),
        ])
        .expect("batch");
        let admission = binding().admit_batch(&batch);
        assert_eq!(admission.accepted_count(), 2);
        assert_eq!(admission.refused.len(), 1);
        assert_eq!(admission.refused[0].index, 1);
        assert_eq!(
            admission.refused[0].failure.code,
            "usage_source_self_asserted"
        );
        assert!(!admission.is_complete());
    }

    #[test]
    fn a_binding_without_grants_or_identity_is_refused() {
        assert!(
            SourceBinding::new(
                "source-1",
                "example.analytics",
                "i",
                1,
                "e",
                Vec::<String>::new()
            )
            .is_err()
        );
        assert!(SourceBinding::new("source-1", "bareword", "i", 1, "e", ["s"]).is_err());
        assert!(SourceBinding::new("source-1", "example.analytics", "", 1, "e", ["s"]).is_err());
        assert!(SourceBinding::new("source-1", "example.analytics", "i", 0, "e", ["s"]).is_err());
        assert!(SourceBinding::new("", "example.analytics", "i", 1, "e", ["s"]).is_err());
    }

    #[test]
    fn a_host_issued_measurement_is_carried_beside_the_observation() {
        let bound = binding()
            .bind_value(payload("obs-1"), Some("call-42"))
            .expect("bound");
        assert_eq!(bound.measurement_ref.as_deref(), Some("call-42"));
        assert!(
            binding().bind_value(payload("obs-1"), Some("")).is_err(),
            "an empty measurement is not a measurement"
        );
    }
}
