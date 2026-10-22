//! Boundary normalization: vendor-shaped records become C11 observations.
//!
//! C11 puts the adaptation at the boundary — a log, an HTTP payload or an
//! OpenTelemetry-shaped metric is standardized where it arrives, and the panels
//! never parse a vendor's own file. This module is that boundary:
//!
//! - [`FieldMapping`] maps an ordinary JSON record through explicit paths to one
//!   observation. Nothing is inferred from a field name, a missing mapped field
//!   is *absent* rather than zero, and a mapped field that carries something
//!   which is not a number is refused rather than dropped.
//! - [`parse_key_value_line`] turns one log line of `key=value` pairs into the
//!   object a [`FieldMapping`] reads, so a log adapter is a mapping plus a
//!   parser rather than a second protocol.
//! - [`OtlpSumNormalizer`] maps the sum-and-gauge subset of an
//!   OpenTelemetry-shaped metric into one observation per data point. A
//!   histogram or a quantile is refused by name: turning a distribution into a
//!   scalar is a claim the data does not support, and averaging sampled
//!   quantiles across sources is wrong for the same reason.
//!
//! All three produce the same [`BoundObservation`] through the same
//! [`SourceBinding`], so a normalized record cannot bypass the transport-bound
//! epoch or the authorized scope.

use licoup_extension_contracts::usage::{
    CostObservation, MetricValue, Quality, Temporality, UsageObservation, UsageOperation,
};
use licoup_extension_contracts::{ApplicationFailure, is_namespaced};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::binding::{BoundObservation, SourceBinding};
use crate::refusal;

/// The most points one OTLP-shaped record may carry, so one record cannot
/// become an unbounded batch.
pub const MAX_OTLP_POINTS: usize = 512;

/// The longest observation id a normalizer will derive.
pub const MAX_DERIVED_OBSERVATION_ID_BYTES: usize = 160;

/// How one mapped field becomes a metric reading.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricFieldMapping {
    /// The namespaced metric this field is reported as.
    pub metric: String,
    pub unit: String,
    pub temporality: Temporality,
    /// What the source says about the value. `Unknown` means the field is
    /// declared but not claimed: no number is read from the record at all.
    pub quality: Quality,
}

impl MetricFieldMapping {
    pub fn new(
        metric: impl Into<String>,
        unit: impl Into<String>,
        temporality: Temporality,
        quality: Quality,
    ) -> Self {
        Self {
            metric: metric.into(),
            unit: unit.into(),
            temporality,
            quality,
        }
    }
}

/// How one record's cost becomes a [`CostObservation`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CostFieldMapping {
    /// Path to the exact decimal amount.
    pub amount: String,
    /// Path to the ISO 4217 currency code.
    pub currency: String,
    pub quality: Quality,
}

/// A metric the record explicitly does not know.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownMetric {
    pub metric: String,
    pub unit: String,
    pub temporality: Temporality,
}

impl UnknownMetric {
    pub fn new(
        metric: impl Into<String>,
        unit: impl Into<String>,
        temporality: Temporality,
    ) -> Self {
        Self {
            metric: metric.into(),
            unit: unit.into(),
            temporality,
        }
    }
}

/// An explicit mapping from one record shape to one observation.
///
/// Paths are dotted object paths. A path that would have to traverse an array is
/// refused rather than guessed: "the third element" is not a mapping, and a
/// mapping that quietly picked one would be reading a different document than
/// the author wrote.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldMapping {
    /// Path to the observation identity.
    pub observation_id: String,
    /// Path to a correction revision. Defaults to 1.
    pub revision: Option<String>,
    /// Path to `upsert` or `retract`. Defaults to `upsert`.
    pub operation: Option<String>,
    /// Path to the observation time.
    pub observed_at: String,
    /// Path to the window or cumulative-series origin.
    pub interval_start: Option<String>,
    /// Path to the scope. `None` means the binding's single granted scope.
    pub scope_ref: Option<String>,
    /// Record path to metric reading.
    pub metrics: BTreeMap<String, MetricFieldMapping>,
    /// Metrics this record reports as explicitly unknown.
    pub unknown_metrics: Vec<UnknownMetric>,
    pub cost: Option<CostFieldMapping>,
}

impl FieldMapping {
    /// Map one record, or return `None` when it carries nothing the mapping
    /// claims.
    ///
    /// `None` is not an error: a specialist record that reports items and
    /// duration simply has no token fields, and inventing zero for them would be
    /// a claim that no tokens were used.
    pub fn normalize(
        &self,
        record: &Value,
        binding: &SourceBinding,
        measurement_ref: Option<&str>,
    ) -> Result<Option<BoundObservation>, ApplicationFailure> {
        let operation =
            match self.optional_string(record, self.operation.as_deref(), "operation")? {
                Some("retract") => UsageOperation::Retract,
                Some("upsert") | None => UsageOperation::Upsert,
                Some(_) => {
                    return Err(refusal("usage_mapping_invalid").with_field("operation"));
                }
            };
        let Some(observation_id) =
            self.required_string(record, &self.observation_id, "observationId")?
        else {
            return Ok(None);
        };
        let Some(observed_at) = self.required_string(record, &self.observed_at, "observedAt")?
        else {
            return Ok(None);
        };
        let revision = match self.revision.as_deref() {
            None => 1,
            Some(path) => match read_path(record, path)
                .map_err(|_| refusal("usage_mapping_invalid").with_field("revision"))?
            {
                None | Some(Value::Null) => 1,
                Some(Value::String(text)) => text
                    .parse::<u32>()
                    .ok()
                    .filter(|revision| *revision >= 1)
                    .ok_or_else(|| refusal("usage_mapping_invalid").with_field("revision"))?,
                Some(Value::Number(number)) => number
                    .as_u64()
                    .and_then(|revision| u32::try_from(revision).ok())
                    .filter(|revision| *revision >= 1)
                    .ok_or_else(|| refusal("usage_mapping_invalid").with_field("revision"))?,
                Some(_) => {
                    return Err(refusal("usage_mapping_invalid").with_field("revision"));
                }
            },
        };
        let interval_start = self
            .optional_string(record, self.interval_start.as_deref(), "intervalStart")?
            .map(str::to_owned);
        let scope_ref = match self.scope_ref.as_deref() {
            Some(path) => self
                .required_string(record, path, "scopeRef")?
                .ok_or_else(|| refusal("usage_mapping_invalid").with_field("scopeRef"))?,
            None => match binding.scope_grants.as_slice() {
                [only] => only.clone(),
                _ => return Err(refusal("usage_mapping_invalid").with_field("scopeRef")),
            },
        };

        let mut metrics = BTreeMap::new();
        for (path, mapping) in &self.metrics {
            if !is_namespaced(&mapping.metric) || mapping.unit.is_empty() {
                return Err(refusal("usage_mapping_invalid").with_field("metrics.metric"));
            }
            if mapping.quality == Quality::Unknown {
                metrics.insert(
                    mapping.metric.clone(),
                    MetricValue::unknown(mapping.unit.clone(), mapping.temporality),
                );
                continue;
            }
            let Some(raw) = read_path(record, path)
                .map_err(|_| refusal("usage_mapping_invalid").with_field("metrics.path"))?
            else {
                continue;
            };
            let reading = match raw {
                Value::Null => MetricValue::unknown(mapping.unit.clone(), mapping.temporality),
                Value::String(text) => reported_with(
                    text.clone(),
                    &mapping.unit,
                    mapping.temporality,
                    mapping.quality,
                ),
                Value::Number(number) => reported_with(
                    number.to_string(),
                    &mapping.unit,
                    mapping.temporality,
                    mapping.quality,
                ),
                _ => {
                    return Err(refusal("usage_metric_invalid").with_field("metrics.value"));
                }
            };
            metrics.insert(mapping.metric.clone(), reading);
        }
        for unknown in &self.unknown_metrics {
            if !is_namespaced(&unknown.metric) || unknown.unit.is_empty() {
                return Err(refusal("usage_mapping_invalid").with_field("unknownMetrics"));
            }
            metrics.insert(
                unknown.metric.clone(),
                MetricValue::unknown(unknown.unit.clone(), unknown.temporality),
            );
        }

        let cost = match &self.cost {
            None => None,
            Some(mapping) => {
                let amount = read_path(record, &mapping.amount)
                    .map_err(|_| refusal("usage_mapping_invalid").with_field("cost.amount"))?;
                let currency = read_path(record, &mapping.currency)
                    .map_err(|_| refusal("usage_mapping_invalid").with_field("cost.currency"))?;
                match (amount, currency) {
                    (None, _) | (_, None) => None,
                    (Some(amount), Some(currency)) => {
                        let currency = currency.as_str().ok_or_else(|| {
                            refusal("usage_cost_invalid").with_field("cost.currency")
                        })?;
                        let amount = match amount {
                            Value::Null if mapping.quality == Quality::Unknown => None,
                            Value::Null => {
                                return Err(refusal("usage_cost_invalid").with_field("cost.amount"));
                            }
                            Value::String(text) => Some(text.clone()),
                            Value::Number(number) => Some(number.to_string()),
                            _ => {
                                return Err(refusal("usage_cost_invalid").with_field("cost.amount"));
                            }
                        };
                        let quality = if amount.is_none() {
                            Quality::Unknown
                        } else {
                            mapping.quality
                        };
                        Some(CostObservation {
                            amount,
                            currency: currency.to_owned(),
                            quality,
                        })
                    }
                }
            }
        };

        if operation == UsageOperation::Upsert && metrics.is_empty() {
            return Ok(None);
        }
        let observation = UsageObservation {
            schema: licoup_extension_contracts::wire::USAGE.to_owned(),
            observation_id,
            revision,
            operation,
            source_epoch: binding.source_epoch.clone(),
            scope_ref,
            observed_at,
            interval_start,
            metrics,
            cost: if operation == UsageOperation::Retract {
                None
            } else {
                cost
            },
        };
        binding
            .bind_observation(observation, measurement_ref)
            .map(Some)
    }

    fn required_string(
        &self,
        record: &Value,
        path: &str,
        field: &str,
    ) -> Result<Option<String>, ApplicationFailure> {
        match read_path(record, path)
            .map_err(|_| refusal("usage_mapping_invalid").with_field(field))?
        {
            None => Ok(None),
            Some(Value::String(text)) if !text.is_empty() => Ok(Some(text.clone())),
            Some(_) => Err(refusal("usage_mapping_invalid").with_field(field)),
        }
    }

    fn optional_string<'a>(
        &self,
        record: &'a Value,
        path: Option<&str>,
        field: &str,
    ) -> Result<Option<&'a str>, ApplicationFailure> {
        let Some(path) = path else {
            return Ok(None);
        };
        match read_path(record, path)
            .map_err(|_| refusal("usage_mapping_invalid").with_field(field))?
        {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.as_str())),
            Some(_) => Err(refusal("usage_mapping_invalid").with_field(field)),
        }
    }
}

/// A reading that carries a value and the quality the mapping declared.
///
/// A free function rather than a method because [`MetricValue`] belongs to the
/// contract crate: this SDK maps inputs onto the contract, it does not extend
/// it.
fn reported_with(
    value: impl Into<String>,
    unit: impl Into<String>,
    temporality: Temporality,
    quality: Quality,
) -> MetricValue {
    MetricValue {
        value: Some(value.into()),
        unit: unit.into(),
        temporality,
        quality,
        includes: Vec::new(),
    }
}

/// Read one dotted object path.
///
/// `Ok(None)` is a missing field; `Err(())` is a path that traverses an array,
/// which the mapping does not support and does not guess at.
fn read_path<'a>(record: &'a Value, path: &str) -> Result<Option<&'a Value>, ()> {
    if path.is_empty() {
        return Err(());
    }
    let mut current = record;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => match map.get(segment) {
                Some(next) => current = next,
                None => return Ok(None),
            },
            Value::Array(_) => return Err(()),
            _ => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// Turn one log line of `key=value` pairs into the object a [`FieldMapping`]
/// reads.
///
/// A quoted value keeps its text; an unquoted integer or decimal becomes a JSON
/// number; anything else stays a string. A line with no pairs, a pair with an
/// empty key, or a repeated key is refused with `None`, because a log adapter
/// that guessed which of two `tokens=` fields counted would be inventing a
/// reading.
pub fn parse_key_value_line(line: &str) -> Option<Value> {
    let mut map = serde_json::Map::new();
    let mut pairs = 0;
    for token in line.split_whitespace() {
        let (key, raw) = token.split_once('=')?;
        if key.is_empty() || map.contains_key(key) {
            return None;
        }
        let value = if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
            Value::String(raw[1..raw.len() - 1].to_owned())
        } else if let Ok(integer) = raw.parse::<i64>() {
            Value::Number(integer.into())
        } else if let Ok(unsigned) = raw.parse::<u64>() {
            Value::Number(unsigned.into())
        } else if let Ok(decimal) = raw.parse::<f64>() {
            serde_json::Number::from_f64(decimal).map(Value::Number)?
        } else {
            Value::String(raw.to_owned())
        };
        map.insert(key.to_owned(), value);
        pairs += 1;
    }
    if pairs == 0 {
        return None;
    }
    Some(Value::Object(map))
}

/// The sum-and-gauge subset of an OpenTelemetry-shaped metric record.
///
/// The record is one metric object with `name`, `unit`, `kind`
/// (`sum` or `gauge`), `temporality` (`delta` or `cumulative`) and `points`
/// carrying `value`, `start` and `time`. It is a boundary shape, not a claim of
/// full OpenTelemetry support: a histogram or a quantile is refused by name, and
/// the real protocol adapter is a packaging concern.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpSumNormalizer {
    /// The namespace vendor metric names are mapped into.
    pub metric_namespace: String,
    /// What the adapter says about the values it read.
    pub quality: Quality,
}

impl OtlpSumNormalizer {
    pub fn new(
        metric_namespace: impl Into<String>,
        quality: Quality,
    ) -> Result<Self, ApplicationFailure> {
        let normalizer = Self {
            metric_namespace: metric_namespace.into(),
            quality,
        };
        if !is_namespaced(&normalizer.metric_namespace) {
            return Err(refusal("usage_mapping_invalid").with_field("metricNamespace"));
        }
        Ok(normalizer)
    }

    /// Map every data point of one record into an observation.
    ///
    /// The observation id is derived from the series origin and the point time,
    /// so a replay of the same point deduplicates while a later point of the
    /// same series is a new reading. A cumulative point keeps its `start` as the
    /// series origin: when a producer restarts and the start changes, the
    /// consumer sees a new origin instead of a negative amount of work.
    pub fn normalize(
        &self,
        record: &Value,
        binding: &SourceBinding,
    ) -> Result<Vec<BoundObservation>, ApplicationFailure> {
        let name = record
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| refusal("usage_mapping_invalid").with_field("name"))?;
        let unit = record
            .get("unit")
            .and_then(Value::as_str)
            .filter(|unit| !unit.is_empty())
            .ok_or_else(|| refusal("usage_mapping_invalid").with_field("unit"))?;
        let kind = record.get("kind").and_then(Value::as_str).unwrap_or("sum");
        if kind != "sum" && kind != "gauge" {
            return Err(refusal("usage_aggregation_unsupported").with_field("kind"));
        }
        let metric = format!("{}/{}", self.metric_namespace, name);
        if !is_namespaced(&metric) {
            return Err(refusal("usage_mapping_invalid").with_field("name"));
        }
        let temporality = match kind {
            "gauge" => Temporality::Gauge,
            _ => match record.get("temporality").and_then(Value::as_str) {
                Some("delta") => Temporality::Delta,
                Some("cumulative") => Temporality::Cumulative,
                _ => {
                    return Err(refusal("usage_mapping_invalid").with_field("temporality"));
                }
            },
        };
        let points = record
            .get("points")
            .and_then(Value::as_array)
            .ok_or_else(|| refusal("usage_mapping_invalid").with_field("points"))?;
        if points.len() > MAX_OTLP_POINTS {
            return Err(refusal("usage_mapping_invalid")
                .with_field("points")
                .with_presentation_arg("limit", &MAX_OTLP_POINTS.to_string()));
        }
        let mut bound = Vec::with_capacity(points.len());
        for point in points {
            let time = point
                .get("time")
                .and_then(Value::as_str)
                .filter(|time| !time.is_empty())
                .ok_or_else(|| refusal("usage_mapping_invalid").with_field("points.time"))?;
            let start = point.get("start").and_then(Value::as_str);
            if temporality == Temporality::Cumulative && start.is_none() {
                return Err(refusal("usage_mapping_invalid").with_field("points.start"));
            }
            let value = match point.get("value") {
                None | Some(Value::Null) => MetricValue::unknown(unit.to_owned(), temporality),
                Some(Value::String(text)) => {
                    reported_with(text.clone(), unit, temporality, self.quality)
                }
                Some(Value::Number(number)) => {
                    reported_with(number.to_string(), unit, temporality, self.quality)
                }
                Some(_) => {
                    return Err(refusal("usage_metric_invalid").with_field("points.value"));
                }
            };
            let observation_id = match start {
                Some(start) => format!("{metric}@{start}@{time}"),
                None => format!("{metric}@{time}"),
            };
            if observation_id.len() > MAX_DERIVED_OBSERVATION_ID_BYTES {
                return Err(refusal("usage_mapping_invalid").with_field("observationId"));
            }
            let observation = UsageObservation {
                schema: licoup_extension_contracts::wire::USAGE.to_owned(),
                observation_id,
                revision: 1,
                operation: UsageOperation::Upsert,
                source_epoch: binding.source_epoch.clone(),
                scope_ref: match binding.scope_grants.as_slice() {
                    [only] => only.clone(),
                    _ => return Err(refusal("usage_mapping_invalid").with_field("scopeRef")),
                },
                observed_at: time.to_owned(),
                interval_start: if temporality == Temporality::Cumulative {
                    start.map(str::to_owned)
                } else {
                    None
                },
                metrics: BTreeMap::from([(metric.clone(), value)]),
                cost: None,
            };
            bound.push(binding.bind_observation(observation, None)?);
        }
        Ok(bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> SourceBinding {
        SourceBinding::new(
            "source:example.specialist#1",
            "example.specialist",
            "instance-1",
            1,
            "epoch-1",
            ["scope-1"],
        )
        .expect("binding")
    }

    fn specialist_mapping() -> FieldMapping {
        FieldMapping {
            observation_id: "id".to_owned(),
            revision: Some("revision".to_owned()),
            operation: None,
            observed_at: "at".to_owned(),
            interval_start: None,
            scope_ref: None,
            metrics: BTreeMap::from([
                (
                    "items".to_owned(),
                    MetricFieldMapping::new(
                        "example.specialist/items",
                        "items",
                        Temporality::Delta,
                        Quality::Reported,
                    ),
                ),
                (
                    "durationMs".to_owned(),
                    MetricFieldMapping::new(
                        "licoup.duration",
                        "ms",
                        Temporality::Delta,
                        Quality::Reported,
                    ),
                ),
            ]),
            unknown_metrics: vec![UnknownMetric::new(
                "licoup.tokens.total",
                "tokens",
                Temporality::Delta,
            )],
            cost: None,
        }
    }

    #[test]
    fn a_specialist_record_reports_its_metrics_and_unknown_tokens() {
        let record = serde_json::json!({
            "id": "run-1",
            "at": "2026-09-21T00:00:00Z",
            "items": 42,
            "durationMs": 1500,
        });
        let bound = specialist_mapping()
            .normalize(&record, &binding(), None)
            .expect("mapped")
            .expect("claims something");
        assert_eq!(bound.observation.metrics.len(), 3);
        assert_eq!(
            bound.observation.metrics["example.specialist/items"]
                .value
                .as_deref(),
            Some("42")
        );
        let tokens = &bound.observation.metrics["licoup.tokens.total"];
        assert_eq!(tokens.quality, Quality::Unknown);
        assert_eq!(tokens.value, None, "a missing count is not a zero");
        assert_eq!(bound.observation.source_epoch, "epoch-1");
        assert_eq!(bound.observation.scope_ref, "scope-1");
    }

    #[test]
    fn a_record_that_claims_nothing_maps_to_nothing() {
        let record = serde_json::json!({ "at": "2026-09-21T00:00:00Z" });
        let mapped = specialist_mapping()
            .normalize(&record, &binding(), None)
            .expect("mapping does not fail");
        assert!(mapped.is_none(), "no id and no metrics is not a claim");
    }

    #[test]
    fn a_mapped_field_that_is_not_a_number_is_refused_not_dropped() {
        let record = serde_json::json!({
            "id": "run-1",
            "at": "2026-09-21T00:00:00Z",
            "items": "many",
        });
        let failure = specialist_mapping()
            .normalize(&record, &binding(), None)
            .expect_err("text is not an item count");
        assert_eq!(failure.code, "usage_metric_invalid");
    }

    #[test]
    fn a_mapping_may_not_guess_through_an_array() {
        let mut mapping = specialist_mapping();
        mapping.metrics.insert(
            "points.0.value".to_owned(),
            MetricFieldMapping::new(
                "example.specialist/points",
                "items",
                Temporality::Delta,
                Quality::Reported,
            ),
        );
        let record = serde_json::json!({
            "id": "run-1",
            "at": "2026-09-21T00:00:00Z",
            "points": [{ "value": 1 }],
        });
        assert_eq!(
            mapping
                .normalize(&record, &binding(), None)
                .expect_err("array traversal")
                .field
                .as_deref(),
            Some("metrics.path")
        );
    }

    #[test]
    fn a_log_line_becomes_a_mapped_record() {
        let record = parse_key_value_line(
            "id=run-1 at=2026-09-21T00:00:00Z items=42 durationMs=1500 tokens=\"unknown\"",
        )
        .expect("parsed");
        let bound = specialist_mapping()
            .normalize(&record, &binding(), None)
            .expect("mapped")
            .expect("claims something");
        assert_eq!(
            bound.observation.metrics["example.specialist/items"]
                .value
                .as_deref(),
            Some("42")
        );
        assert!(parse_key_value_line("").is_none());
        assert!(
            parse_key_value_line("id=a id=b").is_none(),
            "a repeated key is ambiguous"
        );
        assert!(parse_key_value_line("novalue").is_none());
    }

    #[test]
    fn an_otlp_cumulative_series_keeps_its_origin_and_refuses_a_distribution() {
        let normalizer =
            OtlpSumNormalizer::new("example.vendor", Quality::Reported).expect("normalizer");
        let record = serde_json::json!({
            "name": "tokens.input",
            "unit": "tokens",
            "kind": "sum",
            "temporality": "cumulative",
            "points": [
                { "value": 100, "start": "2026-09-21T00:00:00Z", "time": "2026-09-21T00:05:00Z" },
                { "value": 175, "start": "2026-09-21T00:00:00Z", "time": "2026-09-21T00:10:00Z" }
            ]
        });
        let bound = normalizer
            .normalize(&record, &binding())
            .expect("normalized");
        assert_eq!(bound.len(), 2);
        assert_eq!(
            bound[0].observation.interval_start.as_deref(),
            Some("2026-09-21T00:00:00Z")
        );
        assert_eq!(
            bound[0].observation.metrics["example.vendor/tokens.input"].temporality,
            Temporality::Cumulative
        );
        assert_ne!(
            bound[0].observation.observation_id, bound[1].observation.observation_id,
            "a later point of the series is a new reading"
        );

        let restarted = serde_json::json!({
            "name": "tokens.input",
            "unit": "tokens",
            "kind": "sum",
            "temporality": "cumulative",
            "points": [
                { "value": 5, "start": "2026-09-21T01:00:00Z", "time": "2026-09-21T01:05:00Z" }
            ]
        });
        let bound = normalizer
            .normalize(&restarted, &binding())
            .expect("normalized");
        assert_eq!(
            bound[0].observation.interval_start.as_deref(),
            Some("2026-09-21T01:00:00Z"),
            "a restart is a new origin, not a negative reading"
        );

        let histogram = serde_json::json!({
            "name": "latency",
            "unit": "ms",
            "kind": "histogram",
            "temporality": "delta",
            "points": [],
        });
        assert_eq!(
            normalizer
                .normalize(&histogram, &binding())
                .expect_err("a distribution is not a scalar")
                .code,
            "usage_aggregation_unsupported"
        );
    }
}
