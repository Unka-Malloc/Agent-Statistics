//! `usage.describe`: what a source says about its own fields and series.
//!
//! A source that reports a specialist metric declares it here — a namespaced
//! name, a unit, a temporality and an aggregation — and a panel may only draw a
//! series the source declared. The declaration is what makes "this source
//! reports items processed" different from "this source reports a number called
//! items", and it is where "units and aggregation must be declared" stops being
//! a convention: an undeclared unit, an undeclared aggregation or a series that
//! names a metric nobody declared is refused.

use licoup_extension_contracts::usage::Temporality;
use licoup_extension_contracts::{ApplicationFailure, is_namespaced};

use crate::refusal;

/// How readings of one metric combine.
///
/// It is declared, never inferred: summing a gauge or averaging a sampled
/// quantile is a claim the data cannot support, and a source that means "last
/// value" or "maximum" says so here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Aggregation {
    /// Readings add: counters of work done.
    Sum,
    /// The latest reading wins: a level, not a quantity.
    Last,
    /// The largest reading wins.
    Max,
    /// A sampled quantile. It cannot be averaged across sources and it cannot be
    /// added; a consumer that needs one takes it from one declared source.
    Quantile,
}

impl Aggregation {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Last => "last",
            Self::Max => "max",
            Self::Quantile => "quantile",
        }
    }

    /// Whether readings of this metric may be added.
    pub const fn is_summable(self) -> bool {
        matches!(self, Self::Sum)
    }
}

/// One metric a source offers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricField {
    pub metric: String,
    pub unit: String,
    pub temporality: Temporality,
    pub aggregation: Aggregation,
}

impl MetricField {
    pub fn new(
        metric: impl Into<String>,
        unit: impl Into<String>,
        temporality: Temporality,
        aggregation: Aggregation,
    ) -> Self {
        Self {
            metric: metric.into(),
            unit: unit.into(),
            temporality,
            aggregation,
        }
    }

    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if !is_namespaced(&self.metric) {
            return Err(refusal("usage_describe_invalid").with_field("fields.metric"));
        }
        if self.unit.is_empty() {
            return Err(refusal("usage_describe_invalid").with_field("fields.unit"));
        }
        Ok(())
    }
}

/// One series a source suggests drawing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesDeclaration {
    pub metric: String,
    pub label: String,
    pub unit: String,
}

impl SeriesDeclaration {
    pub fn new(
        metric: impl Into<String>,
        label: impl Into<String>,
        unit: impl Into<String>,
    ) -> Self {
        Self {
            metric: metric.into(),
            label: label.into(),
            unit: unit.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if !is_namespaced(&self.metric) || self.label.is_empty() || self.unit.is_empty() {
            return Err(refusal("usage_describe_invalid").with_field("series"));
        }
        Ok(())
    }
}

/// What one source reports.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceDescription {
    pub fields: Vec<MetricField>,
    pub series: Vec<SeriesDeclaration>,
}

impl SourceDescription {
    pub fn new(fields: Vec<MetricField>, series: Vec<SeriesDeclaration>) -> Self {
        Self { fields, series }
    }

    pub fn field(&self, metric: &str) -> Option<&MetricField> {
        self.fields.iter().find(|field| field.metric == metric)
    }

    /// Structural validation: unique, well-formed fields and series whose unit
    /// agrees with the field that declares them.
    ///
    /// A series for an undeclared metric is refused rather than shown as a
    /// blank chart: a panel may not invent a field its source never claimed.
    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        for (index, field) in self.fields.iter().enumerate() {
            field.validate()?;
            if self.fields[..index]
                .iter()
                .any(|earlier| earlier.metric == field.metric)
            {
                return Err(refusal("usage_describe_invalid").with_field("fields.metric"));
            }
        }
        for series in &self.series {
            series.validate()?;
            let Some(field) = self.field(&series.metric) else {
                return Err(refusal("usage_describe_invalid").with_field("series.metric"));
            };
            if field.unit != series.unit {
                return Err(refusal("usage_describe_invalid").with_field("series.unit"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn description() -> SourceDescription {
        SourceDescription::new(
            vec![
                MetricField::new(
                    "licoup.tokens.input",
                    "tokens",
                    Temporality::Delta,
                    Aggregation::Sum,
                ),
                MetricField::new(
                    "example.agent/items",
                    "items",
                    Temporality::Delta,
                    Aggregation::Sum,
                ),
                MetricField::new(
                    "example.agent/latency",
                    "ms",
                    Temporality::Gauge,
                    Aggregation::Quantile,
                ),
            ],
            vec![
                SeriesDeclaration::new("example.agent/items", "Items", "items"),
                SeriesDeclaration::new("example.agent/latency", "Latency p95", "ms"),
            ],
        )
    }

    #[test]
    fn a_specialist_declares_its_own_fields() {
        assert!(description().validate().is_ok());
        assert_eq!(
            description()
                .field("example.agent/items")
                .expect("declared")
                .aggregation,
            Aggregation::Sum
        );
    }

    #[test]
    fn aggregation_is_declared_and_a_quantile_is_not_summable() {
        assert!(Aggregation::Sum.is_summable());
        assert!(!Aggregation::Max.is_summable());
        assert!(!Aggregation::Last.is_summable());
        assert!(!Aggregation::Quantile.is_summable());
        assert_eq!(Aggregation::Quantile.id(), "quantile");
    }

    #[test]
    fn an_undeclared_unit_or_series_is_refused() {
        let mut bad = description();
        bad.fields[0].unit = String::new();
        assert_eq!(
            bad.validate().expect_err("no unit").field.as_deref(),
            Some("fields.unit")
        );

        let mut bad = description();
        bad.fields[0].metric = "tokens".to_owned();
        assert_eq!(
            bad.validate().expect_err("not namespaced").field.as_deref(),
            Some("fields.metric")
        );

        let mut bad = description();
        bad.series.push(SeriesDeclaration::new(
            "example.agent/undeclared",
            "Other",
            "items",
        ));
        assert_eq!(
            bad.validate()
                .expect_err("undeclared series")
                .field
                .as_deref(),
            Some("series.metric")
        );

        let mut bad = description();
        bad.series[0].unit = "tokens".to_owned();
        assert_eq!(
            bad.validate()
                .expect_err("unit disagreement")
                .field
                .as_deref(),
            Some("series.unit")
        );

        let mut bad = description();
        bad.fields.push(MetricField::new(
            "example.agent/items",
            "items",
            Temporality::Delta,
            Aggregation::Sum,
        ));
        assert_eq!(
            bad.validate()
                .expect_err("duplicate field")
                .field
                .as_deref(),
            Some("fields.metric")
        );
    }
}
