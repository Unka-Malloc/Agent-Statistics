//! The general metric vocabulary C11 publishes, as canonical ids.
//!
//! C11 lists the optional general metrics — input, output and total tokens,
//! cached input, duration, cost, requests, items, errors and queue time — and
//! says a specialist metric may be defined by a source as long as its unit and
//! aggregation are declared. This module fixes the *names* and the subset
//! relations of the general set so a source, a panel and the consumer cannot
//! disagree about whether `licoup.tokens.total` already contains
//! `licoup.tokens.cached-input` (it does, through `licoup.tokens.input`), which
//! is the difference between one total and a double count.

use licoup_extension_contracts::usage::Temporality;

use crate::describe::Aggregation;

/// Input tokens.
pub const TOKENS_INPUT: &str = "licoup.tokens.input";
/// Output tokens.
pub const TOKENS_OUTPUT: &str = "licoup.tokens.output";
/// Total tokens. It contains input and output, which contains cached input.
pub const TOKENS_TOTAL: &str = "licoup.tokens.total";
/// Input tokens the provider served from its cache. A subset of input tokens.
pub const TOKENS_CACHED_INPUT: &str = "licoup.tokens.cached-input";
/// Wall-clock duration.
pub const DURATION: &str = "licoup.duration";
/// Incurred cost. Its unit is the ISO 4217 code of the amount.
pub const COST: &str = "licoup.cost";
/// Requests observed.
pub const REQUESTS: &str = "licoup.requests";
/// Domain items processed.
pub const ITEMS: &str = "licoup.items";
/// Errors observed.
pub const ERRORS: &str = "licoup.errors";
/// Time spent waiting before work started.
pub const QUEUE_TIME: &str = "licoup.queue-time";

/// One general metric with its declared unit, temporality, aggregation and
/// subset relation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandardMetric {
    pub metric: &'static str,
    pub unit: &'static str,
    pub temporality: Temporality,
    pub aggregation: Aggregation,
    /// Metrics this one already contains. A total is never added to its own
    /// subset.
    pub includes: &'static [&'static str],
}

/// The general metrics, in contract order.
pub const STANDARD: [StandardMetric; 10] = [
    StandardMetric {
        metric: TOKENS_INPUT,
        unit: "tokens",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[TOKENS_CACHED_INPUT],
    },
    StandardMetric {
        metric: TOKENS_OUTPUT,
        unit: "tokens",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: TOKENS_TOTAL,
        unit: "tokens",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[TOKENS_INPUT, TOKENS_OUTPUT],
    },
    StandardMetric {
        metric: TOKENS_CACHED_INPUT,
        unit: "tokens",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: DURATION,
        unit: "ms",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: COST,
        unit: "currency",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: REQUESTS,
        unit: "requests",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: ITEMS,
        unit: "items",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: ERRORS,
        unit: "errors",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
    StandardMetric {
        metric: QUEUE_TIME,
        unit: "ms",
        temporality: Temporality::Delta,
        aggregation: Aggregation::Sum,
        includes: &[],
    },
];

/// Every general metric.
pub fn standard() -> &'static [StandardMetric] {
    &STANDARD
}

/// The general metric with this id, if it is one.
pub fn standard_metric(metric: &str) -> Option<&'static StandardMetric> {
    STANDARD.iter().find(|standard| standard.metric == metric)
}

/// Whether `metric` is one of the general metrics.
pub fn is_standard(metric: &str) -> bool {
    standard_metric(metric).is_some()
}

/// Whether `outer` already contains `inner`, directly or through another
/// general metric, so the two must not be added together.
pub fn includes(outer: &str, inner: &str) -> bool {
    let Some(standard) = standard_metric(outer) else {
        return false;
    };
    standard
        .includes
        .iter()
        .any(|included| *included == inner || includes(included, inner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use licoup_extension_contracts::is_namespaced;

    #[test]
    fn every_general_metric_is_namespaced_and_declared_once() {
        for metric in standard() {
            assert!(
                is_namespaced(metric.metric),
                "{} must be namespaced",
                metric.metric
            );
            assert!(!metric.unit.is_empty());
        }
        assert_eq!(standard().len(), STANDARD.len());
        for (index, metric) in standard().iter().enumerate() {
            assert!(
                !standard()[..index]
                    .iter()
                    .any(|earlier| earlier.metric == metric.metric),
                "{} is declared once",
                metric.metric
            );
        }
    }

    #[test]
    fn a_total_contains_its_subsets_through_the_declared_chain() {
        assert!(includes(TOKENS_TOTAL, TOKENS_INPUT));
        assert!(includes(TOKENS_TOTAL, TOKENS_CACHED_INPUT));
        assert!(includes(TOKENS_INPUT, TOKENS_CACHED_INPUT));
        assert!(!includes(TOKENS_INPUT, TOKENS_TOTAL));
        assert!(!includes(TOKENS_OUTPUT, TOKENS_CACHED_INPUT));
        assert!(!includes("example.other/tokens", TOKENS_INPUT));
    }
}
