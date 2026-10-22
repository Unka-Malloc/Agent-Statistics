//! What a balance read reports: one fact at a time.
//!
//! One provider query may answer several questions — a remaining credit and
//! one or more subscription windows — so the reading is a list of
//! [`BalanceFact`], each of which is exactly one claim. A fact is produced
//! only from fields the response actually carried: a provider that does not
//! publish a monthly quota has no monthly quota fact, not a quota of zero.

use licoup_extension_contracts::{ApplicationFailure, is_namespaced};

use crate::refusal;

/// The most facts one read may carry, so one response cannot become an
/// unbounded list.
pub const MAX_FACTS_PER_READ: usize = 32;

/// The subscription period a quota fact describes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuotaWindow {
    /// A rolling weekly limit.
    Weekly,
    /// A rolling monthly limit.
    Monthly,
    /// A short-lived period the provider resets inside a session or day.
    Session,
    /// A named period the provider declares and the vocabulary does not know.
    Custom(String),
}

impl QuotaWindow {
    /// The wire label of the window.
    pub fn id(&self) -> &str {
        match self {
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
            Self::Session => "session",
            Self::Custom(label) => label.as_str(),
        }
    }
}

/// One fact a provider reported about an account's remaining capacity.
#[derive(Clone, Debug, PartialEq)]
pub enum BalanceFact {
    /// A pay-as-you-go remainder.
    Credit {
        /// The exact remaining amount as the provider wrote it. Exactness is
        /// the provider's claim; the detector never rounds or converts.
        remaining: String,
        /// ISO 4217 currency code.
        currency: String,
    },
    /// A subscription quota window.
    Quota {
        window: QuotaWindow,
        /// Remaining share of the window, an exact percentage `0`–`100`.
        /// `None` means the provider reported an amount instead, or said
        /// nothing about the share.
        remaining_percent: Option<String>,
        /// Remaining absolute amount in `unit`, when the provider reports one.
        remaining_amount: Option<String>,
        /// The unit `remaining_amount` counts (tokens, requests, credits).
        unit: Option<String>,
        /// When the window resets, as the provider wrote it.
        resets_at: Option<String>,
    },
    /// The account reports no limit for the asked surface.
    Unlimited,
}

impl BalanceFact {
    /// Structural validation of one fact.
    ///
    /// A percentage must parse inside `0`–`100`, a currency must be a
    /// three-letter code, an amount must be non-empty, and a custom window must
    /// be namespaced so two providers cannot collide on `promo`.
    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        match self {
            Self::Credit {
                remaining,
                currency,
            } => {
                if remaining.is_empty()
                    || remaining.len() > 64
                    || !remaining
                        .chars()
                        .all(|character| character.is_ascii_digit() || ".-+".contains(character))
                {
                    return Err(refusal("balance_fact_invalid").with_field("credit.remaining"));
                }
                if currency.len() != 3 || !currency.chars().all(|c| c.is_ascii_alphabetic()) {
                    return Err(refusal("balance_fact_invalid").with_field("credit.currency"));
                }
            }
            Self::Quota {
                window,
                remaining_percent,
                remaining_amount,
                unit,
                resets_at,
            } => {
                if let QuotaWindow::Custom(label) = window
                    && (label.is_empty() || !is_namespaced(label))
                {
                    return Err(refusal("balance_fact_invalid").with_field("quota.window"));
                }
                if let Some(percent) = remaining_percent {
                    let valid = percent
                        .parse::<f64>()
                        .is_ok_and(|value| (0.0..=100.0).contains(&value) && percent.len() <= 32);
                    if !valid {
                        return Err(
                            refusal("balance_fact_invalid").with_field("quota.remainingPercent")
                        );
                    }
                }
                if remaining_amount.is_none() != unit.is_none() {
                    return Err(refusal("balance_fact_invalid").with_field("quota.unit"));
                }
                if let Some(amount) = remaining_amount
                    && (amount.is_empty() || amount.len() > 64)
                {
                    return Err(refusal("balance_fact_invalid").with_field("quota.remainingAmount"));
                }
                if let Some(resets) = resets_at
                    && (resets.is_empty() || resets.len() > 64)
                {
                    return Err(refusal("balance_fact_invalid").with_field("quota.resetsAt"));
                }
                if remaining_percent.is_none() && remaining_amount.is_none() {
                    return Err(refusal("balance_fact_invalid").with_field("quota"));
                }
            }
            Self::Unlimited => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credit_fact_requires_an_exact_amount_and_a_currency() {
        let fact = BalanceFact::Credit {
            remaining: "12.50".to_owned(),
            currency: "USD".to_owned(),
        };
        assert!(fact.validate().is_ok());
        let bad = BalanceFact::Credit {
            remaining: "many".to_owned(),
            currency: "USD".to_owned(),
        };
        assert_eq!(
            bad.validate().expect_err("not an amount").field.as_deref(),
            Some("credit.remaining")
        );
        let bad = BalanceFact::Credit {
            remaining: "12.50".to_owned(),
            currency: "dollars".to_owned(),
        };
        assert_eq!(
            bad.validate().expect_err("not a code").field.as_deref(),
            Some("credit.currency")
        );
    }

    #[test]
    fn a_quota_window_reports_a_share_an_amount_or_nothing() {
        let weekly = BalanceFact::Quota {
            window: QuotaWindow::Weekly,
            remaining_percent: Some("42.5".to_owned()),
            remaining_amount: None,
            unit: None,
            resets_at: Some("2026-09-28T00:00:00Z".to_owned()),
        };
        assert!(weekly.validate().is_ok());

        let over = BalanceFact::Quota {
            window: QuotaWindow::Weekly,
            remaining_percent: Some("140".to_owned()),
            remaining_amount: None,
            unit: None,
            resets_at: Some("2026-09-28T00:00:00Z".to_owned()),
        };
        assert_eq!(
            over.validate().expect_err("over 100").field.as_deref(),
            Some("quota.remainingPercent")
        );

        let silent = BalanceFact::Quota {
            remaining_percent: None,
            remaining_amount: None,
            unit: None,
            resets_at: None,
            window: QuotaWindow::Monthly,
        };
        assert_eq!(
            silent
                .validate()
                .expect_err("a window saying nothing")
                .field
                .as_deref(),
            Some("quota")
        );

        let orphan_unit = BalanceFact::Quota {
            remaining_percent: None,
            remaining_amount: Some("900".to_owned()),
            unit: None,
            resets_at: None,
            window: QuotaWindow::Monthly,
        };
        assert_eq!(
            orphan_unit
                .validate()
                .expect_err("amount without unit")
                .field
                .as_deref(),
            Some("quota.unit")
        );
    }

    #[test]
    fn a_custom_window_is_namespaced() {
        let fact = BalanceFact::Quota {
            window: QuotaWindow::Custom("example.provider/promo".to_owned()),
            remaining_percent: Some("80".to_owned()),
            remaining_amount: None,
            unit: None,
            resets_at: None,
        };
        assert!(fact.validate().is_ok());
        let bad = BalanceFact::Quota {
            window: QuotaWindow::Custom("promo".to_owned()),
            remaining_percent: Some("80".to_owned()),
            remaining_amount: None,
            unit: None,
            resets_at: None,
        };
        assert_eq!(
            bad.validate().expect_err("bare window").field.as_deref(),
            Some("quota.window")
        );
    }
}
