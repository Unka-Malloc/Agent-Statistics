//! Declarative response mapping: provider JSON becomes bound facts.
//!
//! The same boundary rule as the usage side applies: the adapter is a mapping
//! plus a reader, not a second protocol. [`BalanceMapping`] declares which
//! response paths carry a credit remainder, which carry subscription windows,
//! and which flag the account as unlimited. A mapped path that is absent
//! produces no fact; a mapped path that carries the wrong shape is refused
//! rather than coerced.
//!
//! One response can legitimately report several windows — a weekly cap and a
//! monthly cap under the same subscription — so the mapping produces one fact
//! per declared window, never a merged "the quota".

use licoup_extension_contracts::ApplicationFailure;
use serde_json::Value;

use crate::binding::{AccountBinding, BoundBalance, self_asserted_account_field};
use crate::fact::{BalanceFact, QuotaWindow};
use crate::refusal;

/// How one response carries a credit remainder.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreditMapping {
    /// Path to the remaining amount. Required for the credit fact.
    pub remaining: String,
    /// Path to the ISO 4217 currency code. Required for the credit fact.
    pub currency: String,
}

/// How one response carries one subscription quota window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotaMapping {
    /// The window this path set describes, fixed by the mapping: the adapter
    /// knows which provider field is the weekly cap.
    pub window: QuotaWindow,
    /// Path to the remaining percentage `0`–`100`.
    pub remaining_percent: Option<String>,
    /// Path to the remaining absolute amount.
    pub remaining_amount: Option<String>,
    /// The unit the amount counts, declared by the mapping — a path is not
    /// needed because the unit is a property of the window, not of one read.
    pub unit: Option<String>,
    /// Path to the window's reset time.
    pub resets_at: Option<String>,
}

impl QuotaMapping {
    pub fn new(window: QuotaWindow) -> Self {
        Self {
            window,
            remaining_percent: None,
            remaining_amount: None,
            unit: None,
            resets_at: None,
        }
    }

    /// Whether the mapping reads anything at all.
    pub fn reads_something(&self) -> bool {
        self.remaining_percent.is_some() || self.remaining_amount.is_some()
    }
}

/// An explicit mapping from one provider response shape to bound facts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BalanceMapping {
    /// The credit remainder, when the provider reports one.
    pub credit: Option<CreditMapping>,
    /// One declaration per window the provider reports.
    pub quotas: Vec<QuotaMapping>,
    /// Path that, when present and truthy, reports the account as unlimited.
    pub unlimited: Option<String>,
}

impl BalanceMapping {
    /// Map one fetched response into a bound read.
    ///
    /// `Ok(None)` is not an error: a response that carries none of the mapped
    /// fields simply claims nothing — the caller decides whether silence is a
    /// retryable failure, never the mapping.
    pub fn normalize(
        &self,
        response: &Value,
        binding: &AccountBinding,
        observed_at: impl Into<String>,
    ) -> Result<Option<BoundBalance>, ApplicationFailure> {
        if let Some(field) = self_asserted_account_field(response) {
            return Err(refusal("balance_self_asserted").with_field(&field));
        }

        let mut facts = Vec::new();
        if let Some(credit) = &self.credit
            && let Some(fact) = self.credit_fact(response, credit)?
        {
            facts.push(fact);
        }
        for quota in &self.quotas {
            if let Some(fact) = self.quota_fact(response, quota)? {
                facts.push(fact);
            }
        }
        if let Some(path) = &self.unlimited
            && read_path(response, path)
                .map_err(|_| refusal("balance_mapping_invalid").with_field("unlimited"))?
                .is_some_and(is_truthy)
        {
            facts.push(BalanceFact::Unlimited);
        }

        if facts.is_empty() {
            return Ok(None);
        }
        binding.bind(facts, observed_at).map(Some)
    }

    fn credit_fact(
        &self,
        response: &Value,
        mapping: &CreditMapping,
    ) -> Result<Option<BalanceFact>, ApplicationFailure> {
        let remaining = read_string(response, &mapping.remaining, "credit.remaining")?;
        let currency = read_string(response, &mapping.currency, "credit.currency")?;
        match (remaining, currency) {
            (Some(remaining), Some(currency)) => {
                let fact = BalanceFact::Credit {
                    remaining,
                    currency,
                };
                fact.validate()?;
                Ok(Some(fact))
            }
            // Half a credit report is not a partial claim: a remainder without
            // its currency (or the reverse) is refused, not silently dropped —
            // dropping it would hide a mapping drift behind "provider said
            // nothing".
            (None, None) => Ok(None),
            (None, Some(_)) => {
                Err(refusal("balance_mapping_invalid").with_field("credit.remaining"))
            }
            (Some(_), None) => {
                Err(refusal("balance_mapping_invalid").with_field("credit.currency"))
            }
        }
    }

    fn quota_fact(
        &self,
        response: &Value,
        mapping: &QuotaMapping,
    ) -> Result<Option<BalanceFact>, ApplicationFailure> {
        let remaining_percent = match &mapping.remaining_percent {
            Some(path) => read_scalar(response, path, "quota.remainingPercent")?,
            None => None,
        };
        let remaining_amount = match &mapping.remaining_amount {
            Some(path) => read_scalar(response, path, "quota.remainingAmount")?,
            None => None,
        };
        let resets_at = match &mapping.resets_at {
            Some(path) => read_string(response, path, "quota.resetsAt")?,
            None => None,
        };
        if remaining_percent.is_none() && remaining_amount.is_none() && resets_at.is_none() {
            return Ok(None);
        }
        let fact = BalanceFact::Quota {
            window: mapping.window.clone(),
            remaining_percent,
            remaining_amount,
            unit: mapping.unit.clone(),
            resets_at,
        };
        fact.validate()?;
        Ok(Some(fact))
    }
}

/// Read one dotted object path, refusing array traversal as the usage side
/// does: "the third element" is not a mapping.
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

/// Read a string-valued path. A missing field is `None`; a wrong shape is a
/// refusal — the response said something, and pretending it did not would
/// invent silence.
fn read_string(
    record: &Value,
    path: &str,
    field: &str,
) -> Result<Option<String>, ApplicationFailure> {
    match read_path(record, path)
        .map_err(|_| refusal("balance_mapping_invalid").with_field(field))?
    {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(refusal("balance_mapping_invalid").with_field(field)),
    }
}

/// Read a number-or-string scalar into its exact text form.
fn read_scalar(
    record: &Value,
    path: &str,
    field: &str,
) -> Result<Option<String>, ApplicationFailure> {
    match read_path(record, path)
        .map_err(|_| refusal("balance_mapping_invalid").with_field(field))?
    {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(Value::Number(number)) => Ok(Some(number.to_string())),
        Some(_) => Err(refusal("balance_mapping_invalid").with_field(field)),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        Value::String(text) => matches!(text.as_str(), "true" | "unlimited" | "yes"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> AccountBinding {
        AccountBinding::new("example.provider", "acct-1", "epoch-1").expect("binding")
    }

    fn subscription_mapping() -> BalanceMapping {
        BalanceMapping {
            credit: None,
            quotas: vec![
                QuotaMapping {
                    window: QuotaWindow::Weekly,
                    remaining_percent: Some("usage.weekly.remainingPercent".to_owned()),
                    remaining_amount: None,
                    unit: None,
                    resets_at: Some("usage.weekly.resetsAt".to_owned()),
                },
                QuotaMapping {
                    window: QuotaWindow::Monthly,
                    remaining_percent: Some("usage.monthly.remainingPercent".to_owned()),
                    remaining_amount: Some("usage.monthly.remainingTokens".to_owned()),
                    unit: Some("tokens".to_owned()),
                    resets_at: None,
                },
            ],
            unlimited: None,
        }
    }

    #[test]
    fn a_subscription_response_reports_each_window_it_carries() {
        let response = serde_json::json!({
            "usage": {
                "weekly": { "remainingPercent": "37.5", "resetsAt": "2026-09-28T00:00:00Z" },
                "monthly": { "remainingPercent": 82, "remainingTokens": 410000 }
            }
        });
        let read = subscription_mapping()
            .normalize(&response, &binding(), "2026-09-21T12:00:00Z")
            .expect("mapped")
            .expect("claims something");
        assert_eq!(read.observed_at, "2026-09-21T12:00:00Z");
        assert_eq!(read.facts.len(), 2);
        let BalanceFact::Quota {
            window,
            remaining_percent,
            resets_at,
            ..
        } = &read.facts[0]
        else {
            panic!("weekly fact");
        };
        assert_eq!(*window, QuotaWindow::Weekly);
        assert_eq!(remaining_percent.as_deref(), Some("37.5"));
        assert_eq!(resets_at.as_deref(), Some("2026-09-28T00:00:00Z"));
        let BalanceFact::Quota {
            window,
            remaining_amount,
            unit,
            ..
        } = &read.facts[1]
        else {
            panic!("monthly fact");
        };
        assert_eq!(*window, QuotaWindow::Monthly);
        assert_eq!(remaining_amount.as_deref(), Some("410000"));
        assert_eq!(unit.as_deref(), Some("tokens"));
    }

    #[test]
    fn a_credit_response_needs_amount_and_currency_together() {
        let mapping = BalanceMapping {
            credit: Some(CreditMapping {
                remaining: "balance.remaining".to_owned(),
                currency: "balance.currency".to_owned(),
            }),
            ..Default::default()
        };
        let response = serde_json::json!({
            "balance": { "remaining": "4.20", "currency": "USD" }
        });
        let read = mapping
            .normalize(&response, &binding(), "t")
            .expect("mapped")
            .expect("claims");
        assert_eq!(
            read.facts[0],
            BalanceFact::Credit {
                remaining: "4.20".to_owned(),
                currency: "USD".to_owned()
            }
        );

        let partial = serde_json::json!({ "balance": { "remaining": "4.20" } });
        assert_eq!(
            mapping
                .normalize(&partial, &binding(), "t")
                .expect_err("amount without currency")
                .field
                .as_deref(),
            Some("credit.currency")
        );
    }

    #[test]
    fn a_window_absent_from_the_response_produces_no_fact() {
        let response = serde_json::json!({ "usage": { "weekly": { "remainingPercent": "50" } } });
        let read = subscription_mapping()
            .normalize(&response, &binding(), "t")
            .expect("mapped")
            .expect("claims");
        assert_eq!(read.facts.len(), 1, "monthly was never reported");
        let silent = serde_json::json!({ "usage": {} });
        assert!(
            subscription_mapping()
                .normalize(&silent, &binding(), "t")
                .expect("not a failure")
                .is_none(),
            "a response claiming nothing is not a zero balance"
        );
    }

    #[test]
    fn a_response_cannot_assert_the_account_or_mislabel_a_value() {
        let asserted = serde_json::json!({
            "accountId": "acct-2",
            "balance": { "remaining": "4.20", "currency": "USD" }
        });
        let mapping = BalanceMapping::default();
        assert_eq!(
            mapping
                .normalize(&asserted, &binding(), "t")
                .expect_err("self-asserted account")
                .code,
            "balance_self_asserted"
        );

        let wrong_shape =
            serde_json::json!({ "balance": { "remaining": [4.2], "currency": "USD" } });
        let mapping = BalanceMapping {
            credit: Some(CreditMapping {
                remaining: "balance.remaining".to_owned(),
                currency: "balance.currency".to_owned(),
            }),
            ..Default::default()
        };
        assert_eq!(
            mapping
                .normalize(&wrong_shape, &binding(), "t")
                .expect_err("array is not an amount")
                .field
                .as_deref(),
            Some("credit.remaining")
        );
    }

    #[test]
    fn an_unlimited_flag_is_read_where_the_mapping_says() {
        let mapping = BalanceMapping {
            unlimited: Some("plan.unlimited".to_owned()),
            ..Default::default()
        };
        let response = serde_json::json!({ "plan": { "unlimited": true } });
        let read = mapping
            .normalize(&response, &binding(), "t")
            .expect("mapped")
            .expect("claims");
        assert_eq!(read.facts, vec![BalanceFact::Unlimited]);
        let limited = serde_json::json!({ "plan": { "unlimited": false } });
        assert!(
            mapping
                .normalize(&limited, &binding(), "t")
                .expect("not a failure")
                .is_none()
        );
    }
}
