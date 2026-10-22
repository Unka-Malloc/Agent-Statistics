//! The host-side account binding: which account a read belongs to.
//!
//! The host issues the query, so the host already knows the answer's identity:
//! which provider surface it asked and for which account. An [`AccountBinding`]
//! is those facts made concrete — provider ref, account ref and the current
//! account epoch — and every fetched payload is mapped through it into
//! [`BoundBalance`].
//!
//! The refusal mirrors the usage side: a response that tries to name the
//! account itself (`account`, `provider`, a credential or key field) is
//! refused before it is read. The provider may legitimately echo plan or tier
//! names — the mapping reads those freely — but the identity of the account
//! being charged is the host's fact, never the payload's claim. That is what
//! keeps one account's balance from being written onto another's ledger when
//! an adapter reuses a response shape.

use licoup_extension_contracts::{ApplicationFailure, is_namespaced};
use serde_json::Value;

use crate::fact::{BalanceFact, MAX_FACTS_PER_READ};
use crate::refusal;

/// The longest provider or account identity accepted.
pub const MAX_REF_BYTES: usize = 160;

/// Payload fields that would assert the host's binding facts.
///
/// Refused by name, normalized case-insensitively ignoring `_` and `-`.
const SELF_ASSERTED_FIELDS: &[&str] = &[
    "account",
    "accountid",
    "provider",
    "providerid",
    "credential",
    "credentialref",
    "apikey",
    "token",
];

/// The host's facts about one provider account.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountBinding {
    /// The namespaced provider surface this binding queries — the contract
    /// family the response is read under, not a URL.
    pub provider_ref: String,
    /// The host-issued account identity the facts are charged to. It is a
    /// ledger identity, never a credential.
    pub account_ref: String,
    /// The current account epoch. A rotated credential or a re-linked account
    /// starts a new one, so facts from before the change cannot be replayed
    /// into the new series.
    pub account_epoch: String,
}

impl AccountBinding {
    pub fn new(
        provider_ref: impl Into<String>,
        account_ref: impl Into<String>,
        account_epoch: impl Into<String>,
    ) -> Result<Self, ApplicationFailure> {
        let binding = Self {
            provider_ref: provider_ref.into(),
            account_ref: account_ref.into(),
            account_epoch: account_epoch.into(),
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        if !is_namespaced(&self.provider_ref) {
            return Err(refusal("balance_binding_invalid").with_field("providerRef"));
        }
        if self.account_ref.is_empty()
            || self.account_ref.len() > MAX_REF_BYTES
            || self.account_ref == self.provider_ref
        {
            return Err(refusal("balance_binding_invalid").with_field("accountRef"));
        }
        if self.account_epoch.is_empty() {
            return Err(refusal("balance_binding_invalid").with_field("accountEpoch"));
        }
        Ok(())
    }

    /// Bind the facts a mapping produced into one read.
    ///
    /// The read is stamped with the host's clock: `observed_at` is when the
    /// host fetched, and the response's own timestamps live inside the facts
    /// where they belong — a reset time is a fact about the window, not about
    /// when the host asked.
    pub fn bind(
        &self,
        facts: Vec<BalanceFact>,
        observed_at: impl Into<String>,
    ) -> Result<BoundBalance, ApplicationFailure> {
        let read = BoundBalance {
            binding: self.clone(),
            observed_at: observed_at.into(),
            facts,
        };
        read.validate()?;
        Ok(read)
    }
}

/// One bound read: the facts one query produced about one account at one time.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundBalance {
    pub binding: AccountBinding,
    /// When the host fetched, stamped by the host.
    pub observed_at: String,
    /// The facts the response claimed, in the order they were produced.
    pub facts: Vec<BalanceFact>,
}

impl BoundBalance {
    pub fn validate(&self) -> Result<(), ApplicationFailure> {
        self.binding.validate()?;
        if self.observed_at.is_empty() {
            return Err(refusal("balance_read_invalid").with_field("observedAt"));
        }
        if self.facts.is_empty() {
            return Err(refusal("balance_read_invalid").with_field("facts"));
        }
        if self.facts.len() > MAX_FACTS_PER_READ {
            return Err(refusal("balance_read_invalid")
                .with_field("facts")
                .with_presentation_arg("limit", &MAX_FACTS_PER_READ.to_string()));
        }
        for fact in &self.facts {
            fact.validate()?;
        }
        Ok(())
    }
}

/// The dotted path of the first payload field that asserts a binding fact.
///
/// Same rule as the usage side: a payload field that names the account, the
/// provider or a credential is reported by path so the refusal can point at
/// the offending record rather than at the shape in general.
pub fn self_asserted_account_field(value: &Value) -> Option<String> {
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
                    if SELF_ASSERTED_FIELDS.contains(&normalized.as_str()) {
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
    use crate::fact::QuotaWindow;

    fn binding() -> AccountBinding {
        AccountBinding::new("example.provider", "acct-1", "epoch-1").expect("binding")
    }

    #[test]
    fn a_binding_requires_namespaced_provider_and_an_epoch() {
        assert!(AccountBinding::new("bareword", "a", "e").is_err());
        assert!(AccountBinding::new("example.provider", "", "e").is_err());
        assert!(AccountBinding::new("example.provider", "a", "").is_err());
        assert!(
            AccountBinding::new("example.provider", "example.provider", "e").is_err(),
            "the account is not the surface itself"
        );
        assert!(binding().validate().is_ok());
    }

    #[test]
    fn a_read_is_bounded_and_stamped_by_the_host() {
        let read = binding()
            .bind(
                vec![
                    BalanceFact::Credit {
                        remaining: "4.20".to_owned(),
                        currency: "USD".to_owned(),
                    },
                    BalanceFact::Quota {
                        window: QuotaWindow::Weekly,
                        remaining_percent: Some("61".to_owned()),
                        remaining_amount: None,
                        unit: None,
                        resets_at: None,
                    },
                ],
                "2026-09-21T12:00:00Z",
            )
            .expect("read");
        assert_eq!(read.facts.len(), 2);
        assert!(binding().bind(vec![], "t").is_err());
        assert!(
            binding()
                .bind(vec![BalanceFact::Unlimited; MAX_FACTS_PER_READ + 1], "t")
                .is_err()
        );
    }

    #[test]
    fn a_payload_cannot_name_the_account_or_its_credential() {
        for key in [
            "account",
            "accountId",
            "provider",
            "credentialRef",
            "api_key",
            "token",
        ] {
            let payload = serde_json::json!({ "balance": { key: "elsewhere" } });
            let field = self_asserted_account_field(&payload)
                .unwrap_or_else(|| panic!("{key} must be reported"));
            assert!(field.contains(key));
        }
        let clean = serde_json::json!({ "plan": "pro", "balance": { "remaining": "5" } });
        assert!(self_asserted_account_field(&clean).is_none());
    }
}
