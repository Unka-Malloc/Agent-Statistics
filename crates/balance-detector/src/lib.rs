//! Balance detection: remaining credit and subscription quota reads.
//!
//! A balance detector answers a different question than a token-usage
//! detector. Usage detection reads observations out of the traffic a host
//! already sees; balance detection goes to the provider's account surface and
//! asks how much is left. Two fact shapes cover the answers providers give:
//!
//! - **credit** — a pay-as-you-go remainder: an exact amount in one currency;
//! - **quota window** — a subscription limit that resets on a period (weekly,
//!   monthly or a provider-named custom period), reported as a remaining
//!   percentage or a remaining amount with its reset time.
//!
//! The crate is the boundary layer, not a provider client: [`source`] is the
//! port a host- or adapter-supplied fetcher implements, [`binding`] attaches
//! the account identity the host already knows, and [`mapping`] turns one
//! provider-shaped JSON response into the facts it actually claims.
//!
//! The structural rules mirror the usage side:
//!
//! - **A payload never names the account it describes.** The host issued the
//!   query and binds the answer; a response carrying `account`, `provider` or
//!   credential fields is refused, because accepting self-asserted identity
//!   would let one account's balance be written onto another.
//! - **A missing field is absent, never zero.** A provider that does not say
//!   how much remains has not said "nothing remains": the fact is simply not
//!   produced.
//! - **A window the provider does not describe is not invented.** Weekly and
//!   monthly quotas are reported only when the response carries them.
//!
//! This crate never opens a network connection, never stores a ledger and
//! never computes a charge; those belong to the fetcher above it and the
//! statistics consumer below it.

pub mod binding;
pub mod fact;
pub mod mapping;
pub mod source;

pub use binding::{AccountBinding, BoundBalance, self_asserted_account_field};
pub use fact::{BalanceFact, QuotaWindow};
pub use mapping::{BalanceMapping, CreditMapping, QuotaMapping};
pub use source::BalanceSource;

use licoup_extension_contracts::{ApplicationFailure, RecoveryAction};

/// The component every failure from this crate names.
pub(crate) const COMPONENT: &str = "balance_detector";

/// The stage every structural refusal reports.
pub(crate) const STAGE: &str = "balance-detector";

/// A permanent refusal about the shape or the binding of an input.
pub(crate) fn refusal(code: &str) -> ApplicationFailure {
    ApplicationFailure::permanent(code, STAGE).with_component(COMPONENT)
}

/// A refusal whose next step is real for the caller.
#[allow(dead_code)]
pub(crate) fn refusal_with(code: &str, recovery: RecoveryAction) -> ApplicationFailure {
    refusal(code).with_recovery(recovery)
}
