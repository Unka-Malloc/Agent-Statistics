//! The fetch port: how a bound read is obtained.
//!
//! The detector maps; it does not fetch. A [`BalanceSource`] is the port the
//! host or an adapter implements to reach a provider's account surface — an
//! HTTP endpoint, a local CLI report, a cached projection. Transport,
//! credentials and retry policy live behind the port: this crate never sees a
//! key and never opens a connection, which is the same line the usage side
//! draws between observation and acquisition.

use licoup_extension_contracts::ApplicationFailure;
use serde_json::Value;

use crate::binding::AccountBinding;

/// A fetcher for one provider surface.
///
/// The implementation owns the wire: authentication resolves at the host's
/// custody boundary, the request is bounded by its own timeout policy, and the
/// returned payload is the raw provider response. Identity arrives through the
/// binding — a source fetches *for* the bound account and never learns a
/// different one from the payload.
///
/// `fetch` returns the decoded JSON body. A source that cannot reach the
/// surface reports an [`ApplicationFailure`] rather than fabricating a
/// balance: an unreachable provider is a fact about availability, not a claim
/// that nothing remains.
pub trait BalanceSource {
    /// Fetch the provider's current balance/quota payload for `binding`.
    fn fetch(&self, binding: &AccountBinding) -> Result<Value, ApplicationFailure>;
}
