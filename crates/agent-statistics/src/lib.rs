//! Local usage measurement and aggregation engine.
//!
//! `agent-statistics` owns the generic machinery behind a host's local token
//! usage reporting: the usage journal, the incremental projection, windowing,
//! attribution, retained-report persistence, the usage-source host composition,
//! model-identity projection and private request counters.
//! Product knowledge — which agents carry usage capability and how one agent's
//! history reads — arrives through the installed [`host::StatisticsHost`] port;
//! per-agent readers that need no product infrastructure live in A4AA and are
//! consumed from there.

pub mod attribution;
mod command;
pub mod model_identity;
mod params;
mod registry;

pub mod contract;
pub mod gateway;
pub mod host;
pub mod incremental;
pub mod journal;
pub mod normalize;
pub mod persistence;
pub mod source_host;
pub mod variant;
pub mod window;

pub use command::{report, scan};
pub use host::{StatisticsHost, install_host};
