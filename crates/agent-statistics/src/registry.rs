//! Model-catalog seam. Production reads the real file-backed catalogue through
//! `licoup-model-catalog`; this crate's own test build substitutes one synthetic
//! snapshot because `cfg(test)` is true here and false in a dependency build.

#[cfg(not(test))]
pub use licoup_model_catalog::identity::{RegistrySnapshot, model_display_name};

#[cfg(test)]
pub use licoup_model_catalog::identity::{RegistrySnapshot, model_display_name};
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
thread_local! {
    static TEST_SNAPSHOT: std::cell::RefCell<Arc<RegistrySnapshot>> =
        std::cell::RefCell::new(Arc::new(RegistrySnapshot::empty()));
}

/// No file or network work. Capture one Arc at each report boundary.
#[cfg(test)]
pub fn snapshot() -> Arc<RegistrySnapshot> {
    TEST_SNAPSHOT.with(|snapshot| Arc::clone(&snapshot.borrow()))
}
