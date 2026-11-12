//! `[storage]` configuration.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::DuplicatePolicy;

/// Storage settings. Every field has a default, so `[storage]` may be empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Holds `tmp/`, `objects/` and `index.sqlite`.
    pub root: PathBuf,
    /// Largest accepted instance, in bytes (spooling aborts beyond this).
    pub max_instance_bytes: u64,
    /// What to do when a SOP Instance UID arrives again with different bytes.
    pub on_duplicate: DuplicatePolicy,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("storage"),
            max_instance_bytes: 2_147_483_648,
            on_duplicate: DuplicatePolicy::Keep,
        }
    }
}
