//! Explicit server-default, disabled, and finite byte budgets.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

/// A byte-budget policy without a numeric sentinel for default or disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ResourceLimit {
    /// Resolve the effective byte limit from authoritative server configuration.
    ServerDefault,
    /// Do not admit any use of this resource.
    Disabled,
    /// Cap resource use at this explicit, nonzero number of bytes.
    Bytes(NonZeroU64),
}

impl ResourceLimit {
    /// Resolves the policy to a finite limit, or `None` when disabled.
    #[must_use]
    pub const fn resolve_bytes(self, server_default: NonZeroU64) -> Option<NonZeroU64> {
        match self {
            Self::ServerDefault => Some(server_default),
            Self::Disabled => None,
            Self::Bytes(limit) => Some(limit),
        }
    }

    /// Returns whether measured byte use satisfies the resolved policy.
    #[must_use]
    pub const fn allows(self, measured_bytes: u64, server_default: NonZeroU64) -> bool {
        match self.resolve_bytes(server_default) {
            Some(limit) => measured_bytes <= limit.get(),
            None => measured_bytes == 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::ResourceLimit;

    #[test]
    fn resource_limit_has_explicit_default_disabled_and_bytes_states() {
        let default = NonZeroU64::new(8).unwrap();
        let bytes: ResourceLimit = serde_json::from_str(r#"{"type":"bytes","value":4}"#).unwrap();

        assert!(bytes.allows(4, default));
        assert!(!bytes.allows(5, default));
        assert!(ResourceLimit::ServerDefault.allows(8, default));
        assert!(!ResourceLimit::ServerDefault.allows(9, default));
        assert!(ResourceLimit::Disabled.allows(0, default));
        assert!(!ResourceLimit::Disabled.allows(1, default));
        assert_eq!(
            serde_json::to_string(&ResourceLimit::ServerDefault).unwrap(),
            r#"{"type":"server_default"}"#
        );
        assert_eq!(
            serde_json::to_string(&ResourceLimit::Disabled).unwrap(),
            r#"{"type":"disabled"}"#
        );
        assert!(serde_json::from_str::<ResourceLimit>(r#"{"type":"bytes","value":0}"#).is_err());
        assert!(serde_json::from_str::<ResourceLimit>(r#"{"type":"unlimited"}"#).is_err());
    }
}
