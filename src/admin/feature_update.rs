// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Details about an update to a finalized feature.
//!
//! Corresponds to `org.apache.kafka.clients.admin.FeatureUpdate`.

use crate::common::KafkaError;

/// Indicates what kind of upgrade should be performed for a
/// [`FeatureUpdate`].
///
/// Corresponds to `org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UpgradeType {
    /// Unknown upgrade type.
    Unknown,
    /// Upgrading the feature level.
    Upgrade,
    /// Only downgrades which do not result in metadata loss are permitted.
    SafeDowngrade,
    /// Any downgrade, including those which may result in metadata loss, are
    /// permitted.
    UnsafeDowngrade,
}

impl UpgradeType {
    /// Returns the wire-protocol code for this upgrade type.
    ///
    /// Mirrors `UpgradeType.code()`.
    pub fn code(&self) -> i8 {
        match self {
            UpgradeType::Unknown => 0,
            UpgradeType::Upgrade => 1,
            UpgradeType::SafeDowngrade => 2,
            UpgradeType::UnsafeDowngrade => 3,
        }
    }

    /// Returns the upgrade type for the given wire-protocol code, falling back
    /// to [`UpgradeType::Unknown`] for unrecognized codes.
    ///
    /// Mirrors `UpgradeType.fromCode(int)`.
    pub fn from_code(code: i32) -> UpgradeType {
        match code {
            1 => UpgradeType::Upgrade,
            2 => UpgradeType::SafeDowngrade,
            3 => UpgradeType::UnsafeDowngrade,
            _ => UpgradeType::Unknown,
        }
    }
}

/// Encapsulates details about an update to a finalized feature.
///
/// Corresponds to `org.apache.kafka.clients.admin.FeatureUpdate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FeatureUpdate {
    max_version_level: i16,
    upgrade_type: UpgradeType,
}

impl FeatureUpdate {
    /// Creates a new `FeatureUpdate`.
    ///
    /// `max_version_level` is the new maximum version level for the finalized
    /// feature. A value of zero is special and indicates that the update is
    /// intended to delete the finalized feature, and should be accompanied by
    /// setting `upgrade_type` to a safe or unsafe downgrade.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::illegal_argument`] (mirroring Java's
    /// `IllegalArgumentException`) if `max_version_level` is zero while
    /// `upgrade_type` is [`UpgradeType::Upgrade`], or if `max_version_level` is
    /// negative.
    pub fn new(max_version_level: i16, upgrade_type: UpgradeType) -> Result<Self, KafkaError> {
        if max_version_level == 0 && upgrade_type == UpgradeType::Upgrade {
            return Err(KafkaError::illegal_argument(format!(
                "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided \
                 maxVersionLevel:{max_version_level} is < 1."
            )));
        }
        if max_version_level < 0 {
            return Err(KafkaError::illegal_argument("Cannot specify a negative version level."));
        }
        Ok(Self { max_version_level, upgrade_type })
    }

    /// Returns the new maximum version level for the finalized feature.
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Returns the upgrade type for this update.
    pub fn upgrade_type(&self) -> UpgradeType {
        self.upgrade_type
    }
}

impl std::fmt::Display for FeatureUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FeatureUpdate{{maxVersionLevel:{}, upgradeType:{:?}}}",
            self.max_version_level, self.upgrade_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_type_codes_round_trip() {
        assert_eq!(UpgradeType::Unknown.code(), 0);
        assert_eq!(UpgradeType::Upgrade.code(), 1);
        assert_eq!(UpgradeType::SafeDowngrade.code(), 2);
        assert_eq!(UpgradeType::UnsafeDowngrade.code(), 3);
        assert_eq!(UpgradeType::from_code(0), UpgradeType::Unknown);
        assert_eq!(UpgradeType::from_code(1), UpgradeType::Upgrade);
        assert_eq!(UpgradeType::from_code(2), UpgradeType::SafeDowngrade);
        assert_eq!(UpgradeType::from_code(3), UpgradeType::UnsafeDowngrade);
        // Unrecognized codes fall back to Unknown.
        assert_eq!(UpgradeType::from_code(42), UpgradeType::Unknown);
    }

    #[test]
    fn new_valid_update() {
        let update = FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap();
        assert_eq!(update.max_version_level(), 2);
        assert_eq!(update.upgrade_type(), UpgradeType::Upgrade);
    }

    #[test]
    fn new_deletion_with_downgrade_is_allowed() {
        let update = FeatureUpdate::new(0, UpgradeType::SafeDowngrade).unwrap();
        assert_eq!(update.max_version_level(), 0);
        assert_eq!(update.upgrade_type(), UpgradeType::SafeDowngrade);
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion`.
    #[test]
    fn new_rejects_deletion_with_upgrade_flag() {
        let err = FeatureUpdate::new(0, UpgradeType::Upgrade).unwrap_err();
        assert_eq!(
            err.message(),
            "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided \
             maxVersionLevel:0 is < 1."
        );
    }

    #[test]
    fn new_rejects_negative_version_level() {
        let err = FeatureUpdate::new(-1, UpgradeType::SafeDowngrade).unwrap_err();
        assert_eq!(err.message(), "Cannot specify a negative version level.");
    }
}
