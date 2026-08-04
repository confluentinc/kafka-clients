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

//! UpdateFeatures request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.UpdateFeaturesRequest`.

use std::io;

use crate::admin::feature_update::UpgradeType;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::update_features_request_data::UpdateFeaturesRequestData;

use super::{ConcreteRequest, ConcreteResponse, RequestBuilder, UpdateFeaturesResponse};

/// A single feature update decoded from an [`UpdateFeaturesRequest`].
///
/// Corresponds to `UpdateFeaturesRequest.FeatureUpdateItem`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateItem {
    feature_name: String,
    feature_level: i16,
    upgrade_type: UpgradeType,
}

impl FeatureUpdateItem {
    /// Creates a new `FeatureUpdateItem`.
    pub fn new(feature_name: String, feature_level: i16, upgrade_type: UpgradeType) -> Self {
        Self { feature_name, feature_level, upgrade_type }
    }

    /// The name of the finalized feature to be updated.
    pub fn feature(&self) -> &str {
        &self.feature_name
    }

    /// The new maximum version level for the finalized feature.
    pub fn version_level(&self) -> i16 {
        self.feature_level
    }

    /// The upgrade type for this update.
    pub fn upgrade_type(&self) -> UpgradeType {
        self.upgrade_type
    }

    /// Whether this update deletes the finalized feature.
    ///
    /// Mirrors `FeatureUpdateItem.isDeleteRequest`.
    pub fn is_delete_request(&self) -> bool {
        self.feature_level < 1 && self.upgrade_type != UpgradeType::Upgrade
    }
}

/// An UpdateFeatures request.
///
/// Corresponds to `org.apache.kafka.common.requests.UpdateFeaturesRequest`.
#[derive(Debug, Clone)]
pub struct UpdateFeaturesRequest {
    data: UpdateFeaturesRequestData,
    version: i16,
}

impl UpdateFeaturesRequest {
    /// Creates a new `UpdateFeaturesRequest` from data and version.
    pub fn new(data: UpdateFeaturesRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &UpdateFeaturesRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut UpdateFeaturesRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UPDATE_FEATURES
    }

    /// Decodes the feature update for the given feature name, resolving the
    /// upgrade type from either the (v0) `allowDowngrade` flag or the (v1+)
    /// `upgradeType` code.
    ///
    /// Mirrors `UpdateFeaturesRequest.getFeature`.
    ///
    /// # Panics
    ///
    /// Panics if `name` is not present in the request (mirroring the Java
    /// contract, where `data.featureUpdates().find(name)` is assumed non-null).
    pub fn get_feature(&self, name: &str) -> FeatureUpdateItem {
        let update = self
            .data
            .feature_updates
            .iter()
            .find(|u| u.feature == name)
            .expect("feature must be present in the request");
        if self.version == 0 {
            let upgrade_type = if update.allow_downgrade {
                UpgradeType::SafeDowngrade
            } else {
                UpgradeType::Upgrade
            };
            FeatureUpdateItem::new(update.feature.clone(), update.max_version_level, upgrade_type)
        } else {
            FeatureUpdateItem::new(
                update.feature.clone(),
                update.max_version_level,
                UpgradeType::from_code(update.upgrade_type as i32),
            )
        }
    }

    /// Returns all feature updates in this request.
    ///
    /// Mirrors `UpdateFeaturesRequest.featureUpdates`.
    pub fn feature_updates(&self) -> Vec<FeatureUpdateItem> {
        self.data.feature_updates.iter().map(|u| self.get_feature(&u.feature)).collect()
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `UpdateFeaturesRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::UpdateFeatures(UpdateFeaturesResponse::create_with_errors(
            *error,
            None,
            &std::collections::BTreeSet::new(),
            throttle_time_ms,
        ))
    }

    /// Parses an `UpdateFeaturesRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = UpdateFeaturesRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for UpdateFeaturesRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UpdateFeaturesRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`UpdateFeaturesRequest`].
///
/// Corresponds to `UpdateFeaturesRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct UpdateFeaturesRequestBuilder {
    data: UpdateFeaturesRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl UpdateFeaturesRequestBuilder {
    /// Creates a builder from the given request data.
    ///
    /// Mirrors `UpdateFeaturesRequest.Builder(UpdateFeaturesRequestData)`.
    pub fn from_data(data: UpdateFeaturesRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::UPDATE_FEATURES.oldest_version(),
            latest_allowed_version: ApiKeys::UPDATE_FEATURES.latest_version(),
        }
    }
}

impl RequestBuilder for UpdateFeaturesRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UPDATE_FEATURES
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::UpdateFeatures(UpdateFeaturesRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update_features_request_data::FeatureUpdateKey;

    fn key(feature: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
        let mut k = FeatureUpdateKey::new();
        k.set_feature(feature.to_string());
        k.set_max_version_level(level);
        k.set_upgrade_type(upgrade_type);
        k
    }

    #[test]
    fn feature_updates_decode_v1_upgrade_type() {
        let mut data = UpdateFeaturesRequestData::new();
        data.set_feature_updates(vec![
            key("f1", 2, UpgradeType::Upgrade.code()),
            key("f2", 0, UpgradeType::SafeDowngrade.code()),
        ]);
        let request = UpdateFeaturesRequest::new(data, 1);
        let updates = request.feature_updates();
        assert_eq!(updates.len(), 2);
        let f1 = updates.iter().find(|u| u.feature() == "f1").unwrap();
        assert_eq!(f1.version_level(), 2);
        assert_eq!(f1.upgrade_type(), UpgradeType::Upgrade);
        assert!(!f1.is_delete_request());
        let f2 = updates.iter().find(|u| u.feature() == "f2").unwrap();
        assert_eq!(f2.upgrade_type(), UpgradeType::SafeDowngrade);
        // level < 1 and not an upgrade -> a deletion request.
        assert!(f2.is_delete_request());
    }

    #[test]
    fn feature_updates_decode_v0_allow_downgrade() {
        let mut allow = FeatureUpdateKey::new();
        allow.set_feature("f1".to_string());
        allow.set_max_version_level(0);
        allow.set_allow_downgrade(true);
        let mut deny = FeatureUpdateKey::new();
        deny.set_feature("f2".to_string());
        deny.set_max_version_level(3);
        deny.set_allow_downgrade(false);
        let mut data = UpdateFeaturesRequestData::new();
        data.set_feature_updates(vec![allow, deny]);
        let request = UpdateFeaturesRequest::new(data, 0);
        let f1 = request.get_feature("f1");
        assert_eq!(f1.upgrade_type(), UpgradeType::SafeDowngrade);
        let f2 = request.get_feature("f2");
        assert_eq!(f2.upgrade_type(), UpgradeType::Upgrade);
    }

    #[test]
    fn get_error_response_carries_top_level_error() {
        let request = UpdateFeaturesRequest::new(UpdateFeaturesRequestData::new(), 2);
        let response = request.get_error_response(100, &Errors::NotController);
        let ConcreteResponse::UpdateFeatures(r) = response else {
            panic!("expected UpdateFeatures response");
        };
        assert_eq!(r.data().error_code, Errors::NotController.code());
        assert_eq!(r.data().throttle_time_ms, 100);
        assert!(r.data().results.is_empty());
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = UpdateFeaturesRequestData::new();
        data.set_timeout_ms(100);
        data.set_feature_updates(vec![key("f", 2, UpgradeType::Upgrade.code())]);
        data.set_validate_only(true);
        let mut builder = UpdateFeaturesRequestBuilder::from_data(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = UpdateFeaturesRequest::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().timeout_ms, 100);
        assert!(parsed.data().validate_only);
        assert_eq!(parsed.data().feature_updates.len(), 1);
        assert_eq!(parsed.data().feature_updates[0].feature, "f");
        assert_eq!(parsed.data().feature_updates[0].max_version_level, 2);
        assert_eq!(parsed.data().feature_updates[0].upgrade_type, UpgradeType::Upgrade.code());
    }

    /// Byte-level encoding test against a known vector. UpdateFeatures v1 is a
    /// flexible version, so the body is:
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   feature_updates: compact array (len+1 = 0x02)
    ///     feature: compact string "f" (0x02, 0x66)
    ///     max_version_level: int16 = 2 (00 02)
    ///     upgrade_type: int8 = 1 (01)
    ///     _tagged_fields: 0x00
    ///   validate_only: bool = false (00)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = UpdateFeaturesRequestData::new();
        data.set_timeout_ms(100);
        data.set_feature_updates(vec![key("f", 2, UpgradeType::Upgrade.code())]);
        data.set_validate_only(false);
        let mut builder = UpdateFeaturesRequestBuilder::from_data(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x02, // feature_updates array length + 1
            0x02, 0x66, // feature "f"
            0x00, 0x02, // max_version_level = 2
            0x01, // upgrade_type = 1 (UPGRADE)
            0x00, // feature_updates element tagged fields
            0x00, // validate_only = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
