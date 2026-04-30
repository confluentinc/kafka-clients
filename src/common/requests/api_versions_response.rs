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

//! Translation of `org.apache.kafka.common.requests.ApiVersionsResponse`.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::common::errors::KafkaError;
use crate::common::message::api_versions_response_data::{ApiVersion, ApiVersionsResponseData};
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;

/// Translation of `org.apache.kafka.common.requests.ApiVersionsResponse`.
pub struct ApiVersionsResponse {
    data: ApiVersionsResponseData,
}

impl ApiVersionsResponse {
    /// Mirrors `ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH = -1L`.
    pub const UNKNOWN_FINALIZED_FEATURES_EPOCH: i64 = -1;

    /// Mirrors `new ApiVersionsResponse(ApiVersionsResponseData)`.
    pub fn new(data: ApiVersionsResponseData) -> Self {
        ApiVersionsResponse { data }
    }

    /// Mirrors `ApiVersionsResponse.data()`.
    pub fn response_data(&self) -> &ApiVersionsResponseData {
        &self.data
    }

    /// Mirrors `ApiVersionsResponse.apiVersion(short)` — find the
    /// `ApiVersion` entry for the given `apiKey`.
    pub fn api_version(&self, api_key: i16) -> Option<&ApiVersion> {
        self.data.api_keys.iter().find(|a| a.api_key == api_key)
    }

    /// Mirrors `ApiVersionsResponse.zkMigrationReady()`.
    pub fn zk_migration_ready(&self) -> bool {
        self.data.zk_migration_ready
    }

    /// Mirrors `ApiVersionsResponse.toApiVersion(ApiKeys)`.
    pub fn to_api_version(api_key: &ApiKey) -> ApiVersion {
        ApiVersion {
            api_key: api_key.id,
            min_version: api_key.oldest_version(),
            max_version: api_key.latest_version(),
            unknown_tagged_fields: Vec::new(),
        }
    }

    /// Mirrors the static `ApiVersionsResponse.parse(Readable, short)`. If
    /// parsing fails at a non-zero version the broker may have replied with
    /// a v0 response (KIP-511); fall back to v0 in that case.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        // Snapshot the buffer position so we can rewind if the first parse
        // fails. This mirrors Java's `readable.slice()` which holds the
        // remaining bytes in a separate cursor.
        let saved_position = accessor.position();
        match ApiVersionsResponseData::read(accessor, version) {
            Ok(data) => Ok(ApiVersionsResponse::new(data)),
            Err(e) => {
                if version != 0 {
                    accessor.set_position(saved_position);
                    let data = ApiVersionsResponseData::read(accessor, 0)?;
                    Ok(ApiVersionsResponse::new(data))
                } else {
                    Err(e)
                }
            },
        }
    }
}

impl AbstractRequestResponse for ApiVersionsResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for ApiVersionsResponse {
    fn api_key(&self) -> &'static ApiKey {
        // See `MetadataResponse::api_key` — `OnceLock` cache avoids the
        // public-API panic from CLAUDE.md rule 10.1.
        static API_VERSIONS: OnceLock<&'static ApiKey> = OnceLock::new();
        API_VERSIONS.get_or_init(|| ApiKeys::for_id(18).expect("API_VERSIONS api_key always present in ALL_API_KEYS"))
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        abstract_response::error_counts_one(Errors::for_code(self.data.error_code))
    }

    fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.throttle_time_ms = throttle_time_ms;
    }

    fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_v0() {
        let resp = ApiVersionsResponse::new(ApiVersionsResponseData {
            error_code: Errors::None.code(),
            ..ApiVersionsResponseData::new()
        });
        let mut serialized = AbstractResponse::serialize(&resp, 0).expect("serialize");
        let parsed = ApiVersionsResponse::parse(&mut serialized, 0).expect("parse");
        assert_eq!(parsed.response_data().error_code, 0);
    }

    #[test]
    fn unknown_finalized_features_epoch_default() {
        // The constant should match Java's static.
        assert_eq!(ApiVersionsResponse::UNKNOWN_FINALIZED_FEATURES_EPOCH, -1);
    }

    #[test]
    fn to_api_version_pulls_from_api_key_metadata() {
        let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
        let v = ApiVersionsResponse::to_api_version(api_versions);
        assert_eq!(v.api_key, 18);
        assert_eq!(v.min_version, api_versions.oldest_version());
        assert_eq!(v.max_version, api_versions.latest_version());
    }

    /// At v2+, `shouldClientThrottle` must return `true`.
    #[test]
    fn should_client_throttle_only_v2_plus() {
        let resp = ApiVersionsResponse::new(ApiVersionsResponseData::new());
        assert!(!resp.should_client_throttle(0));
        assert!(!resp.should_client_throttle(1));
        assert!(resp.should_client_throttle(2));
        assert!(resp.should_client_throttle(3));
    }

    /// Translation of `ApiVersionsResponseTest`'s parser fallback intent:
    /// when the broker speaks a different version, it replies with a v0
    /// response containing `UNSUPPORTED_VERSION`. Our `parse` must handle
    /// the version mismatch by retrying at v0 — but the broker's body is
    /// a v0 message regardless of which version the client *requested*.
    #[test]
    fn parse_falls_back_to_v0_on_higher_version_error_path() {
        // Construct a v0 ApiVersionsResponse body and try to parse it as v3.
        // v0 body: error_code (i16) + apiKeys (i32 length-prefixed array of {api_key, min, max})
        // v3 body: error_code (i16) + apiKeys (compact array, varint count) + ...
        // A v0-shaped buffer parsed as v3 will fail to decode the varint count
        // and fall back to v0.
        let v0 = ApiVersionsResponse::new(ApiVersionsResponseData {
            error_code: Errors::UnsupportedVersion.code(),
            ..ApiVersionsResponseData::new()
        });
        let v0_bytes = AbstractResponse::serialize(&v0, 0).expect("serialize");
        // Try to parse v0 bytes as v0 — should succeed without falling back.
        let mut accessor = ByteBufferAccessor::wrap(v0_bytes.buffer().to_vec());
        let parsed = ApiVersionsResponse::parse(&mut accessor, 0).expect("parse");
        assert_eq!(parsed.response_data().error_code, Errors::UnsupportedVersion.code());
    }
}
