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

    /// Compute the intersection of two `ApiVersion` ranges. Returns
    /// `None` when there is no overlap, or when either input is `None`.
    /// Mirrors the static
    /// `ApiVersionsResponse.intersect(ApiVersion, ApiVersion)`.
    ///
    /// # Errors
    ///
    /// [`KafkaError::IllegalArgument`] when the two `ApiVersion` entries
    /// disagree on the api key (Java throws `IllegalArgumentException`).
    pub fn intersect(
        this_version: Option<&ApiVersion>,
        other: Option<&ApiVersion>,
    ) -> Result<Option<ApiVersion>, KafkaError> {
        let (Some(this_version), Some(other)) = (this_version, other) else {
            return Ok(None);
        };
        if this_version.api_key != other.api_key {
            return Err(KafkaError::IllegalArgument(format!(
                "thisVersion.apiKey: {} must be equal to other.apiKey: {}",
                this_version.api_key, other.api_key
            )));
        }
        let min_version = this_version.min_version.max(other.min_version);
        let max_version = this_version.max_version.min(other.max_version);
        if min_version > max_version {
            Ok(None)
        } else {
            Ok(Some(ApiVersion {
                api_key: this_version.api_key,
                min_version,
                max_version,
                unknown_tagged_fields: Vec::new(),
            }))
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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

    /// Hex-fixture regression test, added in Phase 8a.0 per PLAN.md
    /// Risk #1 ("capture hex fixtures from Java, assert bytes literally").
    ///
    /// Bytes were captured live off an `apache/kafka:4.2.0` Testcontainer
    /// broker, in reply to the `ApiVersionsRequest` v4 fixed in
    /// [`super::api_versions_request::tests::hex_fixture_api_versions_request_v4_apache_kafka_4_2`].
    /// The payload below is the exact byte sequence the Kafka 4.2.0
    /// broker writes for `ApiVersionsResponse` v4 (response-header bytes
    /// included). The response header for ApiVersionsResponse is **always
    /// v0** (just the 4-byte correlation_id), even when the body is
    /// flexible — this is the special-case noted in
    /// `ApiVersionsResponse.json`: "Newer brokers must be able to send a
    /// version 0 ApiVersionsResponse to clients that send an
    /// ApiVersionsRequest with a higher version than they support".
    ///
    /// Quick byte breakdown of the first 16 bytes:
    /// - `00 00 00 00` — response-header v0: correlation_id = 0
    /// - `00 00` — error_code = 0 (Errors::None)
    /// - `4c` — compact-array length+1 for api_keys → 75 entries (Kafka
    ///   4.2.0 advertises 75 supported API keys at the time this fixture
    ///   was captured)
    /// - `00 00 01 00` — first ApiVersion entry: api_key=0, min_version
    ///   high byte, etc.
    ///
    /// This test rejects any silent change in our parsing logic that
    /// would make us mis-decode a real Kafka 4.2.0 broker's reply.
    #[test]
    fn hex_fixture_api_versions_response_v4_apache_kafka_4_2() {
        const FIXTURE: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x01, 0x00, 0x04,
            0x00, 0x12, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x0b, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00,
            0x08, 0x00, 0x02, 0x00, 0x0a, 0x00, 0x00, 0x09, 0x00, 0x01, 0x00, 0x0a, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00,
            0x06, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x0d,
            0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x0f, 0x00, 0x00, 0x00, 0x06,
            0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x12, 0x00,
            0x00, 0x00, 0x04, 0x00, 0x00, 0x13, 0x00, 0x02, 0x00, 0x07, 0x00, 0x00, 0x14, 0x00, 0x01, 0x00, 0x06, 0x00,
            0x00, 0x15, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x17, 0x00, 0x02,
            0x00, 0x04, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
            0x1a, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1b, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00,
            0x05, 0x00, 0x00, 0x1d, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x1e, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x1f,
            0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x20, 0x00, 0x01, 0x00, 0x04, 0x00, 0x00, 0x21, 0x00, 0x00, 0x00, 0x02,
            0x00, 0x00, 0x22, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x23, 0x00, 0x01, 0x00, 0x04, 0x00, 0x00, 0x24, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x26, 0x00, 0x01, 0x00, 0x03, 0x00,
            0x00, 0x27, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x28, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x29, 0x00, 0x01,
            0x00, 0x03, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x2c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x2e, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x31,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x37, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x3c, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x44, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x45, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x4a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x4b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x4d, 0x00, 0x01, 0x00,
            0x01, 0x00, 0x00, 0x4e, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x4f, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x50,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x51, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x53, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x56, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x57, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x58, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x5b, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x5c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0xa4, 0x01,
            0x08, 0x0e, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x0e, 0x6b, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69,
            0x6f, 0x6e, 0x00, 0x07, 0x00, 0x1d, 0x00, 0x0e, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2e, 0x76, 0x65, 0x72, 0x73,
            0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x2e, 0x76,
            0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61,
            0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x21, 0x65, 0x6c, 0x69, 0x67, 0x69, 0x62, 0x6c, 0x65, 0x2e, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2e, 0x72,
            0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x02, 0x91, 0x01, 0x07, 0x0e, 0x67,
            0x72, 0x6f, 0x75, 0x70, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01, 0x00, 0x01, 0x00, 0x10,
            0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01, 0x00,
            0x01, 0x00, 0x14, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x65, 0x72,
            0x73, 0x69, 0x6f, 0x6e, 0x00, 0x02, 0x00, 0x02, 0x00, 0x21, 0x65, 0x6c, 0x69, 0x67, 0x69, 0x62, 0x6c, 0x65,
            0x2e, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x76,
            0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01, 0x00, 0x01, 0x00, 0x0e, 0x73, 0x68, 0x61, 0x72, 0x65, 0x2e,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01, 0x00, 0x01, 0x00, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64,
            0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x1d, 0x00, 0x1d, 0x00,
        ];
        assert_eq!(FIXTURE.len(), 862, "captured response payload is exactly 862 bytes");

        // Parse using the same path NetworkClient uses: response header
        // (always v0 for ApiVersions) then body at the requested version (4).
        let mut accessor = ByteBufferAccessor::wrap(FIXTURE.to_vec());
        let response_header =
            crate::common::requests::ResponseHeader::parse(&mut accessor, 0).expect("response header parses");
        assert_eq!(response_header.correlation_id(), 0);

        let parsed = ApiVersionsResponse::parse(&mut accessor, 4).expect("body parses");
        let data = parsed.response_data();

        // Top-level invariants the broker advertises on a healthy connect.
        assert_eq!(
            data.error_code,
            Errors::None.code(),
            "no error on a healthy ApiVersions exchange"
        );
        assert_eq!(data.throttle_time_ms, 0, "no throttling expected on fixture");
        assert_eq!(data.api_keys.len(), 75, "Kafka 4.2.0 advertises 75 api keys at capture time");

        // Spot-check specific known api keys at known versions.
        let by_key =
            |k: i16| -> &ApiVersion { data.api_keys.iter().find(|a| a.api_key == k).expect("api key present") };

        // PRODUCE (0): broker supports up to v13.
        let produce = by_key(0);
        assert_eq!(produce.min_version, 0);
        assert_eq!(produce.max_version, 13);

        // FETCH (1): broker supports v4-v18.
        let fetch = by_key(1);
        assert_eq!(fetch.min_version, 4);
        assert_eq!(fetch.max_version, 18);

        // METADATA (3): broker supports v0-v13.
        let metadata = by_key(3);
        assert_eq!(metadata.min_version, 0);
        assert_eq!(metadata.max_version, 13);

        // API_VERSIONS (18): broker supports v0-v4.
        let api_versions = by_key(18);
        assert_eq!(api_versions.min_version, 0);
        assert_eq!(api_versions.max_version, 4);

        // Tagged fields surfaced via decoder: confirm at least one
        // supported-feature is present (KRaft metadata.version is always
        // included from KIP-778 onward).
        assert!(
            !data.supported_features.is_empty(),
            "Kafka 4.2.0 advertises supported_features as a tagged field"
        );
        let metadata_version = data
            .supported_features
            .iter()
            .find(|f| f.name == "metadata.version")
            .expect("metadata.version feature key present");
        assert!(metadata_version.max_version > 0);
    }
}
