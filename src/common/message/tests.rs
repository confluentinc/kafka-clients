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

//! Integration tests that exercise generator-emitted modules against the
//! Phase 2c runtime traits. These tests live one level above the
//! generated `*_data` modules (in `src/common/message/`) because they
//! validate the *runtime contract* of the codegen pipeline rather than
//! testing any single module in isolation.
//!
//! Phase 2d-1 covers `RequestHeaderData`; Phase 2d-2 starts adding the
//! remaining message specs. Phase 2d-3/4 will continue with the
//! Metadata and Produce request/response pairs.

use crate::common::message::api_versions_request_data::ApiVersionsRequestData;
use crate::common::message::api_versions_response_data::{
    ApiVersion, ApiVersionsResponseData, FinalizedFeatureKey, SupportedFeatureKey,
};
use crate::common::message::metadata_request_data::{MetadataRequestData, MetadataRequestTopic};
use crate::common::message::metadata_response_data::{
    MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
};
use crate::common::message::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};
use crate::common::message::produce_response_data::{
    BatchIndexAndErrorMessage, LeaderIdAndEpoch, NodeEndpoint, PartitionProduceResponse, ProduceResponseData,
    TopicProduceResponse,
};
use crate::common::message::request_header_data::RequestHeaderData;
use crate::common::message::response_header_data::ResponseHeaderData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::protocol::types::RawTaggedField;
use crate::common::protocol::{Message, Readable};
use crate::common::uuid::Uuid;

/// Helper: encode a `Message` at the given version into a byte buffer (the
/// same idiom used at every Phase 2d test site — sizer pass, allocate,
/// write, flip).
fn encode<M: Message>(msg: &M, version: i16) -> Vec<u8> {
    let mut cache = ObjectSerializationCache::new();
    let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
    Message::add_size(msg, &mut sizer, &mut cache, version);
    let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
    Message::write(msg, &mut accessor, &cache, version).expect("write succeeds");
    accessor.flip();
    accessor.buffer().to_vec()
}

/// Header version 2 — the first flexible version of `RequestHeader`. We
/// encode the data at this version so the test exercises the
/// flexible-versions code path (tagged fields, varint counts) end-to-end.
const HEADER_VERSION_FLEXIBLE: i16 = 2;

/// Phase 2d-1's proof-of-concept round trip. Builds a
/// `RequestHeaderData` for a Produce request at API version 11, encodes
/// it via `Message::write` + `ByteBufferAccessor`, and decodes it back
/// via `Message::read`.
///
/// The byte-level assertions also lock the **G1 fix** in: the
/// `client_id` field on `RequestHeader` uses length-prefixed (i16)
/// encoding at every header version, even when the surrounding message
/// is flexible. This is a per-field `flexibleVersions: "none"` override
/// in `RequestHeader.json` and is wire-incompatible with Java brokers
/// when violated.
#[test]
fn request_header_data_round_trip_flexible_v2() {
    let original = RequestHeaderData {
        request_api_key: 0, // Produce
        request_api_version: 11,
        correlation_id: 42,
        client_id: Some("test-client".to_string()),
        unknown_tagged_fields: Vec::new(),
    };

    // --- Encode using the Phase 2c runtime trait `Message::write` ---
    let mut cache = ObjectSerializationCache::new();
    let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
    Message::add_size(&original, &mut sizer, &mut cache, HEADER_VERSION_FLEXIBLE);
    let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
    Message::write(&original, &mut accessor, &cache, HEADER_VERSION_FLEXIBLE).expect("write succeeds");
    accessor.flip();

    // --- Byte-level assertions (G1 lock + fixed prefix layout) ---
    //
    // Wire layout for RequestHeader v2:
    //   request_api_key      (i16, 2 bytes BE)
    //   request_api_version  (i16, 2 bytes BE)
    //   correlation_id       (i32, 4 bytes BE)
    //   client_id            (i16 length BE  +  UTF-8 bytes — length-prefixed,
    //                         NOT compact varint, per G1)
    //   tagged-field count   (varint, 0 here)
    let bytes = accessor.buffer();

    // Fixed prefix.
    assert_eq!(&bytes[0..2], &0i16.to_be_bytes(), "api_key encoded as i16 BE");
    assert_eq!(&bytes[2..4], &11i16.to_be_bytes(), "api_version encoded as i16 BE");
    assert_eq!(&bytes[4..8], &42i32.to_be_bytes(), "correlation_id encoded as i32 BE");

    // G1 assertion: even at the flexible header version (v2), client_id
    // must be encoded as `i16 length` + `bytes`, not `varint(length+1)`.
    let client_id_str = "test-client";
    let client_id_len = client_id_str.len() as i16;
    assert_eq!(
        &bytes[8..10],
        &client_id_len.to_be_bytes(),
        "client_id length encoded as i16 BE (length-prefixed) — \
         G1: per-field `flexibleVersions: none` override forces length-prefixed encoding"
    );
    assert_eq!(
        &bytes[10..10 + client_id_str.len()],
        client_id_str.as_bytes(),
        "client_id bytes follow the length prefix"
    );

    // Tagged-fields trailer at v2 (flexible).
    let after_client_id = 10 + client_id_str.len();
    assert_eq!(
        bytes[after_client_id], 0,
        "no tagged fields means a single 0-byte unsigned varint trailer"
    );
    assert_eq!(
        bytes.len(),
        after_client_id + 1,
        "trailer is 1 byte, message has no extra trailing bytes"
    );

    // --- Decode and round-trip equality ---
    let mut decode_accessor = ByteBufferAccessor::wrap(bytes.to_vec());
    let mut decoded = RequestHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, HEADER_VERSION_FLEXIBLE).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0, "decoder consumes the entire buffer");
}

/// Sanity round trip at the non-flexible header version (v1) where
/// `client_id` is unconditionally length-prefixed and there are no
/// tagged fields. We do not need the byte-level G1 assertion at v1
/// because v1 is non-flexible by design — the assertion above at v2 is
/// what locks the G1 invariant in.
#[test]
fn request_header_data_round_trip_non_flexible_v1() {
    let original = RequestHeaderData {
        request_api_key: 3, // Metadata
        request_api_version: 7,
        correlation_id: -1,
        client_id: Some("kafka-rust".to_string()),
        unknown_tagged_fields: Vec::new(),
    };

    let mut cache = ObjectSerializationCache::new();
    let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
    Message::add_size(&original, &mut sizer, &mut cache, 1);
    let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
    Message::write(&original, &mut accessor, &cache, 1).expect("write succeeds");
    accessor.flip();

    let mut decode_accessor = ByteBufferAccessor::wrap(accessor.buffer().to_vec());
    let mut decoded = RequestHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, 1).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// `client_id = None` round-trips correctly (length-prefix encoded as -1
/// at non-flexible header versions). Mirrors a Java broker request that
/// did not send a client id.
#[test]
fn request_header_data_round_trip_null_client_id_v1() {
    let original = RequestHeaderData {
        request_api_key: 18, // ApiVersions
        request_api_version: 0,
        correlation_id: 100,
        client_id: None,
        unknown_tagged_fields: Vec::new(),
    };

    let mut cache = ObjectSerializationCache::new();
    let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
    Message::add_size(&original, &mut sizer, &mut cache, 1);
    let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
    Message::write(&original, &mut accessor, &cache, 1).expect("write succeeds");
    accessor.flip();

    // Last 2 bytes encode `-1i16` as the client_id null sentinel.
    let bytes = accessor.buffer();
    let null_marker = i16::from_be_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);
    assert_eq!(null_marker, -1, "null client_id encodes as i16(-1)");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes.to_vec());
    let mut decoded = RequestHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, 1).expect("read succeeds");
    assert_eq!(decoded, original);
}

// =============================================================================
// ResponseHeaderData (Phase 2d-2)
//
// `ResponseHeader.json` validVersions = 0-1, flexibleVersions = 1+. v0 is the
// non-flexible, fixed 4-byte correlation id. v1 is flexible and adds a
// trailing tagged-fields varint.
// =============================================================================

/// Round trip at the lowest supported version (v0, non-flexible). The
/// wire image is exactly 4 bytes: the i32 BE correlation id.
#[test]
fn response_header_data_round_trip_v0() {
    let original = ResponseHeaderData { correlation_id: 42, unknown_tagged_fields: Vec::new() };

    let bytes = encode(&original, 0);
    assert_eq!(bytes.len(), 4, "v0 ResponseHeader is exactly 4 bytes (correlation id)");
    assert_eq!(&bytes[..], &42i32.to_be_bytes(), "correlation_id encoded as i32 BE");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ResponseHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, 0).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at the highest supported version (v1, flexible). Encodes a
/// non-empty tagged field to exercise the flexible-versions trailer.
#[test]
fn response_header_data_round_trip_v1_flexible_with_tagged_field() {
    let original = ResponseHeaderData {
        correlation_id: -1,
        unknown_tagged_fields: vec![RawTaggedField::new(42, vec![0xCA, 0xFE])],
    };

    let bytes = encode(&original, 1);

    // Wire layout for ResponseHeader v1:
    //   correlation_id     (i32 BE, 4 bytes)
    //   tagged-field count (unsigned varint = 1)
    //   tagged field 0:
    //     tag             (unsigned varint = 42)
    //     size            (unsigned varint = 2)
    //     data            (2 bytes)
    assert_eq!(&bytes[0..4], &(-1i32).to_be_bytes(), "correlation_id encoded as i32 BE");
    assert_eq!(bytes[4], 1, "single tagged field in trailer");
    assert_eq!(bytes[5], 42, "tag varint");
    assert_eq!(bytes[6], 2, "size varint");
    assert_eq!(&bytes[7..9], &[0xCA, 0xFE], "tagged field data");
    assert_eq!(
        bytes.len(),
        9,
        "v1 ResponseHeader = 4 + varint(1) + varint(42) + varint(2) + 2 bytes"
    );

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ResponseHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, 1).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// At v1 with no tagged fields, the trailer is a single 0-byte varint.
#[test]
fn response_header_data_round_trip_v1_flexible_empty() {
    let original = ResponseHeaderData { correlation_id: 99, unknown_tagged_fields: Vec::new() };
    let bytes = encode(&original, 1);
    assert_eq!(bytes.len(), 5, "v1 ResponseHeader empty = 4 bytes + varint(0)");
    assert_eq!(bytes[4], 0, "no tagged fields → varint(0)");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ResponseHeaderData::new();
    Message::read(&mut decoded, &mut decode_accessor, 1).expect("read succeeds");
    assert_eq!(decoded, original);
}
// =============================================================================
// ApiVersionsRequestData (Phase 2d-2)
//
// `ApiVersionsRequest.json` validVersions = 0-4, flexibleVersions = 3+.
// `ClientSoftwareName` and `ClientSoftwareVersion` are added in v3+; both
// are non-nullable strings (their default is empty per Phase 2a's G2 rule).
// =============================================================================

/// Round trip at v0 (lowest supported, non-flexible). At v0 the message
/// is empty on the wire — neither client software field exists, no
/// flexible trailer.
#[test]
fn api_versions_request_data_round_trip_v0() {
    let original = ApiVersionsRequestData {
        client_software_name: String::new(),
        client_software_version: String::new(),
        unknown_tagged_fields: Vec::new(),
    };
    let bytes = encode(&original, 0);
    assert_eq!(bytes.len(), 0, "v0 ApiVersionsRequest is empty on the wire");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 0).expect("read succeeds");
    assert_eq!(decoded, original);
}

/// Round trip at v4 (highest supported, flexible) with non-empty client
/// software identifiers and an unknown tagged field, exercising both the
/// G2 default (non-empty strings round-trip) and the tagged-field
/// trailer.
#[test]
fn api_versions_request_data_round_trip_v4_flexible() {
    let original = ApiVersionsRequestData {
        client_software_name: "kafka-rust".to_string(),
        client_software_version: "0.1.0".to_string(),
        unknown_tagged_fields: vec![RawTaggedField::new(7, vec![0x01, 0x02, 0x03])],
    };

    let bytes = encode(&original, 4);

    // Wire layout for ApiVersionsRequest v4 (all fields are flexible):
    //   client_software_name length (varint = name_len + 1)
    //   client_software_name bytes
    //   client_software_version length (varint = version_len + 1)
    //   client_software_version bytes
    //   tagged-field count (varint = 1)
    //   tagged field 0: tag(7) size(3) data(0x01 0x02 0x03)
    let name_bytes = "kafka-rust".as_bytes();
    let version_bytes = "0.1.0".as_bytes();
    assert_eq!(bytes[0], (name_bytes.len() + 1) as u8, "compact string varint length for name");
    assert_eq!(&bytes[1..1 + name_bytes.len()], name_bytes);
    let after_name = 1 + name_bytes.len();
    assert_eq!(
        bytes[after_name],
        (version_bytes.len() + 1) as u8,
        "compact string varint length for version"
    );
    assert_eq!(&bytes[after_name + 1..after_name + 1 + version_bytes.len()], version_bytes);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 4).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Empty client identifiers at v4: encoded as `varint(0+1)=varint(1)` with
/// zero bytes of payload — confirms the Phase 2a G2 empty-default behavior
/// for non-nullable strings on flexible versions.
#[test]
fn api_versions_request_data_round_trip_v4_empty_strings() {
    let original = ApiVersionsRequestData {
        client_software_name: String::new(),
        client_software_version: String::new(),
        unknown_tagged_fields: Vec::new(),
    };
    let bytes = encode(&original, 4);
    // varint(1) + 0-byte name + varint(1) + 0-byte version + varint(0) trailer
    assert_eq!(bytes.len(), 3);
    assert_eq!(bytes, vec![1u8, 1u8, 0u8]);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 4).expect("read succeeds");
    assert_eq!(decoded, original);
}
// =============================================================================
// ApiVersionsResponseData (Phase 2d-2)
//
// `ApiVersionsResponse.json` validVersions = 0-4, flexibleVersions = 3+.
// This spec exercises:
//   - Arrays of structs (`ApiKeys: Vec<ApiVersion>`)
//   - int64 fields (FinalizedFeaturesEpoch)
//   - bool fields (ZkMigrationReady)
//   - Tagged fields with non-trivial encodings (struct arrays, primitives)
// =============================================================================

/// Round trip at v0 (lowest supported, non-flexible) with a populated
/// `api_keys` array. At v0 the only fields are `error_code` and
/// `api_keys` (Vec<ApiVersion>), encoded as i32-prefixed array of fixed
/// 6-byte ApiVersion entries (no tagged-fields trailer in either parent
/// or child).
#[test]
fn api_versions_response_data_round_trip_v0() {
    let original = ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 0, // Produce
                min_version: 0,
                max_version: 11,
                unknown_tagged_fields: Vec::new(),
            },
            ApiVersion {
                api_key: 18, // ApiVersions
                min_version: 0,
                max_version: 4,
                unknown_tagged_fields: Vec::new(),
            },
        ],
        throttle_time_ms: 0,
        supported_features: Vec::new(),
        finalized_features_epoch: -1,
        finalized_features: Vec::new(),
        zk_migration_ready: false,
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 0);
    // Wire layout for v0:
    //   error_code (i16 BE = 0)
    //   array length (i32 BE = 2)
    //   ApiVersion[0]: api_key=0, min=0, max=11 (3 i16 BE = 6 bytes)
    //   ApiVersion[1]: api_key=18, min=0, max=4 (6 bytes)
    // Total: 2 + 4 + 6 + 6 = 18 bytes
    assert_eq!(bytes.len(), 18);
    assert_eq!(&bytes[0..2], &0i16.to_be_bytes(), "error_code = 0");
    assert_eq!(&bytes[2..6], &2i32.to_be_bytes(), "array length = 2 (i32 BE at v0)");
    // ApiVersion[0]
    assert_eq!(&bytes[6..8], &0i16.to_be_bytes(), "api_keys[0].api_key");
    assert_eq!(&bytes[8..10], &0i16.to_be_bytes(), "api_keys[0].min_version");
    assert_eq!(&bytes[10..12], &11i16.to_be_bytes(), "api_keys[0].max_version");
    // ApiVersion[1]
    assert_eq!(&bytes[12..14], &18i16.to_be_bytes(), "api_keys[1].api_key");
    assert_eq!(&bytes[14..16], &0i16.to_be_bytes(), "api_keys[1].min_version");
    assert_eq!(&bytes[16..18], &4i16.to_be_bytes(), "api_keys[1].max_version");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 0).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v4 (highest supported, flexible) with all tagged fields
/// populated, exercising the most complex encoding paths: compact array
/// (length+1 varint), struct-typed tagged fields, int64 tagged field,
/// bool tagged field, and an unknown tagged field for forward-compat.
#[test]
fn api_versions_response_data_round_trip_v4_flexible_full() {
    let original = ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 0, min_version: 0, max_version: 11, unknown_tagged_fields: Vec::new() },
            ApiVersion { api_key: 18, min_version: 0, max_version: 4, unknown_tagged_fields: Vec::new() },
        ],
        throttle_time_ms: 250,
        supported_features: vec![
            SupportedFeatureKey {
                name: "metadata.version".to_string(),
                min_version: 1,
                max_version: 14,
                unknown_tagged_fields: Vec::new(),
            },
            SupportedFeatureKey {
                name: "kraft.version".to_string(),
                min_version: 0,
                max_version: 1,
                unknown_tagged_fields: Vec::new(),
            },
        ],
        finalized_features_epoch: 100,
        finalized_features: vec![FinalizedFeatureKey {
            name: "metadata.version".to_string(),
            max_version_level: 14,
            min_version_level: 14,
            unknown_tagged_fields: Vec::new(),
        }],
        zk_migration_ready: true,
        // unknown tag (99) for forward-compat — round-trips through the
        // unknown_tagged_fields catch-all.
        unknown_tagged_fields: vec![RawTaggedField::new(99, vec![0xDE, 0xAD])],
    };

    let bytes = encode(&original, 4);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 4).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v4 with the *minimum* payload — no API keys, no tagged
/// fields populated. Exercises the "all defaults" path: error_code = 0,
/// empty array (compact-encoded as varint(1)), no tagged fields.
#[test]
fn api_versions_response_data_round_trip_v4_flexible_empty() {
    let original = ApiVersionsResponseData::new();
    let bytes = encode(&original, 4);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ApiVersionsResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 4).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}
// =============================================================================
// MetadataRequestData (Phase 2d-3)
//
// `MetadataRequest.json` validVersions = 0-13, flexibleVersions = 9+.
// v0 is non-flexible, topics is non-nullable, no AllowAutoTopicCreation, no
// authorized-operations gates. v13 (highest) is flexible, has TopicId
// (uuid, v10+) on each topic, AllowAutoTopicCreation (v4+),
// IncludeTopicAuthorizedOperations (v8+), and topic Name is nullable
// (v10+). At v13 IncludeClusterAuthorizedOperations is gone (removed in v11+).
// =============================================================================

/// Round trip at v0 (lowest supported, non-flexible). At v0:
///   - Topics array is i32-prefixed (non-flexible) and non-nullable
///   - Each topic has only `Name` (i16-prefixed string)
///   - No `AllowAutoTopicCreation`, no authorized-operations bytes
///   - No tagged-fields trailer
#[test]
fn metadata_request_data_round_trip_v0() {
    let original = MetadataRequestData {
        topics: Some(vec![
            MetadataRequestTopic {
                topic_id: Uuid::zero(),
                name: Some("topic-1".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
            MetadataRequestTopic {
                topic_id: Uuid::zero(),
                name: Some("topic-2".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
        ]),
        // Defaults from MetadataRequestData::new() — none of these fields
        // exist on the wire at v0, but the round-trip must reproduce the
        // initial values.
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 0);

    // Wire layout for v0:
    //   topics array length (i32 BE = 2)
    //   topic[0]: name length (i16 BE = 7) + "topic-1" (7 bytes)
    //   topic[1]: name length (i16 BE = 7) + "topic-2" (7 bytes)
    // Total: 4 + (2 + 7) + (2 + 7) = 22 bytes.
    assert_eq!(bytes.len(), 22, "v0 MetadataRequest with 2 topics is 22 bytes");
    assert_eq!(&bytes[0..4], &2i32.to_be_bytes(), "topics array length = 2 (i32 BE)");
    assert_eq!(&bytes[4..6], &7i16.to_be_bytes(), "topic[0] name length");
    assert_eq!(&bytes[6..13], b"topic-1", "topic[0] name bytes");
    assert_eq!(&bytes[13..15], &7i16.to_be_bytes(), "topic[1] name length");
    assert_eq!(&bytes[15..22], b"topic-2", "topic[1] name bytes");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 0).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v13 (highest supported, flexible). Exercises:
///   - Compact varint-prefixed nullable topics array
///   - `topic_id: Uuid` (added v10) — populated with a non-zero random Uuid
///   - Nullable topic name (added v10) — round-trip the `None` path
///   - `allow_auto_topic_creation` (v4+) and
///     `include_topic_authorized_operations` (v8+)
///   - Tagged fields on the parent message AND on a nested topic struct
#[test]
fn metadata_request_data_round_trip_v13_flexible_full() {
    let topic_id_a = Uuid::random();
    let topic_id_b = Uuid::random();
    let original = MetadataRequestData {
        topics: Some(vec![
            // Topic looked up by name only — topic_id is the zero UUID.
            MetadataRequestTopic {
                topic_id: Uuid::zero(),
                name: Some("topic-by-name".to_string()),
                unknown_tagged_fields: vec![RawTaggedField::new(0, vec![0xAA])],
            },
            // Topic looked up by id only — name is null (v10+ nullable path).
            MetadataRequestTopic { topic_id: topic_id_a, name: None, unknown_tagged_fields: Vec::new() },
            // Topic with both id and name populated, mirroring the broker's
            // tolerance for either lookup key.
            MetadataRequestTopic {
                topic_id: topic_id_b,
                name: Some("topic-with-id".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
        ]),
        allow_auto_topic_creation: false,
        // include_cluster_authorized_operations is only present at v8-10;
        // at v13 it's not on the wire so its value does not affect round-trip.
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: true,
        unknown_tagged_fields: vec![RawTaggedField::new(99, vec![0xDE, 0xAD, 0xBE, 0xEF])],
    };

    let bytes = encode(&original, 13);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v13 with the `topics: None` (null array) path. Mirrors
/// "request metadata for all topics" in the v1+ broker semantics — encoded
/// as a compact varint(0).
#[test]
fn metadata_request_data_round_trip_v13_null_topics() {
    let original = MetadataRequestData {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 13);

    // Wire layout at v13 with null topics + all defaults:
    //   varint(0) for null topics                                (1 byte)
    //   bool allow_auto_topic_creation = true                    (1 byte)
    //   bool include_topic_authorized_operations = false         (1 byte)
    //   varint(0) tagged-fields trailer                          (1 byte)
    // Total: 4 bytes.
    assert_eq!(bytes.len(), 4, "v13 MetadataRequest null topics + all defaults = 4 bytes");
    assert_eq!(bytes[0], 0, "null topics encodes as varint(0)");
    assert_eq!(bytes[1], 1, "allow_auto_topic_creation = true");
    assert_eq!(bytes[2], 0, "include_topic_authorized_operations = false");
    assert_eq!(bytes[3], 0, "no tagged fields → varint(0)");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

// =============================================================================
// MetadataResponseData (Phase 2d-3)
//
// `MetadataResponse.json` validVersions = 0-13, flexibleVersions = 9+.
// This spec exercises the broadest set of encoding paths covered so far:
//   - Multiple topics, each with multiple partitions, each with multiple
//     replicas / ISRs / offline replicas
//   - Nullable Uuid (topic_id, v10+) — both zero and random values
//   - Nullable string with `default: "null"` (cluster_id v2+, broker.rack
//     v1+) — round-trip the `None` path
//   - i16/i32 primitives at every nesting level
// =============================================================================

/// Round trip at v0 (lowest supported, non-flexible). At v0:
///   - No throttle_time, no cluster_id, no controller_id, no top-level error
///   - Brokers have only NodeId/Host/Port (no rack)
///   - Topics have only ErrorCode/Name/Partitions (no topic_id, no
///     is_internal, no authorized_operations)
///   - Partitions have only ErrorCode/PartitionIndex/LeaderId/ReplicaNodes/
///     IsrNodes (no leader_epoch, no offline_replicas)
#[test]
fn metadata_response_data_round_trip_v0() {
    let original = MetadataResponseData {
        throttle_time_ms: 0,
        brokers: vec![
            MetadataResponseBroker {
                node_id: 1,
                host: "broker-1.example.com".to_string(),
                port: 9092,
                rack: None, // not on the wire at v0
                unknown_tagged_fields: Vec::new(),
            },
            MetadataResponseBroker {
                node_id: 2,
                host: "broker-2.example.com".to_string(),
                port: 9092,
                rack: None,
                unknown_tagged_fields: Vec::new(),
            },
        ],
        cluster_id: None,
        // Default from MetadataResponseData::new() (-1) — not on the wire at v0.
        controller_id: -1,
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some("the-topic".to_string()),
            topic_id: Uuid::zero(), // not on the wire at v0
            is_internal: false,     // not on the wire at v0
            partitions: vec![MetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: -1, // not on the wire at v0
                replica_nodes: vec![1, 2],
                isr_nodes: vec![1, 2],
                offline_replicas: Vec::new(), // not on the wire at v0
                unknown_tagged_fields: Vec::new(),
            }],
            topic_authorized_operations: -2147483648, // not on the wire at v0
            unknown_tagged_fields: Vec::new(),
        }],
        cluster_authorized_operations: -2147483648, // not on the wire at v0
        error_code: 0,                              // not on the wire at v0
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 0);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 0).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v13 (highest supported, flexible) with two topics, two
/// partitions per topic, two replicas per partition. Exercises every
/// per-version field, including `topic_id: Uuid` (random values),
/// nullable `cluster_id` and broker `rack` (the `default: "null"` path),
/// and tagged fields at every nesting level (top-level + topic + partition
/// + broker).
#[test]
fn metadata_response_data_round_trip_v13_flexible_full() {
    let topic_id_a = Uuid::random();
    let topic_id_b = Uuid::random();
    let original = MetadataResponseData {
        throttle_time_ms: 250,
        brokers: vec![
            MetadataResponseBroker {
                node_id: 1,
                host: "broker-1.example.com".to_string(),
                port: 9092,
                rack: Some("rack-a".to_string()),
                unknown_tagged_fields: vec![RawTaggedField::new(11, vec![0x11])],
            },
            MetadataResponseBroker {
                node_id: 2,
                host: "broker-2.example.com".to_string(),
                port: 9092,
                // Cover the default-null path at the broker-level rack.
                rack: None,
                unknown_tagged_fields: Vec::new(),
            },
        ],
        cluster_id: Some("test-cluster".to_string()),
        controller_id: 1,
        topics: vec![
            MetadataResponseTopic {
                error_code: 0,
                name: Some("topic-a".to_string()),
                topic_id: topic_id_a,
                is_internal: false,
                partitions: vec![
                    MetadataResponsePartition {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 5,
                        replica_nodes: vec![1, 2],
                        isr_nodes: vec![1, 2],
                        offline_replicas: vec![],
                        unknown_tagged_fields: vec![RawTaggedField::new(7, vec![0x77])],
                    },
                    MetadataResponsePartition {
                        error_code: 0,
                        partition_index: 1,
                        leader_id: 2,
                        leader_epoch: 5,
                        replica_nodes: vec![2, 1],
                        isr_nodes: vec![2, 1],
                        offline_replicas: vec![1],
                        unknown_tagged_fields: Vec::new(),
                    },
                ],
                // ClusterAuthorizedOperations is not on the wire at v13
                // (deprecated v11+), but TopicAuthorizedOperations is (v8+).
                topic_authorized_operations: 0xCAFEBABEu32 as i32,
                unknown_tagged_fields: vec![RawTaggedField::new(0, vec![0xA0])],
            },
            MetadataResponseTopic {
                error_code: 5, // LEADER_NOT_AVAILABLE
                // Cover the v12+ nullable name path.
                name: None,
                topic_id: topic_id_b,
                is_internal: true,
                partitions: vec![
                    MetadataResponsePartition {
                        error_code: 9, // REPLICA_NOT_AVAILABLE
                        partition_index: 0,
                        leader_id: -1,
                        leader_epoch: -1,
                        replica_nodes: vec![1, 2, 3],
                        isr_nodes: vec![1],
                        offline_replicas: vec![2, 3],
                        unknown_tagged_fields: Vec::new(),
                    },
                    MetadataResponsePartition {
                        error_code: 0,
                        partition_index: 1,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1, 2, 3],
                        isr_nodes: vec![1, 2, 3],
                        offline_replicas: vec![],
                        unknown_tagged_fields: Vec::new(),
                    },
                ],
                topic_authorized_operations: 0,
                unknown_tagged_fields: Vec::new(),
            },
        ],
        cluster_authorized_operations: -2147483648, // not on wire at v13
        error_code: 0,                              // top-level error v13+
        unknown_tagged_fields: vec![RawTaggedField::new(99, vec![0xDE, 0xAD, 0xBE, 0xEF])],
    };

    let bytes = encode(&original, 13);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);

    // The randomly generated topic ids must round-trip byte-for-byte —
    // explicit assertion to lock the nullable Uuid encoding path in.
    let decoded_a = decoded.topics.iter().find(|t| t.name.as_deref() == Some("topic-a")).unwrap();
    assert_eq!(decoded_a.topic_id, topic_id_a, "random topic_id must round-trip");
    let decoded_b = decoded.topics.iter().find(|t| t.name.is_none()).unwrap();
    assert_eq!(decoded_b.topic_id, topic_id_b, "random topic_id (null name) must round-trip");
}

/// Round trip at v13 with a top-level error and minimal payload — locks in
/// the v13-only `ErrorCode` encoding (i16 BE, just before the trailing
/// tagged-fields varint).
#[test]
fn metadata_response_data_round_trip_v13_top_level_error() {
    let original = MetadataResponseData {
        throttle_time_ms: 0,
        brokers: Vec::new(),
        cluster_id: None,
        controller_id: -1,
        topics: Vec::new(),
        cluster_authorized_operations: -2147483648,
        error_code: 41, // NOT_CONTROLLER
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 13);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = MetadataResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decoded.error_code, 41, "top-level v13 error code is preserved");
    assert_eq!(decode_accessor.remaining(), 0);
}

// =============================================================================
// ProduceRequestData (Phase 2d-4)
//
// `ProduceRequest.json` validVersions = 3-13, flexibleVersions = 9+.
// Versions 0-2 were removed in Apache Kafka 4.0 (v3 is the new baseline).
//
// Per-version behavior covered:
//   - v3 (lowest): non-flexible, transactional_id added
//   - v9: first flexible version (compact strings/arrays, tagged fields)
//   - v13 (highest): topic_id replaces topic name (KIP-516, name dropped at v13+)
//
// `Records` field is `Option<Vec<u8>>` placeholder (Phase 3 will replace with
// `MemoryRecords`). For round-trip tests, raw bytes are sufficient.
// =============================================================================

/// Round trip at v3 (lowest supported, non-flexible). At v3:
///   - transactional_id is i16-prefixed nullable string
///   - topics array is i32-prefixed
///   - topic name is i16-prefixed string (no topic_id)
///   - records is i32-prefixed nullable byte array
///   - no tagged-fields trailer
#[test]
fn produce_request_data_round_trip_v3() {
    let original = ProduceRequestData {
        transactional_id: Some("txn-1".to_string()),
        acks: -1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: "topic-a".to_string(),
            topic_id: Uuid::zero(), // not on the wire at v3
            partition_data: vec![
                PartitionProduceData { index: 0, records: Some(b"hello".to_vec()), unknown_tagged_fields: Vec::new() },
                PartitionProduceData {
                    index: 1,
                    // null records path
                    records: None,
                    unknown_tagged_fields: Vec::new(),
                },
            ],
            unknown_tagged_fields: Vec::new(),
        }],
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 3);

    // Wire layout for v3:
    //   transactional_id length (i16 BE = 5) + "txn-1" (5 bytes)
    //   acks (i16 BE = -1)
    //   timeout_ms (i32 BE = 30000)
    //   topic_data array length (i32 BE = 1)
    //   topic[0]:
    //     name length (i16 BE = 7) + "topic-a"
    //     partition_data array length (i32 BE = 2)
    //     partition[0]:
    //       index (i32 BE = 0)
    //       records length (i32 BE = 5) + "hello"
    //     partition[1]:
    //       index (i32 BE = 1)
    //       records length (i32 BE = -1, null sentinel)
    let txn_len = 5;
    let topic_name_len = 7;
    let expected_len = 2 + txn_len // transactional_id
        + 2 // acks
        + 4 // timeout_ms
        + 4 // topic_data length
        + 2 + topic_name_len // topic name
        + 4 // partition_data length
        + 4 + 4 + b"hello".len() // partition[0]
        + 4 + 4; // partition[1] (null records)
    assert_eq!(bytes.len(), expected_len, "v3 ProduceRequest expected size");
    assert_eq!(&bytes[0..2], &(txn_len as i16).to_be_bytes(), "transactional_id length");
    assert_eq!(&bytes[2..7], b"txn-1");
    assert_eq!(&bytes[7..9], &(-1i16).to_be_bytes(), "acks");
    assert_eq!(&bytes[9..13], &30_000i32.to_be_bytes(), "timeout_ms");

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 3).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v9 (first flexible version). Exercises:
///   - Compact varint-prefixed nullable transactional_id
///   - Compact varint-prefixed topics array
///   - Compact varint-prefixed nullable records
///   - Tagged fields at every nesting level
#[test]
fn produce_request_data_round_trip_v9_flexible() {
    let original = ProduceRequestData {
        // Null transactional_id (non-transactional producer).
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![
            TopicProduceData {
                name: "topic-a".to_string(),
                topic_id: Uuid::zero(), // not on the wire at v9 (v13+ only)
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(b"payload-a".to_vec()),
                    unknown_tagged_fields: vec![RawTaggedField::new(7, vec![0x77])],
                }],
                unknown_tagged_fields: vec![RawTaggedField::new(0, vec![0xA0])],
            },
            TopicProduceData {
                name: "topic-b".to_string(),
                topic_id: Uuid::zero(),
                partition_data: vec![
                    PartitionProduceData {
                        index: 0,
                        records: Some(vec![1, 2, 3, 4, 5]),
                        unknown_tagged_fields: Vec::new(),
                    },
                    PartitionProduceData { index: 1, records: None, unknown_tagged_fields: Vec::new() },
                ],
                unknown_tagged_fields: Vec::new(),
            },
        ],
        unknown_tagged_fields: vec![RawTaggedField::new(99, vec![0xDE, 0xAD])],
    };

    let bytes = encode(&original, 9);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 9).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v13 (highest supported, flexible) where topic name is
/// dropped from the wire (KIP-516) and `topic_id: Uuid` takes its place.
#[test]
fn produce_request_data_round_trip_v13_flexible_full() {
    let topic_id_a = Uuid::random();
    let topic_id_b = Uuid::random();
    let original = ProduceRequestData {
        transactional_id: Some("txn-with-id".to_string()),
        acks: -1,
        timeout_ms: 60_000,
        topic_data: vec![
            TopicProduceData {
                // At v13 the name is not encoded — round-trip must reproduce
                // the *initial* value (default empty string from new()).
                name: String::new(),
                topic_id: topic_id_a,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(b"records-for-topic-a".to_vec()),
                    unknown_tagged_fields: Vec::new(),
                }],
                unknown_tagged_fields: Vec::new(),
            },
            TopicProduceData {
                name: String::new(),
                topic_id: topic_id_b,
                partition_data: vec![
                    PartitionProduceData {
                        index: 0,
                        records: Some(vec![0xAA, 0xBB, 0xCC]),
                        unknown_tagged_fields: Vec::new(),
                    },
                    PartitionProduceData {
                        index: 1,
                        records: Some(Vec::new()), // empty records (not null)
                        unknown_tagged_fields: Vec::new(),
                    },
                ],
                unknown_tagged_fields: Vec::new(),
            },
        ],
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 13);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceRequestData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);

    let decoded_a = decoded.topic_data.iter().find(|t| t.topic_id == topic_id_a).unwrap();
    assert_eq!(
        decoded_a.partition_data[0].records.as_deref(),
        Some(b"records-for-topic-a".as_slice())
    );
}

// =============================================================================
// ProduceResponseData (Phase 2d-4)
//
// `ProduceResponse.json` validVersions = 3-13, flexibleVersions = 9+.
// Notable per-version fields:
//   - v3: ThrottleTimeMs added
//   - v8: RecordErrors and ErrorMessage added (KIP-467)
//   - v9: flexible versions
//   - v10: CurrentLeader (struct-typed tagged field) and NodeEndpoints
//          (array-typed tagged field) added (KIP-951)
//   - v13: TopicId replaces topic name (KIP-516)
// =============================================================================

/// Round trip at v3 (lowest supported, non-flexible). At v3:
///   - responses array is i32-prefixed
///   - topic name is i16-prefixed (no topic_id)
///   - partition fields: index, error_code, base_offset, log_append_time_ms
///   - no log_start_offset (added v5), no record_errors / error_message
///   - throttle_time_ms (added v1)
///   - no tagged-fields trailer
#[test]
fn produce_response_data_round_trip_v3() {
    let original = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            name: "topic-a".to_string(),
            topic_id: Uuid::zero(), // not on the wire at v3
            partition_responses: vec![
                PartitionProduceResponse {
                    index: 0,
                    error_code: 0,
                    base_offset: 12345,
                    log_append_time_ms: -1,
                    log_start_offset: -1, // not on the wire at v3
                    record_errors: Vec::new(),
                    error_message: None,
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                },
                PartitionProduceResponse {
                    index: 1,
                    error_code: 0,
                    base_offset: 67890,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                    record_errors: Vec::new(),
                    error_message: None,
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                },
            ],
            unknown_tagged_fields: Vec::new(),
        }],
        throttle_time_ms: 100,
        node_endpoints: Vec::new(), // not on the wire at v3
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 3);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 3).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v8 (highest non-flexible). Exercises RecordErrors and
/// ErrorMessage which were added in v8 (KIP-467) but before flexible
/// versions kicked in at v9.
#[test]
fn produce_response_data_round_trip_v8() {
    let original = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            name: "the-topic".to_string(),
            topic_id: Uuid::zero(),
            partition_responses: vec![PartitionProduceResponse {
                index: 0,
                error_code: 7, // CORRUPT_MESSAGE
                base_offset: -1,
                log_append_time_ms: -1,
                log_start_offset: 0,
                record_errors: vec![
                    BatchIndexAndErrorMessage {
                        batch_index: 2,
                        batch_index_error_message: Some("bad record at idx 2".to_string()),
                        unknown_tagged_fields: Vec::new(),
                    },
                    BatchIndexAndErrorMessage {
                        batch_index: 5,
                        batch_index_error_message: None, // null path
                        unknown_tagged_fields: Vec::new(),
                    },
                ],
                error_message: Some("batch dropped".to_string()),
                current_leader: LeaderIdAndEpoch::new(),
                unknown_tagged_fields: Vec::new(),
            }],
            unknown_tagged_fields: Vec::new(),
        }],
        throttle_time_ms: 0,
        node_endpoints: Vec::new(),
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 8);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 8).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);
}

/// Round trip at v10 (flexible + KIP-951). Exercises:
///   - CurrentLeader (struct-typed tagged field, tag 0)
///   - NodeEndpoints (array-typed tagged field, tag 0 at parent)
///   - All flexible-version paths
#[test]
fn produce_response_data_round_trip_v10_flexible_with_tagged_structs() {
    let original = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            name: "topic-a".to_string(),
            topic_id: Uuid::zero(),
            partition_responses: vec![PartitionProduceResponse {
                index: 0,
                error_code: 6, // NOT_LEADER_OR_FOLLOWER
                base_offset: -1,
                log_append_time_ms: -1,
                log_start_offset: -1,
                record_errors: Vec::new(),
                error_message: Some("not leader".to_string()),
                // Non-default CurrentLeader → emitted as tagged field 0.
                current_leader: LeaderIdAndEpoch { leader_id: 2, leader_epoch: 7, unknown_tagged_fields: Vec::new() },
                unknown_tagged_fields: Vec::new(),
            }],
            unknown_tagged_fields: Vec::new(),
        }],
        throttle_time_ms: 0,
        // Non-empty NodeEndpoints → emitted as parent-level tagged field 0.
        node_endpoints: vec![
            NodeEndpoint {
                node_id: 2,
                host: "broker-2.example.com".to_string(),
                port: 9092,
                rack: Some("rack-b".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
            NodeEndpoint {
                node_id: 3,
                host: "broker-3.example.com".to_string(),
                port: 9092,
                rack: None, // default-null path
                unknown_tagged_fields: Vec::new(),
            },
        ],
        unknown_tagged_fields: Vec::new(),
    };

    let bytes = encode(&original, 10);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 10).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);

    // Verify the tagged struct round-trip explicitly.
    assert_eq!(decoded.responses[0].partition_responses[0].current_leader.leader_id, 2);
    assert_eq!(decoded.responses[0].partition_responses[0].current_leader.leader_epoch, 7);
    assert_eq!(decoded.node_endpoints.len(), 2);
}

/// Round trip at v13 (highest supported, flexible). KIP-516 replaces topic
/// name with topic_id on the wire. Multiple topics + multiple partitions.
#[test]
fn produce_response_data_round_trip_v13_flexible_full() {
    let topic_id_a = Uuid::random();
    let topic_id_b = Uuid::random();
    let original = ProduceResponseData {
        responses: vec![
            TopicProduceResponse {
                // At v13 the name is not encoded — must reproduce default.
                name: String::new(),
                topic_id: topic_id_a,
                partition_responses: vec![
                    PartitionProduceResponse {
                        index: 0,
                        error_code: 0,
                        base_offset: 1000,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        record_errors: Vec::new(),
                        error_message: None,
                        current_leader: LeaderIdAndEpoch::new(),
                        unknown_tagged_fields: Vec::new(),
                    },
                    PartitionProduceResponse {
                        index: 1,
                        error_code: 0,
                        base_offset: 2000,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        record_errors: Vec::new(),
                        error_message: None,
                        current_leader: LeaderIdAndEpoch::new(),
                        unknown_tagged_fields: vec![RawTaggedField::new(99, vec![0x99])],
                    },
                ],
                unknown_tagged_fields: vec![RawTaggedField::new(7, vec![0x07, 0x77])],
            },
            TopicProduceResponse {
                name: String::new(),
                topic_id: topic_id_b,
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: 100, // UNKNOWN_TOPIC_ID
                    base_offset: -1,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                    record_errors: Vec::new(),
                    error_message: Some("unknown topic id".to_string()),
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                }],
                unknown_tagged_fields: Vec::new(),
            },
        ],
        throttle_time_ms: 250,
        node_endpoints: Vec::new(),
        unknown_tagged_fields: vec![RawTaggedField::new(123, vec![0xCA, 0xFE])],
    };

    let bytes = encode(&original, 13);

    let mut decode_accessor = ByteBufferAccessor::wrap(bytes);
    let mut decoded = ProduceResponseData::new();
    Message::read(&mut decoded, &mut decode_accessor, 13).expect("read succeeds");
    assert_eq!(decoded, original);
    assert_eq!(decode_accessor.remaining(), 0);

    // Topic IDs must round-trip byte-for-byte.
    let decoded_a = decoded.responses.iter().find(|t| t.topic_id == topic_id_a).unwrap();
    assert_eq!(decoded_a.partition_responses.len(), 2);
    let decoded_b = decoded.responses.iter().find(|t| t.topic_id == topic_id_b).unwrap();
    assert_eq!(decoded_b.partition_responses[0].error_code, 100);
}

// =============================================================================
// Byte-vector encoding fixtures captured from Apache Kafka 4.2.0 Java client
// (Phase 2 DoD requirement; see PLAN.md lines 184–186).
//
// These are the **gold-standard wire-compatibility tests**: each constant
// below is a hex string captured by running a one-off Java tool that builds
// the same `*Data` payload and serializes via
// `MessageUtil.toByteBufferAccessor(message, version).buffer()`. The Rust
// encoder must produce **byte-for-byte identical** output for the broker to
// accept the request.
//
// Capture tool source (NOT committed): `/tmp/kafka-fixture-capture/CaptureFixtures.java`.
// Compile and run:
//   javac -cp kafka-clients-4.2.0.jar:slf4j-api-1.7.36.jar CaptureFixtures.java
//   java  -cp .:kafka-clients-4.2.0.jar:slf4j-api-1.7.36.jar CaptureFixtures
//
// To re-capture (e.g. when bumping the Apache Kafka version), recreate that
// tool from the values constructed in each test below.
// =============================================================================

/// Decode a hex string into a `Vec<u8>`. Inline helper avoids pulling in the
/// `hex` crate just for these fixture tests.
fn hex_to_vec(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "hex string must have even length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

/// Helper: encode the given Rust message at the given version and assert the
/// bytes match the captured hex fixture.
fn assert_encodes_to<M: Message>(msg: &M, version: i16, expected_hex: &str, what: &str) {
    let bytes = encode(msg, version);
    let expected = hex_to_vec(expected_hex);
    assert_eq!(
        bytes, expected,
        "{} byte-vector encoding mismatch — captured from Apache Kafka 4.2.0 Java client",
        what
    );
}

// --- RequestHeader ----------------------------------------------------------

/// RequestHeader v2 fixture — locks G1 (per-field `flexibleVersions: none`
/// override on `client_id` forces length-prefixed encoding even on flexible
/// header versions). The `client_id` length is `0x000b` (i16), NOT
/// `varint(11+1)`, which is what the Java broker expects.
///
/// Captured from Java:
/// ```java
/// new RequestHeaderData()
///     .setRequestApiKey((short) 0)
///     .setRequestApiVersion((short) 11)
///     .setCorrelationId(42)
///     .setClientId("test-client");
/// MessageUtil.toByteBufferAccessor(data, (short) 2);
/// ```
const REQUEST_HEADER_V2_HEX: &str = "0000000b0000002a000b746573742d636c69656e7400";

#[test]
fn request_header_v2_byte_fixture_matches_java() {
    let data = RequestHeaderData {
        request_api_key: 0,
        request_api_version: 11,
        correlation_id: 42,
        client_id: Some("test-client".to_string()),
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 2, REQUEST_HEADER_V2_HEX, "RequestHeader v2 (G1 lock)");
}

// --- ResponseHeader ---------------------------------------------------------

/// ResponseHeader v0 fixture — non-flexible, exactly 4 bytes (i32 BE
/// correlation_id).
const RESPONSE_HEADER_V0_HEX: &str = "0000002a";

#[test]
fn response_header_v0_byte_fixture_matches_java() {
    let data = ResponseHeaderData { correlation_id: 42, unknown_tagged_fields: Vec::new() };
    assert_encodes_to(&data, 0, RESPONSE_HEADER_V0_HEX, "ResponseHeader v0");
}

/// ResponseHeader v1 fixture — flexible, 4 bytes correlation_id + 1-byte
/// varint(0) tagged-fields trailer = 5 bytes.
const RESPONSE_HEADER_V1_HEX: &str = "0000006300";

#[test]
fn response_header_v1_byte_fixture_matches_java() {
    let data = ResponseHeaderData { correlation_id: 99, unknown_tagged_fields: Vec::new() };
    assert_encodes_to(&data, 1, RESPONSE_HEADER_V1_HEX, "ResponseHeader v1");
}

// --- ApiVersionsRequest -----------------------------------------------------

/// ApiVersionsRequest v0 fixture — empty wire image (no fields exist at v0).
const API_VERSIONS_REQUEST_V0_HEX: &str = "";

#[test]
fn api_versions_request_v0_byte_fixture_matches_java() {
    let data = ApiVersionsRequestData::new();
    assert_encodes_to(&data, 0, API_VERSIONS_REQUEST_V0_HEX, "ApiVersionsRequest v0");
}

/// ApiVersionsRequest v4 fixture — flexible. compact strings + tagged trailer.
///   varint(11) "kafka-rust" varint(6) "0.1.0" varint(0)
const API_VERSIONS_REQUEST_V4_HEX: &str = "0b6b61666b612d7275737406302e312e3000";

#[test]
fn api_versions_request_v4_byte_fixture_matches_java() {
    let data = ApiVersionsRequestData {
        client_software_name: "kafka-rust".to_string(),
        client_software_version: "0.1.0".to_string(),
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 4, API_VERSIONS_REQUEST_V4_HEX, "ApiVersionsRequest v4");
}

// --- ApiVersionsResponse ----------------------------------------------------

/// ApiVersionsResponse v0 fixture — non-flexible, two ApiVersion entries.
///   error_code i16 = 0
///   array length i32 = 2
///   ApiVersion[0]: api_key=0, min=0, max=11
///   ApiVersion[1]: api_key=18, min=0, max=4
const API_VERSIONS_RESPONSE_V0_HEX: &str = "00000000000200000000000b001200000004";

#[test]
fn api_versions_response_v0_byte_fixture_matches_java() {
    let data = ApiVersionsResponseData {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 0, min_version: 0, max_version: 11, unknown_tagged_fields: Vec::new() },
            ApiVersion { api_key: 18, min_version: 0, max_version: 4, unknown_tagged_fields: Vec::new() },
        ],
        throttle_time_ms: 0,
        supported_features: Vec::new(),
        finalized_features_epoch: -1,
        finalized_features: Vec::new(),
        zk_migration_ready: false,
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 0, API_VERSIONS_RESPONSE_V0_HEX, "ApiVersionsResponse v0");
}

// --- MetadataRequest --------------------------------------------------------

/// MetadataRequest v0 fixture — non-flexible, two topics by name.
///   array length i32 = 2
///   topic[0]: name length i16 = 7, "topic-1"
///   topic[1]: name length i16 = 7, "topic-2"
const METADATA_REQUEST_V0_HEX: &str = "000000020007746f7069632d310007746f7069632d32";

#[test]
fn metadata_request_v0_byte_fixture_matches_java() {
    let data = MetadataRequestData {
        topics: Some(vec![
            MetadataRequestTopic {
                topic_id: Uuid::zero(),
                name: Some("topic-1".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
            MetadataRequestTopic {
                topic_id: Uuid::zero(),
                name: Some("topic-2".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
        ]),
        // Java's MetadataRequestData defaults to allow_auto_topic_creation = true.
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 0, METADATA_REQUEST_V0_HEX, "MetadataRequest v0");
}

/// MetadataRequest v13 fixture — flexible, null topics + defaults.
///   varint(0) for null topics
///   bool allow_auto_topic_creation = true (1)
///   bool include_topic_authorized_operations = false (0)
///   varint(0) tagged-fields trailer
const METADATA_REQUEST_V13_NULL_TOPICS_HEX: &str = "00010000";

#[test]
fn metadata_request_v13_null_topics_byte_fixture_matches_java() {
    let data = MetadataRequestData {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(
        &data,
        13,
        METADATA_REQUEST_V13_NULL_TOPICS_HEX,
        "MetadataRequest v13 null topics",
    );
}

// --- MetadataResponse -------------------------------------------------------

/// MetadataResponse v13 fixture with top-level error.
///   throttle_time_ms i32 = 0
///   brokers compact array varint(1)  (empty)
///   cluster_id varint(0) — null
///   controller_id i32 = -1
///   topics compact array varint(1)  (empty)
///   error_code i16 = 41 (NOT_CONTROLLER)
///   varint(0) tagged-fields trailer
const METADATA_RESPONSE_V13_TOP_LEVEL_ERROR_HEX: &str = "000000000100ffffffff01002900";

#[test]
fn metadata_response_v13_top_level_error_byte_fixture_matches_java() {
    let data = MetadataResponseData {
        throttle_time_ms: 0,
        brokers: Vec::new(),
        cluster_id: None,
        controller_id: -1,
        topics: Vec::new(),
        cluster_authorized_operations: -2147483648,
        error_code: 41, // NOT_CONTROLLER
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 13, METADATA_RESPONSE_V13_TOP_LEVEL_ERROR_HEX, "MetadataResponse v13");
}

// --- ProduceRequest ---------------------------------------------------------

/// ProduceRequest v3 fixture — non-flexible.
///   transactional_id length i16 = 5, "txn-1"
///   acks i16 = -1 (0xffff)
///   timeout_ms i32 = 30000 (0x00007530)
///   topic_data array length i32 = 1
///   topic[0]:
///     name length i16 = 7, "topic-a"
///     partition_data array length i32 = 2
///     partition[0]:
///       index i32 = 0
///       records length i32 = 5, "hello"
///     partition[1]:
///       index i32 = 1
///       records length i32 = -1 (null)
const PRODUCE_REQUEST_V3_HEX: &str =
    "000574786e2d31ffff00007530000000010007746f7069632d6100000002000000000000000568656c6c6f00000001ffffffff";

#[test]
fn produce_request_v3_byte_fixture_matches_java() {
    let data = ProduceRequestData {
        transactional_id: Some("txn-1".to_string()),
        acks: -1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: "topic-a".to_string(),
            topic_id: Uuid::zero(),
            partition_data: vec![
                PartitionProduceData { index: 0, records: Some(b"hello".to_vec()), unknown_tagged_fields: Vec::new() },
                PartitionProduceData { index: 1, records: None, unknown_tagged_fields: Vec::new() },
            ],
            unknown_tagged_fields: Vec::new(),
        }],
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 3, PRODUCE_REQUEST_V3_HEX, "ProduceRequest v3");
}

// --- ProduceResponse --------------------------------------------------------

/// ProduceResponse v3 fixture — non-flexible.
///   responses array length i32 = 1
///   topic[0]:
///     name length i16 = 7, "topic-a"
///     partition_responses array length i32 = 2
///     partition[0]: index=0, error_code=0, base_offset=12345, log_append_time=-1
///     partition[1]: index=1, error_code=0, base_offset=67890, log_append_time=-1
///   throttle_time_ms i32 = 100
const PRODUCE_RESPONSE_V3_HEX: &str = "000000010007746f7069632d61000000020000000000000000000000003039ffffffffffffffff0000000100000000000000010932ffffffffffffffff00000064";

#[test]
fn produce_response_v3_byte_fixture_matches_java() {
    let data = ProduceResponseData {
        responses: vec![TopicProduceResponse {
            name: "topic-a".to_string(),
            topic_id: Uuid::zero(),
            partition_responses: vec![
                PartitionProduceResponse {
                    index: 0,
                    error_code: 0,
                    base_offset: 12345,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                    record_errors: Vec::new(),
                    error_message: None,
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                },
                PartitionProduceResponse {
                    index: 1,
                    error_code: 0,
                    base_offset: 67890,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                    record_errors: Vec::new(),
                    error_message: None,
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                },
            ],
            unknown_tagged_fields: Vec::new(),
        }],
        throttle_time_ms: 100,
        node_endpoints: Vec::new(),
        unknown_tagged_fields: Vec::new(),
    };
    assert_encodes_to(&data, 3, PRODUCE_RESPONSE_V3_HEX, "ProduceResponse v3");
}
