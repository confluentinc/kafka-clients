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
//! Phase 2d-1 covers `RequestHeaderData` only; Phase 2d-2/3/4 will add
//! tests for the remaining specs as they are wired up.

use crate::common::message::request_header_data::RequestHeaderData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::protocol::{Message, Readable};

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
