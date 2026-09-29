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

//! Translated from `org.apache.kafka.clients.consumer.OffsetAndMetadataTest`.
//!
//! Skipped tests:
//! - `testSerializationRoundtrip`,
//!   `testDeserializationCompatibilityBeforeLeaderEpoch`,
//!   `testDeserializationCompatibilityWithLeaderEpoch` — these tests
//!   exercise Java's built-in `Serializable` framework (`ObjectInputStream` /
//!   `ObjectOutputStream`) and persisted-bytes files compatibility, which
//!   is a Java-platform-specific contract. Rust users serialize with serde
//!   or other crates; we don't expose a Java-compatible binary format.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use confluent_kafka::consumer::OffsetAndMetadata;

fn hash_of<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// Translated from `OffsetAndMetadataTest.testInvalidNegativeOffset`.
#[test]
fn test_invalid_negative_offset() {
    let err = OffsetAndMetadata::with_leader_epoch_metadata(-239, Some(15), String::new()).unwrap_err();
    assert!(err.message().contains("Invalid negative offset"), "got: {}", err.message());
}

/// Translated from `OffsetAndMetadataTest.testEqualsWithNullAndNegativeLeaderEpoch`.
#[test]
fn test_equals_with_null_and_negative_leader_epoch() {
    let metadata_with_null = OffsetAndMetadata::with_leader_epoch_metadata(100, None, "metadata").unwrap();
    let metadata_with_negative = OffsetAndMetadata::with_leader_epoch_metadata(100, Some(-1), "metadata").unwrap();
    assert_eq!(metadata_with_null, metadata_with_negative);
    assert_eq!(hash_of(&metadata_with_null), hash_of(&metadata_with_negative));
}

/// Translated from `OffsetAndMetadataTest.testEqualsWithNullAndEmptyMetadata`.
///
/// In Java, `null` metadata is normalized to `""` inside the constructor; in
/// Rust, the canonical "no metadata" representation is `""` directly (callers
/// pass it explicitly). The behavioral assertion — that empty-string metadata
/// equals empty-string metadata — is preserved.
#[test]
fn test_equals_with_null_and_empty_metadata() {
    // The Rust equivalent of passing `null` is passing `""` (the same value
    // Java normalizes null to). Both forms should be equal and have the
    // same hash.
    let metadata_with_empty_a = OffsetAndMetadata::with_leader_epoch_metadata(100, Some(1), "").unwrap();
    let metadata_with_empty_b = OffsetAndMetadata::with_leader_epoch_metadata(100, Some(1), "").unwrap();
    assert_eq!(metadata_with_empty_a, metadata_with_empty_b);
    assert_eq!(hash_of(&metadata_with_empty_a), hash_of(&metadata_with_empty_b));
}
