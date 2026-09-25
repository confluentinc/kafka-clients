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

//! `FetchResponse` wrapper around the auto-generated `FetchResponseData`.
//!
//! Translated from `org.apache.kafka.common.requests.FetchResponse`.
//!
//! Phase 7a-d only needs a subset of the Java surface — the client-side
//! accessors used by `FetchSessionHandler::handle_response` and
//! `AbstractFetch::handle_fetch_success`. Server-side construction helpers
//! (`of(Errors, throttleTimeMs, sessionId, ...)`) and broker-only methods
//! (`sizeOf`, `toMessage`) are intentionally out of scope.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use crate::FetchResponseData;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors};
use crate::fetch_response_data::PartitionData;

/// A FETCH RPC response.
///
/// Wraps the auto-generated [`FetchResponseData`] and exposes the
/// client-side accessors the consumer fetch path needs.
///
/// Corresponds to `org.apache.kafka.common.requests.FetchResponse`.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    data: FetchResponseData,
}

impl FetchResponse {
    /// Sentinel value for an invalid (uninitialized) high-watermark.
    pub const INVALID_HIGH_WATERMARK: i64 = -1;

    /// Sentinel value for an invalid last-stable-offset.
    pub const INVALID_LAST_STABLE_OFFSET: i64 = -1;

    /// Sentinel value for an invalid log-start-offset.
    pub const INVALID_LOG_START_OFFSET: i64 = -1;

    /// Sentinel value indicating that no preferred replica was returned.
    pub const INVALID_PREFERRED_REPLICA_ID: i32 = -1;

    /// Returns the records buffer from a [`PartitionData`], borrowing the
    /// underlying bytes — no copy.
    ///
    /// The Java equivalent is `FetchResponse.recordsOrFail(PartitionData)`
    /// which returns a non-null `Records`. The Rust translation returns
    /// `&[u8]`; an absent buffer yields an empty slice.
    ///
    /// **This is not the zero-copy entry point.** The slice itself is a borrow,
    /// but `MemoryRecords::readable_records` is `Bytes::copy_from_slice`, so
    /// anything that wraps this return value copies the whole partition payload.
    /// The §27 route is the `partition.records` field directly: it is already a
    /// refcounted [`bytes::Bytes`] slice of the response buffer, so cloning it is
    /// a refcount bump. `FetchCollector::initialize` takes that route; this helper
    /// exists to translate the Java method and for callers that only need to read
    /// the bytes in place.
    pub fn records_or_fail(partition: &PartitionData) -> &[u8] {
        partition.records.as_deref().unwrap_or(&[])
    }

    /// Returns the size in bytes of the partition's records, or 0 if absent.
    ///
    /// Translates `FetchResponse.recordsSize(PartitionData)`.
    pub fn records_size(partition: &PartitionData) -> i32 {
        partition.records.as_ref().map(|r| r.len() as i32).unwrap_or(0)
    }

    /// Returns true if the partition response carries a diverging-epoch entry
    /// the client should act on.
    ///
    /// Translates `FetchResponse.isDivergingEpoch(PartitionData)`.
    pub fn is_diverging_epoch(partition: &PartitionData) -> bool {
        partition.diverging_epoch.epoch >= 0
    }

    /// Returns the diverging-epoch entry if present.
    ///
    /// Translates `FetchResponse.divergingEpoch(PartitionData)`.
    pub fn diverging_epoch(partition: &PartitionData) -> Option<&crate::fetch_response_data::EpochEndOffset> {
        if partition.diverging_epoch.epoch < 0 {
            None
        } else {
            Some(&partition.diverging_epoch)
        }
    }

    /// Returns the preferred read-replica id if the broker advertised one.
    ///
    /// Translates `FetchResponse.preferredReadReplica(PartitionData)`.
    pub fn preferred_read_replica(partition: &PartitionData) -> Option<i32> {
        if partition.preferred_read_replica == Self::INVALID_PREFERRED_REPLICA_ID {
            None
        } else {
            Some(partition.preferred_read_replica)
        }
    }

    /// Returns true if the partition response carries a preferred-replica
    /// recommendation.
    ///
    /// Translates `FetchResponse.isPreferredReplica(PartitionData)`.
    pub fn is_preferred_replica(partition: &PartitionData) -> bool {
        partition.preferred_read_replica != Self::INVALID_PREFERRED_REPLICA_ID
    }

    /// Builds an empty partition response carrying the given error code.
    ///
    /// Translates `FetchResponse.partitionResponse(int, Errors)`.
    pub fn partition_response(partition: i32, error: Errors) -> PartitionData {
        let mut pd = PartitionData::new();
        pd.set_partition_index(partition);
        pd.set_error_code(error.code());
        pd.set_high_watermark(Self::INVALID_HIGH_WATERMARK);
        // Java sets records to MemoryRecords.EMPTY; the auto-generated Rust
        // PartitionData uses Option<Bytes>, so we leave it as `Some(Bytes::new())`
        // to mirror that "empty but non-null" semantics.
        pd.set_records(Some(bytes::Bytes::new()));
        pd
    }

    /// Constructs a `FetchResponse` from auto-generated data.
    ///
    /// Mirrors Java's private constructor (used by `parse` and `of`).
    pub fn new(data: FetchResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &FetchResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut FetchResponseData {
        &mut self.data
    }

    /// Returns the API key of this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FETCH
    }

    /// Returns the top-level error code as an [`Errors`].
    ///
    /// Translates `FetchResponse.error()`.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the fetch session id.
    ///
    /// Translates `FetchResponse.sessionId()`.
    pub fn session_id(&self) -> i32 {
        self.data.session_id
    }

    /// Returns the throttle time the broker recommends the client back off
    /// for, in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time the broker recommends the client back off
    /// for. Server-side helper; included for parity. Named
    /// `maybe_set_throttle_time_ms` to match Java's
    /// `FetchResponse.maybeSetThrottleTimeMs` and the rest of the
    /// `ConcreteResponse` dispatch surface.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle on this response.
    ///
    /// Java: `shouldClientThrottle(short version) { return version >= 8; }`.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 8
    }

    /// Returns the per-partition response data keyed by [`TopicPartition`]
    /// in the order they appear in the response (Java returns
    /// `LinkedHashMap`).
    ///
    /// For versions < 13, the topic name comes from the response body.
    /// For versions >= 13, it is resolved through the `topic_names` map; any
    /// partition whose topic name cannot be resolved is silently skipped
    /// (Java does the same — see `responseData(...)`).
    ///
    /// Translates `FetchResponse.responseData(Map<Uuid, String>, short)`.
    pub fn response_data(
        &self,
        topic_names: &HashMap<Uuid, String>,
        version: i16,
    ) -> IndexMap<TopicPartition, PartitionData> {
        let mut out: IndexMap<TopicPartition, PartitionData> = IndexMap::new();
        for topic_response in &self.data.responses {
            let name = if version < 13 {
                topic_response.topic.clone()
            } else {
                match topic_names.get(&topic_response.topic_id) {
                    Some(n) => n.clone(),
                    None => continue,
                }
            };
            for partition in &topic_response.partitions {
                out.insert(TopicPartition::new(name.clone(), partition.partition_index), partition.clone());
            }
        }
        out
    }

    /// Returns the set of [`TopicPartition`] keys present in this response,
    /// resolving topic names exactly as [`Self::response_data`] does but
    /// **without** cloning any [`PartitionData`] (and therefore without
    /// copying the per-partition record bytes).
    ///
    /// This is the keys-only equivalent of
    /// `self.response_data(topic_names, version).keys().cloned().collect()`,
    /// used by `FetchSessionHandler::handle_response`, which only needs the
    /// key set to validate the session and discards the payload (Phase 20
    /// Fix #2a). The version-gated topic-name resolution and the silent skip
    /// of unresolved v13+ topic IDs match `response_data` exactly.
    pub fn response_partition_keys(
        &self,
        topic_names: &HashMap<Uuid, String>,
        version: i16,
    ) -> HashSet<TopicPartition> {
        let mut out: HashSet<TopicPartition> = HashSet::new();
        for topic_response in &self.data.responses {
            let name = if version < 13 {
                topic_response.topic.clone()
            } else {
                match topic_names.get(&topic_response.topic_id) {
                    Some(n) => n.clone(),
                    None => continue,
                }
            };
            for partition in &topic_response.partitions {
                out.insert(TopicPartition::new(name.clone(), partition.partition_index));
            }
        }
        out
    }

    /// Consumes the response and returns the per-partition response data
    /// keyed by [`TopicPartition`], **moving** each [`PartitionData`] (and
    /// its owned record bytes) out of the response — no clone, no copy of
    /// the record buffer (§27 zero-copy receive contract, Phase 20 Fix #2b).
    ///
    /// Topic-name resolution and the silent skip of unresolved v13+ topic
    /// IDs match [`Self::response_data`] exactly; only the ownership differs
    /// (move vs clone).
    ///
    /// Translates the consuming variant of
    /// `FetchResponse.responseData(Map<Uuid, String>, short)`.
    pub fn into_response_data(
        self,
        topic_names: &HashMap<Uuid, String>,
        version: i16,
    ) -> IndexMap<TopicPartition, PartitionData> {
        let mut out: IndexMap<TopicPartition, PartitionData> = IndexMap::new();
        for topic_response in self.data.responses {
            let name = if version < 13 {
                topic_response.topic
            } else {
                match topic_names.get(&topic_response.topic_id) {
                    Some(n) => n.clone(),
                    None => continue,
                }
            };
            for partition in topic_response.partitions {
                out.insert(TopicPartition::new(name.clone(), partition.partition_index), partition);
            }
        }
        out
    }

    /// Returns the set of non-zero topic IDs reported in this response.
    ///
    /// The implementation does not gate on the protocol version — it
    /// simply filters out the zero UUID. On v12 the broker writes
    /// `Uuid::zero()` for every topic, so the resulting set is empty in
    /// practice; on v13+ the broker populates topic IDs, so the
    /// resulting set carries them. (A malformed v12 response that
    /// included a non-zero topic ID would still be reported — mirrors
    /// Java's behavior, which is also version-agnostic at this level.)
    ///
    /// Translates `FetchResponse.topicIds()`.
    pub fn topic_ids(&self) -> HashSet<Uuid> {
        let zero = Uuid::zero();
        self.data
            .responses
            .iter()
            .map(|r| r.topic_id)
            .filter(|id| *id != zero)
            .collect()
    }

    /// Aggregates the response error counts: top-level plus per-partition.
    ///
    /// Translates `FetchResponse.errorCounts()`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts: HashMap<Errors, i32> = HashMap::new();
        *counts.entry(self.error()).or_insert(0) += 1;
        for topic_response in &self.data.responses {
            for partition in &topic_response.partitions {
                *counts.entry(Errors::for_code(partition.error_code)).or_insert(0) += 1;
            }
        }
        counts
    }
}

impl std::fmt::Display for FetchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FetchResponse(error={:?}, sessionId={}, throttleTimeMs={})",
            self.error(),
            self.session_id(),
            self.throttle_time_ms()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetch_response_data::FetchableTopicResponse;

    fn make_response(error: Errors, session_id: i32, throttle: i32) -> FetchResponse {
        let mut data = FetchResponseData::new();
        data.set_error_code(error.code());
        data.set_session_id(session_id);
        data.set_throttle_time_ms(throttle);
        FetchResponse::new(data)
    }

    #[test]
    fn test_top_level_error_passthrough() {
        let r = make_response(Errors::FetchSessionIdNotFound, 0, 0);
        assert_eq!(Errors::FetchSessionIdNotFound, r.error());
        assert_eq!(0, r.session_id());
        assert_eq!(0, r.throttle_time_ms());
    }

    #[test]
    fn test_throttle_time_setter() {
        let mut r = make_response(Errors::None, 5, 0);
        r.maybe_set_throttle_time_ms(42);
        assert_eq!(42, r.throttle_time_ms());
    }

    #[test]
    fn test_should_client_throttle() {
        let r = make_response(Errors::None, 0, 0);
        assert!(!r.should_client_throttle(7));
        assert!(r.should_client_throttle(8));
        assert!(r.should_client_throttle(15));
    }

    #[test]
    fn test_topic_ids_excludes_zero_uuid() {
        let id1 = Uuid::new(1, 1);
        let id2 = Uuid::new(2, 2);
        let mut t1 = FetchableTopicResponse::new();
        t1.set_topic_id(id1);
        let mut t2 = FetchableTopicResponse::new();
        t2.set_topic_id(Uuid::zero());
        let mut t3 = FetchableTopicResponse::new();
        t3.set_topic_id(id2);

        let mut data = FetchResponseData::new();
        data.set_responses(vec![t1, t2, t3]);
        let r = FetchResponse::new(data);
        let ids = r.topic_ids();
        assert_eq!(2, ids.len());
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_response_data_v12_uses_topic_field() {
        let mut p = PartitionData::new();
        p.set_partition_index(3);
        let mut t = FetchableTopicResponse::new();
        t.set_topic("name-in-response".to_string());
        t.set_partitions(vec![p]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        let map = r.response_data(&HashMap::new(), 12);
        assert_eq!(1, map.len());
        let (tp, _) = map.iter().next().unwrap();
        assert_eq!("name-in-response", tp.topic());
        assert_eq!(3, tp.partition());
    }

    #[test]
    fn test_response_data_v13_resolves_via_topic_names() {
        let topic_id = Uuid::new(7, 7);
        let mut p = PartitionData::new();
        p.set_partition_index(0);
        let mut t = FetchableTopicResponse::new();
        t.set_topic_id(topic_id);
        t.set_partitions(vec![p]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        let mut topic_names = HashMap::new();
        topic_names.insert(topic_id, "resolved".to_string());
        let map = r.response_data(&topic_names, 13);
        let (tp, _) = map.iter().next().unwrap();
        assert_eq!("resolved", tp.topic());
    }

    #[test]
    fn test_response_data_v13_skips_unresolved_topic_id() {
        let topic_id = Uuid::new(8, 8);
        let mut p = PartitionData::new();
        p.set_partition_index(0);
        let mut t = FetchableTopicResponse::new();
        t.set_topic_id(topic_id);
        t.set_partitions(vec![p]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        let map = r.response_data(&HashMap::new(), 13);
        assert!(map.is_empty(), "Unresolved topic ID must be skipped on v13+");
    }

    #[test]
    fn test_records_or_fail_returns_empty_slice_when_absent() {
        let mut p = PartitionData::new();
        p.set_records(None);
        assert_eq!(0, FetchResponse::records_or_fail(&p).len());
        assert_eq!(0, FetchResponse::records_size(&p));
    }

    #[test]
    fn test_records_or_fail_borrows_bytes() {
        let bytes = vec![1u8, 2, 3, 4, 5];
        let mut p = PartitionData::new();
        p.set_records(Some(bytes::Bytes::from(bytes.clone())));
        // Zero-copy borrow — the returned slice points into p.records.
        let borrowed = FetchResponse::records_or_fail(&p);
        assert_eq!(&bytes[..], borrowed);
        assert_eq!(5, FetchResponse::records_size(&p));
    }

    #[test]
    fn test_diverging_epoch_present_and_absent() {
        let mut p_present = PartitionData::new();
        let mut ee = crate::fetch_response_data::EpochEndOffset::new();
        ee.set_epoch(3);
        ee.set_end_offset(100);
        p_present.set_diverging_epoch(ee);
        assert!(FetchResponse::is_diverging_epoch(&p_present));
        assert_eq!(3, FetchResponse::diverging_epoch(&p_present).unwrap().epoch);

        let p_absent = PartitionData::new();
        assert!(!FetchResponse::is_diverging_epoch(&p_absent));
        assert!(FetchResponse::diverging_epoch(&p_absent).is_none());
    }

    #[test]
    fn test_preferred_read_replica_present_and_absent() {
        let mut p_present = PartitionData::new();
        p_present.set_preferred_read_replica(2);
        assert!(FetchResponse::is_preferred_replica(&p_present));
        assert_eq!(Some(2), FetchResponse::preferred_read_replica(&p_present));

        let mut p_absent = PartitionData::new();
        p_absent.set_preferred_read_replica(FetchResponse::INVALID_PREFERRED_REPLICA_ID);
        assert!(!FetchResponse::is_preferred_replica(&p_absent));
        assert!(FetchResponse::preferred_read_replica(&p_absent).is_none());
    }

    #[test]
    fn test_partition_response_factory() {
        let pd = FetchResponse::partition_response(7, Errors::OffsetOutOfRange);
        assert_eq!(7, pd.partition_index);
        assert_eq!(Errors::OffsetOutOfRange.code(), pd.error_code);
        assert_eq!(FetchResponse::INVALID_HIGH_WATERMARK, pd.high_watermark);
        assert_eq!(Some(0), pd.records.as_ref().map(|v| v.len()));
    }

    fn partition_with_records(index: i32, offset: i64, records: Vec<u8>) -> PartitionData {
        let mut p = PartitionData::new();
        p.set_partition_index(index);
        p.set_high_watermark(offset + 1);
        p.set_records(Some(bytes::Bytes::from(records)));
        p
    }

    /// Phase 20 Fix #2a: `response_partition_keys` must produce exactly the
    /// same key set as the old `response_data(...).keys().cloned().collect()`,
    /// for both v12 (topic in body) and v13+ (topic resolved via topic_names),
    /// including the silent skip of unresolved v13+ topic IDs.
    #[test]
    fn test_response_partition_keys_matches_response_data_keys_v12() {
        let mut t = FetchableTopicResponse::new();
        t.set_topic("topic-a".to_string());
        t.set_partitions(vec![
            partition_with_records(0, 0, vec![1, 2, 3]),
            partition_with_records(1, 10, vec![4, 5, 6, 7]),
        ]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        let expected: HashSet<TopicPartition> = r.response_data(&HashMap::new(), 12).keys().cloned().collect();
        let keys = r.response_partition_keys(&HashMap::new(), 12);
        assert_eq!(expected, keys);
        assert_eq!(2, keys.len());
        assert!(keys.contains(&TopicPartition::new("topic-a", 0)));
        assert!(keys.contains(&TopicPartition::new("topic-a", 1)));
    }

    #[test]
    fn test_response_partition_keys_matches_response_data_keys_v13_with_skip() {
        let resolved = Uuid::new(1, 1);
        let unresolved = Uuid::new(9, 9);
        let mut t_ok = FetchableTopicResponse::new();
        t_ok.set_topic_id(resolved);
        t_ok.set_partitions(vec![partition_with_records(0, 0, vec![1, 2, 3])]);
        let mut t_skip = FetchableTopicResponse::new();
        t_skip.set_topic_id(unresolved);
        t_skip.set_partitions(vec![partition_with_records(0, 0, vec![8, 8])]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t_ok, t_skip]);
        let r = FetchResponse::new(data);

        let mut topic_names = HashMap::new();
        topic_names.insert(resolved, "resolved".to_string());

        let expected: HashSet<TopicPartition> = r.response_data(&topic_names, 13).keys().cloned().collect();
        let keys = r.response_partition_keys(&topic_names, 13);
        assert_eq!(expected, keys);
        assert_eq!(1, keys.len(), "unresolved v13+ topic must be skipped, same as response_data");
        assert!(keys.contains(&TopicPartition::new("resolved", 0)));
    }

    /// Phase 20 Fix #2b: `into_response_data` MOVES each `PartitionData` out
    /// of the response (consuming it). The resulting map must carry exactly
    /// the same keys and record bytes as the cloning `response_data`, proving
    /// no record is dropped, duplicated, or corrupted by the move.
    #[test]
    fn test_into_response_data_moves_records_intact() {
        let recs0 = vec![10u8, 11, 12];
        let recs1 = vec![20u8, 21, 22, 23, 24];
        let mut t = FetchableTopicResponse::new();
        t.set_topic("topic-a".to_string());
        t.set_partitions(vec![
            partition_with_records(0, 100, recs0.clone()),
            partition_with_records(1, 200, recs1.clone()),
        ]);
        let mut data = FetchResponseData::new();
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        // Reference (cloning) map to compare against.
        let cloned = r.response_data(&HashMap::new(), 12);
        // Consuming map (moves the PartitionData payloads out).
        let moved = r.into_response_data(&HashMap::new(), 12);

        assert_eq!(cloned.len(), moved.len());
        // Same keys, same order (IndexMap preserves insertion order).
        let cloned_keys: Vec<&TopicPartition> = cloned.keys().collect();
        let moved_keys: Vec<&TopicPartition> = moved.keys().collect();
        assert_eq!(cloned_keys, moved_keys);

        let p0 = &moved[&TopicPartition::new("topic-a", 0)];
        let p1 = &moved[&TopicPartition::new("topic-a", 1)];
        assert_eq!(recs0.as_slice(), FetchResponse::records_or_fail(p0));
        assert_eq!(recs1.as_slice(), FetchResponse::records_or_fail(p1));
        assert_eq!(101, p0.high_watermark);
        assert_eq!(201, p1.high_watermark);
    }

    #[test]
    fn test_error_counts_aggregates_top_level_and_partitions() {
        let mut p_ok = PartitionData::new();
        p_ok.set_error_code(Errors::None.code());
        let mut p_err = PartitionData::new();
        p_err.set_error_code(Errors::NotLeaderOrFollower.code());
        let mut t = FetchableTopicResponse::new();
        t.set_partitions(vec![p_ok, p_err.clone()]);
        let mut data = FetchResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_responses(vec![t]);
        let r = FetchResponse::new(data);

        let counts = r.error_counts();
        // top-level None + partition None = 2 None; partition NotLeader = 1
        assert_eq!(2, *counts.get(&Errors::None).unwrap());
        assert_eq!(1, *counts.get(&Errors::NotLeaderOrFollower).unwrap());
    }
}
