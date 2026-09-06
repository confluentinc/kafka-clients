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

//! `FetchRequest` wrapper around the auto-generated `FetchRequestData`.
//!
//! Translated from `org.apache.kafka.common.requests.FetchRequest`.
//!
//! Mirrors the `MetadataRequest` precedent (composed of an auto-generated
//! `*Data` struct plus a `Builder`). The Phase 7a-d consumer path only needs
//! the consumer-side surface; broker-side `forReplica`, RaftClient
//! `SimpleBuilder`, and follower-only methods are intentionally not
//! translated.

#![allow(dead_code)]

use std::collections::HashMap;

use indexmap::IndexMap;

use crate::common::IsolationLevel;
use crate::common::TopicIdPartition;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::ApiKeys;
use crate::common::requests::fetch_metadata::FetchMetadata;
use crate::fetch_request_data::{FetchPartition, FetchRequestData, FetchTopic, ForgottenTopic};

/// The replica id used by ordinary consumers.
pub const CONSUMER_REPLICA_ID: i32 = -1;

/// Default response max bytes (used for versions that lack a request-level
/// limit). Mirrors Java's `Integer.MAX_VALUE`.
pub const DEFAULT_RESPONSE_MAX_BYTES: i32 = i32::MAX;

/// Sentinel value for a missing log-start offset on a fetch request.
pub const INVALID_LOG_START_OFFSET: i64 = -1;

/// Sentinel value indicating that the partition leader epoch is unknown.
///
/// Mirrors `org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH`.
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// Per-partition fetch state carried in the request.
///
/// Corresponds to `FetchRequest.PartitionData` in Java.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionData {
    /// Topic ID (may be the zero UUID for versions that don't support it).
    pub topic_id: Uuid,
    /// Offset at which to start fetching.
    pub fetch_offset: i64,
    /// Earliest offset the broker should keep. -1 disables.
    pub log_start_offset: i64,
    /// Maximum bytes to return for this partition.
    pub max_bytes: i32,
    /// Current leader epoch known to the client, or `None` if unknown.
    pub current_leader_epoch: Option<i32>,
    /// Last leader epoch the client read from, or `None` if unknown.
    pub last_fetched_epoch: Option<i32>,
}

impl PartitionData {
    /// Constructs a partition entry without a `last_fetched_epoch`.
    ///
    /// Translates the 5-arg Java constructor.
    pub fn new(
        topic_id: Uuid,
        fetch_offset: i64,
        log_start_offset: i64,
        max_bytes: i32,
        current_leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            topic_id,
            fetch_offset,
            log_start_offset,
            max_bytes,
            current_leader_epoch,
            last_fetched_epoch: None,
        }
    }

    /// Constructs a partition entry with all fields explicit.
    ///
    /// Translates the 6-arg Java constructor.
    pub fn new_last_fetched_epoch(
        topic_id: Uuid,
        fetch_offset: i64,
        log_start_offset: i64,
        max_bytes: i32,
        current_leader_epoch: Option<i32>,
        last_fetched_epoch: Option<i32>,
    ) -> Self {
        Self {
            topic_id,
            fetch_offset,
            log_start_offset,
            max_bytes,
            current_leader_epoch,
            last_fetched_epoch,
        }
    }
}

/// A FETCH RPC, wrapping the auto-generated [`FetchRequestData`] plus the
/// fetch session metadata derived from it.
///
/// Corresponds to `org.apache.kafka.common.requests.FetchRequest`.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    data: FetchRequestData,
    version: i16,
    metadata: FetchMetadata,
}

impl FetchRequest {
    /// Constructs a `FetchRequest` from data + version. The fetch-session
    /// metadata is derived from `data.session_id()` / `data.session_epoch()`.
    pub fn new(data: FetchRequestData, version: i16) -> Self {
        let metadata = FetchMetadata::new(data.session_id, data.session_epoch);
        Self { data, version, metadata }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &FetchRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut FetchRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FETCH
    }

    /// Returns the maximum wait time the server should hold the response
    /// (`max.wait.ms` parity).
    pub fn max_wait(&self) -> i32 {
        self.data.max_wait_ms
    }

    /// Returns the minimum number of bytes the server should return.
    pub fn min_bytes(&self) -> i32 {
        self.data.min_bytes
    }

    /// Returns the maximum number of bytes the server should return.
    pub fn max_bytes(&self) -> i32 {
        self.data.max_bytes
    }

    /// Returns the replica id of this fetch request.
    ///
    /// On v15+, the `replicaId` lives inside `replica_state`.
    pub fn replica_id(&self) -> i32 {
        if self.version < 15 {
            self.data.replica_id
        } else {
            self.data.replica_state.replica_id
        }
    }

    /// Returns whether this request comes from a broker follower.
    pub fn is_from_follower(&self) -> bool {
        self.replica_id() >= 0
    }

    /// Returns the request's isolation level.
    ///
    /// # Errors
    ///
    /// Returns an error if the encoded isolation level is unknown.
    pub fn isolation_level(&self) -> Result<IsolationLevel, crate::common::Error> {
        IsolationLevel::for_id(self.data.isolation_level as u8)
    }

    /// Returns the fetch session metadata derived from `session_id` and
    /// `session_epoch`.
    pub fn metadata(&self) -> FetchMetadata {
        self.metadata
    }

    /// Returns the rack id carried in the request.
    pub fn rack_id(&self) -> &str {
        &self.data.rack_id
    }

    /// Returns an error response with the top-level error code set.
    ///
    /// Translates Java's `getErrorResponse(int throttleTimeMs, Throwable e)`.
    /// The Java implementation also walks per-topic per-partition data
    /// and stamps the error code on each entry for v<13 (see
    /// `FetchRequest.java:342-380`). This Rust translation only sets the
    /// top-level error/session-id because the KIP-848 consumer always
    /// negotiates v12+ where the per-partition stamping is redundant
    /// (the per-partition status is already absent on the wire). If a
    /// caller ever needs to construct error responses for v<13 wire,
    /// translate the per-partition walk at that point.
    pub fn get_error_response(
        &self,
        throttle_time_ms: i32,
        error: &crate::common::protocol::Errors,
    ) -> crate::common::requests::ConcreteResponse {
        let mut data = crate::fetch_response_data::FetchResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(error.code());
        data.set_session_id(self.metadata.session_id());
        crate::common::requests::ConcreteResponse::Fetch(crate::common::requests::FetchResponse::new(data))
    }
}

impl std::fmt::Display for FetchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FetchRequest(version={}, metadata={})", self.version, self.metadata)
    }
}

/// Builder for [`FetchRequest`].
///
/// Mirrors `FetchRequest.Builder` in Java. Only the consumer flavor is
/// translated; `forReplica` and `SimpleBuilder` are out of scope per
/// `consumer-threading.md` §20.
#[derive(Debug, Clone)]
pub struct FetchRequestBuilder {
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
    replica_id: i32,
    replica_epoch: i64,
    max_wait: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: IsolationLevel,
    metadata: FetchMetadata,
    rack_id: String,
    /// Insertion-ordered map from `TopicPartition` to `PartitionData` — Java
    /// uses a `LinkedHashMap` here so that the wire-protocol ordering is
    /// deterministic across rebuilds.
    to_fetch: IndexMap<TopicPartition, PartitionData>,
    removed: Vec<TopicIdPartition>,
    replaced: Vec<TopicIdPartition>,
}

impl FetchRequestBuilder {
    /// Creates a builder configured for a consumer fetch.
    ///
    /// Translates `FetchRequest.Builder.forConsumer(maxVersion, maxWait, minBytes, fetchData)`.
    pub fn for_consumer(
        max_version: i16,
        max_wait: i32,
        min_bytes: i32,
        fetch_data: IndexMap<TopicPartition, PartitionData>,
    ) -> Self {
        Self {
            oldest_allowed_version: ApiKeys::FETCH.oldest_version(),
            latest_allowed_version: max_version,
            replica_id: CONSUMER_REPLICA_ID,
            replica_epoch: -1,
            max_wait,
            min_bytes,
            max_bytes: DEFAULT_RESPONSE_MAX_BYTES,
            isolation_level: IsolationLevel::ReadUncommitted,
            metadata: FetchMetadata::LEGACY,
            rack_id: String::new(),
            to_fetch: fetch_data,
            removed: Vec::new(),
            replaced: Vec::new(),
        }
    }

    /// Sets the isolation level.
    pub fn set_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Returns the current metadata. Visible for testing.
    pub fn metadata(&self) -> FetchMetadata {
        self.metadata
    }

    /// Sets the fetch session metadata.
    pub fn set_metadata(mut self, metadata: FetchMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the rack id.
    pub fn set_rack_id(mut self, rack_id: impl Into<String>) -> Self {
        self.rack_id = rack_id.into();
        self
    }

    /// Returns a reference to the fetch-data map.
    pub fn fetch_data(&self) -> &IndexMap<TopicPartition, PartitionData> {
        &self.to_fetch
    }

    /// Sets the per-request response cap.
    pub fn set_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Returns the removed-partitions list.
    ///
    /// Translates `FetchRequest.Builder.removed()`.
    pub fn removed(&self) -> &[TopicIdPartition] {
        &self.removed
    }

    /// Sets the removed-partitions list.
    pub fn set_removed(mut self, removed: Vec<TopicIdPartition>) -> Self {
        self.removed = removed;
        self
    }

    /// Returns the replaced-partitions list.
    ///
    /// Translates `FetchRequest.Builder.replaced()`.
    pub fn replaced(&self) -> &[TopicIdPartition] {
        &self.replaced
    }

    /// Sets the replaced-partitions list.
    pub fn set_replaced(mut self, replaced: Vec<TopicIdPartition>) -> Self {
        self.replaced = replaced;
        self
    }

    /// Returns the oldest allowed version for the produced request.
    pub fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    /// Returns the latest allowed version for the produced request.
    pub fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    /// Builds the request at the latest allowed version.
    pub fn build(&self) -> FetchRequest {
        self.build_version(self.latest_allowed_version)
    }

    /// Builds the request at the given version.
    pub fn build_version(&self, version: i16) -> FetchRequest {
        let effective_max_bytes = if version < 3 {
            DEFAULT_RESPONSE_MAX_BYTES
        } else {
            self.max_bytes
        };

        let mut data = FetchRequestData::new();
        data.set_max_wait_ms(self.max_wait);
        data.set_min_bytes(self.min_bytes);
        data.set_max_bytes(effective_max_bytes);
        data.set_isolation_level(self.isolation_level.id() as i8);
        data.set_forgotten_topics_data(Vec::new());
        if version < 15 {
            data.set_replica_id(self.replica_id);
        } else {
            let mut replica_state = crate::fetch_request_data::ReplicaState::new();
            replica_state.set_replica_id(self.replica_id);
            replica_state.set_replica_epoch(self.replica_epoch);
            data.set_replica_state(replica_state);
        }

        // Build the forgotten-topics list, preserving Java's grouping by
        // topic name via a LinkedHashMap. We use IndexMap here too for
        // deterministic wire ordering.
        let mut forgotten: IndexMap<String, ForgottenTopic> = IndexMap::new();
        add_to_forgotten_topic_map(&self.removed, &mut forgotten);
        // For versions older than 13, replaced partitions are not sent in
        // the forget set in order to avoid removing the newly added
        // partition in the fetch set.
        if version >= 13 {
            add_to_forgotten_topic_map(&self.replaced, &mut forgotten);
        }
        data.set_forgotten_topics_data(forgotten.into_iter().map(|(_, ft)| ft).collect());

        // Build the fetch topics list. The Java implementation groups
        // partitions into a `FetchTopic` only when they appear sequentially
        // in `to_fetch`; we preserve that exact behavior.
        data.set_topics(Vec::new());
        let mut current_topic_idx: Option<usize> = None;
        for (topic_partition, partition_data) in &self.to_fetch {
            let same_topic = current_topic_idx
                .map(|idx| data.topics[idx].topic == *topic_partition.topic())
                .unwrap_or(false);
            if !same_topic {
                let mut ft = FetchTopic::new();
                ft.set_topic(topic_partition.topic().to_string());
                ft.set_topic_id(partition_data.topic_id);
                ft.set_partitions(Vec::new());
                data.topics.push(ft);
                current_topic_idx = Some(data.topics.len() - 1);
            }

            let mut fp = FetchPartition::new();
            fp.set_partition(topic_partition.partition());
            fp.set_current_leader_epoch(partition_data.current_leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH));
            fp.set_last_fetched_epoch(partition_data.last_fetched_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH));
            fp.set_fetch_offset(partition_data.fetch_offset);
            fp.set_log_start_offset(partition_data.log_start_offset);
            fp.set_partition_max_bytes(partition_data.max_bytes);
            // Safe: we just pushed onto `topics` above on the !same_topic
            // branch, and `current_topic_idx` is otherwise unchanged.
            let idx = current_topic_idx.expect("at least one topic group exists after first fetch entry");
            data.topics[idx].partitions.push(fp);
        }

        data.set_session_epoch(self.metadata.epoch());
        data.set_session_id(self.metadata.session_id());
        data.set_rack_id(self.rack_id.clone());

        FetchRequest::new(data, version)
    }
}

/// Helper used by `build_version` to bucket a list of `TopicIdPartition`s
/// into per-topic `ForgottenTopic` entries.
fn add_to_forgotten_topic_map(to_forget: &[TopicIdPartition], forgotten_map: &mut IndexMap<String, ForgottenTopic>) {
    for tip in to_forget {
        let topic = tip.topic().to_string();
        let entry = forgotten_map.entry(topic.clone()).or_insert_with(|| {
            let mut ft = ForgottenTopic::new();
            ft.set_topic(topic);
            ft.set_topic_id(tip.topic_id());
            ft.set_partitions(Vec::new());
            ft
        });
        entry.partitions.push(tip.partition());
    }
}

/// Resolves the replica id from raw fetch-request data, handling v15+ which
/// stores it in `replica_state`. Mirrors Java's `FetchRequest.replicaId(...)`.
pub fn replica_id_from(data: &FetchRequestData) -> i32 {
    if data.replica_id != -1 {
        data.replica_id
    } else {
        data.replica_state.replica_id
    }
}

/// Returns true if the broker id is non-negative.
pub fn is_valid_broker_id(broker_id: i32) -> bool {
    broker_id >= 0
}

/// Returns true if the replica id identifies a consumer (rather than a broker
/// follower or future-local replica).
///
/// Mirrors Java's `FetchRequest.isConsumer(int)`.
pub fn is_consumer(replica_id: i32) -> bool {
    const FUTURE_LOCAL_REPLICA_ID: i32 = -3;
    replica_id < 0 && replica_id != FUTURE_LOCAL_REPLICA_ID
}

/// Returns a human-readable description of a replica id.
///
/// Mirrors Java's `FetchRequest.describeReplicaId(int)`.
pub fn describe_replica_id(replica_id: i32) -> String {
    const ORDINARY_CONSUMER_ID: i32 = -1;
    const DEBUGGING_CONSUMER_ID: i32 = -2;
    const FUTURE_LOCAL_REPLICA_ID: i32 = -3;
    match replica_id {
        ORDINARY_CONSUMER_ID => "consumer".to_string(),
        DEBUGGING_CONSUMER_ID => "debug consumer".to_string(),
        FUTURE_LOCAL_REPLICA_ID => "future local replica".to_string(),
        _ if is_valid_broker_id(replica_id) => format!("replica [{replica_id}]"),
        _ => format!("invalid replica [{replica_id}]"),
    }
}

/// For versions < 13, builds the partition data map using only the request
/// data; for versions >= 13, also consults the topic-id-to-name map.
///
/// Translates `FetchRequest.fetchData(Map<Uuid, String>)`.
pub fn fetch_data_from(
    request: &FetchRequest,
    topic_names: &HashMap<Uuid, String>,
) -> IndexMap<TopicIdPartition, PartitionData> {
    let mut out: IndexMap<TopicIdPartition, PartitionData> = IndexMap::new();
    let version = request.version();
    for topic in &request.data.topics {
        let name = if version < 13 {
            topic.topic.clone() // never null per the protocol
        } else {
            topic_names.get(&topic.topic_id).cloned().unwrap_or_default()
        };
        for fp in &topic.partitions {
            let tip = TopicIdPartition::from_parts(topic.topic_id, fp.partition, name.clone());
            let pd = PartitionData::new_last_fetched_epoch(
                topic.topic_id,
                fp.fetch_offset,
                fp.log_start_offset,
                fp.partition_max_bytes,
                optional_epoch(fp.current_leader_epoch),
                optional_epoch(fp.last_fetched_epoch),
            );
            out.insert(tip, pd);
        }
    }
    out
}

fn optional_epoch(raw: i32) -> Option<i32> {
    if raw < 0 { None } else { Some(raw) }
}

impl crate::common::requests::RequestBuilder for FetchRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FETCH
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> std::io::Result<crate::common::requests::ConcreteRequest> {
        Ok(crate::common::requests::ConcreteRequest::Fetch(
            FetchRequestBuilder::build_version(self, version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(name: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(name.to_string(), partition)
    }

    fn pd(topic_id: Uuid, fetch_offset: i64) -> PartitionData {
        PartitionData::new(topic_id, fetch_offset, INVALID_LOG_START_OFFSET, 1024, None)
    }

    #[test]
    fn test_for_consumer_defaults() {
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new());
        assert_eq!(builder.replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(builder.max_wait, 500);
        assert_eq!(builder.min_bytes, 1);
        assert_eq!(builder.max_bytes, DEFAULT_RESPONSE_MAX_BYTES);
        assert_eq!(builder.isolation_level, IsolationLevel::ReadUncommitted);
        assert_eq!(builder.metadata, FetchMetadata::LEGACY);
        assert_eq!(builder.rack_id, "");
        assert_eq!(builder.latest_allowed_version(), 15);
    }

    #[test]
    fn test_build_for_consumer_v15_replica_state() {
        let topic_id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("t", 0), pd(topic_id, 100));
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, data);
        let req = builder.build_version(15);
        // v15 stores replica id in replica_state.
        assert_eq!(CONSUMER_REPLICA_ID, req.replica_id());
        assert_eq!(500, req.max_wait());
        assert_eq!(1, req.min_bytes());
        assert!(!req.is_from_follower());
        assert_eq!(15, req.version());
    }

    #[test]
    fn test_build_for_consumer_v12_replica_id_field() {
        let topic_id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("t", 0), pd(topic_id, 100));
        let builder = FetchRequestBuilder::for_consumer(12, 500, 1, data);
        let req = builder.build_version(12);
        assert_eq!(CONSUMER_REPLICA_ID, req.replica_id());
        assert_eq!(12, req.version());
    }

    #[test]
    fn test_build_groups_consecutive_partitions_by_topic() {
        let id_a = Uuid::random_uuid();
        let id_b = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("a", 0), pd(id_a, 0));
        data.insert(tp("a", 1), pd(id_a, 0));
        data.insert(tp("b", 0), pd(id_b, 0));
        data.insert(tp("a", 2), pd(id_a, 0));
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, data);
        let req = builder.build();
        // Three groups: a[0,1], b[0], a[2].
        assert_eq!(3, req.data().topics.len());
        assert_eq!("a", req.data().topics[0].topic);
        assert_eq!(2, req.data().topics[0].partitions.len());
        assert_eq!("b", req.data().topics[1].topic);
        assert_eq!(1, req.data().topics[1].partitions.len());
        assert_eq!("a", req.data().topics[2].topic);
        assert_eq!(1, req.data().topics[2].partitions.len());
    }

    #[test]
    fn test_build_v12_forces_default_max_bytes() {
        let id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("t", 0), pd(id, 0));
        let req = FetchRequestBuilder::for_consumer(12, 500, 1, data)
            .set_max_bytes(123_456)
            .build_version(2);
        assert_eq!(DEFAULT_RESPONSE_MAX_BYTES, req.max_bytes());
    }

    #[test]
    fn test_build_v3_honors_max_bytes() {
        let id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("t", 0), pd(id, 0));
        let req = FetchRequestBuilder::for_consumer(15, 500, 1, data)
            .set_max_bytes(123_456)
            .build_version(3);
        assert_eq!(123_456, req.max_bytes());
    }

    #[test]
    fn test_build_removed_partitions_v12_no_replaced() {
        let id = Uuid::random_uuid();
        let removed = vec![TopicIdPartition::from_parts(id, 5, "x")];
        let replaced = vec![TopicIdPartition::from_parts(id, 6, "y")];
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new())
            .set_removed(removed)
            .set_replaced(replaced);
        let req = builder.build_version(12);
        // v12 only includes removed; replaced is dropped.
        let forgotten = &req.data().forgotten_topics_data;
        assert_eq!(1, forgotten.len());
        assert_eq!("x", forgotten[0].topic);
    }

    #[test]
    fn test_build_removed_and_replaced_v13() {
        let id = Uuid::random_uuid();
        let removed = vec![TopicIdPartition::from_parts(id, 5, "x")];
        let replaced = vec![TopicIdPartition::from_parts(id, 6, "y")];
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new())
            .set_removed(removed)
            .set_replaced(replaced);
        let req = builder.build_version(13);
        let forgotten = &req.data().forgotten_topics_data;
        assert_eq!(2, forgotten.len());
    }

    #[test]
    fn test_removed_and_replaced_round_trip() {
        let id = Uuid::random_uuid();
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new());
        // Java's Builder defaults both to `Collections.emptyList()`.
        assert!(builder.removed().is_empty());
        assert!(builder.replaced().is_empty());

        let removed = vec![TopicIdPartition::from_parts(id, 5, "x")];
        let replaced = vec![TopicIdPartition::from_parts(id, 6, "y")];
        let builder = builder.set_removed(removed.clone()).set_replaced(replaced.clone());
        assert_eq!(removed.as_slice(), builder.removed());
        assert_eq!(replaced.as_slice(), builder.replaced());
    }

    #[test]
    fn test_isolation_level_round_trip() {
        let builder = FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new())
            .set_isolation_level(IsolationLevel::ReadCommitted);
        let req = builder.build_version(15);
        assert_eq!(IsolationLevel::ReadCommitted, req.isolation_level().unwrap());
    }

    #[test]
    fn test_metadata_round_trip() {
        let builder =
            FetchRequestBuilder::for_consumer(15, 500, 1, IndexMap::new()).set_metadata(FetchMetadata::new(42, 7));
        let req = builder.build_version(15);
        assert_eq!(FetchMetadata::new(42, 7), req.metadata());
    }

    #[test]
    fn test_describe_replica_id() {
        assert_eq!("consumer", describe_replica_id(-1));
        assert_eq!("debug consumer", describe_replica_id(-2));
        assert_eq!("future local replica", describe_replica_id(-3));
        assert_eq!("replica [3]", describe_replica_id(3));
        assert_eq!("invalid replica [-10]", describe_replica_id(-10));
    }

    #[test]
    fn test_is_consumer() {
        assert!(is_consumer(-1));
        assert!(is_consumer(-2));
        assert!(!is_consumer(-3)); // FUTURE_LOCAL_REPLICA_ID
        assert!(!is_consumer(0));
        assert!(!is_consumer(5));
    }

    #[test]
    fn test_fetch_data_from_v15_uses_topic_id_map() {
        let id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("orig", 0), pd(id, 100));
        let req = FetchRequestBuilder::for_consumer(15, 500, 1, data).build_version(15);

        let mut topic_names = HashMap::new();
        topic_names.insert(id, "resolved".to_string());
        let resolved = fetch_data_from(&req, &topic_names);
        assert_eq!(1, resolved.len());
        let (tip, partition_data) = resolved.iter().next().unwrap();
        assert_eq!("resolved", tip.topic());
        assert_eq!(0, tip.partition());
        assert_eq!(id, partition_data.topic_id);
        assert_eq!(100, partition_data.fetch_offset);
    }

    #[test]
    fn test_fetch_data_from_v12_uses_topic_name_in_data() {
        let id = Uuid::random_uuid();
        let mut data = IndexMap::new();
        data.insert(tp("name-in-data", 0), pd(id, 100));
        let req = FetchRequestBuilder::for_consumer(15, 500, 1, data).build_version(12);

        let resolved = fetch_data_from(&req, &HashMap::new());
        let (tip, _) = resolved.iter().next().unwrap();
        // v12 keeps the topic name from the request body.
        assert_eq!("name-in-data", tip.topic());
    }
}
