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

//! Per-node fetch session state machine implementing the KIP-227 incremental
//! fetch protocol.
//!
//! Translated from `org.apache.kafka.clients.FetchSessionHandler`. Lives at
//! `src/fetch_session_handler.rs` to mirror Java's
//! `org.apache.kafka.clients` package — same precedent as `src/kafka_client.rs`.
//!
//! # API shape
//!
//! Java models the builder as an inner class that, on `build()`, mutates the
//! enclosing handler's `sessionPartitions` and `nextMetadata`. Rust does not
//! permit an inner type to mutate its outer struct through a borrowed
//! reference without lifetime gymnastics, so the Rust translation moves the
//! mutation into the handler:
//!
//! ```ignore
//! let mut handler = FetchSessionHandler::new(node);
//! let mut builder = handler.new_builder();
//! builder.add(tp, partition_data);
//! let data = handler.build_request(builder); // mutates handler
//! ```
//!
//! Semantically equivalent: a single call site builds a single `FetchRequestData`
//! per fetch round, and the handler's session state advances as it does in
//! Java. The builder owns the proposed partitions and topic-id-name mapping;
//! `build_request` consumes the builder and either replaces the session
//! (full fetch) or diffs against it (incremental fetch).

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use log::{debug, info, trace};

use crate::common::TopicIdPartition;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::requests::fetch_metadata::{FetchMetadata, INVALID_SESSION_ID};
use crate::common::requests::fetch_request::PartitionData;
use crate::common::requests::fetch_response::FetchResponse;

/// The state and diff produced by [`FetchSessionHandler::build_request`].
///
/// Mirrors Java's `FetchSessionHandler.FetchRequestData` (the inner class is
/// renamed here to `FetchSessionRequestData` to avoid collision with the
/// auto-generated [`crate::fetch_request_data::FetchRequestData`]).
///
/// `pub(crate)` to match Java's package-private visibility — only
/// `AbstractFetch` and Phase 7b's `FetchRequestManager` consume this
/// internally.
#[derive(Clone, Debug)]
pub(crate) struct FetchSessionRequestData {
    /// Partitions to send in the fetch request.
    pub(crate) to_send: IndexMap<TopicPartition, PartitionData>,
    /// Partitions in the request's `forget` list.
    pub(crate) to_forget: Vec<TopicIdPartition>,
    /// Partitions in the request's `replaced` list (v13+).
    pub(crate) to_replace: Vec<TopicIdPartition>,
    /// All partitions in the fetch session.
    pub(crate) session_partitions: IndexMap<TopicPartition, PartitionData>,
    /// Fetch metadata (session id + epoch) for this request.
    pub(crate) metadata: FetchMetadata,
    /// True if every topic involved in the request carries a topic ID.
    pub(crate) can_use_topic_ids: bool,
}

/// Per-node fetch session handler.
///
/// Corresponds to `org.apache.kafka.clients.FetchSessionHandler`.
#[derive(Debug)]
pub struct FetchSessionHandler {
    node: i32,
    /// Metadata for the next fetch request.
    next_metadata: FetchMetadata,
    /// All partitions in the current fetch session, in insertion order
    /// (insertion order matters for the wire protocol — full fetch requests
    /// truncate partitions in this order when the response size cap is
    /// exceeded).
    session_partitions: IndexMap<TopicPartition, PartitionData>,
    /// Topic-id-to-name map for the partitions in the session.
    session_topic_names: HashMap<Uuid, String>,
}

impl FetchSessionHandler {
    /// Constructs a fresh handler for the given broker node id.
    pub fn new(node: i32) -> Self {
        Self {
            node,
            next_metadata: FetchMetadata::INITIAL,
            session_partitions: IndexMap::new(),
            session_topic_names: HashMap::new(),
        }
    }

    /// Returns the broker node id this handler tracks.
    pub fn node(&self) -> i32 {
        self.node
    }

    /// Returns the session id of the current (or pending) session.
    pub fn session_id(&self) -> i32 {
        self.next_metadata.session_id()
    }

    /// Returns a reference to the topic-id-to-name map.
    pub fn session_topic_names(&self) -> &HashMap<Uuid, String> {
        &self.session_topic_names
    }

    /// Returns the set of partitions currently in the session.
    pub fn session_topic_partitions(&self) -> HashSet<TopicPartition> {
        self.session_partitions.keys().cloned().collect()
    }

    /// Creates a new builder for the next fetch request.
    pub fn new_builder(&self) -> Builder {
        Builder::default()
    }

    /// Creates a builder pre-sized for the given number of partitions.
    ///
    /// # Divergence from Java
    ///
    /// Java's `newBuilder(int initialSize, boolean copySessionPartitions)`
    /// accepts a flag that, when `true`, pre-populates the builder's `next`
    /// map with the current `sessionPartitions`. The Rust translation drops
    /// that parameter because:
    ///
    /// - The Rust `build_request` consumes the builder's `next` map directly
    ///   and diffs it against `self.session_partitions` (the handler owns
    ///   the latter). The `next` map is always disjoint from
    ///   `session_partitions`, so pre-populating would only cause a
    ///   downstream `IndexMap::swap_remove` to immediately discard each
    ///   pre-populated entry.
    /// - No call site in Phase 7a passes `true` and `prepareFetchRequests`
    ///   in Java always passes `false`.
    ///
    /// If a future caller needs the `true` behavior, the right place to
    /// implement it is in `build_request`'s diff loop (so we can compute the
    /// diff without forcing the caller to pre-populate).
    pub fn new_builder_sized(&self, initial_size: usize) -> Builder {
        Builder { next: IndexMap::with_capacity(initial_size), ..Builder::default() }
    }

    /// Marks the session as pending close. The next built request will
    /// signal close via the `FINAL_EPOCH` epoch.
    pub fn notify_close(&mut self) {
        debug!(
            "Set the metadata for next fetch request to close the existing session ID={}",
            self.next_metadata.session_id()
        );
        self.next_metadata = self.next_metadata.next_close_existing();
    }

    /// Records that a fetch request failed. The next built request will
    /// attempt to recreate the session.
    pub fn handle_error(&mut self, _t: &crate::common::KafkaError) {
        info!("Error sending fetch request {} to node {}", self.next_metadata, self.node);
        self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
    }

    /// Applies a fetch response to advance the session state.
    ///
    /// Returns `true` if the response is well-formed and the session state
    /// can advance; `false` if the response cannot be processed (missing or
    /// extra partitions, a top-level error, etc.) and the next request
    /// should close/recreate the session.
    ///
    /// Translates `FetchSessionHandler.handleResponse(FetchResponse, short)`.
    pub fn handle_response(&mut self, response: &FetchResponse, version: i16) -> bool {
        if response.error() != Errors::None {
            info!(
                "Node {} was unable to process the fetch request with {}: {:?}.",
                self.node,
                self.next_metadata,
                response.error()
            );
            if response.error() == Errors::FetchSessionIdNotFound {
                self.next_metadata = FetchMetadata::INITIAL;
            } else {
                self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
            }
            return false;
        }

        // Phase 20 Fix #2a: collect only the partition keys, without cloning the
        // PartitionData payloads (the full response_data() clone was discarded after
        // extracting its keys). Equivalent to the old
        // `response.response_data(...).keys().cloned().collect()`.
        let topic_partitions: HashSet<TopicPartition> =
            response.response_partition_keys(&self.session_topic_names, version);
        let topic_ids = response.topic_ids();

        if self.next_metadata.is_full() {
            if topic_partitions.is_empty() && response.throttle_time_ms() > 0 {
                // KIP-219: empty full fetch + throttle is the broker
                // signaling throttling, not an error. The response still
                // can't be processed.
                debug!(
                    "Node {} sent an empty full fetch response to indicate that this client \
                     should be throttled for {} ms.",
                    self.node,
                    response.throttle_time_ms()
                );
                self.next_metadata = FetchMetadata::INITIAL;
                return false;
            }
            if let Some(problem) = self.verify_full_fetch_response_partitions(&topic_partitions, &topic_ids, version) {
                info!("Node {} sent an invalid full fetch response with {}", self.node, problem);
                self.next_metadata = FetchMetadata::INITIAL;
                return false;
            }
            if response.session_id() == INVALID_SESSION_ID {
                debug!("Node {} sent a full fetch response with no session.", self.node);
                self.next_metadata = FetchMetadata::INITIAL;
                true
            } else {
                debug!(
                    "Node {} sent a full fetch response that created a new incremental fetch session {}",
                    self.node,
                    response.session_id()
                );
                self.next_metadata = FetchMetadata::new_incremental(response.session_id());
                true
            }
        } else {
            // Incremental.
            if let Some(problem) =
                self.verify_incremental_fetch_response_partitions(&topic_partitions, &topic_ids, version)
            {
                info!("Node {} sent an invalid incremental fetch response with {}", self.node, problem);
                self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
                return false;
            }
            if response.session_id() == INVALID_SESSION_ID {
                debug!(
                    "Node {} sent an incremental fetch response closing session {}",
                    self.node,
                    self.next_metadata.session_id()
                );
                self.next_metadata = FetchMetadata::INITIAL;
            } else {
                self.next_metadata = self.next_metadata.next_incremental();
            }
            true
        }
    }

    /// Builds the request data from the given builder, advancing the
    /// session state. Consumes the builder so it cannot be reused.
    ///
    /// `pub(crate)` because the return type
    /// [`FetchSessionRequestData`] is internal — match Java's
    /// package-private `FetchSessionHandler.Builder.build()` visibility.
    pub(crate) fn build_request(&mut self, mut builder: Builder) -> FetchSessionRequestData {
        let can_use_topic_ids_input = builder.partitions_without_topic_ids == 0;

        if self.next_metadata.is_full() {
            debug!(
                "Built full fetch {} for node {} with {} partitions.",
                self.next_metadata,
                self.node,
                builder.next.len()
            );
            self.session_partitions = builder.next;
            self.session_topic_names = if can_use_topic_ids_input {
                builder.topic_names
            } else {
                HashMap::new()
            };
            let to_send = self.session_partitions.clone();
            let session_partitions = self.session_partitions.clone();
            let metadata = self.next_metadata;
            return FetchSessionRequestData {
                to_send,
                to_forget: Vec::new(),
                to_replace: Vec::new(),
                session_partitions,
                metadata,
                can_use_topic_ids: can_use_topic_ids_input,
            };
        }

        // Incremental fetch — diff `builder.next` against `self.session_partitions`.
        let mut added: Vec<TopicIdPartition> = Vec::new();
        let mut removed: Vec<TopicIdPartition> = Vec::new();
        let mut altered: Vec<TopicIdPartition> = Vec::new();
        let mut replaced: Vec<TopicIdPartition> = Vec::new();
        let mut can_use_topic_ids = can_use_topic_ids_input;

        // Walk `session_partitions` in insertion order. The Java code uses
        // an iterator with `iter.remove()` and re-inserts touched entries
        // at the end of `next` so that the resulting `next` map carries
        // only the partitions whose state has changed.
        //
        // Rust port: collect the partition keys up front (cheap clone),
        // then process them while we mutate `session_partitions` and `next`.
        let session_keys: Vec<TopicPartition> = self.session_partitions.keys().cloned().collect();
        for topic_partition in session_keys {
            // `prev_data` is the current state in the session.
            // We re-borrow inside the loop so we don't hold an outer
            // reference across mutations.
            let prev_data = self
                .session_partitions
                .get(&topic_partition)
                .expect("session key collected above")
                .clone();

            // Try to look up the requested next-state for this partition.
            let next_data_opt = builder.next.swap_remove(&topic_partition);
            if let Some(next_data) = next_data_opt {
                // Both prev and next exist — check for topic-id change /
                // payload change.
                let zero = Uuid::zero();
                if prev_data.topic_id != next_data.topic_id && prev_data.topic_id != zero && next_data.topic_id != zero
                {
                    // Topic ID changed (Uuid -> different Uuid). For v13+
                    // the replaced partition is forgotten. We re-add the
                    // new entry to the end of `next` and update the
                    // session entry.
                    self.session_partitions.insert(topic_partition.clone(), next_data.clone());
                    builder.next.insert(topic_partition.clone(), next_data);
                    replaced.push(TopicIdPartition::new(prev_data.topic_id, topic_partition));
                } else if prev_data != next_data {
                    // Altered payload (offset, max bytes, etc.). Re-insert
                    // at end of next; update session.
                    self.session_partitions.insert(topic_partition.clone(), next_data.clone());
                    builder.next.insert(topic_partition.clone(), next_data.clone());
                    altered.push(TopicIdPartition::new(next_data.topic_id, topic_partition));
                }
                // else: identical — leave session in place; the partition
                // remains implied (not in toSend).
            } else {
                // The next round does not include this partition — forget it.
                let prev_topic_id = prev_data.topic_id;
                self.session_partitions.shift_remove(&topic_partition);
                removed.push(TopicIdPartition::new(prev_topic_id, topic_partition.clone()));
                // If we do not have a topic id for the removed partition
                // (and were otherwise using them), we can no longer use
                // topic IDs.
                if can_use_topic_ids && prev_topic_id == Uuid::zero() {
                    can_use_topic_ids = false;
                }
            }
        }

        // Anything left in `next` is brand new — add it to the session.
        // The Java code uses `containsKey` plus a `break` because the
        // earlier loop has already moved all "touched" partitions to the
        // end. In Rust, `swap_remove` above ensured those keys are gone
        // from `next` (we re-added them to the end). Stop on the first key
        // that already exists in the session — those are the re-inserted
        // touched partitions that we've already accounted for.
        let new_partition_keys: Vec<TopicPartition> = builder.next.keys().cloned().collect();
        for tp in new_partition_keys {
            if self.session_partitions.contains_key(&tp) {
                // The remaining entries are the touched (altered/replaced)
                // ones we re-added — already counted. Stop.
                break;
            }
            let data = builder.next.get(&tp).expect("just enumerated").clone();
            self.session_partitions.insert(tp.clone(), data.clone());
            added.push(TopicIdPartition::new(data.topic_id, tp));
        }

        // Track topic IDs based on the final state.
        self.session_topic_names = if can_use_topic_ids {
            builder.topic_names
        } else {
            HashMap::new()
        };

        trace!(
            "Built incremental fetch {} for node {}: +{} ~{} -{} replaced{}",
            self.next_metadata,
            self.node,
            added.len(),
            altered.len(),
            removed.len(),
            replaced.len()
        );

        let to_send = builder.next;
        let session_partitions = self.session_partitions.clone();
        let metadata = self.next_metadata;
        FetchSessionRequestData {
            to_send,
            to_forget: removed,
            to_replace: replaced,
            session_partitions,
            metadata,
            can_use_topic_ids,
        }
    }

    /// Verifies that a full-fetch response contains exactly the session's
    /// partitions. Returns `None` if everything matches; otherwise returns
    /// a human-readable description.
    pub fn verify_full_fetch_response_partitions(
        &self,
        topic_partitions: &HashSet<TopicPartition>,
        ids: &HashSet<Uuid>,
        version: i16,
    ) -> Option<String> {
        let session: HashSet<TopicPartition> = self.session_partitions.keys().cloned().collect();
        let extra = find_missing(topic_partitions, &session);
        let omitted = find_missing(&session, topic_partitions);
        let extra_ids: HashSet<Uuid> = if version >= 13 {
            let session_ids: HashSet<Uuid> = self.session_topic_names.keys().cloned().collect();
            find_missing(ids, &session_ids)
        } else {
            HashSet::new()
        };
        if extra.is_empty() && omitted.is_empty() && extra_ids.is_empty() {
            return None;
        }
        let mut bld = String::new();
        if !omitted.is_empty() {
            bld.push_str(&format!("omittedPartitions=({}), ", join_partitions(&omitted)));
        }
        if !extra.is_empty() {
            bld.push_str(&format!("extraPartitions=({}), ", join_partitions(&extra)));
        }
        if !extra_ids.is_empty() {
            bld.push_str(&format!("extraIds=({}), ", join_ids(&extra_ids)));
        }
        bld.push_str(&format!("response=({})", join_partitions(topic_partitions)));
        Some(bld)
    }

    /// Verifies that an incremental fetch response only contains partitions
    /// from the session. Returns `None` if everything matches.
    pub fn verify_incremental_fetch_response_partitions(
        &self,
        topic_partitions: &HashSet<TopicPartition>,
        ids: &HashSet<Uuid>,
        version: i16,
    ) -> Option<String> {
        let session: HashSet<TopicPartition> = self.session_partitions.keys().cloned().collect();
        let extra = find_missing(topic_partitions, &session);
        let extra_ids: HashSet<Uuid> = if version >= 13 {
            let session_ids: HashSet<Uuid> = self.session_topic_names.keys().cloned().collect();
            find_missing(ids, &session_ids)
        } else {
            HashSet::new()
        };
        if extra.is_empty() && extra_ids.is_empty() {
            return None;
        }
        let mut bld = String::new();
        if !extra.is_empty() {
            bld.push_str(&format!("extraPartitions=({}), ", join_partitions(&extra)));
        }
        if !extra_ids.is_empty() {
            bld.push_str(&format!("extraIds=({}), ", join_ids(&extra_ids)));
        }
        bld.push_str(&format!("response=({})", join_partitions(topic_partitions)));
        Some(bld)
    }
}

/// Builder for the next fetch round.
///
/// Use [`FetchSessionHandler::new_builder`] to construct. After populating
/// via [`Builder::add`], pass to [`FetchSessionHandler::build_request`] to
/// produce a [`FetchSessionRequestData`].
#[derive(Debug, Default)]
pub struct Builder {
    /// Insertion-ordered partitions for the upcoming fetch. The wire
    /// protocol truncates from the end of this list when the response cap
    /// is exceeded, so order matters.
    next: IndexMap<TopicPartition, PartitionData>,
    /// Map from topic id to topic name for the partitions in `next`.
    topic_names: HashMap<Uuid, String>,
    /// Counter of partitions in `next` whose topic id is the zero UUID
    /// (i.e. that lack a topic id).
    partitions_without_topic_ids: i32,
}

impl Builder {
    /// Marks that we want data from this partition in the upcoming fetch.
    pub fn add(&mut self, topic_partition: TopicPartition, data: PartitionData) {
        if data.topic_id == Uuid::zero() {
            self.partitions_without_topic_ids += 1;
        } else {
            // putIfAbsent semantics.
            self.topic_names
                .entry(data.topic_id)
                .or_insert_with(|| topic_partition.topic().to_string());
        }
        self.next.insert(topic_partition, data);
    }
}

/// Returns items in `to_find` that are missing from `to_search`.
///
/// Mirrors Java's static `findMissing` (which uses `LinkedHashSet`); the
/// Rust translation returns `HashSet` because the consumers (`verify*`
/// methods) don't depend on iteration order.
pub fn find_missing<T: Clone + Eq + std::hash::Hash>(to_find: &HashSet<T>, to_search: &HashSet<T>) -> HashSet<T> {
    to_find.iter().filter(|item| !to_search.contains(*item)).cloned().collect()
}

fn join_partitions(set: &HashSet<TopicPartition>) -> String {
    set.iter().map(|tp| tp.to_string()).collect::<Vec<_>>().join(", ")
}

fn join_ids(set: &HashSet<Uuid>) -> String {
    set.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ApiKeys;
    use crate::fetch_response_data::{FetchResponseData, FetchableTopicResponse, PartitionData as RespPartitionData};

    fn tp(name: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(name.to_string(), partition)
    }

    fn pd(topic_id: Uuid, fetch_offset: i64, log_start: i64, max_bytes: i32) -> PartitionData {
        PartitionData::new(topic_id, fetch_offset, log_start, max_bytes, None)
    }

    /// Builds a FetchResponse with the given top-level error, session id,
    /// and per-topic response entries. The `entries` argument lists
    /// `(topic_name, topic_id, partition, error_code)` tuples.
    fn build_response(
        error: Errors,
        session_id: i32,
        throttle_ms: i32,
        entries: &[(String, Uuid, i32, i16)],
    ) -> FetchResponse {
        let mut data = FetchResponseData::new();
        data.set_error_code(error.code());
        data.set_session_id(session_id);
        data.set_throttle_time_ms(throttle_ms);
        // Group entries by topic-id (insertion-stable).
        let mut topics: IndexMap<(String, Uuid), Vec<RespPartitionData>> = IndexMap::new();
        for (name, id, partition, err_code) in entries {
            let mut p = RespPartitionData::new();
            p.set_partition_index(*partition);
            p.set_error_code(*err_code);
            p.set_high_watermark(10);
            topics.entry((name.clone(), *id)).or_default().push(p);
        }
        let mut responses = Vec::new();
        for ((name, id), parts) in topics {
            let mut t = FetchableTopicResponse::new();
            t.set_topic(name);
            t.set_topic_id(id);
            t.set_partitions(parts);
            responses.push(t);
        }
        data.set_responses(responses);
        FetchResponse::new(data)
    }

    /// Mirror Java's `addTopicId(topicIds, topicNames, name, version)` —
    /// only assigns an ID when `version >= 13`.
    fn add_topic_id(
        topic_ids: &mut HashMap<String, Uuid>,
        topic_names: &mut HashMap<Uuid, String>,
        name: &str,
        version: i16,
    ) {
        if version >= 13 {
            let id = Uuid::random_uuid();
            topic_ids.insert(name.to_string(), id);
            topic_names.insert(id, name.to_string());
        }
    }

    /// Translated from `FetchSessionHandlerTest.testFindMissing`.
    #[test]
    fn test_find_missing() {
        let foo0 = tp("foo", 0);
        let foo1 = tp("foo", 1);
        let bar0 = tp("bar", 0);
        let bar1 = tp("bar", 1);
        let baz0 = tp("baz", 0);
        let baz1 = tp("baz", 1);

        fn s(arr: Vec<TopicPartition>) -> HashSet<TopicPartition> {
            arr.into_iter().collect()
        }

        assert!(find_missing(&s(vec![foo0.clone()]), &s(vec![foo0.clone()])).is_empty());
        assert_eq!(
            s(vec![foo0.clone()]),
            find_missing(&s(vec![foo0.clone()]), &s(vec![foo1.clone()]))
        );
        assert_eq!(
            s(vec![foo0.clone(), foo1.clone()]),
            find_missing(&s(vec![foo0.clone(), foo1.clone()]), &s(vec![baz0.clone()])),
        );
        assert_eq!(
            s(vec![bar1.clone(), foo0.clone(), foo1.clone()]),
            find_missing(
                &s(vec![foo0.clone(), foo1.clone(), bar0.clone(), bar1.clone()]),
                &s(vec![bar0.clone(), baz0.clone(), baz1.clone()]),
            ),
        );
        assert!(
            find_missing(
                &s(vec![foo0.clone(), foo1.clone(), bar0.clone(), bar1.clone(), baz1.clone()]),
                &s(vec![foo0, foo1, bar0, bar1, baz0, baz1]),
            )
            .is_empty()
        );
    }

    /// Translated from `FetchSessionHandlerTest.testSessionless`. Iterates
    /// over v12 (no topic IDs) and the latest fetch version (topic IDs).
    #[test]
    fn test_sessionless() {
        for version in [12i16, ApiKeys::FETCH.latest_version()] {
            let mut topic_ids = HashMap::new();
            let mut topic_names = HashMap::new();
            add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
            let foo_id = topic_ids.get("foo").copied().unwrap_or(Uuid::zero());

            let mut handler = FetchSessionHandler::new(1);
            let mut builder = handler.new_builder();
            builder.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            builder.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
            let data = handler.build_request(builder);
            assert_eq!(2, data.to_send.len());
            assert_eq!(data.to_send, data.session_partitions);
            assert_eq!(INVALID_SESSION_ID, data.metadata.session_id());
            assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, data.metadata.epoch());

            // Sessionless response: session id == INVALID_SESSION_ID.
            let response = build_response(
                Errors::None,
                INVALID_SESSION_ID,
                0,
                &[("foo".to_string(), foo_id, 0, 0), ("foo".to_string(), foo_id, 1, 0)],
            );
            assert!(handler.handle_response(&response, version));

            // Next round still treated as FULL.
            let mut b2 = handler.new_builder();
            b2.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            let data2 = handler.build_request(b2);
            assert_eq!(INVALID_SESSION_ID, data2.metadata.session_id());
            assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, data2.metadata.epoch());
            assert_eq!(1, data2.to_send.len());
        }
    }

    /// Translated from `FetchSessionHandlerTest.testIncrementals`.
    #[test]
    fn test_incrementals() {
        for version in [12i16, ApiKeys::FETCH.latest_version()] {
            let mut topic_ids = HashMap::new();
            let mut topic_names = HashMap::new();
            add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
            let foo_id = topic_ids.get("foo").copied().unwrap_or(Uuid::zero());

            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            b1.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
            let d1 = handler.build_request(b1);
            assert_eq!(2, d1.to_send.len());

            // Broker responds with session id 123.
            let r1 = build_response(
                Errors::None,
                123,
                0,
                &[("foo".to_string(), foo_id, 0, 0), ("foo".to_string(), foo_id, 1, 0)],
            );
            assert!(handler.handle_response(&r1, version));

            // Round 2: add bar/0, modify foo/1.
            add_topic_id(&mut topic_ids, &mut topic_names, "bar", version);
            let bar_id = topic_ids.get("bar").copied().unwrap_or(Uuid::zero());
            let mut b2 = handler.new_builder();
            b2.add(tp("foo", 0), pd(foo_id, 0, 100, 200)); // unchanged
            b2.add(tp("foo", 1), pd(foo_id, 10, 120, 210)); // altered
            b2.add(tp("bar", 0), pd(bar_id, 20, 200, 200)); // new
            let d2 = handler.build_request(b2);
            assert!(!d2.metadata.is_full());
            assert_eq!(3, d2.session_partitions.len()); // foo0, foo1, bar0
            // to_send carries only altered + added partitions: foo1, bar0.
            assert_eq!(2, d2.to_send.len());
            assert!(d2.to_send.contains_key(&tp("foo", 1)));
            assert!(d2.to_send.contains_key(&tp("bar", 0)));

            // Broker responds.
            let r2 = build_response(Errors::None, 123, 0, &[("foo".to_string(), foo_id, 1, 0)]);
            assert!(handler.handle_response(&r2, version));

            // Round 3: simulate invalid fetch session epoch — should reset.
            let r3 = build_response(Errors::InvalidFetchSessionEpoch, INVALID_SESSION_ID, 0, &[]);
            assert!(!handler.handle_response(&r3, version));

            // Round 4: full fetch (since session was reset on round 3).
            let mut b4 = handler.new_builder();
            b4.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            b4.add(tp("foo", 1), pd(foo_id, 10, 120, 210));
            b4.add(tp("bar", 0), pd(bar_id, 20, 200, 200));
            let d4 = handler.build_request(b4);
            assert!(d4.metadata.is_full());
            // Session id retained from previous round (close-existing-attempt-new
            // preserves session id but sets epoch=INITIAL_EPOCH).
            assert_eq!(123, d4.metadata.session_id());
            assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, d4.metadata.epoch());
            assert_eq!(d4.to_send, d4.session_partitions);
        }
    }

    /// Translated from `FetchSessionHandlerTest.testIncrementalPartitionRemoval`.
    #[test]
    fn test_incremental_partition_removal() {
        for version in [12i16, ApiKeys::FETCH.latest_version()] {
            let mut topic_ids = HashMap::new();
            let mut topic_names = HashMap::new();
            add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
            add_topic_id(&mut topic_ids, &mut topic_names, "bar", version);
            let foo_id = topic_ids.get("foo").copied().unwrap_or(Uuid::zero());
            let bar_id = topic_ids.get("bar").copied().unwrap_or(Uuid::zero());

            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            b1.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
            b1.add(tp("bar", 0), pd(bar_id, 20, 120, 220));
            let d1 = handler.build_request(b1);
            assert_eq!(3, d1.to_send.len());
            assert!(d1.metadata.is_full());

            // Response with session id 123.
            let r1 = build_response(
                Errors::None,
                123,
                0,
                &[
                    ("foo".to_string(), foo_id, 0, 0),
                    ("foo".to_string(), foo_id, 1, 0),
                    ("bar".to_string(), bar_id, 0, 0),
                ],
            );
            assert!(handler.handle_response(&r1, version));

            // Round 2: only keep foo/1.
            let mut b2 = handler.new_builder();
            b2.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
            let d2 = handler.build_request(b2);
            assert!(!d2.metadata.is_full());
            assert_eq!(123, d2.metadata.session_id());
            assert_eq!(1, d2.metadata.epoch());
            assert_eq!(1, d2.session_partitions.len());
            assert!(d2.to_send.is_empty());
            // to_forget should contain foo/0 and bar/0.
            assert_eq!(2, d2.to_forget.len());
            let forgotten: HashSet<TopicPartition> = d2.to_forget.iter().map(|t| t.topic_partition().clone()).collect();
            assert!(forgotten.contains(&tp("foo", 0)));
            assert!(forgotten.contains(&tp("bar", 0)));

            // Round 3: FETCH_SESSION_ID_NOT_FOUND -> reset to INITIAL.
            let r2 = build_response(Errors::FetchSessionIdNotFound, INVALID_SESSION_ID, 0, &[]);
            assert!(!handler.handle_response(&r2, version));

            // Round 4: full fetch with brand-new (INVALID) session id.
            let mut b3 = handler.new_builder();
            b3.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            let d3 = handler.build_request(b3);
            assert!(d3.metadata.is_full());
            assert_eq!(INVALID_SESSION_ID, d3.metadata.session_id());
            assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, d3.metadata.epoch());
        }
    }

    /// Translated from
    /// `FetchSessionHandlerTest.testTopicIdUsageGrantedOnIdUpgrade` (the
    /// partition=0 case — updating an existing partition).
    #[test]
    fn test_topic_id_usage_granted_on_id_upgrade_update_existing() {
        let mut handler = FetchSessionHandler::new(1);
        let mut b1 = handler.new_builder();
        b1.add(tp("foo", 0), pd(Uuid::zero(), 0, 100, 200));
        let d1 = handler.build_request(b1);
        assert!(d1.metadata.is_full());
        assert!(!d1.can_use_topic_ids);

        let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), Uuid::zero(), 0, 0)]);
        assert!(handler.handle_response(&r1, 12));

        // Add a topic ID to the existing partition.
        let topic_id = Uuid::random_uuid();
        let mut b2 = handler.new_builder();
        b2.add(tp("foo", 0), pd(topic_id, 10, 110, 210));
        let d2 = handler.build_request(b2);
        assert_eq!(123, d2.metadata.session_id());
        assert_eq!(1, d2.metadata.epoch());
        assert!(d2.can_use_topic_ids);
    }

    /// Translated from `testTopicIdUsageGrantedOnIdUpgrade` (the partition=1
    /// case — adding a brand-new partition).
    #[test]
    fn test_topic_id_usage_granted_on_id_upgrade_new_partition() {
        let mut handler = FetchSessionHandler::new(1);
        let mut b1 = handler.new_builder();
        b1.add(tp("foo", 0), pd(Uuid::zero(), 0, 100, 200));
        let d1 = handler.build_request(b1);
        assert!(d1.metadata.is_full());
        assert!(!d1.can_use_topic_ids);

        let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), Uuid::zero(), 0, 0)]);
        assert!(handler.handle_response(&r1, 12));

        // Add a brand-new partition with a topic ID. Java says
        // `canUseTopicIds` stays false because the existing partition
        // (foo/0) still has no topic ID.
        let topic_id = Uuid::random_uuid();
        let mut b2 = handler.new_builder();
        b2.add(tp("foo", 1), pd(topic_id, 10, 110, 210));
        let d2 = handler.build_request(b2);
        assert_eq!(123, d2.metadata.session_id());
        assert_eq!(1, d2.metadata.epoch());
        assert!(!d2.can_use_topic_ids);
    }

    /// Translated from `testIdUsageRevokedOnIdDowngrade`.
    ///
    /// Java loops over `partitions = [0, 1]`: partition=0 is the
    /// "updating an existing partition" case; partition=1 is the
    /// "adding a brand-new partition" case. Both expect the same
    /// outcome: `canUseTopicIds = false` because at least one partition
    /// in the session no longer carries a topic ID.
    #[test]
    fn test_id_usage_revoked_on_id_downgrade() {
        for partition in [0i32, 1i32] {
            let foo_id = Uuid::random_uuid();
            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            let d1 = handler.build_request(b1);
            assert!(d1.metadata.is_full(), "partition={partition}");
            assert!(d1.can_use_topic_ids, "partition={partition}");

            let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), foo_id, 0, 0)]);
            assert!(handler.handle_response(&r1, ApiKeys::FETCH.latest_version()));

            // partition=0: strip topic id from existing partition.
            // partition=1: add a brand-new partition without an id.
            let mut b2 = handler.new_builder();
            b2.add(tp("foo", partition), pd(Uuid::zero(), 10, 110, 210));
            let d2 = handler.build_request(b2);
            assert_eq!(123, d2.metadata.session_id(), "partition={partition}");
            assert_eq!(1, d2.metadata.epoch(), "partition={partition}");
            assert!(!d2.can_use_topic_ids, "partition={partition}");
        }
    }

    /// Translated from `FetchSessionHandlerTest.testTopicIdReplaced`.
    ///
    /// Loops over Java's `idUsageCombinations` —
    /// `(startsWithTopicIds, endsWithTopicIds)` ∈ {TT, TF, FT, FF}.
    #[test]
    fn test_topic_id_replaced() {
        for (starts_with_topic_ids, ends_with_topic_ids) in [(true, true), (true, false), (false, true), (false, false)]
        {
            let topic_partition = tp("foo", 0);
            let topic_id_1 = if starts_with_topic_ids {
                Uuid::random_uuid()
            } else {
                Uuid::zero()
            };

            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(topic_partition.clone(), pd(topic_id_1, 0, 100, 200));
            let d1 = handler.build_request(b1);
            assert!(d1.metadata.is_full(), "combo=({starts_with_topic_ids},{ends_with_topic_ids})");
            assert_eq!(
                starts_with_topic_ids, d1.can_use_topic_ids,
                "combo=({starts_with_topic_ids},{ends_with_topic_ids})"
            );

            let response_version = if starts_with_topic_ids {
                ApiKeys::FETCH.latest_version()
            } else {
                12
            };
            let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), topic_id_1, 0, 0)]);
            assert!(
                handler.handle_response(&r1, response_version),
                "combo=({starts_with_topic_ids},{ends_with_topic_ids})"
            );

            // Try to add a new topic ID (zero if endsWithTopicIds=false).
            let topic_id_2 = if ends_with_topic_ids {
                Uuid::random_uuid()
            } else {
                Uuid::zero()
            };
            let mut b2 = handler.new_builder();
            b2.add(topic_partition.clone(), pd(topic_id_2, 0, 100, 200));
            let d2 = handler.build_request(b2);

            // Java case analysis on `(startsWithTopicIds, endsWithTopicIds)`:
            if starts_with_topic_ids && ends_with_topic_ids {
                // Both true: the old topic id goes into to_replace, the
                // new one into to_send. sessionTopicNames carries the new.
                assert_eq!(1, d2.to_replace.len());
                assert_eq!(topic_id_1, d2.to_replace[0].topic_id());
                assert_eq!(1, d2.to_send.len());
                assert_eq!(1, handler.session_topic_names().len());
                assert!(handler.session_topic_names().contains_key(&topic_id_2));
            } else if starts_with_topic_ids || ends_with_topic_ids {
                // Downgrade or upgrade: nothing in to_replace; the new
                // partition data is in to_send. `to_replace` is reserved
                // for the v13+ same-partition-different-id case.
                assert_eq!(0, d2.to_replace.len());
                assert_eq!(1, d2.to_send.len());
                if ends_with_topic_ids {
                    assert_eq!(1, handler.session_topic_names().len());
                    assert!(handler.session_topic_names().contains_key(&topic_id_2));
                } else {
                    assert!(handler.session_topic_names().is_empty());
                }
            } else {
                // Both false: identical payload, nothing to send/replace.
                assert!(d2.to_replace.is_empty());
                assert!(d2.to_send.is_empty());
                assert!(handler.session_topic_names().is_empty());
            }

            assert_eq!(
                123,
                d2.metadata.session_id(),
                "combo=({starts_with_topic_ids},{ends_with_topic_ids})"
            );
            assert_eq!(1, d2.metadata.epoch(), "combo=({starts_with_topic_ids},{ends_with_topic_ids})");
            assert_eq!(
                ends_with_topic_ids, d2.can_use_topic_ids,
                "combo=({starts_with_topic_ids},{ends_with_topic_ids})"
            );
        }
    }

    /// Translated from `testSessionEpochWhenMixedUsageOfTopicIDs`.
    ///
    /// Java loops over `startsWithTopicIds = {true, false}`. In both
    /// cases the second build mixes a partition with an id and one
    /// without, and the handler must report `canUseTopicIds = false`.
    #[test]
    fn test_session_epoch_when_mixed_usage_of_topic_ids() {
        for starts_with_topic_ids in [true, false] {
            let foo_id = if starts_with_topic_ids {
                Uuid::random_uuid()
            } else {
                Uuid::zero()
            };
            let bar_id = if starts_with_topic_ids {
                Uuid::zero()
            } else {
                Uuid::random_uuid()
            };
            let response_version = if starts_with_topic_ids {
                ApiKeys::FETCH.latest_version()
            } else {
                12
            };

            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            let d1 = handler.build_request(b1);
            assert!(d1.metadata.is_full(), "starts_with_topic_ids={starts_with_topic_ids}");
            assert_eq!(
                starts_with_topic_ids, d1.can_use_topic_ids,
                "starts_with_topic_ids={starts_with_topic_ids}"
            );

            let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), foo_id, 0, 0)]);
            assert!(
                handler.handle_response(&r1, response_version),
                "starts_with_topic_ids={starts_with_topic_ids}"
            );

            // Re-add foo/0 + add a partition with the opposite id usage.
            let mut b2 = handler.new_builder();
            b2.add(tp("foo", 0), pd(foo_id, 10, 110, 210));
            b2.add(tp("bar", 1), pd(bar_id, 0, 100, 200));
            let d2 = handler.build_request(b2);
            assert_eq!(123, d2.metadata.session_id(), "starts_with_topic_ids={starts_with_topic_ids}");
            assert_eq!(1, d2.metadata.epoch(), "starts_with_topic_ids={starts_with_topic_ids}");
            assert!(!d2.can_use_topic_ids, "starts_with_topic_ids={starts_with_topic_ids}");
        }
    }

    /// Translated from `testIdUsageWithAllForgottenPartitions`.
    ///
    /// Java loops over `useTopicIds = {true, false}` — the test is
    /// the same except for the topic id value and the response version.
    #[test]
    fn test_id_usage_with_all_forgotten_partitions() {
        for use_topic_ids in [true, false] {
            let topic_id = if use_topic_ids {
                Uuid::random_uuid()
            } else {
                Uuid::zero()
            };
            let response_version = if use_topic_ids {
                ApiKeys::FETCH.latest_version()
            } else {
                12
            };

            let mut handler = FetchSessionHandler::new(1);
            let mut b1 = handler.new_builder();
            b1.add(tp("foo", 0), pd(topic_id, 0, 100, 200));
            let d1 = handler.build_request(b1);
            assert!(d1.metadata.is_full(), "use_topic_ids={use_topic_ids}");
            assert_eq!(use_topic_ids, d1.can_use_topic_ids, "use_topic_ids={use_topic_ids}");

            let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), topic_id, 0, 0)]);
            assert!(handler.handle_response(&r1, response_version), "use_topic_ids={use_topic_ids}");

            // Remove the partition from the session.
            let b2 = handler.new_builder();
            let d2 = handler.build_request(b2);
            assert_eq!(1, d2.to_forget.len(), "use_topic_ids={use_topic_ids}");
            assert_eq!(topic_id, d2.to_forget[0].topic_id(), "use_topic_ids={use_topic_ids}");
            assert_eq!(tp("foo", 0), *d2.to_forget[0].topic_partition());
            assert_eq!(123, d2.metadata.session_id(), "use_topic_ids={use_topic_ids}");
            assert_eq!(1, d2.metadata.epoch(), "use_topic_ids={use_topic_ids}");
            assert_eq!(use_topic_ids, d2.can_use_topic_ids, "use_topic_ids={use_topic_ids}");
        }
    }

    /// Translated from `testOkToAddNewIdAfterTopicRemovedFromSession`.
    #[test]
    fn test_ok_to_add_new_id_after_topic_removed_from_session() {
        let topic_id_1 = Uuid::random_uuid();
        let mut handler = FetchSessionHandler::new(1);
        let mut b1 = handler.new_builder();
        b1.add(tp("foo", 0), pd(topic_id_1, 0, 100, 200));
        let d1 = handler.build_request(b1);
        assert!(d1.metadata.is_full());
        assert!(d1.can_use_topic_ids);

        let r1 = build_response(Errors::None, 123, 0, &[("foo".to_string(), topic_id_1, 0, 0)]);
        assert!(handler.handle_response(&r1, ApiKeys::FETCH.latest_version()));

        // Remove foo/0.
        let b2 = handler.new_builder();
        let d2 = handler.build_request(b2);
        assert!(d2.to_send.is_empty());
        assert!(d2.session_partitions.is_empty());

        let r2 = build_response(Errors::None, 123, 0, &[]);
        assert!(handler.handle_response(&r2, ApiKeys::FETCH.latest_version()));

        // Add foo/0 back with a brand-new topic id.
        let mut b3 = handler.new_builder();
        let topic_id_2 = Uuid::random_uuid();
        b3.add(tp("foo", 0), pd(topic_id_2, 0, 100, 200));
        let d3 = handler.build_request(b3);
        assert_eq!(123, d3.metadata.session_id());
        assert_eq!(2, d3.metadata.epoch());
        assert!(d3.can_use_topic_ids);
    }

    /// Translated from `testVerifyFullFetchResponsePartitions`.
    #[test]
    fn test_verify_full_fetch_response_partitions() {
        for version in [12i16, ApiKeys::FETCH.latest_version()] {
            let mut topic_ids = HashMap::new();
            let mut topic_names = HashMap::new();
            add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
            add_topic_id(&mut topic_ids, &mut topic_names, "bar", version);
            let foo_id = topic_ids.get("foo").copied().unwrap_or(Uuid::zero());
            let bar_id = topic_ids.get("bar").copied().unwrap_or(Uuid::zero());

            let mut handler = FetchSessionHandler::new(1);

            // Before any session — every partition is "extra".
            let resp1 = build_response(
                Errors::None,
                INVALID_SESSION_ID,
                0,
                &[
                    ("foo".to_string(), foo_id, 0, 0),
                    ("foo".to_string(), foo_id, 1, 0),
                    ("bar".to_string(), bar_id, 0, 0),
                ],
            );
            let r1_partitions: HashSet<TopicPartition> =
                resp1.response_data(&topic_names, version).keys().cloned().collect();
            let issue = handler
                .verify_full_fetch_response_partitions(&r1_partitions, &resp1.topic_ids(), version)
                .unwrap();
            assert!(issue.contains("extraPartitions="));
            assert!(!issue.contains("omittedPartitions="));

            // After establishing the session — no extras/omitted.
            let mut b = handler.new_builder();
            b.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
            b.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
            b.add(tp("bar", 0), pd(bar_id, 20, 120, 220));
            handler.build_request(b);

            let resp2 = build_response(
                Errors::None,
                INVALID_SESSION_ID,
                0,
                &[
                    ("foo".to_string(), foo_id, 0, 0),
                    ("foo".to_string(), foo_id, 1, 0),
                    ("bar".to_string(), bar_id, 0, 0),
                ],
            );
            let r2_partitions: HashSet<TopicPartition> =
                resp2.response_data(&topic_names, version).keys().cloned().collect();
            assert!(
                handler
                    .verify_full_fetch_response_partitions(&r2_partitions, &resp2.topic_ids(), version)
                    .is_none()
            );

            // Drop bar/0 from the response — should be reported omitted.
            let resp3 = build_response(
                Errors::None,
                INVALID_SESSION_ID,
                0,
                &[("foo".to_string(), foo_id, 0, 0), ("foo".to_string(), foo_id, 1, 0)],
            );
            let r3_partitions: HashSet<TopicPartition> =
                resp3.response_data(&topic_names, version).keys().cloned().collect();
            let issue3 = handler
                .verify_full_fetch_response_partitions(&r3_partitions, &resp3.topic_ids(), version)
                .unwrap();
            assert!(issue3.contains("omittedPartitions="));
            assert!(!issue3.contains("extraPartitions="));
        }
    }

    /// Translated from
    /// `FetchSessionHandlerTest.testVerifyFullFetchResponsePartitionsWithTopicIds`.
    ///
    /// Exercises the v13+ `extraIds` reporting branch in
    /// `verifyFullFetchResponsePartitions`: a response that carries a
    /// topic id (`extra2`) not in the session must produce an
    /// `extraPartitions=` + `extraIds=` issue string.
    #[test]
    fn test_verify_full_fetch_response_partitions_with_topic_ids() {
        let mut topic_ids = HashMap::new();
        let mut topic_names = HashMap::new();
        let version = ApiKeys::FETCH.latest_version();
        add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
        add_topic_id(&mut topic_ids, &mut topic_names, "bar", version);
        add_topic_id(&mut topic_ids, &mut topic_names, "extra2", version);
        let foo_id = topic_ids["foo"];
        let bar_id = topic_ids["bar"];
        let extra2_id = topic_ids["extra2"];

        let mut handler = FetchSessionHandler::new(1);

        // Before any session — every partition (including extra2) is
        // "extra".
        let resp1 = build_response(
            Errors::None,
            INVALID_SESSION_ID,
            0,
            &[
                ("foo".to_string(), foo_id, 0, 0),
                ("extra2".to_string(), extra2_id, 1, 0),
                ("bar".to_string(), bar_id, 0, 0),
            ],
        );
        let r1_partitions: HashSet<TopicPartition> =
            resp1.response_data(&topic_names, version).keys().cloned().collect();
        let issue = handler
            .verify_full_fetch_response_partitions(&r1_partitions, &resp1.topic_ids(), version)
            .unwrap();
        assert!(issue.contains("extraPartitions="), "{issue}");
        assert!(!issue.contains("omittedPartitions="), "{issue}");

        // Seed the session with foo/0 and bar/0; do NOT include extra2.
        let mut b = handler.new_builder();
        b.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
        b.add(tp("bar", 0), pd(bar_id, 20, 120, 220));
        handler.build_request(b);

        // Response still includes extra2 → extraIds should be reported.
        let resp2 = build_response(
            Errors::None,
            INVALID_SESSION_ID,
            0,
            &[
                ("foo".to_string(), foo_id, 0, 0),
                ("extra2".to_string(), extra2_id, 1, 0),
                ("bar".to_string(), bar_id, 0, 0),
            ],
        );
        let r2_partitions: HashSet<TopicPartition> =
            resp2.response_data(&topic_names, version).keys().cloned().collect();
        let issue2 = handler
            .verify_full_fetch_response_partitions(&r2_partitions, &resp2.topic_ids(), version)
            .unwrap();
        assert!(issue2.contains("extraPartitions="), "{issue2}");
        assert!(!issue2.contains("omittedPartitions="), "{issue2}");

        // Response without extra2 → no issue.
        let resp3 = build_response(
            Errors::None,
            INVALID_SESSION_ID,
            0,
            &[("foo".to_string(), foo_id, 0, 0), ("bar".to_string(), bar_id, 0, 0)],
        );
        let r3_partitions: HashSet<TopicPartition> =
            resp3.response_data(&topic_names, version).keys().cloned().collect();
        assert!(
            handler
                .verify_full_fetch_response_partitions(&r3_partitions, &resp3.topic_ids(), version)
                .is_none()
        );
    }

    /// Translated from `testTopLevelErrorResetsMetadata`.
    #[test]
    fn test_top_level_error_resets_metadata() {
        let mut topic_ids = HashMap::new();
        let mut topic_names = HashMap::new();
        let version = ApiKeys::FETCH.latest_version();
        add_topic_id(&mut topic_ids, &mut topic_names, "foo", version);
        let foo_id = topic_ids.get("foo").copied().unwrap_or(Uuid::zero());

        let mut handler = FetchSessionHandler::new(1);
        let mut b1 = handler.new_builder();
        b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
        b1.add(tp("foo", 1), pd(foo_id, 10, 110, 210));
        let d1 = handler.build_request(b1);
        assert_eq!(INVALID_SESSION_ID, d1.metadata.session_id());
        assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, d1.metadata.epoch());

        let r1 = build_response(
            Errors::None,
            123,
            0,
            &[("foo".to_string(), foo_id, 0, 0), ("foo".to_string(), foo_id, 1, 0)],
        );
        assert!(handler.handle_response(&r1, version));

        // Top-level UNKNOWN_TOPIC_ID error should mark the session as
        // needing reset on the next builder.
        let r2 = build_response(Errors::UnknownTopicId, 123, 0, &[]);
        assert!(!handler.handle_response(&r2, version));

        // Next builder should produce a full fetch (epoch=INITIAL) but
        // retain the session id.
        let b3 = handler.new_builder();
        let d3 = handler.build_request(b3);
        assert_eq!(123, d3.metadata.session_id());
        assert_eq!(crate::common::requests::fetch_metadata::INITIAL_EPOCH, d3.metadata.epoch());
    }

    /// Empty full-fetch responses with throttle_time_ms > 0 should be
    /// treated as a no-op (per KIP-219), not as an error condition.
    #[test]
    fn test_empty_full_fetch_throttle_response() {
        let mut handler = FetchSessionHandler::new(1);
        let b = handler.new_builder();
        let _ = handler.build_request(b);

        let r = build_response(Errors::None, 0, 500, &[]);
        // Returns false (response can't be processed) but throttle is
        // silently accepted.
        assert!(!handler.handle_response(&r, 12));
        // After an empty full fetch + throttle, metadata is reset to INITIAL.
        let next = handler.new_builder();
        let d = handler.build_request(next);
        assert!(d.metadata.is_full());
        assert_eq!(INVALID_SESSION_ID, d.metadata.session_id());
    }

    /// `notify_close` and `handle_error` advance metadata to close-existing
    /// states.
    #[test]
    fn test_notify_close_and_handle_error() {
        let foo_id = Uuid::random_uuid();
        let mut handler = FetchSessionHandler::new(1);
        let mut b1 = handler.new_builder();
        b1.add(tp("foo", 0), pd(foo_id, 0, 100, 200));
        handler.build_request(b1);
        // Response must include foo/0 so the full-fetch verifier passes.
        let r1 = build_response(Errors::None, 42, 0, &[("foo".to_string(), foo_id, 0, 0)]);
        assert!(handler.handle_response(&r1, ApiKeys::FETCH.latest_version()));

        // notify_close should drive epoch to FINAL_EPOCH.
        handler.notify_close();
        assert_eq!(42, handler.session_id());
        assert_eq!(
            crate::common::requests::fetch_metadata::FINAL_EPOCH,
            handler.next_metadata.epoch()
        );

        // handle_error should reset epoch to INITIAL_EPOCH (close + new).
        handler.handle_error(&crate::common::KafkaError::illegal_state("simulated"));
        assert_eq!(42, handler.session_id());
        assert_eq!(
            crate::common::requests::fetch_metadata::INITIAL_EPOCH,
            handler.next_metadata.epoch()
        );
    }

    /// `session_topic_partitions` returns the current session's TPs.
    #[test]
    fn test_session_topic_partitions() {
        let id = Uuid::random_uuid();
        let mut handler = FetchSessionHandler::new(1);
        let mut b = handler.new_builder();
        b.add(tp("foo", 0), pd(id, 0, 100, 200));
        b.add(tp("foo", 1), pd(id, 0, 100, 200));
        handler.build_request(b);
        assert_eq!(2, handler.session_topic_partitions().len());
        assert!(handler.session_topic_partitions().contains(&tp("foo", 0)));
        assert!(handler.session_topic_partitions().contains(&tp("foo", 1)));
    }
}
