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

//! `TxnOffsetCommit` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitResponse`.
//!
//! Errors are reported per partition; there is no top-level error code at any
//! version. From v6 (KIP-1319) topics are identified by id and carry no name on
//! the wire, so a reader keys them by id ([`TxnOffsetCommitResponse::use_topic_ids`]).
//!
//! Possible error codes:
//!  - `InvalidProducerEpoch` (47)
//!  - `NotCoordinator` (16)
//!  - `CoordinatorNotAvailable` (15)
//!  - `CoordinatorLoadInProgress` (14)
//!  - `OffsetMetadataTooLarge` (12)
//!  - `GroupAuthorizationFailed` (30)
//!  - `InvalidCommitOffsetSize` (28)
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `UnsupportedForMessageFormat` (43)
//!  - `RequestTimedOut` (7)
//!  - `UnknownMemberId` (25)
//!  - `FencedInstanceId` (82)
//!  - `IllegalGeneration` (22)

use std::collections::HashMap;
use std::io;

use crate::TxnOffsetCommitResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{Error, TopicPartition, Uuid};
use crate::txn_offset_commit_request_data::TxnOffsetCommitRequestPartition;
use crate::txn_offset_commit_response_data::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic};

use super::AbstractResponse;

/// A `TxnOffsetCommit` response.
///
/// Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse")]
pub struct TxnOffsetCommitResponse {
    data: TxnOffsetCommitResponseData,
}

impl TxnOffsetCommitResponse {
    /// Whether the wire protocol identifies topics by id at the given version
    /// (v6+, KIP-1319).
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#useTopicIds")]
    pub fn use_topic_ids(version: i16) -> bool {
        version >= 6
    }

    /// Returns a response builder keyed by topic id (`use_topic_ids`) or by topic
    /// name.
    ///
    /// Corresponds to Java's static `newBuilder(boolean)`, which returns a
    /// `TopicIdBuilder` or a `TopicNameBuilder`.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#newBuilder")]
    pub fn new_builder(use_topic_ids: bool) -> Builder {
        Builder {
            data: TxnOffsetCommitResponseData::new(),
            index: if use_topic_ids {
                TopicIndex::ByTopicId(HashMap::new())
            } else {
                TopicIndex::ByTopicName(HashMap::new())
            },
        }
    }

    /// Creates a new `TxnOffsetCommitResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#TxnOffsetCommitResponse")]
    pub fn with_data(data: TxnOffsetCommitResponseData) -> Self {
        Self { data }
    }

    /// Builds a response from a per-partition error map, keyed by topic name.
    ///
    /// Corresponds to Java's second constructor,
    /// `TxnOffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors>)`.
    /// Java groups through a `HashMap` and so has unspecified order; this sorts by
    /// topic name and then partition index for a deterministic encoding — see
    /// `.claude/rules/producer-transactions.md` §10.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#TxnOffsetCommitResponse")]
    pub fn with_request_throttle_ms_response_data(
        request_throttle_ms: i32,
        response_data: &HashMap<TopicPartition, Errors>,
    ) -> Self {
        let mut by_topic: HashMap<&str, Vec<(i32, Errors)>> = HashMap::new();
        for (topic_partition, error) in response_data {
            by_topic
                .entry(topic_partition.topic())
                .or_default()
                .push((topic_partition.partition(), *error));
        }

        let mut names: Vec<&str> = by_topic.keys().copied().collect();
        names.sort_unstable();

        let topics = names
            .into_iter()
            .map(|name| {
                let mut entries = by_topic[name].clone();
                entries.sort_unstable_by_key(|(index, _)| *index);

                let partitions = entries
                    .into_iter()
                    .map(|(index, error)| {
                        let mut partition = TxnOffsetCommitResponsePartition::new();
                        partition.set_error_code(error.code()).set_partition_index(index);
                        partition
                    })
                    .collect();

                let mut topic = TxnOffsetCommitResponseTopic::new();
                topic.set_name(name.to_string()).set_partitions(partitions);
                topic
            })
            .collect();

        let mut data = TxnOffsetCommitResponseData::new();
        data.set_topics(topics).set_throttle_time_ms(request_throttle_ms);
        Self::with_data(data)
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::TXN_OFFSET_COMMIT
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#data")]
    pub fn data(&self) -> &TxnOffsetCommitResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut TxnOffsetCommitResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v1+.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#shouldClientThrottle")]
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Returns error counts by [`Errors`].
    ///
    /// Counts every partition's error. Unlike most responses there is no
    /// top-level code to fold in.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses a `TxnOffsetCommitResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = TxnOffsetCommitResponseData::read(readable, version)?;
        Ok(Self::with_data(data))
    }
}

impl std::fmt::Display for TxnOffsetCommitResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Assembles a [`TxnOffsetCommitResponse`] topic by topic (20c2450e5b, baa064e422).
///
/// Corresponds to Java's abstract `TxnOffsetCommitResponse.Builder` **and** its two
/// subclasses `TopicIdBuilder` / `TopicNameBuilder`. Java's subclasses differ only
/// in the key of the lookup map behind the abstract `add` / `get` / `getOrCreate`;
/// Rust has no inheritance, so that map is the private `TopicIndex` enum and
/// [`TxnOffsetCommitResponse::new_builder`] picks the variant, as Java's factory
/// picks the subclass. This is a justified deviation (DoD #7): one type instead of
/// three, with the same behaviour, including Java's `IllegalArgumentException` on a
/// missing key.
///
/// Java's map holds the topic objects themselves; the index here holds each
/// topic's position in the response's topic list, which is the same thing for a
/// list that only grows.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder")]
pub struct Builder {
    data: TxnOffsetCommitResponseData,
    index: TopicIndex,
}

/// The lookup map of Java's `TopicIdBuilder.byTopicId` / `TopicNameBuilder.byTopicName`.
#[derive(Debug, Clone)]
enum TopicIndex {
    /// `TopicIdBuilder`: topics keyed by id.
    ByTopicId(HashMap<Uuid, usize>),
    /// `TopicNameBuilder`: topics keyed by name.
    ByTopicName(HashMap<String, usize>),
}

impl Builder {
    /// Java's `IllegalArgumentException("TopicId cannot be null.")` /
    /// `("TopicName cannot be null.")`: the key this builder indexes by is absent.
    fn missing_key_error(&self) -> Error {
        Error::local_illegal_argument(match self.index {
            TopicIndex::ByTopicId(_) => "TopicId cannot be null.",
            TopicIndex::ByTopicName(_) => "TopicName cannot be null.",
        })
    }

    /// Java's abstract `add(TxnOffsetCommitResponseTopic)`: appends a topic and
    /// indexes it. Its callers pass a topic whose id and name are always present
    /// (the generated fields are not nullable), so Java's null check has no
    /// reachable failure here.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#add")]
    fn add(&mut self, topic: TxnOffsetCommitResponseTopic) {
        let position = self.data.topics.len();
        match &mut self.index {
            TopicIndex::ByTopicId(by_topic_id) => {
                by_topic_id.insert(topic.topic_id, position);
            },
            TopicIndex::ByTopicName(by_topic_name) => {
                by_topic_name.insert(topic.name.clone(), position);
            },
        }
        self.data.topics.push(topic);
    }

    /// Java's abstract `get(Uuid, String)`: the position of the topic with this key,
    /// if any.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#get")]
    fn get(&self, topic_id: Uuid, topic_name: &str) -> Option<usize> {
        match &self.index {
            TopicIndex::ByTopicId(by_topic_id) => by_topic_id.get(&topic_id).copied(),
            TopicIndex::ByTopicName(by_topic_name) => by_topic_name.get(topic_name).copied(),
        }
    }

    /// Java's abstract `getOrCreate(Uuid, String)`: the topic with this key,
    /// appended (with both the id and the name it was given) if it is new.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalArgument`] when the key this builder indexes by is
    /// `None`, with Java's message.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#getOrCreate")]
    fn get_or_create(
        &mut self,
        topic_id: Option<Uuid>,
        topic_name: Option<&str>,
    ) -> Result<&mut TxnOffsetCommitResponseTopic, Error> {
        let existing = match &self.index {
            TopicIndex::ByTopicId(by_topic_id) => {
                let topic_id = topic_id.ok_or_else(|| self.missing_key_error())?;
                by_topic_id.get(&topic_id).copied()
            },
            TopicIndex::ByTopicName(by_topic_name) => {
                let topic_name = topic_name.ok_or_else(|| self.missing_key_error())?;
                by_topic_name.get(topic_name).copied()
            },
        };
        let position = match existing {
            Some(position) => position,
            None => {
                // Java sets whatever it was given, `null` included; the generated
                // fields are not nullable, so `null` becomes their default.
                let mut topic = TxnOffsetCommitResponseTopic::new();
                topic
                    .set_name(topic_name.unwrap_or_default().to_string())
                    .set_topic_id(topic_id.unwrap_or_else(Uuid::zero));
                self.add(topic);
                self.data.topics.len() - 1
            },
        };
        Ok(&mut self.data.topics[position])
    }

    /// Adds one partition with `error` to the topic identified by `topic_id` /
    /// `topic_name`, creating the topic if needed.
    ///
    /// Java's `addPartition(Uuid, String, int, Errors)`. Java's id and name are
    /// nullable references, so they are `Option`s here.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalArgument`] if the key this builder indexes by is `None`
    /// (`"TopicId cannot be null."` / `"TopicName cannot be null."`), as Java's
    /// `IllegalArgumentException`.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#addPartition")]
    pub fn add_partition(
        &mut self,
        topic_id: Option<Uuid>,
        topic_name: Option<&str>,
        partition_index: i32,
        error: Errors,
    ) -> Result<&mut Self, Error> {
        let topic_response = self.get_or_create(topic_id, topic_name)?;
        let mut partition = TxnOffsetCommitResponsePartition::new();
        partition.set_partition_index(partition_index).set_error_code(error.code());
        topic_response.partitions.push(partition);
        Ok(self)
    }

    /// Adds every partition in `partitions`, with `error`, to the topic identified
    /// by `topic_id` / `topic_name`; `partition_index` extracts each index.
    ///
    /// Java's `addPartitions(Uuid, String, List<..>, Function<.., Integer>, Errors)`.
    ///
    /// # Errors
    ///
    /// As [`Self::add_partition`].
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#addPartitions")]
    pub fn add_partitions(
        &mut self,
        topic_id: Option<Uuid>,
        topic_name: Option<&str>,
        partitions: &[TxnOffsetCommitRequestPartition],
        partition_index: impl Fn(&TxnOffsetCommitRequestPartition) -> i32,
        error: Errors,
    ) -> Result<&mut Self, Error> {
        let topic_response = self.get_or_create(topic_id, topic_name)?;
        for partition in partitions {
            let mut response_partition = TxnOffsetCommitResponsePartition::new();
            response_partition
                .set_partition_index(partition_index(partition))
                .set_error_code(error.code());
            topic_response.partitions.push(response_partition);
        }
        Ok(self)
    }

    /// Merges `new_data` into the response being built.
    ///
    /// Java's `merge(TxnOffsetCommitResponseData)`. As in Java, an empty builder
    /// adopts `new_data` wholesale — without indexing its topics — and otherwise
    /// each topic is either appended or has its partitions appended to the
    /// existing topic with the same key. Partitions are expected not to overlap;
    /// as in Java, that is not checked.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#merge")]
    pub fn merge(&mut self, new_data: TxnOffsetCommitResponseData) -> &mut Self {
        if self.data.topics.is_empty() {
            // If the current data is empty, we can discard it and use the new data.
            self.data = new_data;
        } else {
            // Otherwise, we have to merge them together.
            for new_topic in new_data.topics {
                match self.get(new_topic.topic_id, &new_topic.name) {
                    // If no topic exists, we can directly copy the new topic data.
                    None => self.add(new_topic),
                    // Otherwise, we add the partitions to the existing one. Note we
                    // expect non-overlapping partitions here as we don't verify
                    // if the partition is already in the list before adding it.
                    Some(position) => self.data.topics[position].partitions.extend(new_topic.partitions),
                }
            }
        }
        self
    }

    /// Returns the response assembled so far.
    ///
    /// Java's `build()`. The builder stays usable, as Java's does.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponse$Builder#build")]
    pub fn build(&self) -> TxnOffsetCommitResponse {
        TxnOffsetCommitResponse::with_data(self.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::ConcreteResponse;

    // Fixture values of `OffsetCommitResponseTest` (Java 41-50), which
    // `TxnOffsetCommitResponseTest` extends.
    const THROTTLE_TIME_MS: i32 = 10;
    const TOPIC_ONE: &str = "topic1";
    const PARTITION_ONE: i32 = 1;
    const ERROR_ONE: Errors = Errors::CoordinatorNotAvailable;
    const ERROR_TWO: Errors = Errors::NotCoordinator;
    const TOPIC_TWO: &str = "topic2";
    const PARTITION_TWO: i32 = 2;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn expected_error_counts() -> HashMap<Errors, i32> {
        HashMap::from([(ERROR_ONE, 1), (ERROR_TWO, 1)])
    }

    fn errors_map() -> HashMap<TopicPartition, Errors> {
        HashMap::from([
            (tp(TOPIC_ONE, PARTITION_ONE), ERROR_ONE),
            (tp(TOPIC_TWO, PARTITION_TWO), ERROR_TWO),
        ])
    }

    fn partition(index: i32, error: Errors) -> TxnOffsetCommitResponsePartition {
        let mut partition = TxnOffsetCommitResponsePartition::new();
        partition.set_partition_index(index).set_error_code(error.code());
        partition
    }

    fn topic(
        topic_id: Uuid,
        name: &str,
        partitions: Vec<TxnOffsetCommitResponsePartition>,
    ) -> TxnOffsetCommitResponseTopic {
        let mut topic = TxnOffsetCommitResponseTopic::new();
        topic
            .set_topic_id(topic_id)
            .set_name(name.to_string())
            .set_partitions(partitions);
        topic
    }

    fn data(topics: Vec<TxnOffsetCommitResponseTopic>) -> TxnOffsetCommitResponseData {
        let mut data = TxnOffsetCommitResponseData::new();
        data.set_topics(topics);
        data
    }

    /// `useTopicIds ? topicId : Uuid.ZERO_UUID`, as Java's parameterised tests
    /// compute their keys.
    fn id_or_zero(use_topic_ids: bool, topic_id: Uuid) -> Uuid {
        if use_topic_ids { topic_id } else { Uuid::zero() }
    }

    /// Translated from
    /// `TxnOffsetCommitResponseTest.testConstructorWithErrorResponse`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testConstructorWithErrorResponse")]
    fn test_constructor_with_error_response() {
        let response = TxnOffsetCommitResponse::with_request_throttle_ms_response_data(THROTTLE_TIME_MS, &errors_map());

        assert_eq!(response.error_counts(), expected_error_counts());
        assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS);
    }

    /// Translated from `TxnOffsetCommitResponseTest.testParse`, over every
    /// version. Java's topics carry neither a name nor an id, so the same data
    /// encodes at every version.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testParse")]
    fn test_parse() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let mut response_data = data(vec![
                topic(Uuid::zero(), "", vec![partition(PARTITION_ONE, ERROR_ONE)]),
                topic(Uuid::zero(), "", vec![partition(PARTITION_TWO, ERROR_TWO)]),
            ]);
            response_data.set_throttle_time_ms(THROTTLE_TIME_MS);

            let mut concrete = ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::with_data(response_data));
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let response = TxnOffsetCommitResponse::parse(&mut buffer, version).expect("parse");

            assert_eq!(response.error_counts(), expected_error_counts(), "v{version}");
            assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
            assert_eq!(response.should_client_throttle(version), version >= 1, "v{version}");
        }
    }

    /// Translated from `TxnOffsetCommitResponseTest.testBuilderAddPartition`
    /// (`@ValueSource(booleans = {false, true})`).
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderAddPartition")]
    fn test_builder_add_partition() {
        let (topic_one_id, topic_two_id) = (Uuid::random_uuid(), Uuid::random_uuid());
        for use_topic_ids in [false, true] {
            let topic_one_id_or_zero = id_or_zero(use_topic_ids, topic_one_id);
            let topic_two_id_or_zero = id_or_zero(use_topic_ids, topic_two_id);

            let mut builder = TxnOffsetCommitResponse::new_builder(use_topic_ids);
            builder
                .add_partition(Some(topic_one_id_or_zero), Some(TOPIC_ONE), PARTITION_ONE, ERROR_ONE)
                .expect("add");
            builder
                .add_partition(Some(topic_one_id_or_zero), Some(TOPIC_ONE), PARTITION_TWO, ERROR_TWO)
                .expect("add");
            builder
                .add_partition(Some(topic_two_id_or_zero), Some(TOPIC_TWO), PARTITION_ONE, ERROR_ONE)
                .expect("add");

            let expected = data(vec![
                topic(
                    topic_one_id_or_zero,
                    TOPIC_ONE,
                    vec![partition(PARTITION_ONE, ERROR_ONE), partition(PARTITION_TWO, ERROR_TWO)],
                ),
                topic(topic_two_id_or_zero, TOPIC_TWO, vec![partition(PARTITION_ONE, ERROR_ONE)]),
            ]);
            assert_eq!(builder.build().data(), &expected, "useTopicIds={use_topic_ids}");
        }
    }

    /// Translated from `TxnOffsetCommitResponseTest.testBuilderAddPartitions`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderAddPartitions")]
    fn test_builder_add_partitions() {
        let topic_one_id = Uuid::random_uuid();
        for use_topic_ids in [false, true] {
            let topic_one_id_or_zero = id_or_zero(use_topic_ids, topic_one_id);
            let request_partitions: Vec<TxnOffsetCommitRequestPartition> = [PARTITION_ONE, PARTITION_TWO]
                .into_iter()
                .map(|index| {
                    let mut partition = TxnOffsetCommitRequestPartition::new();
                    partition.set_partition_index(index);
                    partition
                })
                .collect();

            let mut builder = TxnOffsetCommitResponse::new_builder(use_topic_ids);
            builder
                .add_partitions(
                    Some(topic_one_id_or_zero),
                    Some(TOPIC_ONE),
                    &request_partitions,
                    |partition| partition.partition_index,
                    ERROR_ONE,
                )
                .expect("add");

            let expected = data(vec![topic(
                topic_one_id_or_zero,
                TOPIC_ONE,
                vec![partition(PARTITION_ONE, ERROR_ONE), partition(PARTITION_TWO, ERROR_ONE)],
            )]);
            assert_eq!(builder.build().data(), &expected, "useTopicIds={use_topic_ids}");
        }
    }

    /// Translated from `TxnOffsetCommitResponseTest.testBuilderMergeIntoEmpty`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderMergeIntoEmpty")]
    fn test_builder_merge_into_empty() {
        let topic_one_id = Uuid::random_uuid();
        for use_topic_ids in [false, true] {
            let new_data = data(vec![topic(
                id_or_zero(use_topic_ids, topic_one_id),
                TOPIC_ONE,
                vec![partition(PARTITION_ONE, ERROR_ONE)],
            )]);

            let response = TxnOffsetCommitResponse::new_builder(use_topic_ids)
                .merge(new_data.clone())
                .build();

            assert_eq!(response.data(), &new_data, "useTopicIds={use_topic_ids}");
        }
    }

    /// Translated from `TxnOffsetCommitResponseTest.testBuilderMergeAddsNewTopic`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderMergeAddsNewTopic")]
    fn test_builder_merge_adds_new_topic() {
        let (topic_one_id, topic_two_id) = (Uuid::random_uuid(), Uuid::random_uuid());
        for use_topic_ids in [false, true] {
            let topic_one_id_or_zero = id_or_zero(use_topic_ids, topic_one_id);
            let topic_two_id_or_zero = id_or_zero(use_topic_ids, topic_two_id);

            let mut builder = TxnOffsetCommitResponse::new_builder(use_topic_ids);
            builder
                .add_partition(Some(topic_one_id_or_zero), Some(TOPIC_ONE), PARTITION_ONE, ERROR_ONE)
                .expect("add");

            let new_data = data(vec![topic(
                topic_two_id_or_zero,
                TOPIC_TWO,
                vec![partition(PARTITION_TWO, ERROR_TWO)],
            )]);

            let expected = data(vec![
                topic(topic_one_id_or_zero, TOPIC_ONE, vec![partition(PARTITION_ONE, ERROR_ONE)]),
                topic(topic_two_id_or_zero, TOPIC_TWO, vec![partition(PARTITION_TWO, ERROR_TWO)]),
            ]);
            assert_eq!(builder.merge(new_data).build().data(), &expected, "useTopicIds={use_topic_ids}");
        }
    }

    /// Translated from
    /// `TxnOffsetCommitResponseTest.testBuilderMergeAppendsToExistingTopic`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderMergeAppendsToExistingTopic"
    )]
    fn test_builder_merge_appends_to_existing_topic() {
        let topic_one_id = Uuid::random_uuid();
        for use_topic_ids in [false, true] {
            let topic_one_id_or_zero = id_or_zero(use_topic_ids, topic_one_id);

            let mut builder = TxnOffsetCommitResponse::new_builder(use_topic_ids);
            builder
                .add_partition(Some(topic_one_id_or_zero), Some(TOPIC_ONE), PARTITION_ONE, ERROR_ONE)
                .expect("add");

            let new_data = data(vec![topic(
                topic_one_id_or_zero,
                TOPIC_ONE,
                vec![partition(PARTITION_TWO, ERROR_TWO)],
            )]);

            let expected = data(vec![topic(
                topic_one_id_or_zero,
                TOPIC_ONE,
                vec![partition(PARTITION_ONE, ERROR_ONE), partition(PARTITION_TWO, ERROR_TWO)],
            )]);
            assert_eq!(builder.merge(new_data).build().data(), &expected, "useTopicIds={use_topic_ids}");
        }
    }

    /// Translated from `TxnOffsetCommitResponseTest.testBuilderRejectsNullKey`:
    /// the id builder rejects a missing id, the name builder a missing name. Java
    /// asserts only the class; the message is asserted here too.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitResponseTest#testBuilderRejectsNullKey")]
    fn test_builder_rejects_null_key() {
        for use_topic_ids in [false, true] {
            let mut builder = TxnOffsetCommitResponse::new_builder(use_topic_ids);
            let (topic_id, topic_name) = if use_topic_ids {
                (None, Some(TOPIC_ONE))
            } else {
                (Some(Uuid::zero()), None)
            };
            let Err(error) = builder.add_partition(topic_id, topic_name, PARTITION_ONE, ERROR_ONE) else {
                panic!("a missing key must be rejected (useTopicIds={use_topic_ids})");
            };
            assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
            assert_eq!(
                error.message(),
                if use_topic_ids {
                    "TopicId cannot be null."
                } else {
                    "TopicName cannot be null."
                }
            );
            assert!(builder.build().data().topics.is_empty(), "nothing is added on rejection");
        }
    }

    /// `useTopicIds` switches at v6 (319dd61cb3).
    #[test]
    fn test_use_topic_ids_from_v6() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            assert_eq!(TxnOffsetCommitResponse::use_topic_ids(version), version >= 6, "v{version}");
        }
    }

    /// Grouping is deterministic (rules §10): topics by name, partitions by index.
    #[test]
    fn test_from_error_map_is_deterministic() {
        let mut map = HashMap::new();
        for (topic, partition) in [("topic-b", 5), ("topic-a", 9), ("topic-b", 1), ("topic-a", 0)] {
            map.insert(tp(topic, partition), Errors::None);
        }

        let response = TxnOffsetCommitResponse::with_request_throttle_ms_response_data(0, &map);
        let topics = &response.data().topics;
        assert_eq!(topics[0].name, "topic-a");
        assert_eq!(
            topics[0].partitions.iter().map(|p| p.partition_index).collect::<Vec<_>>(),
            vec![0, 9]
        );
        assert_eq!(topics[1].name, "topic-b");
        assert_eq!(
            topics[1].partitions.iter().map(|p| p.partition_index).collect::<Vec<_>>(),
            vec![1, 5]
        );
    }

    #[test]
    fn test_error_counts_aggregates_across_topics() {
        let map = HashMap::from([
            (tp("topic-a", 0), Errors::NotCoordinator),
            (tp("topic-a", 1), Errors::NotCoordinator),
            (tp("topic-b", 0), Errors::None),
        ]);
        let response = TxnOffsetCommitResponse::with_request_throttle_ms_response_data(0, &map);

        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator), Some(&2));
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.len(), 2);
    }

    /// This response has no top-level error code, so an empty topic list yields
    /// no counts at all — unlike most responses, which would still count `None`.
    #[test]
    fn test_error_counts_is_empty_when_no_partitions() {
        let response = TxnOffsetCommitResponse::with_data(TxnOffsetCommitResponseData::new());
        assert!(response.error_counts().is_empty());
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = TxnOffsetCommitResponse::with_data(TxnOffsetCommitResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(88);
        assert_eq!(response.throttle_time_ms(), 88);
    }

    #[test]
    fn test_should_client_throttle() {
        let response = TxnOffsetCommitResponse::with_data(TxnOffsetCommitResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
        assert!(response.should_client_throttle(ApiKeys::TXN_OFFSET_COMMIT.latest_version()));
    }

    #[test]
    fn test_api_key() {
        let response = TxnOffsetCommitResponse::with_data(TxnOffsetCommitResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
    }

    /// Round trip at every version: below v6 the name survives and the id is
    /// dropped, from v6 the id survives and the name is dropped (both fields are
    /// ignorable, `producer-transactions.md` §11).
    #[test]
    fn test_serialization_round_trip_all_versions() {
        let topic_id = Uuid::random_uuid();
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let mut response_data = data(vec![topic(
                topic_id,
                "topic-a",
                vec![partition(0, Errors::None), partition(2, Errors::IllegalGeneration)],
            )]);
            response_data.set_throttle_time_ms(11);
            let mut concrete =
                ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::with_data(response_data.clone()));
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let parsed = TxnOffsetCommitResponse::parse(&mut buffer, version).expect("parse");

            let expected_topic = &mut response_data.topics_mut()[0];
            if TxnOffsetCommitResponse::use_topic_ids(version) {
                expected_topic.set_name(String::new());
            } else {
                expected_topic.set_topic_id(Uuid::zero());
            }
            assert_eq!(parsed.data(), &response_data, "v{version}");
        }
    }

    /// Byte-level v6 and v5 encodings (DoD #3), derived from
    /// `TxnOffsetCommitResponse.json` (flexible from v3): v6 writes the topic's
    /// 16-byte id where v5 writes its compact name.
    #[test]
    fn test_v5_and_v6_encodings() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let encode = |version: i16| {
            let mut response_data = data(vec![topic(topic_id, "foo", vec![partition(3, Errors::GroupIdNotFound)])]);
            response_data.set_throttle_time_ms(7);
            let mut concrete = ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::with_data(response_data));
            concrete.serialize(version).expect("serialize").into_buffer()
        };
        let expected = |topic_key: &[u8]| {
            let mut bytes = 7i32.to_be_bytes().to_vec(); // ThrottleTimeMs
            bytes.push(0x02); // Topics: compact array of 1
            bytes.extend_from_slice(topic_key);
            bytes.push(0x02); // Partitions: compact array of 1
            bytes.extend_from_slice(&3i32.to_be_bytes()); // PartitionIndex
            bytes.extend_from_slice(&Errors::GroupIdNotFound.code().to_be_bytes()); // ErrorCode
            bytes.extend_from_slice(&[0x00, 0x00, 0x00]); // partition, topic, response tagged fields
            bytes
        };

        assert_eq!(
            encode(6),
            expected(&[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
            ])
        );
        assert_eq!(encode(5), expected(&[0x04, b'f', b'o', b'o']));
    }
}
