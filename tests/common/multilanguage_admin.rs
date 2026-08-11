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

//! An [`AdminBackend`] implementation that tunnels every call over gRPC to a
//! server in another language (Python sync, Python asyncio, or C++), mirroring
//! [`crate::common::multilanguage_consumer::MultilanguageConsumer`].
//!
//! Unlike the consumer client, this does not implement a production trait — see
//! [`AdminBackend`] for why the real `Admin` trait is not implementable from
//! `tests/`. Every method is one unary RPC that the server awaits, so the
//! response carries already-resolved results.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::time::Duration;

use confluent_kafka::admin::{
    Config, ConfigEntry, ConfigSource, ConfigType, CreatePartitionsOptions, CreateTopicsOptions, DeleteRecordsOptions,
    DeleteTopicsOptions, DeletedRecords, DescribeTopicsOptions, ListTopicsOptions, NewPartitions, NewTopic,
    RecordsToDelete, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use confluent_kafka::common::acl::AclOperation;
use confluent_kafka::common::{KafkaError, Node, TopicPartition, TopicPartitionInfo, Uuid};
use multilanguage_test_server::proto::admin_service_client::AdminServiceClient;
use multilanguage_test_server::proto::{self};
use tonic::transport::Channel;

use crate::common::admin_backend::{AdminBackend, Outcomes};
use crate::common::multilanguage_producer::{kafka_error_from_proto, status_to_kafka_error};

/// gRPC-backed admin client driven by an out-of-process server in another
/// language that ultimately calls the same Rust client through a binding.
pub struct MultilanguageAdmin {
    admin_id: u64,
    client: AdminServiceClient<Channel>,
    backend: &'static str,
}

impl MultilanguageAdmin {
    /// Connect to `channel` and create a server-side admin client from
    /// `config`.
    pub async fn new(
        channel: Channel,
        config: HashMap<String, String>,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        Self::create(channel, proto::CreateAdminRequest { config, num_brokers: None }, backend).await
    }

    /// Connect to `channel` and create a server-side *mock* admin client with
    /// `num_brokers` brokers. An empty config is what selects the mock, matching
    /// the producer / consumer backends.
    pub async fn new_mock(channel: Channel, num_brokers: i32, backend: &'static str) -> Result<Self, KafkaError> {
        let request = proto::CreateAdminRequest { config: HashMap::new(), num_brokers: Some(num_brokers) };
        Self::create(channel, request, backend).await
    }

    async fn create(
        channel: Channel,
        request: proto::CreateAdminRequest,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        let mut client = AdminServiceClient::new(channel);
        let response = client
            .create_admin(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Self { admin_id: response.admin_id, client, backend })
    }

    /// The opaque server-local handle id, for log messages.
    pub fn admin_id(&self) -> u64 {
        self.admin_id
    }

    /// Issues one unary RPC on a cloned client, mapping a transport failure to a
    /// `KafkaError` that names the backend.
    ///
    /// The clone is what lets the RPC methods take `&self`: tonic's generated
    /// client needs `&mut self`, and `AdminBackend` is `&self` because Java's
    /// `Admin` methods are (a `&mut self` trait would forbid the perfectly legal
    /// concurrent use a later slice may want).
    async fn call<T, F, Fut>(&self, rpc: F) -> Result<T, KafkaError>
    where
        F: FnOnce(AdminServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        rpc(self.client.clone())
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| status_to_kafka_error(&status, self.backend))
    }

    /// A failure of the harness itself rather than of Kafka: the server sent a
    /// response the wire contract forbids.
    fn protocol_error(&self, what: impl std::fmt::Display) -> KafkaError {
        KafkaError::illegal_state(format!("{} backend: {what}", self.backend))
    }

    /// Reads the `name` variant of a [`proto::ResultKey`], which is the only
    /// variant `rpc` is allowed to answer with.
    fn name_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<String, KafkaError> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Name(name)) => Ok(name),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a topic name"))),
        }
    }

    /// Reads the `topic_id` variant of a [`proto::ResultKey`].
    fn topic_id_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<Uuid, KafkaError> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::TopicId(id)) => self.parse_uuid(&id, "ResultKey.topic_id"),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a topic id"))),
        }
    }

    /// Reads the `partition` variant of a [`proto::ResultKey`].
    fn partition_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<TopicPartition, KafkaError> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Partition(tp)) => Ok(TopicPartition::new(tp.topic, tp.partition)),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a TopicPartition"))),
        }
    }

    /// Parses a canonical (base64) topic id, the form both bindings expose.
    fn parse_uuid(&self, text: &str, what: &str) -> Result<Uuid, KafkaError> {
        Uuid::from_string(text).map_err(|e| self.protocol_error(format!("{what} {text:?} is not a topic id: {e}")))
    }

    /// Builds the shared part of both `describeTopics` requests.
    fn describe_request(
        &self,
        topics: proto::describe_topics_request::Topics,
        options: DescribeTopicsOptions,
    ) -> proto::DescribeTopicsRequest {
        proto::DescribeTopicsRequest {
            admin_id: self.admin_id,
            topics: Some(topics),
            timeout_ms: options.timeout(),
            include_authorized_operations: options.should_include_authorized_operations(),
            partition_size_limit_per_response: Some(options.partition_size_limit()),
        }
    }

    /// Decodes one `describeTopics` entry's outcome, shared by the by-name and
    /// by-id methods.
    fn describe_outcome(
        &self,
        outcome: Option<proto::describe_topics_entry::Outcome>,
    ) -> Result<Result<TopicDescription, KafkaError>, KafkaError> {
        match outcome {
            Some(proto::describe_topics_entry::Outcome::Error(e)) => Ok(Err(kafka_error_from_proto(e))),
            Some(proto::describe_topics_entry::Outcome::Value(v)) => Ok(Ok(self.topic_description(v)?)),
            None => Err(self.protocol_error("DescribeTopicsEntry with no outcome")),
        }
    }

    fn topic_description(&self, description: proto::TopicDescription) -> Result<TopicDescription, KafkaError> {
        let topic_id = self.parse_uuid(&description.topic_id, "TopicDescription.topic_id")?;
        // Java's nullable Set<AclOperation>: absent means the broker did not
        // report the operations, which is not the same as reporting none.
        let authorized_operations = description
            .authorized_operations
            .map(|ops| ops.operations.iter().map(|code| AclOperation::from_code(*code as i8)).collect::<BTreeSet<_>>());
        Ok(TopicDescription::with_authorized_operations(
            description.name,
            description.is_internal,
            description.partitions.into_iter().map(partition_info_from_proto).collect(),
            authorized_operations,
            topic_id,
        ))
    }

    /// Decodes `createTopics`' per-key value, which carries an error of its own
    /// when the topic was created but its metadata was not reported (see the
    /// envelope commentary in `admin_service.proto`).
    fn topic_metadata_and_config(
        &self,
        value: proto::TopicMetadataAndConfig,
    ) -> Result<TopicMetadataAndConfig, KafkaError> {
        match value.result {
            Some(proto::topic_metadata_and_config::Result::Error(e)) => {
                Ok(TopicMetadataAndConfig::with_error(kafka_error_from_proto(e)))
            },
            Some(proto::topic_metadata_and_config::Result::Metadata(m)) => {
                let topic_id = self.parse_uuid(&m.topic_id, "TopicMetadata.topic_id")?;
                Ok(TopicMetadataAndConfig::new(
                    topic_id,
                    m.num_partitions,
                    m.replication_factor,
                    Config::new(m.configs.into_iter().map(config_entry_from_proto).collect::<Vec<_>>()),
                ))
            },
            None => Err(self.protocol_error("TopicMetadataAndConfig with neither metadata nor error")),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire <-> domain conversions
// ---------------------------------------------------------------------------

/// Collects a per-key response into [`Outcomes`], or returns the whole-call
/// error if the server set one.
///
/// Entry order is unspecified by the wire contract (no backend can promise
/// request order — see `admin_service.proto`), so entries are looked up by their
/// [`proto::ResultKey`] and never by index; collecting into a map is what makes
/// that structural here.
fn keyed<E, K, V>(
    error: Option<proto::KafkaError>,
    entries: Vec<E>,
    mut decode: impl FnMut(E) -> Result<(K, Result<V, KafkaError>), KafkaError>,
) -> Result<Outcomes<K, V>, KafkaError>
where
    K: std::hash::Hash + Eq,
{
    if let Some(err) = error {
        return Err(kafka_error_from_proto(err));
    }
    let mut outcomes = HashMap::with_capacity(entries.len());
    for entry in entries {
        let (key, outcome) = decode(entry)?;
        outcomes.insert(key, outcome);
    }
    Ok(outcomes)
}

/// A [`proto::VoidResultEntry`]'s outcome: for a `KafkaFuture<Void>` there is no
/// value, so an absent error *is* the success signal.
fn void_outcome(error: Option<proto::KafkaError>) -> Result<(), KafkaError> {
    match error {
        Some(err) => Err(kafka_error_from_proto(err)),
        None => Ok(()),
    }
}

fn tp_to_proto(tp: &TopicPartition) -> proto::TopicPartition {
    proto::TopicPartition { topic: tp.topic().to_string(), partition: tp.partition() }
}

fn new_topic_to_proto(topic: &NewTopic) -> proto::NewTopic {
    proto::NewTopic {
        name: topic.name().to_string(),
        // Both getters already render "unset" as -1, which is exactly the
        // encoding the wire and both bindings use.
        num_partitions: topic.num_partitions(),
        replication_factor: topic.replication_factor() as i32,
        configs: topic.config_map().cloned().unwrap_or_default().into_iter().collect(),
        replicas_assignments: topic
            .replicas_assignments()
            .map(|assignments| {
                assignments
                    .iter()
                    .map(|(partition, brokers)| proto::ReplicaAssignment {
                        partition: *partition,
                        broker_ids: brokers.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn new_partitions_to_proto(topic: &str, new_partitions: &NewPartitions) -> proto::NewPartitions {
    proto::NewPartitions {
        topic: topic.to_string(),
        total_count: new_partitions.total_count(),
        // Absent selects Java's increaseTo(int); present selects
        // increaseTo(int, List<List<Integer>>) — two different broker requests.
        new_assignments: new_partitions.assignments().map(|assignments| proto::NewPartitionAssignments {
            assignments: assignments
                .iter()
                .map(|brokers| proto::BrokerIdList { broker_ids: brokers.clone() })
                .collect(),
        }),
    }
}

fn node_from_proto(node: proto::Node) -> Node {
    Node::with_rack(node.id, node.host, node.port, node.rack)
}

fn partition_info_from_proto(info: proto::TopicPartitionInfo) -> TopicPartitionInfo {
    TopicPartitionInfo::new(
        info.partition,
        info.leader.map(node_from_proto),
        info.replicas.into_iter().map(node_from_proto).collect(),
        info.isr.into_iter().map(node_from_proto).collect(),
        // Java's elr()/lastKnownElr() are nullable; `TopicPartitionInfo::new`
        // takes plain Vecs, so an absent list becomes empty. Both bindings
        // collapse the distinction the same way, so no backend can observe it.
        info.elr.map(|l| l.nodes.into_iter().map(node_from_proto).collect()).unwrap_or_default(),
        info.last_known_elr.map(|l| l.nodes.into_iter().map(node_from_proto).collect()).unwrap_or_default(),
    )
}

/// Rebuilds a [`ConfigEntry`] from the five fields `createTopics` reports
/// through every binding. `source` is re-derived from `is_default` only —
/// see `comparable_config` in `admin_backend.rs` for why that is the whole
/// comparable set and why the native backend is projected the same way.
fn config_entry_from_proto(entry: proto::ConfigEntry) -> ConfigEntry {
    ConfigEntry::with_metadata(
        entry.name,
        entry.value,
        if entry.is_default {
            ConfigSource::DefaultConfig
        } else {
            ConfigSource::Unknown
        },
        entry.is_sensitive,
        entry.is_read_only,
        Vec::new(),
        ConfigType::Unknown,
        None,
    )
}

impl AdminBackend for MultilanguageAdmin {
    async fn create_topics(
        &self,
        new_topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, KafkaError> {
        let request = proto::CreateTopicsRequest {
            admin_id: self.admin_id,
            topics: new_topics.iter().map(new_topic_to_proto).collect(),
            timeout_ms: options.timeout(),
            validate_only: options.should_validate_only(),
            retry_on_quota_violation: Some(options.should_retry_on_quota_violation()),
        };
        let response = self.call(|mut c| async move { c.create_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "createTopics")?;
            let outcome = match entry.outcome {
                Some(proto::create_topics_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::create_topics_entry::Outcome::Value(v)) => Ok(self.topic_metadata_and_config(v)?),
                None => return Err(self.protocol_error("CreateTopicsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn delete_topics(
        &self,
        names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        let request = proto::DeleteTopicsRequest {
            admin_id: self.admin_id,
            topics: Some(proto::delete_topics_request::Topics::Names(proto::StringList {
                values: names.to_vec(),
            })),
            timeout_ms: options.timeout(),
            retry_on_quota_violation: Some(options.should_retry_on_quota_violation()),
        };
        let response = self.call(|mut c| async move { c.delete_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.name_key(entry.key, "deleteTopics")?, void_outcome(entry.error)))
        })
    }

    async fn delete_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<Uuid, ()>, KafkaError> {
        let request = proto::DeleteTopicsRequest {
            admin_id: self.admin_id,
            topics: Some(proto::delete_topics_request::Topics::TopicIds(proto::StringList {
                values: topic_ids.iter().map(Uuid::to_string).collect(),
            })),
            timeout_ms: options.timeout(),
            retry_on_quota_violation: Some(options.should_retry_on_quota_violation()),
        };
        let response = self.call(|mut c| async move { c.delete_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.topic_id_key(entry.key, "deleteTopics")?, void_outcome(entry.error)))
        })
    }

    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, KafkaError> {
        let request = proto::AdminListTopicsRequest {
            admin_id: self.admin_id,
            timeout_ms: options.timeout(),
            list_internal: options.should_list_internal(),
        };
        let response = self.call(|mut c| async move { c.list_topics(request).await }).await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        response
            .listings
            .into_iter()
            .map(|listing| {
                let topic_id = self.parse_uuid(&listing.topic_id, "TopicListing.topic_id")?;
                Ok((listing.name.clone(), TopicListing::new(listing.name, topic_id, listing.is_internal)))
            })
            .collect()
    }

    async fn describe_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, KafkaError> {
        let request = self.describe_request(
            proto::describe_topics_request::Topics::Names(proto::StringList { values: names.to_vec() }),
            options,
        );
        let response = self.call(|mut c| async move { c.describe_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "describeTopics")?;
            Ok((key, self.describe_outcome(entry.outcome)?))
        })
    }

    async fn describe_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<Uuid, TopicDescription>, KafkaError> {
        let request = self.describe_request(
            proto::describe_topics_request::Topics::TopicIds(proto::StringList {
                values: topic_ids.iter().map(Uuid::to_string).collect(),
            }),
            options,
        );
        let response = self.call(|mut c| async move { c.describe_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.topic_id_key(entry.key, "describeTopics")?;
            Ok((key, self.describe_outcome(entry.outcome)?))
        })
    }

    async fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        let request = proto::CreatePartitionsRequest {
            admin_id: self.admin_id,
            partitions: new_partitions.iter().map(|(topic, np)| new_partitions_to_proto(topic, np)).collect(),
            timeout_ms: options.timeout(),
            validate_only: options.should_validate_only(),
            retry_on_quota_violation: Some(options.should_retry_on_quota_violation()),
        };
        let response = self.call(|mut c| async move { c.create_partitions(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.name_key(entry.key, "createPartitions")?, void_outcome(entry.error)))
        })
    }

    async fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, KafkaError> {
        let request = proto::DeleteRecordsRequest {
            admin_id: self.admin_id,
            records: records_to_delete
                .iter()
                .map(|(tp, records)| proto::RecordsToDelete {
                    partition: Some(tp_to_proto(tp)),
                    before_offset: records.before_offset_value(),
                })
                .collect(),
            timeout_ms: options.timeout(),
        };
        let response = self.call(|mut c| async move { c.delete_records(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.partition_key(entry.key, "deleteRecords")?;
            let outcome = match entry.outcome {
                Some(proto::delete_records_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::delete_records_entry::Outcome::Value(v)) => Ok(DeletedRecords::new(v.low_watermark)),
                None => return Err(self.protocol_error("DeleteRecordsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let request =
            proto::AdminCloseRequest { admin_id: self.admin_id, timeout_ms: timeout.map(|t| t.as_millis() as i64) };
        let response = client
            .close(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    fn name(&self) -> &'static str {
        self.backend
    }
}
