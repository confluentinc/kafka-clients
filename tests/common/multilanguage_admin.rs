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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use confluent_kafka::admin::{
    AlterConfigOp, AlterConfigsOptions, AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, Config,
    ConfigEntry, ConfigSource, ConfigType, CreatePartitionsOptions, CreateTopicsOptions, DeleteRecordsOptions,
    DeleteTopicsOptions, DeletedRecords, DescribeClusterOptions, DescribeConfigsOptions, DescribeLogDirsOptions,
    DescribeReplicaLogDirsOptions, DescribeTopicsOptions, ElectLeadersOptions, ListConfigResourcesOptions,
    ListOffsetsOptions, ListOffsetsResultInfo, ListPartitionReassignmentsOptions, ListTopicsOptions, LogDirDescription,
    NewPartitionReassignment, NewPartitions, NewTopic, OffsetSpec, PartitionReassignment, RecordsToDelete,
    ReplicaInfo, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
#[allow(deprecated)]
use confluent_kafka::admin::{ClientMetricsResourceListing, ListClientMetricsResourcesOptions};
use confluent_kafka::common::acl::AclOperation;
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::{
    ElectionType, KafkaError, Node, TopicPartition, TopicPartitionInfo, TopicPartitionReplica, Uuid,
};
use multilanguage_test_server::proto::admin_service_client::AdminServiceClient;
use multilanguage_test_server::proto::{self};
use tonic::transport::Channel;

use crate::common::admin_backend::{
    AdminBackend, ClusterDescription, ConfigEntryView, ConfigSynonymView, ConfigView, Outcomes, ReplicaLogDirInfoView,
};
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

    /// Reads the `config_resource` variant of a [`proto::ResultKey`].
    fn config_resource_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<ConfigResource, KafkaError> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::ConfigResource(resource)) => self.config_resource(resource),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a ConfigResource"))),
        }
    }

    /// Reads the `replica` variant of a [`proto::ResultKey`].
    fn replica_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<TopicPartitionReplica, KafkaError> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Replica(replica)) => Ok(replica_from_proto(replica)),
            other => {
                Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a TopicPartitionReplica")))
            },
        }
    }

    /// Rebuilds a [`ConfigResource`] from the wire's `Type.id()` code.
    ///
    /// `ConfigResourceType::for_id` maps an unrecognized id to `Unknown` rather
    /// than failing, so an id that does not even fit Java's `byte` is rejected
    /// here instead of silently truncating into a valid-looking type.
    fn config_resource(&self, resource: proto::ConfigResource) -> Result<ConfigResource, KafkaError> {
        let id = i8::try_from(resource.resource_type).map_err(|_| {
            self.protocol_error(format!(
                "ConfigResource.resource_type {} is not a ConfigResource.Type id",
                resource.resource_type
            ))
        })?;
        Ok(ConfigResource::new(ConfigResourceType::for_id(id), resource.name))
    }

    /// Rebuilds a [`LogDirDescription`], preserving the log dir's own error and
    /// the two `OptionalLong` volume sizes.
    fn log_dir_description(&self, description: proto::LogDirDescription) -> Result<LogDirDescription, KafkaError> {
        let mut replica_infos = HashMap::with_capacity(description.replica_infos.len());
        for replica in description.replica_infos {
            let tp = replica
                .partition
                .ok_or_else(|| self.protocol_error("ReplicaInfoEntry with no partition"))?;
            replica_infos.insert(
                TopicPartition::new(tp.topic, tp.partition),
                ReplicaInfo::new(replica.size, replica.offset_lag, replica.is_future),
            );
        }
        // `LogDirDescription::new` is the two-argument Java constructor, which
        // records both volume sizes as absent; `with_volume_bytes` is the
        // four-argument one. Java has no constructor for one present and the
        // other absent, and no broker sends that, so the mixed case is a
        // protocol error rather than a guess.
        match (description.total_bytes, description.usable_bytes) {
            (None, None) => Ok(LogDirDescription::new(
                description.error.map(kafka_error_from_proto),
                replica_infos,
            )),
            (Some(total), Some(usable)) => Ok(LogDirDescription::with_volume_bytes(
                description.error.map(kafka_error_from_proto),
                replica_infos,
                total,
                usable,
            )),
            (total, usable) => Err(self.protocol_error(format!(
                "LogDirDescription reported totalBytes={} and usableBytes={}; Java has no constructor for one \
                 without the other",
                total.is_some(),
                usable.is_some()
            ))),
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
        let authorized_operations = description.authorized_operations.as_ref().map(acl_operations_from_proto);
        let partitions = description
            .partitions
            .into_iter()
            .map(|info| partition_info_from_proto(self.backend, info))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TopicDescription::with_authorized_operations(
            description.name,
            description.is_internal,
            partitions,
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

/// Rebuilds a [`TopicPartitionInfo`], preserving whether the broker reported
/// eligible-leader-replica information at all.
///
/// `elr` / `last_known_elr` are nullable in Java, and "the broker did not report
/// them" is not the same as "the broker reported none" — the distinction the
/// wire's [`proto::NodeList`] wrapper and both bindings' `has_elr` predicates
/// exist to carry. Decoding an absent list as an empty one would make
/// `elr().is_none()` true on the native backend and false on the other three: a
/// three-against-one disagreement with no defect behind it.
///
/// Java offers only a 4-argument constructor (both null) and a 6-argument one
/// (both set), and Rust mirrors that, so one present with the other absent is
/// not representable in either language — hence a protocol error rather than a
/// guess.
fn partition_info_from_proto(
    backend: &'static str,
    info: proto::TopicPartitionInfo,
) -> Result<TopicPartitionInfo, KafkaError> {
    let partition = info.partition;
    let leader = info.leader.map(node_from_proto);
    let replicas = info.replicas.into_iter().map(node_from_proto).collect();
    let isr = info.isr.into_iter().map(node_from_proto).collect();
    let nodes = |list: proto::NodeList| list.nodes.into_iter().map(node_from_proto).collect::<Vec<_>>();
    match (info.elr, info.last_known_elr) {
        (None, None) => Ok(TopicPartitionInfo::with_leader_replicas_isr(partition, leader, replicas, isr)),
        (Some(elr), Some(last_known_elr)) => Ok(TopicPartitionInfo::new(
            partition,
            leader,
            replicas,
            isr,
            nodes(elr),
            nodes(last_known_elr),
        )),
        (elr, last_known_elr) => Err(KafkaError::illegal_state(format!(
            "{backend} backend: TopicPartitionInfo reported elr={} and lastKnownElr={}; Java has no \
             constructor for one without the other",
            elr.is_some(),
            last_known_elr.is_some()
        ))),
    }
}

/// Rebuilds a [`ConfigEntry`] from the five fields `createTopics` reports
/// through every binding. `source` is re-derived from `is_default` only —
/// see `comparable_config` in `admin_backend.rs` for why that is the whole
/// comparable set and why the native backend is projected the same way.
/// Decodes an [`proto::AclOperationList`] into Java's `Set<AclOperation>`.
///
/// The wire carries `AclOperation.code()` values because that is what both
/// bindings expose (`kafka_admin_*_authorized_operation` returns the code as an
/// int32; `admin.py` hands back the same ints). The *list* being absent rather
/// than empty is what says the broker did not report the operations at all, and
/// that distinction is preserved by the caller's `Option`.
fn acl_operations_from_proto(ops: &proto::AclOperationList) -> BTreeSet<AclOperation> {
    ops.operations.iter().map(|code| AclOperation::from_code(*code as i8)).collect()
}

fn config_resource_to_proto(resource: &ConfigResource) -> proto::ConfigResource {
    proto::ConfigResource {
        resource_type: i32::from(resource.resource_type().id()),
        name: resource.name().to_string(),
    }
}

fn replica_to_proto(replica: &TopicPartitionReplica) -> proto::TopicPartitionReplica {
    proto::TopicPartitionReplica {
        topic: replica.topic().to_string(),
        partition: replica.partition(),
        broker_id: replica.broker_id(),
    }
}

fn replica_from_proto(replica: proto::TopicPartitionReplica) -> TopicPartitionReplica {
    TopicPartitionReplica::new(replica.topic, replica.partition, replica.broker_id)
}

/// Rebuilds the nine-field [`ConfigEntryView`] `describeConfigs` reports.
///
/// Unlike [`config_entry_from_proto`], which serves `createTopics` and keeps only
/// the five fields that RPC carries, this preserves `source` / `config_type` /
/// `documentation` / `synonyms` — the enums as the constant-name strings the C
/// boundary uses, since `ConfigSynonym`'s constructor is `pub(crate)` and the
/// production types are not reachable from here anyway (see [`ConfigEntryView`]).
fn full_config_entry_from_proto(entry: proto::ConfigEntry) -> ConfigEntryView {
    ConfigEntryView {
        name: entry.name,
        value: entry.value,
        is_default: entry.is_default,
        is_sensitive: entry.is_sensitive,
        is_read_only: entry.is_read_only,
        source: entry.source,
        config_type: entry.config_type,
        documentation: entry.documentation,
        synonyms: entry
            .synonyms
            .into_iter()
            .map(|synonym| ConfigSynonymView { name: synonym.name, value: synonym.value, source: synonym.source })
            .collect(),
    }
}

/// Encodes Java's **nullable** partition set (`electLeaders`' null `Set`,
/// `listPartitionReassignments`' `Optional.empty()`), both meaning "every
/// partition in the cluster".
///
/// `None` must stay absent on the wire rather than becoming an empty
/// [`proto::TopicPartitionList`]: absent is a cluster-wide operation and empty
/// is a no-op, and the two really are different broker requests
/// (`ElectLeadersRequest.json:29` marks `TopicPartitions`
/// `"nullableVersions": "0+"`). Every layer below this one keeps them apart with
/// an explicit discriminant rather than an emptiness test — the C entry points
/// take an `all_partitions` flag (`read_optional_partition_set` returns without
/// reading the arrays when it is set), `admin.py` computes `partitions is None`
/// into its own column, and `ElectLeadersRequestBuilder::build` calls
/// `set_topic_partitions(None)` versus `Some(vec)`. That is what distinguishes
/// these two from `NewPartitions.new_assignments`, where the FFI's builder
/// *does* collapse absent into empty via `is_empty()`.
fn optional_partitions_to_proto(partitions: Option<HashSet<TopicPartition>>) -> Option<proto::TopicPartitionList> {
    partitions.map(|set| proto::TopicPartitionList { partitions: set.iter().map(tp_to_proto).collect() })
}

/// Encodes an [`OffsetSpec`] as the wire's named kind plus, for
/// `forTimestamp`, its timestamp.
///
/// The kind is deliberately *not* the `ListOffsets` sentinel the C boundary
/// takes; see `admin_service.proto`'s `OffsetSpec` for why naming the variant
/// makes each server's own sentinel table differential instead of merely
/// forwarding one written here.
fn offset_spec_to_proto(spec: OffsetSpec) -> proto::OffsetSpec {
    let (kind, timestamp) = match spec {
        OffsetSpec::Earliest => (proto::offset_spec::Kind::Earliest, None),
        OffsetSpec::Latest => (proto::offset_spec::Kind::Latest, None),
        OffsetSpec::MaxTimestamp => (proto::offset_spec::Kind::MaxTimestamp, None),
        OffsetSpec::EarliestLocal => (proto::offset_spec::Kind::EarliestLocal, None),
        OffsetSpec::LatestTiered => (proto::offset_spec::Kind::LatestTiered, None),
        OffsetSpec::EarliestPendingUpload => (proto::offset_spec::Kind::EarliestPendingUpload, None),
        OffsetSpec::Timestamp(ts) => (proto::offset_spec::Kind::ForTimestamp, Some(ts)),
    };
    proto::OffsetSpec { kind: kind as i32, timestamp }
}

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
                Ok((
                    listing.name.clone(),
                    TopicListing::new(listing.name, topic_id, listing.is_internal),
                ))
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
            partitions: new_partitions
                .iter()
                .map(|(topic, np)| new_partitions_to_proto(topic, np))
                .collect(),
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

    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, KafkaError> {
        let request = proto::DescribeClusterRequest {
            admin_id: self.admin_id,
            timeout_ms: options.timeout(),
            include_authorized_operations: options.should_include_authorized_operations(),
            include_fenced_brokers: options.should_include_fenced_brokers(),
        };
        let response = self.call(|mut c| async move { c.describe_cluster(request).await }).await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        let description = response
            .description
            .ok_or_else(|| self.protocol_error("DescribeClusterResponse with neither description nor error"))?;
        Ok(ClusterDescription {
            cluster_id: description.cluster_id,
            nodes: description.nodes.into_iter().map(node_from_proto).collect(),
            // Java's `controller()` is nullable; absent stays absent rather than
            // becoming a fabricated Node.
            controller: description.controller.map(node_from_proto),
            authorized_operations: description.authorized_operations.map(|ops| acl_operations_from_proto(&ops)),
        })
    }

    async fn describe_configs(
        &self,
        resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ConfigView>, KafkaError> {
        let request = proto::DescribeConfigsRequest {
            admin_id: self.admin_id,
            resources: resources.iter().map(config_resource_to_proto).collect(),
            timeout_ms: options.timeout(),
            include_synonyms: options.should_include_synonyms(),
            include_documentation: options.should_include_documentation(),
        };
        let response = self.call(|mut c| async move { c.describe_configs(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.config_resource_key(entry.key, "describeConfigs")?;
            let outcome = match entry.outcome {
                Some(proto::describe_configs_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_configs_entry::Outcome::Value(v)) => {
                    Ok(ConfigView { entries: v.entries.into_iter().map(full_config_entry_from_proto).collect() })
                },
                None => return Err(self.protocol_error("DescribeConfigsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ()>, KafkaError> {
        let request = proto::IncrementalAlterConfigsRequest {
            admin_id: self.admin_id,
            configs: configs
                .iter()
                .map(|(resource, ops)| proto::ConfigResourceOps {
                    resource: Some(config_resource_to_proto(resource)),
                    ops: ops
                        .iter()
                        .map(|op| proto::AlterConfigOp {
                            name: op.config_entry().name().to_string(),
                            // Absent is Java's null value, which a DELETE carries.
                            value: op.config_entry().value().map(str::to_string),
                            op_type: i32::from(op.op_type().id()),
                        })
                        .collect(),
                })
                .collect(),
            timeout_ms: options.timeout(),
            validate_only: options.should_validate_only(),
        };
        let response = self
            .call(|mut c| async move { c.incremental_alter_configs(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            Ok((
                self.config_resource_key(entry.key, "incrementalAlterConfigs")?,
                void_outcome(entry.error),
            ))
        })
    }

    async fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ConfigResource>, KafkaError> {
        let request = proto::ListConfigResourcesRequest {
            admin_id: self.admin_id,
            resource_types: config_resource_types.iter().map(|t| i32::from(t.id())).collect(),
            timeout_ms: options.timeout(),
        };
        let response = self.call(|mut c| async move { c.list_config_resources(request).await }).await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        response
            .resources
            .into_iter()
            .map(|resource| self.config_resource(resource))
            .collect()
    }

    #[allow(deprecated)]
    async fn list_client_metrics_resources(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> Result<Vec<ClientMetricsResourceListing>, KafkaError> {
        let request =
            proto::ListClientMetricsResourcesRequest { admin_id: self.admin_id, timeout_ms: options.timeout() };
        let response = self
            .call(|mut c| async move { c.list_client_metrics_resources(request).await })
            .await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(response
            .resources
            .into_iter()
            .map(|resource| ClientMetricsResourceListing::new(resource.name))
            .collect())
    }

    async fn describe_log_dirs(
        &self,
        brokers: &[i32],
        options: DescribeLogDirsOptions,
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, KafkaError> {
        let request = proto::DescribeLogDirsRequest {
            admin_id: self.admin_id,
            brokers: brokers.to_vec(),
            timeout_ms: options.timeout(),
        };
        let response = self.call(|mut c| async move { c.describe_log_dirs(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = match entry.key.and_then(|k| k.key) {
                Some(proto::result_key::Key::BrokerId(broker)) => broker,
                other => {
                    return Err(
                        self.protocol_error(format!("describeLogDirs entry keyed by {other:?}, expected a broker id"))
                    );
                },
            };
            let outcome = match entry.outcome {
                Some(proto::describe_log_dirs_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_log_dirs_entry::Outcome::Value(map)) => {
                    // The nested level: one description per log-dir path, each
                    // with its own error.
                    let mut log_dirs = HashMap::with_capacity(map.log_dirs.len());
                    for (path, description) in map.log_dirs {
                        log_dirs.insert(path, self.log_dir_description(description)?);
                    }
                    Ok(log_dirs)
                },
                None => return Err(self.protocol_error("DescribeLogDirsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, KafkaError> {
        let request = proto::AlterReplicaLogDirsRequest {
            admin_id: self.admin_id,
            assignments: replica_assignment
                .iter()
                .map(|(replica, log_dir)| proto::ReplicaLogDirAssignment {
                    replica: Some(replica_to_proto(replica)),
                    log_dir: log_dir.clone(),
                })
                .collect(),
            timeout_ms: options.timeout(),
        };
        let response = self
            .call(|mut c| async move { c.alter_replica_log_dirs(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.replica_key(entry.key, "alterReplicaLogDirs")?, void_outcome(entry.error)))
        })
    }

    async fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, KafkaError> {
        let request = proto::DescribeReplicaLogDirsRequest {
            admin_id: self.admin_id,
            replicas: replicas.iter().map(replica_to_proto).collect(),
            timeout_ms: options.timeout(),
        };
        let response = self
            .call(|mut c| async move { c.describe_replica_log_dirs(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.replica_key(entry.key, "describeReplicaLogDirs")?;
            let outcome = match entry.outcome {
                Some(proto::describe_replica_log_dirs_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_replica_log_dirs_entry::Outcome::Value(v)) => Ok(ReplicaLogDirInfoView {
                    current_replica_log_dir: v.current_replica_log_dir,
                    current_replica_offset_lag: v.current_replica_offset_lag,
                    future_replica_log_dir: v.future_replica_log_dir,
                    future_replica_offset_lag: v.future_replica_offset_lag,
                }),
                None => return Err(self.protocol_error("DescribeReplicaLogDirsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        let request = proto::ElectLeadersRequest {
            admin_id: self.admin_id,
            // Java's public `byte value` field, which is what both bindings take.
            election_type: i32::from(election_type.value()),
            partitions: optional_partitions_to_proto(partitions),
            timeout_ms: options.timeout(),
        };
        let response = self.call(|mut c| async move { c.elect_leaders(request).await }).await?;
        // A VoidKeyedResponse, but note what its two error levels mean here: the
        // top-level one is a failure of Java's *single* future over the whole
        // map, and an absent per-entry error is the `Optional.empty()` inside
        // that map — the election succeeded for that partition.
        keyed(response.error, response.entries, |entry| {
            Ok((self.partition_key(entry.key, "electLeaders")?, void_outcome(entry.error)))
        })
    }

    async fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        let request = proto::AlterPartitionReassignmentsRequest {
            admin_id: self.admin_id,
            reassignments: reassignments
                .iter()
                .map(|(tp, reassignment)| proto::PartitionReassignmentSpec {
                    partition: Some(tp_to_proto(tp)),
                    // Absent is Java's empty `Optional`, which *cancels* the
                    // reassignment. It must not become a present wrapper with an
                    // empty replica list — a state Java rejects — so the `map`
                    // here is load-bearing and every layer below keeps the two
                    // apart with an explicit flag (the C `cancel[i]` argument,
                    // `admin.py`'s `r is None` column).
                    reassignment: reassignment.as_ref().map(|r| proto::NewPartitionReassignment {
                        target_replicas: r.target_replicas().to_vec(),
                    }),
                })
                .collect(),
            timeout_ms: options.timeout(),
            // Java's default is true, so this is optional on the wire.
            allow_replication_factor_change: Some(options.should_allow_replication_factor_change()),
        };
        let response = self
            .call(|mut c| async move { c.alter_partition_reassignments(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            Ok((
                self.partition_key(entry.key, "alterPartitionReassignments")?,
                void_outcome(entry.error),
            ))
        })
    }

    async fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, KafkaError> {
        let request = proto::ListPartitionReassignmentsRequest {
            admin_id: self.admin_id,
            partitions: optional_partitions_to_proto(partitions),
            timeout_ms: options.timeout(),
        };
        let response = self
            .call(|mut c| async move { c.list_partition_reassignments(request).await })
            .await?;
        // A whole-value response: one Java future for the entire map, so any
        // failure arrives here and nothing is keyed.
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        let mut reassignments = HashMap::with_capacity(response.reassignments.len());
        for ongoing in response.reassignments {
            let tp = ongoing
                .partition
                .ok_or_else(|| self.protocol_error("OngoingPartitionReassignment with no partition"))?;
            let reassignment = ongoing
                .reassignment
                .ok_or_else(|| self.protocol_error("OngoingPartitionReassignment with no reassignment"))?;
            reassignments.insert(
                TopicPartition::new(tp.topic, tp.partition),
                PartitionReassignment::new(
                    reassignment.replicas,
                    reassignment.adding_replicas,
                    reassignment.removing_replicas,
                ),
            );
        }
        Ok(reassignments)
    }

    async fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, KafkaError> {
        let request = proto::ListOffsetsRequest {
            admin_id: self.admin_id,
            specs: topic_partition_offsets
                .iter()
                .map(|(tp, spec)| proto::OffsetSpecEntry {
                    partition: Some(tp_to_proto(tp)),
                    spec: Some(offset_spec_to_proto(*spec)),
                })
                .collect(),
            timeout_ms: options.timeout(),
            // Java's `IsolationLevel.id()` wire code.
            isolation_level: i32::from(options.isolation_level().id()),
        };
        let response = self.call(|mut c| async move { c.list_offsets(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.partition_key(entry.key, "listOffsets")?;
            let outcome = match entry.outcome {
                Some(proto::list_offsets_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::list_offsets_entry::Outcome::Value(v)) => {
                    // `leader_epoch` absent is Java's `Optional.empty()`, which
                    // is not the same as epoch 0.
                    Ok(ListOffsetsResultInfo::new(v.offset, v.timestamp, v.leader_epoch))
                },
                None => return Err(self.protocol_error("ListOffsetsEntry with no outcome")),
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
