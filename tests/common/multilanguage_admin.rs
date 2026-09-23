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

use confluent_kafka::admin::ConfigEntryOptionsBuilder;
use confluent_kafka::admin::config_entry::ConfigSource;
use confluent_kafka::admin::{
    AbortTransactionOptions, AbortTransactionSpec, AlterClientQuotasOptions, AlterConfigOp, AlterConfigsOptions,
    AlterConsumerGroupOffsetsOptions, AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions,
    AlterUserScramCredentialsOptions, ClassicGroupDescription, Config, ConfigEntry, ConsumerGroupDescription,
    CreateAclsOptions, CreateDelegationTokenOptions, CreatePartitionsOptions, CreateTopicsOptions, DeleteAclsOptions,
    DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
    DeletedRecords, DescribeAclsOptions, DescribeClassicGroupsOptions, DescribeClientQuotasOptions,
    DescribeClusterOptions, DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeDelegationTokenOptions,
    DescribeFeaturesOptions, DescribeLogDirsOptions, DescribeProducersOptions, DescribeReplicaLogDirsOptions,
    DescribeTopicsOptions, DescribeTransactionsOptions, DescribeUserScramCredentialsOptions, ElectLeadersOptions,
    ExpireDelegationTokenOptions, FeatureUpdate, FenceProducersOptions, FilterResult, FilterResults,
    FinalizedVersionRange, GroupListing, GroupOffsets, ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec, ListGroupsOptions, ListOffsetsOptions, ListOffsetsResultInfo,
    ListPartitionReassignmentsOptions, ListTopicsOptions, ListTransactionsOptions, LogDirDescription, MemberAssignment,
    MemberDescription, NewPartitionReassignment, NewPartitions, NewTopic, OffsetSpec, PartitionProducerState,
    PartitionReassignment, ProducerState, RecordsToDelete, RemoveMembersFromConsumerGroupOptions,
    RenewDelegationTokenOptions, ReplicaInfo, ScramCredentialInfo, ScramMechanism, SupportedVersionRange,
    TerminateTransactionOptions, TopicDescription, TopicListing, TopicMetadataAndConfig, TransactionDescription,
    TransactionListing, TransactionState, UpdateFeaturesOptions, UserScramCredentialAlteration,
    UserScramCredentialsDescription,
};
#[allow(deprecated)]
use confluent_kafka::admin::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions, ListConsumerGroupsOptions,
};
use confluent_kafka::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::quota::{
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaMatch,
};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};
use confluent_kafka::common::security::auth::KafkaPrincipal;
use confluent_kafka::common::security::token::delegation::{DelegationToken, TokenInformation};
use confluent_kafka::common::utils::ProducerIdAndEpoch;
use confluent_kafka::common::{
    ClassicGroupState, ElectionType, Error, GroupState, GroupType, Node, TopicPartition, TopicPartitionInfo,
    TopicPartitionReplica, Uuid,
};
use confluent_kafka::consumer::OffsetAndMetadata;
use multilanguage_test_server::proto::admin_service_client::AdminServiceClient;
use multilanguage_test_server::proto::{self};
use tonic::transport::Channel;

use crate::common::admin_backend::{
    AdminBackend, ClusterDescription, ConfigEntryView, ConfigSynonymView, ConfigView, FeatureMetadataView, Listings,
    Outcomes, ReplicaLogDirInfoView,
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
    pub async fn new(channel: Channel, config: HashMap<String, String>, backend: &'static str) -> Result<Self, Error> {
        Self::create(channel, proto::CreateAdminRequest { config, num_brokers: None }, backend).await
    }

    /// Connect to `channel` and create a server-side *mock* admin client with
    /// `num_brokers` brokers. An empty config is what selects the mock, matching
    /// the producer / consumer backends.
    pub async fn new_mock(channel: Channel, num_brokers: i32, backend: &'static str) -> Result<Self, Error> {
        let request = proto::CreateAdminRequest { config: HashMap::new(), num_brokers: Some(num_brokers) };
        Self::create(channel, request, backend).await
    }

    async fn create(
        channel: Channel,
        request: proto::CreateAdminRequest,
        backend: &'static str,
    ) -> Result<Self, Error> {
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
    /// `Error` that names the backend.
    ///
    /// The clone is what lets the RPC methods take `&self`: tonic's generated
    /// client needs `&mut self`, and `AdminBackend` is `&self` because Java's
    /// `Admin` methods are (a `&mut self` trait would forbid the perfectly legal
    /// concurrent use a later slice may want).
    async fn call<T, F, Fut>(&self, rpc: F) -> Result<T, Error>
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
    fn protocol_error(&self, what: impl std::fmt::Display) -> Error {
        Error::local_illegal_state(format!("{} backend: {what}", self.backend))
    }

    /// Reads the `name` variant of a [`proto::ResultKey`], which is the only
    /// variant `rpc` is allowed to answer with.
    fn name_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<String, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Name(name)) => Ok(name),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a topic name"))),
        }
    }

    /// Reads the `topic_id` variant of a [`proto::ResultKey`].
    fn topic_id_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<Uuid, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::TopicId(id)) => self.parse_uuid(&id, "ResultKey.topic_id"),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a topic id"))),
        }
    }

    /// Reads the `partition` variant of a [`proto::ResultKey`].
    fn partition_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<TopicPartition, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Partition(tp)) => Ok(TopicPartition::new(tp.topic, tp.partition)),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a TopicPartition"))),
        }
    }

    /// Reads the `config_resource` variant of a [`proto::ResultKey`].
    fn config_resource_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<ConfigResource, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::ConfigResource(resource)) => self.config_resource(resource),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a ConfigResource"))),
        }
    }

    /// Reads the `replica` variant of a [`proto::ResultKey`].
    fn replica_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<TopicPartitionReplica, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::Replica(replica)) => Ok(replica_from_proto(replica)),
            other => {
                Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a TopicPartitionReplica")))
            },
        }
    }

    /// Narrows a wire `int32` enum code to the Java `byte` it stands for.
    ///
    /// Proto3 has no `int8`, so every enum code crosses as `int32`. A value that
    /// does not even fit Java's `byte` is rejected here rather than silently
    /// truncating into a valid-looking constant — the same rule the
    /// [`Self::config_resource`] narrowing applies.
    fn enum_code(&self, code: i32, what: &str) -> Result<i8, Error> {
        i8::try_from(code).map_err(|_| self.protocol_error(format!("{what} {code} is not a Java byte")))
    }

    /// Narrows a wire `int32` to the Java `short` it stands for.
    fn short(&self, value: i32, what: &str) -> Result<i16, Error> {
        i16::try_from(value).map_err(|_| self.protocol_error(format!("{what} {value} is not a Java short")))
    }

    /// Rebuilds an [`AclOperation`] from its `code()`.
    ///
    /// `from_code` maps an unrecognised code to `Unknown` (faithfully — Java's
    /// `AclOperation.fromCode` does too), which would absorb a garbled field into
    /// a valid value, so a code that decodes to `Unknown` without *being* the
    /// `Unknown` code is a protocol error. Same rule as the group-enum names in
    /// [`Self::group_state`].
    fn acl_operation(&self, code: i32) -> Result<AclOperation, Error> {
        let code = self.enum_code(code, "AclOperation.code")?;
        let operation = AclOperation::from_code(code);
        if operation.is_unknown() && code != AclOperation::Unknown.code() {
            return Err(self.protocol_error(format!("AclOperation.code {code} is not a known operation")));
        }
        Ok(operation)
    }

    /// Rebuilds an [`AclPermissionType`] from its `code()`. See
    /// [`Self::acl_operation`].
    fn acl_permission_type(&self, code: i32) -> Result<AclPermissionType, Error> {
        let code = self.enum_code(code, "AclPermissionType.code")?;
        let permission = AclPermissionType::from_code(code);
        if permission.is_unknown() && code != AclPermissionType::Unknown.code() {
            return Err(self.protocol_error(format!("AclPermissionType.code {code} is not a known permission type")));
        }
        Ok(permission)
    }

    /// Rebuilds a [`ResourceType`] from its `code()`. See [`Self::acl_operation`].
    fn resource_type(&self, code: i32) -> Result<ResourceType, Error> {
        let code = self.enum_code(code, "ResourceType.code")?;
        let resource_type = ResourceType::from_code(code);
        if resource_type.is_unknown() && code != ResourceType::Unknown.code() {
            return Err(self.protocol_error(format!("ResourceType.code {code} is not a known resource type")));
        }
        Ok(resource_type)
    }

    /// Rebuilds a [`PatternType`] from its `code()`. See [`Self::acl_operation`].
    fn pattern_type(&self, code: i32) -> Result<PatternType, Error> {
        let code = self.enum_code(code, "PatternType.code")?;
        let pattern_type = PatternType::from_code(code);
        if pattern_type.is_unknown() && code != PatternType::Unknown.code() {
            return Err(self.protocol_error(format!("PatternType.code {code} is not a known pattern type")));
        }
        Ok(pattern_type)
    }

    /// Rebuilds a [`ScramMechanism`] from its `type()` indicator. See
    /// [`Self::acl_operation`]; `ScramMechanism::from_type` likewise falls through
    /// to `Unknown`.
    fn scram_mechanism(&self, mechanism: i32) -> Result<ScramMechanism, Error> {
        let mechanism = self.enum_code(mechanism, "ScramMechanism.type")?;
        let parsed = ScramMechanism::from_type(mechanism);
        if parsed == ScramMechanism::Unknown && mechanism != ScramMechanism::Unknown.r#type() {
            return Err(self.protocol_error(format!("ScramMechanism.type {mechanism} is not a known mechanism")));
        }
        Ok(parsed)
    }

    /// Rebuilds an [`AclBinding`] from the wire's seven flat fields.
    ///
    /// `ResourcePattern::new` and `AccessControlEntry::new` are the fallible
    /// constructors Java also has (they reject the match-any states an ACL to be
    /// created cannot hold), so their rejection surfaces as a protocol error
    /// rather than being papered over.
    fn acl_binding(&self, binding: proto::AclBinding) -> Result<AclBinding, Error> {
        let pattern = ResourcePattern::new(
            self.resource_type(binding.resource_type)?,
            binding.resource_name,
            self.pattern_type(binding.pattern_type)?,
        )
        .map_err(|e| self.protocol_error(format!("AclBinding carries an unbuildable ResourcePattern: {e}")))?;
        let entry = AccessControlEntry::new(
            binding.principal,
            binding.host,
            self.acl_operation(binding.operation)?,
            self.acl_permission_type(binding.permission_type)?,
        )
        .map_err(|e| self.protocol_error(format!("AclBinding carries an unbuildable AccessControlEntry: {e}")))?;
        Ok(AclBinding::new(pattern, entry))
    }

    /// Rebuilds an [`AclBindingFilter`], preserving each of the three nullable
    /// strings.
    ///
    /// An absent `resource_name` / `principal` / `host` is Java's null — *match
    /// any* — and must not become `Some("")`, which matches only the resource,
    /// principal or host literally named the empty string. Both filter
    /// constructors are infallible in Java and in Rust, since a filter may legally
    /// hold every match-any state.
    fn acl_binding_filter(&self, filter: proto::AclBindingFilter) -> Result<AclBindingFilter, Error> {
        Ok(AclBindingFilter::new(
            ResourcePatternFilter::new(
                self.resource_type(filter.resource_type)?,
                filter.resource_name,
                self.pattern_type(filter.pattern_type)?,
            ),
            AccessControlEntryFilter::new(
                filter.principal,
                filter.host,
                self.acl_operation(filter.operation)?,
                self.acl_permission_type(filter.permission_type)?,
            ),
        ))
    }

    /// Rebuilds a [`FilterResults`], preserving each matched ACL's own error.
    ///
    /// Both halves of a [`FilterResult`] are independent optionals rather than a
    /// `oneof` (the enclosing per-filter future already resolved), so both are
    /// carried through unchanged; a backend that set neither or both is visible to
    /// the scenario rather than normalised here.
    fn filter_results(&self, results: proto::FilterResults) -> Result<FilterResults, Error> {
        let mut values = Vec::with_capacity(results.values.len());
        for deleted in results.values {
            let binding = match deleted.binding {
                Some(binding) => Some(self.acl_binding(binding)?),
                None => None,
            };
            values.push(FilterResult::new(binding, deleted.exception.map(kafka_error_from_proto)));
        }
        Ok(FilterResults::new(values))
    }

    /// Rebuilds a [`ClientQuotaEntity`], preserving each nullable entity name.
    ///
    /// An absent name is Java's null: the built-in *default* entity of that type,
    /// which is not the entity named `""`.
    fn client_quota_entity(&self, entity: proto::ClientQuotaEntity) -> ClientQuotaEntity {
        ClientQuotaEntity::new(
            entity
                .entries
                .into_iter()
                .map(|entry| (entry.entity_type, entry.entity_name))
                .collect(),
        )
    }

    /// Rebuilds a [`KafkaPrincipal`], including `token_authenticated`.
    ///
    /// That flag has to be carried and asserted explicitly, because Rust's
    /// hand-written `PartialEq` for `KafkaPrincipal` ignores it (as Java's
    /// `equals` does), so a comparison of whole principals could not see it.
    fn kafka_principal(&self, principal: proto::KafkaPrincipal) -> KafkaPrincipal {
        KafkaPrincipal::with_token_authenticated(
            principal.principal_type,
            principal.name,
            principal.token_authenticated,
        )
    }

    /// Rebuilds a [`TokenInformation`].
    ///
    /// `with_token_requester` rather than `new`: `new` sets the requester equal to the
    /// owner, which would silently repair a backend that dropped or transposed the
    /// requester.
    fn token_information(&self, info: proto::TokenInformation) -> Result<TokenInformation, Error> {
        let owner = info
            .owner
            .ok_or_else(|| self.protocol_error("TokenInformation with no owner"))?;
        let requester = info
            .token_requester
            .ok_or_else(|| self.protocol_error("TokenInformation with no token_requester"))?;
        Ok(TokenInformation::with_token_requester(
            info.token_id,
            self.kafka_principal(owner),
            self.kafka_principal(requester),
            info.renewers.into_iter().map(|p| self.kafka_principal(p)).collect(),
            info.issue_timestamp,
            info.max_timestamp,
            info.expiry_timestamp,
        ))
    }

    /// Rebuilds a [`DelegationToken`] and checks the wire's derived base64 HMAC
    /// against the one recomputed from `hmac`.
    ///
    /// `hmacAsBase64String()` is derived in Java, so the reconstructed token would
    /// re-derive it and any scenario assertion would pass no matter what the wire
    /// said. Comparing the two here is what turns a carried derived field into
    /// real coverage (the rule slice G4 established for the derived group-state and
    /// `isSimpleConsumerGroup` fields).
    fn delegation_token(&self, token: proto::DelegationToken) -> Result<DelegationToken, Error> {
        let info = token
            .token_information
            .ok_or_else(|| self.protocol_error("DelegationToken with no token_information"))?;
        let rebuilt = DelegationToken::new(self.token_information(info)?, token.hmac);
        let derived = rebuilt.hmac_as_base64_string();
        if derived != token.hmac_as_base64 {
            return Err(self.protocol_error(format!(
                "DelegationToken.hmac_as_base64 is {:?} but the hmac base64-encodes to {derived:?}",
                token.hmac_as_base64
            )));
        }
        Ok(rebuilt)
    }

    /// Rebuilds a [`UserScramCredentialsDescription`].
    fn scram_description(
        &self,
        description: proto::UserScramCredentialsDescription,
    ) -> Result<UserScramCredentialsDescription, Error> {
        let mut infos = Vec::with_capacity(description.credential_infos.len());
        for info in description.credential_infos {
            infos.push(ScramCredentialInfo::new(self.scram_mechanism(info.mechanism)?, info.iterations));
        }
        Ok(UserScramCredentialsDescription::new(description.name, infos))
    }

    /// Rebuilds a [`FeatureMetadataView`].
    ///
    /// `finalized_features_epoch` stays an `Option`: an absent epoch is Java's
    /// empty `Optional<Long>`, not epoch 0. The two range constructors are
    /// fallible in Java and in Rust (they reject a negative or inverted range), so
    /// a rejection is a protocol error.
    fn feature_metadata(&self, metadata: proto::FeatureMetadata) -> Result<FeatureMetadataView, Error> {
        let mut finalized_features = HashMap::with_capacity(metadata.finalized_features.len());
        for (feature, range) in metadata.finalized_features {
            let built = FinalizedVersionRange::new(
                self.short(range.min_version_level, "FinalizedVersionRange.min_version_level")?,
                self.short(range.max_version_level, "FinalizedVersionRange.max_version_level")?,
            )
            .map_err(|e| self.protocol_error(format!("finalized feature `{feature}` has an unbuildable range: {e}")))?;
            finalized_features.insert(feature, built);
        }
        let mut supported_features = HashMap::with_capacity(metadata.supported_features.len());
        for (feature, range) in metadata.supported_features {
            let built = SupportedVersionRange::new(
                self.short(range.min_version, "SupportedVersionRange.min_version")?,
                self.short(range.max_version, "SupportedVersionRange.max_version")?,
            )
            .map_err(|e| self.protocol_error(format!("supported feature `{feature}` has an unbuildable range: {e}")))?;
            supported_features.insert(feature, built);
        }
        Ok(FeatureMetadataView {
            finalized_features,
            finalized_features_epoch: metadata.finalized_features_epoch,
            supported_features,
        })
    }

    /// Reads the `acl_binding` variant of a [`proto::ResultKey`].
    fn acl_binding_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<AclBinding, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::AclBinding(binding)) => self.acl_binding(binding),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected an AclBinding"))),
        }
    }

    /// Reads the `acl_binding_filter` variant of a [`proto::ResultKey`].
    fn acl_binding_filter_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<AclBindingFilter, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::AclBindingFilter(filter)) => self.acl_binding_filter(filter),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected an AclBindingFilter"))),
        }
    }

    /// Reads the `client_quota_entity` variant of a [`proto::ResultKey`].
    fn client_quota_entity_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<ClientQuotaEntity, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::ClientQuotaEntity(entity)) => Ok(self.client_quota_entity(entity)),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a ClientQuotaEntity"))),
        }
    }

    /// Rebuilds a [`ConfigResource`] from the wire's `Type.id()` code.
    ///
    /// `ConfigResourceType::for_id` maps an unrecognized id to `Unknown` rather
    /// than failing, so an id that does not even fit Java's `byte` is rejected
    /// here instead of silently truncating into a valid-looking type.
    fn config_resource(&self, resource: proto::ConfigResource) -> Result<ConfigResource, Error> {
        let id = i8::try_from(resource.resource_type).map_err(|_| {
            self.protocol_error(format!(
                "ConfigResource.resource_type {} is not a ConfigResource.Type id",
                resource.resource_type
            ))
        })?;
        Ok(ConfigResource::new(ConfigResourceType::for_id(id), resource.name))
    }

    /// Rebuilds a [`LogDirDescription`], preserving the log dir's own error, the
    /// two `OptionalLong` volume sizes and the `isCordoned()` flag (KIP-1066).
    fn log_dir_description(&self, description: proto::LogDirDescription) -> Result<LogDirDescription, Error> {
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
        // Java's `isCordoned()` is a plain bool; an absent proto field (a server
        // that predates the flag) is false, matching the Java default.
        let is_cordoned = description.is_cordoned.unwrap_or(false);
        // The five-argument constructor is the KIP-1066 one; passing
        // `UNKNOWN_VOLUME_BYTES` (-1) for a volume size records it as absent,
        // exactly as the two-argument Java constructor does. Java has no
        // constructor for one volume size present and the other absent, and no
        // broker sends that, so the mixed case is a protocol error rather than a
        // guess.
        const UNKNOWN: i64 = confluent_kafka::common::requests::DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES;
        match (description.total_bytes, description.usable_bytes) {
            (None, None) => Ok(LogDirDescription::with_total_bytes_usable_bytes_is_cordoned(
                description.error.map(kafka_error_from_proto),
                replica_infos,
                UNKNOWN,
                UNKNOWN,
                is_cordoned,
            )),
            (Some(total), Some(usable)) => Ok(LogDirDescription::with_total_bytes_usable_bytes_is_cordoned(
                description.error.map(kafka_error_from_proto),
                replica_infos,
                total,
                usable,
                is_cordoned,
            )),
            (total, usable) => Err(self.protocol_error(format!(
                "LogDirDescription reported totalBytes={} and usableBytes={}; Java has no constructor for one \
                 without the other",
                total.is_some(),
                usable.is_some()
            ))),
        }
    }

    /// Rebuilds a [`GroupState`] from the enum constant name the wire carries.
    ///
    /// `GroupState::parse` maps anything it does not recognise to `Unknown`
    /// (faithfully — Java's `parse` does the same), which would silently absorb a
    /// dropped or garbled field into a valid-looking value. So a name that parses
    /// to `Unknown` without *being* "Unknown" is rejected as a protocol error
    /// instead.
    fn group_state(&self, name: &str, what: &str) -> Result<GroupState, Error> {
        let parsed = GroupState::parse(name);
        if parsed == GroupState::Unknown && !name.eq_ignore_ascii_case("Unknown") {
            return Err(self.protocol_error(format!("{what} {name:?} is not a GroupState constant name")));
        }
        Ok(parsed)
    }

    /// Rebuilds a [`GroupType`] from its enum constant name. See
    /// [`Self::group_state`] for why an unrecognised name is an error.
    fn group_type(&self, name: &str, what: &str) -> Result<GroupType, Error> {
        let parsed = GroupType::parse(name);
        if parsed == GroupType::Unknown && !name.eq_ignore_ascii_case("Unknown") {
            return Err(self.protocol_error(format!("{what} {name:?} is not a GroupType constant name")));
        }
        Ok(parsed)
    }

    /// Rebuilds a [`ClassicGroupState`] from its enum constant name. See
    /// [`Self::group_state`].
    fn classic_group_state(&self, name: &str, what: &str) -> Result<ClassicGroupState, Error> {
        let parsed = ClassicGroupState::parse(name);
        if parsed == ClassicGroupState::Unknown && !name.eq_ignore_ascii_case("Unknown") {
            return Err(self.protocol_error(format!("{what} {name:?} is not a ClassicGroupState constant name")));
        }
        Ok(parsed)
    }

    /// Rebuilds a [`GroupListing`].
    ///
    /// `is_simple_consumer_group` is *derived* in Java
    /// (`group_type == Classic && protocol.isEmpty()`) and therefore cannot be
    /// passed to `GroupListing::new`. It still crosses the wire, because both
    /// bindings expose it as an accessor of its own, and it is checked against the
    /// derived value here — otherwise a backend that reported it wrongly would be
    /// invisible, since the reconstructed value would come from the other two
    /// fields regardless.
    fn group_listing(&self, listing: proto::GroupListing) -> Result<GroupListing, Error> {
        let group_type = match &listing.group_type {
            Some(name) => Some(self.group_type(name, "GroupListing.group_type")?),
            None => None,
        };
        let group_state = match &listing.group_state {
            Some(name) => Some(self.group_state(name, "GroupListing.group_state")?),
            None => None,
        };
        let rebuilt = GroupListing::new(listing.group_id.clone(), group_type, listing.protocol.clone(), group_state);
        if rebuilt.is_simple_consumer_group() != listing.is_simple_consumer_group {
            return Err(self.protocol_error(format!(
                "GroupListing for {:?} reported is_simple_consumer_group={} but Java derives {} from \
                 group_type={:?} and protocol={:?}",
                listing.group_id,
                listing.is_simple_consumer_group,
                rebuilt.is_simple_consumer_group(),
                listing.group_type,
                listing.protocol,
            )));
        }
        Ok(rebuilt)
    }

    /// Rebuilds a [`ConsumerGroupListing`]. Unlike [`Self::group_listing`],
    /// `is_simple_consumer_group` is a real constructor argument here, so it is
    /// carried rather than checked; the deprecated `state` is the derived one and
    /// is checked instead (see [`Self::check_derived_state`]).
    #[allow(deprecated)]
    fn consumer_group_listing(&self, listing: proto::ConsumerGroupListing) -> Result<ConsumerGroupListing, Error> {
        let group_state = match &listing.group_state {
            Some(name) => Some(self.group_state(name, "ConsumerGroupListing.group_state")?),
            None => None,
        };
        let group_type = match &listing.group_type {
            Some(name) => Some(self.group_type(name, "ConsumerGroupListing.group_type")?),
            None => None,
        };
        let rebuilt = ConsumerGroupListing::with_group_state_group_type(
            listing.group_id.clone(),
            group_state,
            group_type,
            listing.is_simple_consumer_group,
        );
        self.check_derived_state(
            &listing.group_id,
            "ConsumerGroupListing",
            listing.state.as_deref(),
            rebuilt.state().map(|s| s.name()),
        )?;
        Ok(rebuilt)
    }

    /// Checks the wire's deprecated `state` against the value Java derives from
    /// `groupState()`.
    ///
    /// Java defines `state() == ConsumerGroupState.parse(groupState().toString())`
    /// (`ConsumerGroupDescription.java`, mirrored in
    /// `src/admin/consumer_group_description.rs`), so the Rust constructors take
    /// only `group_state` and re-derive `state`. Both bindings nonetheless expose
    /// both, and without this check a backend that dropped or transposed `state`
    /// would be invisible: the reconstructed object would derive the right value
    /// from `group_state` and every scenario assertion would pass.
    fn check_derived_state(
        &self,
        group_id: &str,
        what: &str,
        reported: Option<&str>,
        derived: Option<&str>,
    ) -> Result<(), Error> {
        if reported != derived {
            return Err(self.protocol_error(format!(
                "{what} for {group_id:?} reported the deprecated state {reported:?}, but Java derives {derived:?} \
                 from its group state"
            )));
        }
        Ok(())
    }

    /// Rebuilds a [`MemberAssignment`].
    fn member_assignment(&self, assignment: proto::MemberAssignment) -> MemberAssignment {
        MemberAssignment::new(
            assignment
                .topic_partitions
                .into_iter()
                .map(|tp| TopicPartition::new(tp.topic, tp.partition))
                .collect(),
        )
    }

    /// Rebuilds a [`MemberDescription`].
    ///
    /// `assignment` is never null in Java, so an absent one is a protocol error
    /// rather than an empty assignment; `target_assignment` *is* nullable and an
    /// absent one must stay `None` rather than becoming an empty assignment.
    fn member_description(&self, member: proto::MemberDescription) -> Result<MemberDescription, Error> {
        let assignment = member
            .assignment
            .ok_or_else(|| self.protocol_error("MemberDescription with no assignment"))?;
        Ok(MemberDescription::new(
            member.consumer_id,
            member.group_instance_id,
            member.rack_id,
            member.client_id,
            member.host,
            self.member_assignment(assignment),
            member.target_assignment.map(|a| self.member_assignment(a)),
            member.member_epoch,
            member.upgraded,
        ))
    }

    /// Rebuilds the members of a described group.
    fn member_descriptions(&self, members: Vec<proto::MemberDescription>) -> Result<Vec<MemberDescription>, Error> {
        members.into_iter().map(|member| self.member_description(member)).collect()
    }

    /// Rebuilds a [`ConsumerGroupDescription`], including the coordinator's full
    /// endpoint.
    ///
    /// The coordinator is why this harness exists: it used to be a fabricated
    /// `Node` with an empty host and port -1. `Node::with_rack` keeps whatever the
    /// wire carried, and the scenarios cross-check it against `describeCluster`.
    fn consumer_group_description(
        &self,
        description: proto::ConsumerGroupDescription,
    ) -> Result<ConsumerGroupDescription, Error> {
        let group_type = self.group_type(&description.group_type, "ConsumerGroupDescription.group_type")?;
        let group_state = self.group_state(&description.group_state, "ConsumerGroupDescription.group_state")?;
        let rebuilt = ConsumerGroupDescription::new(
            description.group_id.clone(),
            description.is_simple_consumer_group,
            self.member_descriptions(description.members)?,
            description.partition_assignor,
            group_type,
            group_state,
            description.coordinator.map(node_from_proto),
            description.authorized_operations.map(|ops| acl_operations_from_proto(&ops)),
            description.group_epoch,
            description.target_assignment_epoch,
        );
        self.check_derived_state(
            &description.group_id,
            "ConsumerGroupDescription",
            Some(description.state.as_str()),
            Some(rebuilt.state().name()),
        )?;
        Ok(rebuilt)
    }

    /// Rebuilds a [`ClassicGroupDescription`].
    ///
    /// `is_simple_consumer_group` is derived in Java (`protocol.isEmpty()`), so it
    /// is checked rather than passed — same reasoning as [`Self::group_listing`].
    fn classic_group_description(
        &self,
        description: proto::ClassicGroupDescription,
    ) -> Result<ClassicGroupDescription, Error> {
        let state = self.classic_group_state(&description.state, "ClassicGroupDescription.state")?;
        let rebuilt = ClassicGroupDescription::new(
            description.group_id.clone(),
            description.protocol.clone(),
            description.protocol_data,
            self.member_descriptions(description.members)?,
            state,
            description.coordinator.map(node_from_proto),
            description.authorized_operations.map(|ops| acl_operations_from_proto(&ops)),
        );
        if rebuilt.is_simple_consumer_group() != description.is_simple_consumer_group {
            return Err(self.protocol_error(format!(
                "ClassicGroupDescription for {:?} reported is_simple_consumer_group={} but Java derives {} from \
                 protocol={:?}",
                description.group_id,
                description.is_simple_consumer_group,
                rebuilt.is_simple_consumer_group(),
                description.protocol,
            )));
        }
        Ok(rebuilt)
    }

    /// Rebuilds one group's committed offsets.
    ///
    /// An absent `offset` is Java's **null map value**: the group has no committed
    /// offset for that partition, which is not a committed offset of 0. It stays
    /// `None`.
    fn group_offsets(&self, offsets: proto::GroupOffsets) -> Result<GroupOffsets, Error> {
        let mut map = GroupOffsets::with_capacity(offsets.offsets.len());
        for entry in offsets.offsets {
            let tp = entry
                .partition
                .ok_or_else(|| self.protocol_error("GroupOffset with no partition"))?;
            let offset = match entry.offset {
                Some(offset) => Some(self.offset_and_metadata(offset)?),
                None => None,
            };
            map.insert(TopicPartition::new(tp.topic, tp.partition), offset);
        }
        Ok(map)
    }

    /// Rebuilds an [`OffsetAndMetadata`].
    ///
    /// `OffsetAndMetadata::with_leader_epoch_metadata` rejects a negative offset (Java's
    /// `IllegalArgumentException("Invalid negative offset")`), so a backend that
    /// reported one is a protocol error rather than a panic.
    fn offset_and_metadata(&self, offset: proto::OffsetAndMetadata) -> Result<OffsetAndMetadata, Error> {
        // Java's `metadata` is never null (its constructor maps a null to ""),
        // hence a plain string on the wire. `leader_epoch` absent is Java's
        // `Optional.empty()`, which is not epoch 0.
        OffsetAndMetadata::with_leader_epoch_metadata(offset.offset, offset.leader_epoch, offset.metadata).map_err(
            |e| {
                self.protocol_error(format!(
                    "OffsetAndMetadata with offset {} is not constructible: {e}",
                    offset.offset
                ))
            },
        )
    }

    /// Reads the `broker_id` variant of a [`proto::ResultKey`].
    fn broker_id_key(&self, key: Option<proto::ResultKey>, rpc: &str) -> Result<i32, Error> {
        match key.and_then(|k| k.key) {
            Some(proto::result_key::Key::BrokerId(broker)) => Ok(broker),
            other => Err(self.protocol_error(format!("{rpc} entry keyed by {other:?}, expected a broker id"))),
        }
    }

    /// Rebuilds a [`TransactionState`] from its `toString()` name.
    ///
    /// `TransactionState::parse` maps anything unrecognised to `Unknown`
    /// (faithfully — Java's `parse` does the same), which would absorb a garbled
    /// field into a valid value, so a name that parses to `Unknown` without
    /// spelling `"Unknown"` is a protocol error. Same rule the three group enums
    /// use. Matching is case-sensitive on both sides, unlike `GroupState::parse`.
    fn transaction_state(&self, name: &str, what: &str) -> Result<TransactionState, Error> {
        let state = TransactionState::parse(name);
        if state == TransactionState::Unknown && name != "Unknown" {
            return Err(self.protocol_error(format!("{what} {name:?} is not a TransactionState name")));
        }
        Ok(state)
    }

    /// Rebuilds one partition's [`PartitionProducerState`].
    ///
    /// The two `Optional` columns stay `None` when absent: every `long` / `int`,
    /// 0 and -1 included, is a legal `coordinatorEpoch` /
    /// `currentTransactionStartOffset`, so absence cannot be a sentinel.
    fn partition_producer_state(&self, value: proto::PartitionProducerState) -> Result<PartitionProducerState, Error> {
        let mut producers = Vec::with_capacity(value.active_producers.len());
        for producer in value.active_producers {
            producers.push(ProducerState::new(
                producer.producer_id,
                producer.producer_epoch,
                producer.last_sequence,
                producer.last_timestamp,
                producer.coordinator_epoch,
                producer.current_transaction_start_offset,
            ));
        }
        Ok(PartitionProducerState::new(producers))
    }

    /// Rebuilds a [`TransactionDescription`].
    fn transaction_description(&self, value: proto::TransactionDescription) -> Result<TransactionDescription, Error> {
        let state = self.transaction_state(&value.state, "TransactionDescription.state")?;
        Ok(TransactionDescription::new(
            value.coordinator_id,
            state,
            value.producer_id,
            value.producer_epoch,
            value.transaction_timeout_ms,
            // Absent is Java's empty `OptionalLong` for a transaction that is not
            // in progress, which is not a start time of 0.
            value.transaction_start_time_ms,
            value
                .topic_partitions
                .into_iter()
                .map(|tp| TopicPartition::new(tp.topic, tp.partition))
                .collect(),
        ))
    }

    /// Rebuilds a [`TransactionListing`].
    fn transaction_listing(&self, listing: proto::TransactionListing) -> Result<TransactionListing, Error> {
        let state = self.transaction_state(&listing.state, "TransactionListing.state")?;
        Ok(TransactionListing::new(listing.transactional_id, listing.producer_id, state))
    }

    /// Rebuilds a [`ProducerIdAndEpoch`], narrowing the epoch to Java's `short`.
    fn producer_id_and_epoch(&self, value: proto::ProducerIdAndEpoch) -> Result<ProducerIdAndEpoch, Error> {
        let epoch = self.short(value.epoch, "ProducerIdAndEpoch.epoch")?;
        Ok(ProducerIdAndEpoch::new(value.producer_id, epoch))
    }

    /// Parses a canonical (base64) topic id, the form both bindings expose.
    fn parse_uuid(&self, text: &str, what: &str) -> Result<Uuid, Error> {
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
            timeout_ms: options.timeout_ms(),
            include_authorized_operations: options.include_authorized_operations(),
            partition_size_limit_per_response: Some(options.partition_size_limit_per_response()),
        }
    }

    /// Decodes one `describeTopics` entry's outcome, shared by the by-name and
    /// by-id methods.
    fn describe_outcome(
        &self,
        outcome: Option<proto::describe_topics_entry::Outcome>,
    ) -> Result<Result<TopicDescription, Error>, Error> {
        match outcome {
            Some(proto::describe_topics_entry::Outcome::Error(e)) => Ok(Err(kafka_error_from_proto(e))),
            Some(proto::describe_topics_entry::Outcome::Value(v)) => Ok(Ok(self.topic_description(v)?)),
            None => Err(self.protocol_error("DescribeTopicsEntry with no outcome")),
        }
    }

    fn topic_description(&self, description: proto::TopicDescription) -> Result<TopicDescription, Error> {
        let topic_id = self.parse_uuid(&description.topic_id, "TopicDescription.topic_id")?;
        // Java's nullable Set<AclOperation>: absent means the broker did not
        // report the operations, which is not the same as reporting none.
        let authorized_operations = description.authorized_operations.as_ref().map(acl_operations_from_proto);
        let partitions = description
            .partitions
            .into_iter()
            .map(|info| partition_info_from_proto(self.backend, info))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TopicDescription::with_authorized_operations_topic_id(
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
    fn topic_metadata_and_config(&self, value: proto::TopicMetadataAndConfig) -> Result<TopicMetadataAndConfig, Error> {
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
    mut decode: impl FnMut(E) -> Result<(K, Result<V, Error>), Error>,
) -> Result<Outcomes<K, V>, Error>
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
fn void_outcome(error: Option<proto::KafkaError>) -> Result<(), Error> {
    match error {
        Some(err) => Err(kafka_error_from_proto(err)),
        None => Ok(()),
    }
}

fn tp_to_proto(tp: &TopicPartition) -> proto::TopicPartition {
    proto::TopicPartition { topic: tp.topic().to_string(), partition: tp.partition() }
}

/// Encodes an [`OffsetAndMetadata`] for `alterConsumerGroupOffsets`.
///
/// `metadata` is a plain string because Java's constructor maps a null to the
/// empty string, so there is no null to preserve; `leader_epoch` stays
/// `Optional`, and an absent one must not become epoch 0 — both bindings carry a
/// separate present-flag for exactly that reason (`admin.py`'s
/// `_alter_consumer_group_offsets_rows` emits `o.leader_epoch is not None` as its
/// own column, and the C entry point takes `has_leader_epoch[i]`).
fn offset_and_metadata_to_proto(offset: &OffsetAndMetadata) -> proto::OffsetAndMetadata {
    proto::OffsetAndMetadata {
        offset: offset.offset(),
        metadata: offset.metadata().to_string(),
        leader_epoch: offset.leader_epoch(),
    }
}

fn new_topic_to_proto(topic: &NewTopic) -> proto::NewTopic {
    proto::NewTopic {
        name: topic.name().to_string(),
        // Both getters already render "unset" as -1, which is exactly the
        // encoding the wire and both bindings use.
        num_partitions: topic.num_partitions(),
        replication_factor: topic.replication_factor() as i32,
        configs: topic.configs().cloned().unwrap_or_default().into_iter().collect(),
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
) -> Result<TopicPartitionInfo, Error> {
    let partition = info.partition;
    let leader = info.leader.map(node_from_proto);
    let replicas = info.replicas.into_iter().map(node_from_proto).collect();
    let isr = info.isr.into_iter().map(node_from_proto).collect();
    let nodes = |list: proto::NodeList| list.nodes.into_iter().map(node_from_proto).collect::<Vec<_>>();
    match (info.elr, info.last_known_elr) {
        (None, None) => Ok(TopicPartitionInfo::new(partition, leader, replicas, isr)),
        (Some(elr), Some(last_known_elr)) => Ok(TopicPartitionInfo::with_elr_last_known_elr(
            partition,
            leader,
            replicas,
            isr,
            nodes(elr),
            nodes(last_known_elr),
        )),
        (elr, last_known_elr) => Err(Error::local_illegal_state(format!(
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
/// `set_topic_partitions(None)` versus `Some(vec)`. `NewPartitions.new_assignments`
/// and `UserScramCredentialUpsertion`'s salt now follow the same rule
/// (`has_assignments` / `has_salts`); both previously collapsed absent into empty
/// via `is_empty()` at the C boundary.
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

/// Flattens an [`AclBinding`] into the wire's seven fields, the shape every
/// binding already builds (`admin.py`'s `_acl_binding_rows`, the C entry point's
/// seven parallel arrays).
fn acl_binding_to_proto(binding: &AclBinding) -> proto::AclBinding {
    let pattern = binding.pattern();
    let entry = binding.entry();
    proto::AclBinding {
        resource_type: i32::from(pattern.resource_type().code()),
        resource_name: pattern.name().to_string(),
        pattern_type: i32::from(pattern.pattern_type().code()),
        principal: entry.principal().to_string(),
        host: entry.host().to_string(),
        operation: i32::from(entry.operation().code()),
        permission_type: i32::from(entry.permission_type().code()),
    }
}

/// Flattens an [`AclBindingFilter`], keeping each nullable string nullable.
///
/// `None` is Java's match-any and must not be encoded as `""`; every layer below
/// carries the same discriminant (a NULL pointer at the C boundary, a `None`
/// column in `admin.py`).
fn acl_binding_filter_to_proto(filter: &AclBindingFilter) -> proto::AclBindingFilter {
    let pattern = filter.pattern_filter();
    let entry = filter.entry_filter();
    proto::AclBindingFilter {
        resource_type: i32::from(pattern.resource_type().code()),
        resource_name: pattern.name().map(str::to_string),
        pattern_type: i32::from(pattern.pattern_type().code()),
        principal: entry.principal().map(str::to_string),
        host: entry.host().map(str::to_string),
        operation: i32::from(entry.operation().code()),
        permission_type: i32::from(entry.permission_type().code()),
    }
}

/// Encodes a [`ClientQuotaEntity`], keeping a `None` entity name absent — that is
/// the built-in default entity of its type, not the entity named `""`.
fn client_quota_entity_to_proto(entity: &ClientQuotaEntity) -> proto::ClientQuotaEntity {
    proto::ClientQuotaEntity {
        entries: entity
            .entries()
            .iter()
            .map(|(entity_type, entity_name)| proto::ClientQuotaEntityEntry {
                entity_type: entity_type.clone(),
                entity_name: entity_name.clone(),
            })
            .collect(),
    }
}

/// Encodes a [`ClientQuotaFilterComponent`] as a *named* match kind plus the name
/// the `Exact` kind carries.
///
/// Naming the kind rather than forwarding Kafka's `MATCH_TYPE_*` integer is what
/// makes the three-value table independently written on each server (see
/// `admin_service.proto`'s `ClientQuotaMatchKind`). `Any` is Java's null name —
/// `ofEntityType`, "match any specified name" — so it carries no name either, and
/// only the kind separates it from `Default`.
fn quota_filter_component_to_proto(component: &ClientQuotaFilterComponent) -> proto::ClientQuotaFilterComponent {
    let (kind, match_name) = match component.match_spec() {
        ClientQuotaMatch::Exact(name) => (proto::ClientQuotaMatchKind::MatchKindExact, Some(name.clone())),
        ClientQuotaMatch::Default => (proto::ClientQuotaMatchKind::MatchKindDefault, None),
        ClientQuotaMatch::Any => (proto::ClientQuotaMatchKind::MatchKindAny, None),
    };
    proto::ClientQuotaFilterComponent {
        entity_type: component.entity_type().to_string(),
        match_kind: kind as i32,
        match_name,
    }
}

/// Encodes a [`ClientQuotaAlteration`].
///
/// An `Op` with no value **removes** that quota (Java's null `Double`), and it
/// stays absent on the wire: encoding it as 0.0 would turn a removal into a
/// zero-valued quota, which the broker reports back as a present key.
fn quota_alteration_to_proto(alteration: &ClientQuotaAlteration) -> proto::ClientQuotaAlteration {
    proto::ClientQuotaAlteration {
        entity: Some(client_quota_entity_to_proto(alteration.entity())),
        ops: alteration
            .ops()
            .iter()
            .map(|op| proto::ClientQuotaOp { key: op.key().to_string(), value: op.value() })
            .collect(),
    }
}

/// Encodes a [`UserScramCredentialAlteration`].
///
/// `is_deletion` is explicit rather than inferred from an absent password, which
/// would conflate a deletion with a malformed upsertion — the reason both bindings
/// carry the flag as its own column.
///
/// The salt is always sent, because a Rust `UserScramCredentialUpsertion` has
/// always materialised one by the time the harness holds it (`new` and
/// `with_bytes` generate a random salt in the constructor). So the wire
/// field's *absent* state — Java's salt-generating three-argument constructor — is
/// not reachable from a scenario, exactly as `removeMembersFromConsumerGroup`'s
/// present-but-empty member list is not: the harness's own input type has no such
/// state. Sending the salt is also what makes the four backends comparable, since
/// each server would otherwise generate a different one and derive a different
/// salted password from the same scenario input.
///
/// The *other* half of that distinction — an explicitly **empty** salt — is
/// representable natively and is now carried faithfully across the C boundary as
/// well: `read_scram_alterations` selects the constructor on an explicit
/// `has_salts` column rather than on `salt.is_empty()`, and
/// `upsert_with_an_explicitly_empty_salt_is_accepted`
/// (`tests/integration/admin_scram_test.rs`) drives it through all four backends.
/// See `UserScramCredentialAlteration.salt` in `admin_service.proto` for the full
/// statement, including why the broker cannot observe which constructor was
/// chosen.
fn scram_alteration_to_proto(alteration: &UserScramCredentialAlteration) -> proto::UserScramCredentialAlteration {
    match alteration {
        UserScramCredentialAlteration::Deletion(deletion) => proto::UserScramCredentialAlteration {
            user: deletion.user().to_string(),
            is_deletion: true,
            mechanism: i32::from(deletion.mechanism().r#type()),
            iterations: 0,
            password: None,
            salt: None,
        },
        UserScramCredentialAlteration::Upsertion(upsertion) => {
            let info = upsertion.credential_info();
            proto::UserScramCredentialAlteration {
                user: upsertion.user().to_string(),
                is_deletion: false,
                mechanism: i32::from(info.mechanism().r#type()),
                iterations: info.iterations(),
                password: Some(upsertion.password().to_vec()),
                salt: Some(upsertion.salt().to_vec()),
            }
        },
    }
}

/// Encodes a [`KafkaPrincipal`].
///
/// `token_authenticated` is sent even though Java's *request* messages do not
/// carry it, because the same message is used in the response direction where the
/// broker and the mock do report it.
fn kafka_principal_to_proto(principal: &KafkaPrincipal) -> proto::KafkaPrincipal {
    proto::KafkaPrincipal {
        principal_type: principal.principal_type().to_string(),
        name: principal.name().to_string(),
        token_authenticated: principal.token_authenticated(),
    }
}

/// Encodes a [`FeatureUpdate`].
fn feature_update_to_proto(update: &FeatureUpdate) -> proto::FeatureUpdate {
    proto::FeatureUpdate {
        max_version_level: i32::from(update.max_version_level()),
        upgrade_type: i32::from(update.upgrade_type().code()),
    }
}

fn config_entry_from_proto(entry: proto::ConfigEntry) -> ConfigEntry {
    let options = ConfigEntryOptionsBuilder::new()
        .set_name(entry.name)
        .set_value(entry.value)
        .set_source(if entry.is_default {
            ConfigSource::DefaultConfig
        } else {
            ConfigSource::Unknown
        })
        .set_is_sensitive(entry.is_sensitive)
        .set_is_read_only(entry.is_read_only)
        .build()
        .unwrap();
    ConfigEntry::with_options(options)
}

impl AdminBackend for MultilanguageAdmin {
    async fn create_topics(
        &self,
        new_topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, Error> {
        let request = proto::CreateTopicsRequest {
            admin_id: self.admin_id,
            topics: new_topics.iter().map(new_topic_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::DeleteTopicsRequest {
            admin_id: self.admin_id,
            topics: Some(proto::delete_topics_request::Topics::Names(proto::StringList {
                values: names.to_vec(),
            })),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<Uuid, ()>, Error> {
        let request = proto::DeleteTopicsRequest {
            admin_id: self.admin_id,
            topics: Some(proto::delete_topics_request::Topics::TopicIds(proto::StringList {
                values: topic_ids.iter().map(Uuid::to_string).collect(),
            })),
            timeout_ms: options.timeout_ms(),
            retry_on_quota_violation: Some(options.should_retry_on_quota_violation()),
        };
        let response = self.call(|mut c| async move { c.delete_topics(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.topic_id_key(entry.key, "deleteTopics")?, void_outcome(entry.error)))
        })
    }

    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, Error> {
        let request = proto::AdminListTopicsRequest {
            admin_id: self.admin_id,
            timeout_ms: options.timeout_ms(),
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

    async fn describe_topics_with_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, Error> {
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
    ) -> Result<Outcomes<Uuid, TopicDescription>, Error> {
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
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::CreatePartitionsRequest {
            admin_id: self.admin_id,
            partitions: new_partitions
                .iter()
                .map(|(topic, np)| new_partitions_to_proto(topic, np))
                .collect(),
            timeout_ms: options.timeout_ms(),
            validate_only: options.validate_only(),
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
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, Error> {
        let request = proto::DeleteRecordsRequest {
            admin_id: self.admin_id,
            records: records_to_delete
                .iter()
                .map(|(tp, records)| proto::RecordsToDelete {
                    partition: Some(tp_to_proto(tp)),
                    before_offset: records.before_offset(),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
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

    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, Error> {
        let request = proto::DescribeClusterRequest {
            admin_id: self.admin_id,
            timeout_ms: options.timeout_ms(),
            include_authorized_operations: options.include_authorized_operations(),
            include_fenced_brokers: options.include_fenced_brokers(),
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
    ) -> Result<Outcomes<ConfigResource, ConfigView>, Error> {
        let request = proto::DescribeConfigsRequest {
            admin_id: self.admin_id,
            resources: resources.iter().map(config_resource_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
            include_synonyms: options.include_synonyms(),
            include_documentation: options.include_documentation(),
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
    ) -> Result<Outcomes<ConfigResource, ()>, Error> {
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
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Vec<ConfigResource>, Error> {
        let request = proto::ListConfigResourcesRequest {
            admin_id: self.admin_id,
            resource_types: config_resource_types.iter().map(|t| i32::from(t.id())).collect(),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Vec<ClientMetricsResourceListing>, Error> {
        let request =
            proto::ListClientMetricsResourcesRequest { admin_id: self.admin_id, timeout_ms: options.timeout_ms() };
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
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, Error> {
        let request = proto::DescribeLogDirsRequest {
            admin_id: self.admin_id,
            brokers: brokers.to_vec(),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, Error> {
        let request = proto::AlterReplicaLogDirsRequest {
            admin_id: self.admin_id,
            assignments: replica_assignment
                .iter()
                .map(|(replica, log_dir)| proto::ReplicaLogDirAssignment {
                    replica: Some(replica_to_proto(replica)),
                    log_dir: log_dir.clone(),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, Error> {
        let request = proto::DescribeReplicaLogDirsRequest {
            admin_id: self.admin_id,
            replicas: replicas.iter().map(replica_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let request = proto::ElectLeadersRequest {
            admin_id: self.admin_id,
            // Java's public `byte value` field, which is what both bindings take.
            election_type: i32::from(election_type.value()),
            partitions: optional_partitions_to_proto(partitions),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
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
                    reassignment: reassignment
                        .as_ref()
                        .map(|r| proto::NewPartitionReassignment { target_replicas: r.target_replicas().to_vec() }),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
            // Java's default is true, so this is optional on the wire.
            allow_replication_factor_change: Some(options.allow_replication_factor_change()),
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
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, Error> {
        let request = proto::ListPartitionReassignmentsRequest {
            admin_id: self.admin_id,
            partitions: optional_partitions_to_proto(partitions),
            timeout_ms: options.timeout_ms(),
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
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, Error> {
        let request = proto::ListOffsetsRequest {
            admin_id: self.admin_id,
            specs: topic_partition_offsets
                .iter()
                .map(|(tp, spec)| proto::OffsetSpecEntry {
                    partition: Some(tp_to_proto(tp)),
                    spec: Some(offset_spec_to_proto(*spec)),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
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

    async fn list_groups(&self, options: ListGroupsOptions) -> Result<Listings<GroupListing>, Error> {
        let request = proto::ListGroupsRequest {
            admin_id: self.admin_id,
            // Enum filters cross as the constant names, so each server reaches
            // its own binding's `parse` rather than forwarding a code table
            // written here. An empty list leaves the filter unset.
            group_states: options.group_states().iter().map(|s| s.name().to_string()).collect(),
            protocol_types: options.protocol_types().iter().cloned().collect(),
            types: options.types().iter().map(|t| t.name().to_string()).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.list_groups(request).await }).await?;
        // A whole-value response whose value is Java's valid()/errors() split.
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Listings {
            valid: response
                .valid
                .into_iter()
                .map(|listing| self.group_listing(listing))
                .collect::<Result<Vec<_>, _>>()?,
            errors: response.listing_errors.into_iter().map(kafka_error_from_proto).collect(),
        })
    }

    #[allow(deprecated)]
    async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Listings<ConsumerGroupListing>, Error> {
        let request = proto::ListConsumerGroupsRequest {
            admin_id: self.admin_id,
            // Java's deprecated `inStates(Set<ConsumerGroupState>)` is defined as
            // `inGroupStates` over `GroupState.parse` of the same names, so the
            // single `group_states` field serves both spellings.
            group_states: options.group_states().iter().map(|s| s.name().to_string()).collect(),
            types: options.types().iter().map(|t| t.name().to_string()).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.list_consumer_groups(request).await }).await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Listings {
            valid: response
                .valid
                .into_iter()
                .map(|listing| self.consumer_group_listing(listing))
                .collect::<Result<Vec<_>, _>>()?,
            errors: response.listing_errors.into_iter().map(kafka_error_from_proto).collect(),
        })
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ConsumerGroupDescription>, Error> {
        let request = proto::DescribeConsumerGroupsRequest {
            admin_id: self.admin_id,
            group_ids: group_ids.to_vec(),
            timeout_ms: options.timeout_ms(),
            include_authorized_operations: options.include_authorized_operations(),
        };
        let response = self
            .call(|mut c| async move { c.describe_consumer_groups(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "describeConsumerGroups")?;
            let outcome = match entry.outcome {
                Some(proto::describe_consumer_groups_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_consumer_groups_entry::Outcome::Value(v)) => {
                    Ok(self.consumer_group_description(v)?)
                },
                None => return Err(self.protocol_error("DescribeConsumerGroupsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> Result<Outcomes<String, ClassicGroupDescription>, Error> {
        let request = proto::DescribeClassicGroupsRequest {
            admin_id: self.admin_id,
            group_ids: group_ids.to_vec(),
            timeout_ms: options.timeout_ms(),
            include_authorized_operations: options.include_authorized_operations(),
        };
        let response = self
            .call(|mut c| async move { c.describe_classic_groups(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "describeClassicGroups")?;
            let outcome = match entry.outcome {
                Some(proto::describe_classic_groups_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_classic_groups_entry::Outcome::Value(v)) => Ok(self.classic_group_description(v)?),
                None => return Err(self.protocol_error("DescribeClassicGroupsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn list_consumer_group_offsets_with_group_specs(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<String, GroupOffsets>, Error> {
        let request = proto::ListConsumerGroupOffsetsRequest {
            admin_id: self.admin_id,
            group_specs: group_specs
                .iter()
                .map(|(group_id, spec)| proto::ListConsumerGroupOffsetsSpec {
                    group_id: group_id.clone(),
                    // Absent is Java's *unset* collection: every partition the
                    // group has committed offsets for. It must not become an
                    // empty list, which selects nothing. Every layer below keeps
                    // the two apart with an explicit discriminant (the C entry
                    // point's `all_partitions[i]`, `admin.py`'s
                    // `partitions is None` column).
                    topic_partitions: spec.topic_partitions().map(|partitions| proto::TopicPartitionList {
                        partitions: partitions.iter().map(tp_to_proto).collect(),
                    }),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
            require_stable: options.require_stable(),
        };
        let response = self
            .call(|mut c| async move { c.list_consumer_group_offsets(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "listConsumerGroupOffsets")?;
            let outcome = match entry.outcome {
                Some(proto::list_consumer_group_offsets_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::list_consumer_group_offsets_entry::Outcome::Value(v)) => Ok(self.group_offsets(v)?),
                None => return Err(self.protocol_error("ListConsumerGroupOffsetsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let request = proto::AlterConsumerGroupOffsetsRequest {
            admin_id: self.admin_id,
            group_id: group_id.to_string(),
            offsets: offsets
                .iter()
                .map(|(tp, offset)| proto::GroupOffsetCommit {
                    partition: Some(tp_to_proto(tp)),
                    offset: Some(offset_and_metadata_to_proto(offset)),
                })
                .collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.alter_consumer_group_offsets(request).await })
            .await?;
        // A VoidKeyedResponse whose top-level error is wide: Java holds one
        // future over the whole map, so its failure lands there with `entries`
        // empty — and with an empty request that is the only observable.
        keyed(response.error, response.entries, |entry| {
            Ok((
                self.partition_key(entry.key, "alterConsumerGroupOffsets")?,
                void_outcome(entry.error),
            ))
        })
    }

    async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let request = proto::DeleteConsumerGroupOffsetsRequest {
            admin_id: self.admin_id,
            group_id: group_id.to_string(),
            partitions: partitions.iter().map(tp_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.delete_consumer_group_offsets(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            Ok((
                self.partition_key(entry.key, "deleteConsumerGroupOffsets")?,
                void_outcome(entry.error),
            ))
        })
    }

    async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::DeleteConsumerGroupsRequest {
            admin_id: self.admin_id,
            group_ids: group_ids.to_vec(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.delete_consumer_groups(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            Ok((self.name_key(entry.key, "deleteConsumerGroups")?, void_outcome(entry.error)))
        })
    }

    async fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::RemoveMembersFromConsumerGroupRequest {
            admin_id: self.admin_id,
            group_id: group_id.to_string(),
            // `removeAll()` is Java's own `members.isEmpty()`
            // (`RemoveMembersFromConsumerGroupOptions.java:57-59`), and it must
            // cross as the absent wrapper rather than as a present-but-empty list:
            // present-and-empty is the collection constructor, which Java rejects
            // with `IllegalArgumentException("Invalid empty members has been
            // provided")`. Both bindings carry the same explicit discriminant (the
            // C entry point's `bool remove_all`, `admin.py`'s
            // `remove_all = members is None`), so the distinction survives every
            // layer.
            members: if options.remove_all() {
                None
            } else {
                Some(proto::MemberToRemoveList {
                    members: options
                        .members()
                        .iter()
                        .map(|member| proto::MemberToRemove {
                            group_instance_id: member.group_instance_id().to_string(),
                        })
                        .collect(),
                })
            },
            reason: options.reason().map(str::to_string),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.remove_members_from_consumer_group(request).await })
            .await?;
        // In `removeAll` mode `entries` is empty and any failure is the top-level
        // error, because Java's `memberResult` is not applicable there.
        keyed(response.error, response.entries, |entry| {
            Ok((
                self.name_key(entry.key, "removeMembersFromConsumerGroup")?,
                void_outcome(entry.error),
            ))
        })
    }

    async fn create_acls(
        &self,
        acls: &[AclBinding],
        options: CreateAclsOptions,
    ) -> Result<Outcomes<AclBinding, ()>, Error> {
        let request = proto::CreateAclsRequest {
            admin_id: self.admin_id,
            acls: acls.iter().map(acl_binding_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.create_acls(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.acl_binding_key(entry.key, "createAcls")?;
            Ok((key, void_outcome(entry.error)))
        })
    }

    async fn describe_acls(
        &self,
        filter: &AclBindingFilter,
        options: DescribeAclsOptions,
    ) -> Result<Vec<AclBinding>, Error> {
        let request = proto::DescribeAclsRequest {
            admin_id: self.admin_id,
            filter: Some(acl_binding_filter_to_proto(filter)),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.describe_acls(request).await }).await?;
        // Whole-value: one future for the whole call, so a failure is the outer
        // `Err` and an empty list is a successful "nothing matched".
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        response.acls.into_iter().map(|acl| self.acl_binding(acl)).collect()
    }

    async fn delete_acls(
        &self,
        filters: &[AclBindingFilter],
        options: DeleteAclsOptions,
    ) -> Result<Outcomes<AclBindingFilter, FilterResults>, Error> {
        let request = proto::DeleteAclsRequest {
            admin_id: self.admin_id,
            filters: filters.iter().map(acl_binding_filter_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.delete_acls(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.acl_binding_filter_key(entry.key, "deleteAcls")?;
            let outcome = match entry.outcome {
                Some(proto::delete_acls_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                // The value-level errors inside are preserved rather than folded
                // into this one: the filter's own future succeeded.
                Some(proto::delete_acls_entry::Outcome::Value(v)) => Ok(self.filter_results(v)?),
                None => return Err(self.protocol_error("DeleteAclsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> Result<HashMap<ClientQuotaEntity, HashMap<String, f64>>, Error> {
        let request = proto::DescribeClientQuotasRequest {
            admin_id: self.admin_id,
            components: filter.components().iter().map(quota_filter_component_to_proto).collect(),
            strict: filter.strict(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.describe_client_quotas(request).await })
            .await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        let mut entities = HashMap::with_capacity(response.entities.len());
        for reported in response.entities {
            let entity = reported
                .entity
                .ok_or_else(|| self.protocol_error("EntityQuotas with no entity"))?;
            let values = reported.values.into_iter().map(|v| (v.key, v.value)).collect();
            entities.insert(self.client_quota_entity(entity), values);
        }
        Ok(entities)
    }

    async fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> Result<Outcomes<ClientQuotaEntity, ()>, Error> {
        let request = proto::AlterClientQuotasRequest {
            admin_id: self.admin_id,
            entries: entries.iter().map(quota_alteration_to_proto).collect(),
            validate_only: options.validate_only(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.alter_client_quotas(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.client_quota_entity_key(entry.key, "alterClientQuotas")?;
            Ok((key, void_outcome(entry.error)))
        })
    }

    async fn describe_user_scram_credentials(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, UserScramCredentialsDescription>, Error> {
        let request = proto::DescribeUserScramCredentialsRequest {
            admin_id: self.admin_id,
            users: users.to_vec(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.describe_user_scram_credentials(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "describeUserScramCredentials")?;
            let outcome = match entry.outcome {
                Some(proto::describe_user_scram_credentials_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_user_scram_credentials_entry::Outcome::Value(v)) => Ok(self.scram_description(v)?),
                None => return Err(self.protocol_error("DescribeUserScramCredentialsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::AlterUserScramCredentialsRequest {
            admin_id: self.admin_id,
            alterations: alterations.iter().map(scram_alteration_to_proto).collect(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.alter_user_scram_credentials(request).await })
            .await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "alterUserScramCredentials")?;
            Ok((key, void_outcome(entry.error)))
        })
    }

    async fn create_delegation_token(&self, options: CreateDelegationTokenOptions) -> Result<DelegationToken, Error> {
        let request = proto::CreateDelegationTokenRequest {
            admin_id: self.admin_id,
            renewers: options.renewers().iter().map(kafka_principal_to_proto).collect(),
            // Absent is Java's unset owner, which makes the requesting principal
            // the owner; both halves of the principal are absent together.
            owner: options.owner().map(kafka_principal_to_proto),
            max_lifetime_ms: options.max_lifetime_ms(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.create_delegation_token(request).await })
            .await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        let token = response
            .token
            .ok_or_else(|| self.protocol_error("CreateDelegationTokenResponse with neither token nor error"))?;
        self.delegation_token(token)
    }

    async fn renew_delegation_token(&self, hmac: &[u8], options: RenewDelegationTokenOptions) -> Result<i64, Error> {
        let request = proto::RenewDelegationTokenRequest {
            admin_id: self.admin_id,
            hmac: hmac.to_vec(),
            renew_time_period_ms: options.renew_time_period_ms(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.renew_delegation_token(request).await })
            .await?;
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(response.expiry_timestamp_ms),
        }
    }

    async fn expire_delegation_token(&self, hmac: &[u8], options: ExpireDelegationTokenOptions) -> Result<i64, Error> {
        let request = proto::ExpireDelegationTokenRequest {
            admin_id: self.admin_id,
            hmac: hmac.to_vec(),
            expiry_time_period_ms: options.expiry_time_period_ms(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.expire_delegation_token(request).await })
            .await?;
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(response.expiry_timestamp_ms),
        }
    }

    async fn describe_delegation_token(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>, Error> {
        let request = proto::DescribeDelegationTokenRequest {
            admin_id: self.admin_id,
            // Absent is Java's unset filter ("every token I may see"), which must
            // stay distinct from an explicitly empty one — the wrapper message is
            // what keeps them apart, never emptiness.
            owners: options.owners().map(|owners| proto::KafkaPrincipalList {
                principals: owners.iter().map(kafka_principal_to_proto).collect(),
            }),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.describe_delegation_token(request).await })
            .await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        response.tokens.into_iter().map(|token| self.delegation_token(token)).collect()
    }

    async fn describe_features(&self, options: DescribeFeaturesOptions) -> Result<FeatureMetadataView, Error> {
        let request = proto::DescribeFeaturesRequest {
            admin_id: self.admin_id,
            // Absent is Java's empty `OptionalInt`; node id 0 is a legal broker,
            // so the absence cannot be encoded as a value.
            node_id: options.node_id(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.describe_features(request).await }).await?;
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        let metadata = response
            .metadata
            .ok_or_else(|| self.protocol_error("DescribeFeaturesResponse with neither metadata nor error"))?;
        self.feature_metadata(metadata)
    }

    async fn update_features(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let request = proto::UpdateFeaturesRequest {
            admin_id: self.admin_id,
            feature_updates: feature_updates
                .iter()
                .map(|(feature, update)| (feature.clone(), feature_update_to_proto(update)))
                .collect(),
            validate_only: options.validate_only(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.update_features(request).await }).await?;
        // The top-level error also carries the *synchronous* rejection Java throws
        // for an empty map or a blank feature name, which no other RPC here has.
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "updateFeatures")?;
            Ok((key, void_outcome(entry.error)))
        })
    }

    async fn describe_producers(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> Result<Outcomes<TopicPartition, PartitionProducerState>, Error> {
        let request = proto::DescribeProducersRequest {
            admin_id: self.admin_id,
            partitions: partitions.iter().map(tp_to_proto).collect(),
            // Absent is Java's empty `OptionalInt` (query each partition's
            // leader); broker id 0 is legal, so the absence is its own state.
            broker_id: options.broker_id(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.describe_producers(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.partition_key(entry.key, "describeProducers")?;
            let outcome = match entry.outcome {
                Some(proto::describe_producers_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_producers_entry::Outcome::Value(v)) => Ok(self.partition_producer_state(v)?),
                None => return Err(self.protocol_error("DescribeProducersEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> Result<Outcomes<String, TransactionDescription>, Error> {
        let request = proto::DescribeTransactionsRequest {
            admin_id: self.admin_id,
            transactional_ids: transactional_ids.to_vec(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.describe_transactions(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "describeTransactions")?;
            let outcome = match entry.outcome {
                Some(proto::describe_transactions_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::describe_transactions_entry::Outcome::Value(v)) => Ok(self.transaction_description(v)?),
                None => return Err(self.protocol_error("DescribeTransactionsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn abort_transaction(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> Result<(), Error> {
        let request = proto::AbortTransactionRequest {
            admin_id: self.admin_id,
            topic_partition: Some(tp_to_proto(spec.topic_partition())),
            producer_id: spec.producer_id(),
            producer_epoch: spec.producer_epoch() as i32,
            coordinator_epoch: spec.coordinator_epoch(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.abort_transaction(request).await }).await?;
        void_outcome(response.error)
    }

    async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> Result<(), Error> {
        let request = proto::ForceTerminateTransactionRequest {
            admin_id: self.admin_id,
            transactional_id: transactional_id.to_string(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self
            .call(|mut c| async move { c.force_terminate_transaction(request).await })
            .await?;
        void_outcome(response.error)
    }

    async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<Outcomes<i32, Vec<TransactionListing>>, Error> {
        let request = proto::ListTransactionsRequest {
            admin_id: self.admin_id,
            // Java's own default for both collections is an empty set meaning "no
            // filter", so there is no null form to preserve here.
            states: options.filtered_states().iter().map(TransactionState::to_string).collect(),
            producer_ids: options.filtered_producer_ids().iter().copied().collect(),
            // Java's own -1 sentinel: negative means no duration filter.
            duration_ms: options.filtered_duration(),
            transactional_id_pattern: options.filtered_transactional_id_pattern().map(str::to_string),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.list_transactions(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.broker_id_key(entry.key, "listTransactions")?;
            let outcome = match entry.outcome {
                Some(proto::list_transactions_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::list_transactions_entry::Outcome::Value(v)) => v
                    .listings
                    .into_iter()
                    .map(|listing| self.transaction_listing(listing))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Ok)?,
                None => return Err(self.protocol_error("ListTransactionsEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn fence_producers(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> Result<Outcomes<String, ProducerIdAndEpoch>, Error> {
        let request = proto::FenceProducersRequest {
            admin_id: self.admin_id,
            transactional_ids: transactional_ids.to_vec(),
            timeout_ms: options.timeout_ms(),
        };
        let response = self.call(|mut c| async move { c.fence_producers(request).await }).await?;
        keyed(response.error, response.entries, |entry| {
            let key = self.name_key(entry.key, "fenceProducers")?;
            let outcome = match entry.outcome {
                Some(proto::fence_producers_entry::Outcome::Error(e)) => Err(kafka_error_from_proto(e)),
                Some(proto::fence_producers_entry::Outcome::Value(v)) => Ok(self.producer_id_and_epoch(v)?),
                None => return Err(self.protocol_error("FenceProducersEntry with no outcome")),
            };
            Ok((key, outcome))
        })
    }

    async fn close(&self, timeout: Option<Duration>) -> Result<(), Error> {
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
