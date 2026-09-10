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

//! Abstract request framework for Kafka protocol requests.
//!
//! Corresponds to `org.apache.kafka.common.requests.AbstractRequest`.
//!
//! Java uses an abstract class with per-type subclasses and an inner `Builder`
//! abstract class. In Rust we use:
//! - `ConcreteRequest` enum with a variant for each supported request type
//! - `RequestBuilder` trait for constructing requests at a specific version

use std::io;

use crate::add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData;
use crate::add_partitions_to_txn_request_data::AddPartitionsToTxnRequestData;
use crate::alter_client_quotas_request_data::AlterClientQuotasRequestData;
use crate::alter_partition_reassignments_request_data::AlterPartitionReassignmentsRequestData;
use crate::alter_replica_log_dirs_request_data::AlterReplicaLogDirsRequestData;
use crate::alter_user_scram_credentials_request_data::AlterUserScramCredentialsRequestData;
use crate::api_versions_request_data::ApiVersionsRequestData;
use crate::common::network::ByteBufferSend;
use crate::common::protocol::Message;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Readable};
use crate::consumer_group_describe_request_data::ConsumerGroupDescribeRequestData;
use crate::consumer_group_heartbeat_request_data::ConsumerGroupHeartbeatRequestData;
use crate::create_acls_request_data::CreateAclsRequestData;
use crate::create_delegation_token_request_data::CreateDelegationTokenRequestData;
use crate::create_partitions_request_data::CreatePartitionsRequestData;
use crate::create_topics_request_data::CreateTopicsRequestData;
use crate::delete_acls_request_data::DeleteAclsRequestData;
use crate::delete_groups_request_data::DeleteGroupsRequestData;
use crate::delete_records_request_data::DeleteRecordsRequestData;
use crate::delete_topics_request_data::DeleteTopicsRequestData;
use crate::describe_acls_request_data::DescribeAclsRequestData;
use crate::describe_client_quotas_request_data::DescribeClientQuotasRequestData;
use crate::describe_cluster_request_data::DescribeClusterRequestData;
use crate::describe_configs_request_data::DescribeConfigsRequestData;
use crate::describe_delegation_token_request_data::DescribeDelegationTokenRequestData;
use crate::describe_groups_request_data::DescribeGroupsRequestData;
use crate::describe_log_dirs_request_data::DescribeLogDirsRequestData;
use crate::describe_producers_request_data::DescribeProducersRequestData;
use crate::describe_transactions_request_data::DescribeTransactionsRequestData;
use crate::describe_user_scram_credentials_request_data::DescribeUserScramCredentialsRequestData;
use crate::elect_leaders_request_data::ElectLeadersRequestData;
use crate::end_txn_request_data::EndTxnRequestData;
use crate::expire_delegation_token_request_data::ExpireDelegationTokenRequestData;
use crate::fetch_request_data::FetchRequestData;
use crate::find_coordinator_request_data::FindCoordinatorRequestData;
use crate::incremental_alter_configs_request_data::IncrementalAlterConfigsRequestData;
use crate::init_producer_id_request_data::InitProducerIdRequestData;
use crate::leave_group_request_data::LeaveGroupRequestData;
use crate::list_config_resources_request_data::ListConfigResourcesRequestData;
use crate::list_groups_request_data::ListGroupsRequestData;
use crate::list_offsets_request_data::ListOffsetsRequestData;
use crate::list_partition_reassignments_request_data::ListPartitionReassignmentsRequestData;
use crate::list_transactions_request_data::ListTransactionsRequestData;
use crate::metadata_request_data::MetadataRequestData;
use crate::offset_commit_request_data::OffsetCommitRequestData;
use crate::offset_delete_request_data::OffsetDeleteRequestData;
use crate::offset_fetch_request_data::OffsetFetchRequestData;
use crate::offset_for_leader_epoch_request_data::OffsetForLeaderEpochRequestData;
use crate::produce_request_data::ProduceRequestData;
use crate::renew_delegation_token_request_data::RenewDelegationTokenRequestData;
use crate::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::sasl_handshake_request_data::SaslHandshakeRequestData;
use crate::txn_offset_commit_request_data::TxnOffsetCommitRequestData;

use super::AddOffsetsToTxnRequest;
use super::AddPartitionsToTxnRequest;
use crate::update_features_request_data::UpdateFeaturesRequestData;
use crate::write_txn_markers_request_data::WriteTxnMarkersRequestData;

use super::AlterClientQuotasRequest;
use super::AlterPartitionReassignmentsRequest;
use super::AlterReplicaLogDirsRequest;
use super::AlterUserScramCredentialsRequest;
use super::ApiVersionsRequest;
use super::ConcreteResponse;
use super::ConsumerGroupDescribeRequest;
use super::ConsumerGroupHeartbeatRequest;
use super::CreateAclsRequest;
use super::CreateDelegationTokenRequest;
use super::CreatePartitionsRequest;
use super::CreateTopicsRequest;
use super::DeleteAclsRequest;
use super::DeleteGroupsRequest;
use super::DeleteRecordsRequest;
use super::DeleteTopicsRequest;
use super::DescribeAclsRequest;
use super::DescribeClientQuotasRequest;
use super::DescribeClusterRequest;
use super::DescribeConfigsRequest;
use super::DescribeDelegationTokenRequest;
use super::DescribeGroupsRequest;
use super::DescribeLogDirsRequest;
use super::DescribeProducersRequest;
use super::DescribeTransactionsRequest;
use super::DescribeUserScramCredentialsRequest;
use super::ElectLeadersRequest;
use super::EndTxnRequest;
use super::ExpireDelegationTokenRequest;
use super::FetchRequest;
use super::FindCoordinatorRequest;
use super::IncrementalAlterConfigsRequest;
use super::InitProducerIdRequest;
use super::LeaveGroupRequest;
use super::ListConfigResourcesRequest;
use super::ListGroupsRequest;
use super::ListOffsetsRequest;
use super::ListPartitionReassignmentsRequest;
use super::ListTransactionsRequest;
use super::MetadataRequest;
use super::OffsetCommitRequest;
use super::OffsetDeleteRequest;
use super::OffsetFetchRequest;
use super::OffsetsForLeaderEpochRequest;
use super::ProduceRequest;
use super::RenewDelegationTokenRequest;
use super::RequestAndSize;
use super::RequestHeader;
use super::SaslAuthenticateRequest;
use super::SaslHandshakeRequest;
use super::SendBuilder;
use super::TxnOffsetCommitRequest;
use super::UpdateFeaturesRequest;
use super::WriteTxnMarkersRequest;

/// Trait for building requests at a specific version.
///
/// Corresponds to the `AbstractRequest.Builder` inner class in Java.
///
/// Each concrete request type provides its own builder that implements this trait.
pub trait RequestBuilder: Send {
    /// Returns the API key for this builder's request type.
    fn api_key(&self) -> &'static ApiKeys;

    /// Returns the oldest allowed version for this builder.
    fn oldest_allowed_version(&self) -> i16;

    /// Returns the latest allowed version for this builder.
    fn latest_allowed_version(&self) -> i16;

    /// Builds the request at the latest allowed version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported or preconditions are violated.
    fn build(&mut self) -> io::Result<ConcreteRequest> {
        self.build_version(self.latest_allowed_version())
    }

    /// Builds the request at the specified version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported or preconditions are violated.
    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest>;
}

/// Enum dispatch for all supported Kafka request types.
///
/// Each variant wraps a concrete request struct. Common methods are dispatched
/// via `match` on the variant.
///
/// Variants will be added as request types are translated.
#[derive(Debug, Clone)]
pub enum ConcreteRequest {
    /// An ApiVersions request.
    ApiVersions(ApiVersionsRequest),
    /// A Metadata request.
    Metadata(MetadataRequest),
    /// A Produce request.
    Produce(ProduceRequest),
    /// A Fetch request (consumer fetch loop).
    Fetch(FetchRequest),
    /// A SASL handshake request.
    SaslHandshake(SaslHandshakeRequest),
    /// A SASL authenticate request.
    SaslAuthenticate(SaslAuthenticateRequest),
    /// A FindCoordinator request.
    FindCoordinator(FindCoordinatorRequest),
    /// A ListGroups request.
    ListGroups(ListGroupsRequest),
    /// A DescribeGroups request.
    DescribeGroups(DescribeGroupsRequest),
    /// A ConsumerGroupDescribe request.
    ConsumerGroupDescribe(ConsumerGroupDescribeRequest),
    /// A ListOffsets request.
    ListOffsets(ListOffsetsRequest),
    /// An OffsetsForLeaderEpoch request.
    OffsetsForLeaderEpoch(OffsetsForLeaderEpochRequest),
    /// A ConsumerGroupHeartbeat request (KIP-848).
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest),
    /// An OffsetCommit request.
    OffsetCommit(OffsetCommitRequest),
    /// A DeleteGroups request.
    DeleteGroups(DeleteGroupsRequest),
    /// A LeaveGroup request.
    LeaveGroup(LeaveGroupRequest),
    /// An OffsetDelete request.
    OffsetDelete(OffsetDeleteRequest),
    /// An OffsetFetch request.
    OffsetFetch(OffsetFetchRequest),
    /// An InitProducerId request.
    InitProducerId(InitProducerIdRequest),
    /// An AddPartitionsToTxn request.
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    /// An AddOffsetsToTxn request.
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    /// An EndTxn request.
    EndTxn(EndTxnRequest),
    /// A TxnOffsetCommit request.
    TxnOffsetCommit(TxnOffsetCommitRequest),
    /// A CreateTopics request.
    CreateTopics(CreateTopicsRequest),
    /// A DeleteTopics request.
    DeleteTopics(DeleteTopicsRequest),
    /// A CreatePartitions request.
    CreatePartitions(CreatePartitionsRequest),
    /// A DeleteRecords request.
    DeleteRecords(DeleteRecordsRequest),
    /// A DescribeConfigs request.
    DescribeConfigs(DescribeConfigsRequest),
    /// An IncrementalAlterConfigs request.
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    /// A ListConfigResources request.
    ListConfigResources(ListConfigResourcesRequest),
    /// A DescribeCluster request.
    DescribeCluster(DescribeClusterRequest),
    /// A DescribeLogDirs request.
    DescribeLogDirs(DescribeLogDirsRequest),
    /// An AlterReplicaLogDirs request.
    AlterReplicaLogDirs(AlterReplicaLogDirsRequest),
    /// An ElectLeaders request.
    ElectLeaders(ElectLeadersRequest),
    /// An AlterPartitionReassignments request.
    AlterPartitionReassignments(AlterPartitionReassignmentsRequest),
    /// A ListPartitionReassignments request.
    ListPartitionReassignments(ListPartitionReassignmentsRequest),
    /// A DescribeAcls request.
    DescribeAcls(DescribeAclsRequest),
    /// A CreateAcls request.
    CreateAcls(CreateAclsRequest),
    /// A DeleteAcls request.
    DeleteAcls(DeleteAclsRequest),
    /// A DescribeClientQuotas request.
    DescribeClientQuotas(DescribeClientQuotasRequest),
    /// An AlterClientQuotas request.
    AlterClientQuotas(AlterClientQuotasRequest),
    /// A DescribeUserScramCredentials request.
    DescribeUserScramCredentials(DescribeUserScramCredentialsRequest),
    /// An AlterUserScramCredentials request.
    AlterUserScramCredentials(AlterUserScramCredentialsRequest),
    /// A CreateDelegationToken request.
    CreateDelegationToken(CreateDelegationTokenRequest),
    /// A RenewDelegationToken request.
    RenewDelegationToken(RenewDelegationTokenRequest),
    /// An ExpireDelegationToken request.
    ExpireDelegationToken(ExpireDelegationTokenRequest),
    /// A DescribeDelegationToken request.
    DescribeDelegationToken(DescribeDelegationTokenRequest),
    /// An UpdateFeatures request.
    UpdateFeatures(UpdateFeaturesRequest),
    /// A DescribeProducers request.
    DescribeProducers(DescribeProducersRequest),
    /// A DescribeTransactions request.
    DescribeTransactions(DescribeTransactionsRequest),
    /// An InitProducerId request.
    /// A WriteTxnMarkers request.
    WriteTxnMarkers(WriteTxnMarkersRequest),
    /// A ListTransactions request.
    ListTransactions(ListTransactionsRequest),
}

impl ConcreteRequest {
    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        match self {
            Self::ApiVersions(r) => r.version(),
            Self::Metadata(r) => r.version(),
            Self::Produce(r) => r.version(),
            Self::Fetch(r) => r.version(),
            Self::SaslHandshake(r) => r.version(),
            Self::SaslAuthenticate(r) => r.version(),
            Self::FindCoordinator(r) => r.version(),
            Self::ListGroups(r) => r.version(),
            Self::DescribeGroups(r) => r.version(),
            Self::ConsumerGroupDescribe(r) => r.version(),
            Self::ListOffsets(r) => r.version(),
            Self::OffsetsForLeaderEpoch(r) => r.version(),
            Self::ConsumerGroupHeartbeat(r) => r.version(),
            Self::OffsetCommit(r) => r.version(),
            Self::DeleteGroups(r) => r.version(),
            Self::LeaveGroup(r) => r.version(),
            Self::OffsetDelete(r) => r.version(),
            Self::OffsetFetch(r) => r.version(),
            Self::InitProducerId(r) => r.version(),
            Self::AddPartitionsToTxn(r) => r.version(),
            Self::AddOffsetsToTxn(r) => r.version(),
            Self::EndTxn(r) => r.version(),
            Self::TxnOffsetCommit(r) => r.version(),
            Self::CreateTopics(r) => r.version(),
            Self::DeleteTopics(r) => r.version(),
            Self::CreatePartitions(r) => r.version(),
            Self::DeleteRecords(r) => r.version(),
            Self::DescribeConfigs(r) => r.version(),
            Self::IncrementalAlterConfigs(r) => r.version(),
            Self::ListConfigResources(r) => r.version(),
            Self::DescribeCluster(r) => r.version(),
            Self::DescribeLogDirs(r) => r.version(),
            Self::AlterReplicaLogDirs(r) => r.version(),
            Self::ElectLeaders(r) => r.version(),
            Self::AlterPartitionReassignments(r) => r.version(),
            Self::ListPartitionReassignments(r) => r.version(),
            Self::DescribeAcls(r) => r.version(),
            Self::CreateAcls(r) => r.version(),
            Self::DeleteAcls(r) => r.version(),
            Self::DescribeClientQuotas(r) => r.version(),
            Self::AlterClientQuotas(r) => r.version(),
            Self::DescribeUserScramCredentials(r) => r.version(),
            Self::AlterUserScramCredentials(r) => r.version(),
            Self::CreateDelegationToken(r) => r.version(),
            Self::RenewDelegationToken(r) => r.version(),
            Self::ExpireDelegationToken(r) => r.version(),
            Self::DescribeDelegationToken(r) => r.version(),
            Self::UpdateFeatures(r) => r.version(),
            Self::DescribeProducers(r) => r.version(),
            Self::DescribeTransactions(r) => r.version(),
            Self::WriteTxnMarkers(r) => r.version(),
            Self::ListTransactions(r) => r.version(),
        }
    }

    /// Returns the API key of this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        match self {
            Self::ApiVersions(r) => r.api_key(),
            Self::Metadata(r) => r.api_key(),
            Self::Produce(r) => r.api_key(),
            Self::Fetch(r) => r.api_key(),
            Self::SaslHandshake(r) => r.api_key(),
            Self::SaslAuthenticate(r) => r.api_key(),
            Self::FindCoordinator(r) => r.api_key(),
            Self::ListGroups(r) => r.api_key(),
            Self::DescribeGroups(r) => r.api_key(),
            Self::ConsumerGroupDescribe(r) => r.api_key(),
            Self::ListOffsets(r) => r.api_key(),
            Self::OffsetsForLeaderEpoch(r) => r.api_key(),
            Self::ConsumerGroupHeartbeat(r) => r.api_key(),
            Self::OffsetCommit(r) => r.api_key(),
            Self::DeleteGroups(r) => r.api_key(),
            Self::LeaveGroup(r) => r.api_key(),
            Self::OffsetDelete(r) => r.api_key(),
            Self::OffsetFetch(r) => r.api_key(),
            Self::InitProducerId(r) => r.api_key(),
            Self::AddPartitionsToTxn(r) => r.api_key(),
            Self::AddOffsetsToTxn(r) => r.api_key(),
            Self::EndTxn(r) => r.api_key(),
            Self::TxnOffsetCommit(r) => r.api_key(),
            Self::CreateTopics(r) => r.api_key(),
            Self::DeleteTopics(r) => r.api_key(),
            Self::CreatePartitions(r) => r.api_key(),
            Self::DeleteRecords(r) => r.api_key(),
            Self::DescribeConfigs(r) => r.api_key(),
            Self::IncrementalAlterConfigs(r) => r.api_key(),
            Self::ListConfigResources(r) => r.api_key(),
            Self::DescribeCluster(r) => r.api_key(),
            Self::DescribeLogDirs(r) => r.api_key(),
            Self::AlterReplicaLogDirs(r) => r.api_key(),
            Self::ElectLeaders(r) => r.api_key(),
            Self::AlterPartitionReassignments(r) => r.api_key(),
            Self::ListPartitionReassignments(r) => r.api_key(),
            Self::DescribeAcls(r) => r.api_key(),
            Self::CreateAcls(r) => r.api_key(),
            Self::DeleteAcls(r) => r.api_key(),
            Self::DescribeClientQuotas(r) => r.api_key(),
            Self::AlterClientQuotas(r) => r.api_key(),
            Self::DescribeUserScramCredentials(r) => r.api_key(),
            Self::AlterUserScramCredentials(r) => r.api_key(),
            Self::CreateDelegationToken(r) => r.api_key(),
            Self::RenewDelegationToken(r) => r.api_key(),
            Self::ExpireDelegationToken(r) => r.api_key(),
            Self::DescribeDelegationToken(r) => r.api_key(),
            Self::UpdateFeatures(r) => r.api_key(),
            Self::DescribeProducers(r) => r.api_key(),
            Self::DescribeTransactions(r) => r.api_key(),
            Self::WriteTxnMarkers(r) => r.api_key(),
            Self::ListTransactions(r) => r.api_key(),
        }
    }

    /// Builds a size-prefixed [`ByteBufferSend`] for network transmission.
    ///
    /// Corresponds to `AbstractRequest.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&mut self, header: &RequestHeader) -> io::Result<ByteBufferSend> {
        match self {
            Self::ApiVersions(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Metadata(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Produce(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Fetch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::SaslHandshake(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::SaslAuthenticate(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::FindCoordinator(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListGroups(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeGroups(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ConsumerGroupDescribe(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListOffsets(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetsForLeaderEpoch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ConsumerGroupHeartbeat(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetCommit(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteGroups(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::LeaveGroup(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetDelete(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetFetch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::InitProducerId(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AddPartitionsToTxn(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AddOffsetsToTxn(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::EndTxn(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::TxnOffsetCommit(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreateTopics(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteTopics(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreatePartitions(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteRecords(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeConfigs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::IncrementalAlterConfigs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListConfigResources(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeCluster(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeLogDirs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AlterReplicaLogDirs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ElectLeaders(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AlterPartitionReassignments(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListPartitionReassignments(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeAcls(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreateAcls(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteAcls(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeClientQuotas(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AlterClientQuotas(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeUserScramCredentials(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AlterUserScramCredentials(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreateDelegationToken(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::RenewDelegationToken(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ExpireDelegationToken(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeDelegationToken(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::UpdateFeatures(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeProducers(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeTransactions(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::WriteTxnMarkers(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListTransactions(r) => SendBuilder::build_request_send(header, r.data_mut()),
        }
    }

    /// Serializes header and body without a size prefix.
    ///
    /// Corresponds to `AbstractRequest.serializeWithHeader` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if the header API key or version does not match this request,
    /// or if serialization fails.
    pub fn serialize_with_header(&mut self, header: &RequestHeader) -> io::Result<ByteBufferAccessor> {
        if header.api_key() != self.api_key() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Could not build request {} with header api key {}",
                    self.api_key(),
                    header.api_key()
                ),
            ));
        }
        if header.api_version() != self.version() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Could not build request version {} with header version {}",
                    self.version(),
                    header.api_version()
                ),
            ));
        }
        let version = self.version();
        match self {
            Self::ApiVersions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Metadata(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Produce(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Fetch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::SaslHandshake(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::SaslAuthenticate(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::FindCoordinator(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListGroups(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeGroups(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ConsumerGroupDescribe(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListOffsets(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetsForLeaderEpoch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ConsumerGroupHeartbeat(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetCommit(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteGroups(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::LeaveGroup(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetDelete(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetFetch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::InitProducerId(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AddPartitionsToTxn(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AddOffsetsToTxn(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::EndTxn(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::TxnOffsetCommit(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreateTopics(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteTopics(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreatePartitions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteRecords(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeConfigs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::IncrementalAlterConfigs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListConfigResources(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeCluster(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeLogDirs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AlterReplicaLogDirs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ElectLeaders(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AlterPartitionReassignments(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListPartitionReassignments(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeAcls(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreateAcls(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteAcls(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeClientQuotas(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AlterClientQuotas(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeUserScramCredentials(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AlterUserScramCredentials(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreateDelegationToken(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::RenewDelegationToken(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ExpireDelegationToken(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeDelegationToken(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::UpdateFeatures(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeProducers(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeTransactions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::WriteTxnMarkers(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListTransactions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
        }
    }

    /// Serializes just the request body (no header, no size prefix).
    ///
    /// Corresponds to `AbstractRequest.serialize` in Java (visible for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize(&mut self) -> io::Result<ByteBufferAccessor> {
        let version = self.version();
        match self {
            Self::ApiVersions(r) => Self::serialize_body(r.data_mut(), version),
            Self::Metadata(r) => Self::serialize_body(r.data_mut(), version),
            Self::Produce(r) => Self::serialize_body(r.data_mut(), version),
            Self::Fetch(r) => Self::serialize_body(r.data_mut(), version),
            Self::SaslHandshake(r) => Self::serialize_body(r.data_mut(), version),
            Self::SaslAuthenticate(r) => Self::serialize_body(r.data_mut(), version),
            Self::FindCoordinator(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListGroups(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeGroups(r) => Self::serialize_body(r.data_mut(), version),
            Self::ConsumerGroupDescribe(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListOffsets(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetsForLeaderEpoch(r) => Self::serialize_body(r.data_mut(), version),
            Self::ConsumerGroupHeartbeat(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetCommit(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteGroups(r) => Self::serialize_body(r.data_mut(), version),
            Self::LeaveGroup(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetDelete(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetFetch(r) => Self::serialize_body(r.data_mut(), version),
            Self::InitProducerId(r) => Self::serialize_body(r.data_mut(), version),
            Self::AddPartitionsToTxn(r) => Self::serialize_body(r.data_mut(), version),
            Self::AddOffsetsToTxn(r) => Self::serialize_body(r.data_mut(), version),
            Self::EndTxn(r) => Self::serialize_body(r.data_mut(), version),
            Self::TxnOffsetCommit(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreateTopics(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteTopics(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreatePartitions(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteRecords(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeConfigs(r) => Self::serialize_body(r.data_mut(), version),
            Self::IncrementalAlterConfigs(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListConfigResources(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeCluster(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeLogDirs(r) => Self::serialize_body(r.data_mut(), version),
            Self::AlterReplicaLogDirs(r) => Self::serialize_body(r.data_mut(), version),
            Self::ElectLeaders(r) => Self::serialize_body(r.data_mut(), version),
            Self::AlterPartitionReassignments(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListPartitionReassignments(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeAcls(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreateAcls(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteAcls(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeClientQuotas(r) => Self::serialize_body(r.data_mut(), version),
            Self::AlterClientQuotas(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeUserScramCredentials(r) => Self::serialize_body(r.data_mut(), version),
            Self::AlterUserScramCredentials(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreateDelegationToken(r) => Self::serialize_body(r.data_mut(), version),
            Self::RenewDelegationToken(r) => Self::serialize_body(r.data_mut(), version),
            Self::ExpireDelegationToken(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeDelegationToken(r) => Self::serialize_body(r.data_mut(), version),
            Self::UpdateFeatures(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeProducers(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeTransactions(r) => Self::serialize_body(r.data_mut(), version),
            Self::WriteTxnMarkers(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListTransactions(r) => Self::serialize_body(r.data_mut(), version),
        }
    }

    /// Serializes a message body at a given version.
    fn serialize_body(msg: &mut impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
        let mut cache = crate::common::protocol::ObjectSerializationCache::new();
        let size = Message::size(msg, &mut cache, version)?;
        let mut buf = ByteBufferAccessor::new(size as usize);
        Message::write(msg, &mut buf, &cache, version)?;
        buf.flip();
        Ok(buf)
    }

    /// Returns an error response for this request.
    ///
    /// Returns `None` when the request type does not expect a response (e.g.,
    /// Produce with acks=0). In Java, `getErrorResponse()` returns `null` in
    /// those cases.
    pub fn get_error_response(
        &self,
        throttle_time_ms: i32,
        error: &crate::common::protocol::Errors,
    ) -> Option<ConcreteResponse> {
        match self {
            Self::ApiVersions(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::Metadata(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::Produce(r) => r.get_error_response(throttle_time_ms, error),
            Self::Fetch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::SaslHandshake(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::SaslAuthenticate(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::FindCoordinator(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListGroups(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeGroups(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ConsumerGroupDescribe(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListOffsets(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetsForLeaderEpoch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ConsumerGroupHeartbeat(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetCommit(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteGroups(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::LeaveGroup(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetDelete(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetFetch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::InitProducerId(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AddPartitionsToTxn(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AddOffsetsToTxn(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::EndTxn(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::TxnOffsetCommit(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreateTopics(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteTopics(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreatePartitions(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteRecords(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeConfigs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::IncrementalAlterConfigs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListConfigResources(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeCluster(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeLogDirs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AlterReplicaLogDirs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ElectLeaders(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AlterPartitionReassignments(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListPartitionReassignments(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeAcls(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreateAcls(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteAcls(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeClientQuotas(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AlterClientQuotas(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeUserScramCredentials(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AlterUserScramCredentials(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreateDelegationToken(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::RenewDelegationToken(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ExpireDelegationToken(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeDelegationToken(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::UpdateFeatures(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeProducers(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeTransactions(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::WriteTxnMarkers(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListTransactions(r) => Some(r.get_error_response(throttle_time_ms, error)),
        }
    }

    /// Factory method for parsing a request object based on API key, version, and readable.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not supported or parsing fails.
    pub fn parse_request(
        api_key: &ApiKeys,
        api_version: i16,
        readable: &mut dyn Readable,
    ) -> io::Result<RequestAndSize> {
        let buffer_size = readable.remaining();
        let request = Self::do_parse_request(api_key, api_version, readable)?;
        Ok(RequestAndSize::new(request, buffer_size))
    }

    fn do_parse_request(api_key: &ApiKeys, api_version: i16, readable: &mut dyn Readable) -> io::Result<Self> {
        match *api_key {
            ApiKeys::API_VERSIONS => {
                let data = ApiVersionsRequestData::read(readable, api_version)?;
                Ok(Self::ApiVersions(ApiVersionsRequest::new(data, api_version)))
            },
            ApiKeys::METADATA => {
                let data = MetadataRequestData::read(readable, api_version)?;
                Ok(Self::Metadata(MetadataRequest::new(data, api_version)))
            },
            ApiKeys::PRODUCE => {
                let data = ProduceRequestData::read(readable, api_version)?;
                Ok(Self::Produce(ProduceRequest::new(data, api_version)))
            },
            ApiKeys::FETCH => {
                let data = FetchRequestData::read(readable, api_version)?;
                Ok(Self::Fetch(FetchRequest::new(data, api_version)))
            },
            ApiKeys::SASL_HANDSHAKE => {
                let data = SaslHandshakeRequestData::read(readable, api_version)?;
                Ok(Self::SaslHandshake(SaslHandshakeRequest::new(data, api_version)))
            },
            ApiKeys::SASL_AUTHENTICATE => {
                let data = SaslAuthenticateRequestData::read(readable, api_version)?;
                Ok(Self::SaslAuthenticate(SaslAuthenticateRequest::new(data, api_version)))
            },
            ApiKeys::FIND_COORDINATOR => {
                let data = FindCoordinatorRequestData::read(readable, api_version)?;
                Ok(Self::FindCoordinator(FindCoordinatorRequest::new(data, api_version)))
            },
            ApiKeys::LIST_GROUPS => {
                let data = ListGroupsRequestData::read(readable, api_version)?;
                Ok(Self::ListGroups(ListGroupsRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_GROUPS => {
                let data = DescribeGroupsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeGroups(DescribeGroupsRequest::new(data, api_version)))
            },
            ApiKeys::CONSUMER_GROUP_DESCRIBE => {
                let data = ConsumerGroupDescribeRequestData::read(readable, api_version)?;
                Ok(Self::ConsumerGroupDescribe(ConsumerGroupDescribeRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::LIST_OFFSETS => {
                let data = ListOffsetsRequestData::read(readable, api_version)?;
                Ok(Self::ListOffsets(ListOffsetsRequest::new(data, api_version)))
            },
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => {
                let data = OffsetForLeaderEpochRequestData::read(readable, api_version)?;
                Ok(Self::OffsetsForLeaderEpoch(OffsetsForLeaderEpochRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::CONSUMER_GROUP_HEARTBEAT => {
                let data = ConsumerGroupHeartbeatRequestData::read(readable, api_version)?;
                Ok(Self::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::OFFSET_COMMIT => {
                let data = OffsetCommitRequestData::read(readable, api_version)?;
                Ok(Self::OffsetCommit(OffsetCommitRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_GROUPS => {
                let data = DeleteGroupsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteGroups(DeleteGroupsRequest::new(data, api_version)))
            },
            ApiKeys::LEAVE_GROUP => {
                let data = LeaveGroupRequestData::read(readable, api_version)?;
                Ok(Self::LeaveGroup(LeaveGroupRequest::new(data, api_version)))
            },
            ApiKeys::OFFSET_DELETE => {
                let data = OffsetDeleteRequestData::read(readable, api_version)?;
                Ok(Self::OffsetDelete(OffsetDeleteRequest::new(data, api_version)))
            },
            ApiKeys::OFFSET_FETCH => {
                let data = OffsetFetchRequestData::read(readable, api_version)?;
                Ok(Self::OffsetFetch(OffsetFetchRequest::new(data, api_version)))
            },
            ApiKeys::CREATE_TOPICS => {
                let data = CreateTopicsRequestData::read(readable, api_version)?;
                Ok(Self::CreateTopics(CreateTopicsRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_TOPICS => {
                let data = DeleteTopicsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteTopics(DeleteTopicsRequest::new(data, api_version)))
            },
            ApiKeys::CREATE_PARTITIONS => {
                let data = CreatePartitionsRequestData::read(readable, api_version)?;
                Ok(Self::CreatePartitions(CreatePartitionsRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_RECORDS => {
                let data = DeleteRecordsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteRecords(DeleteRecordsRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_CONFIGS => {
                let data = DescribeConfigsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeConfigs(DescribeConfigsRequest::new(data, api_version)))
            },
            ApiKeys::INCREMENTAL_ALTER_CONFIGS => {
                let data = IncrementalAlterConfigsRequestData::read(readable, api_version)?;
                Ok(Self::IncrementalAlterConfigs(IncrementalAlterConfigsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::LIST_CONFIG_RESOURCES => {
                let data = ListConfigResourcesRequestData::read(readable, api_version)?;
                Ok(Self::ListConfigResources(ListConfigResourcesRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_CLUSTER => {
                let data = DescribeClusterRequestData::read(readable, api_version)?;
                Ok(Self::DescribeCluster(DescribeClusterRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_LOG_DIRS => {
                let data = DescribeLogDirsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeLogDirs(DescribeLogDirsRequest::new(data, api_version)))
            },
            ApiKeys::ALTER_REPLICA_LOG_DIRS => {
                let data = AlterReplicaLogDirsRequestData::read(readable, api_version)?;
                Ok(Self::AlterReplicaLogDirs(AlterReplicaLogDirsRequest::new(data, api_version)))
            },
            ApiKeys::ELECT_LEADERS => {
                let data = ElectLeadersRequestData::read(readable, api_version)?;
                Ok(Self::ElectLeaders(ElectLeadersRequest::new(data, api_version)))
            },
            ApiKeys::ALTER_PARTITION_REASSIGNMENTS => {
                let data = AlterPartitionReassignmentsRequestData::read(readable, api_version)?;
                Ok(Self::AlterPartitionReassignments(AlterPartitionReassignmentsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::LIST_PARTITION_REASSIGNMENTS => {
                let data = ListPartitionReassignmentsRequestData::read(readable, api_version)?;
                Ok(Self::ListPartitionReassignments(ListPartitionReassignmentsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::DESCRIBE_ACLS => {
                let data = DescribeAclsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeAcls(DescribeAclsRequest::new(data, api_version)))
            },
            ApiKeys::CREATE_ACLS => {
                let data = CreateAclsRequestData::read(readable, api_version)?;
                Ok(Self::CreateAcls(CreateAclsRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_ACLS => {
                let data = DeleteAclsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteAcls(DeleteAclsRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_CLIENT_QUOTAS => {
                let data = DescribeClientQuotasRequestData::read(readable, api_version)?;
                Ok(Self::DescribeClientQuotas(DescribeClientQuotasRequest::new(data, api_version)))
            },
            ApiKeys::ALTER_CLIENT_QUOTAS => {
                let data = AlterClientQuotasRequestData::read(readable, api_version)?;
                Ok(Self::AlterClientQuotas(AlterClientQuotasRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS => {
                let data = DescribeUserScramCredentialsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeUserScramCredentials(DescribeUserScramCredentialsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::ALTER_USER_SCRAM_CREDENTIALS => {
                let data = AlterUserScramCredentialsRequestData::read(readable, api_version)?;
                Ok(Self::AlterUserScramCredentials(AlterUserScramCredentialsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::CREATE_DELEGATION_TOKEN => {
                let data = CreateDelegationTokenRequestData::read(readable, api_version)?;
                Ok(Self::CreateDelegationToken(CreateDelegationTokenRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::RENEW_DELEGATION_TOKEN => {
                let data = RenewDelegationTokenRequestData::read(readable, api_version)?;
                Ok(Self::RenewDelegationToken(RenewDelegationTokenRequest::new(data, api_version)))
            },
            ApiKeys::EXPIRE_DELEGATION_TOKEN => {
                let data = ExpireDelegationTokenRequestData::read(readable, api_version)?;
                Ok(Self::ExpireDelegationToken(ExpireDelegationTokenRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::DESCRIBE_DELEGATION_TOKEN => {
                let data = DescribeDelegationTokenRequestData::read(readable, api_version)?;
                Ok(Self::DescribeDelegationToken(DescribeDelegationTokenRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::UPDATE_FEATURES => {
                let data = UpdateFeaturesRequestData::read(readable, api_version)?;
                Ok(Self::UpdateFeatures(UpdateFeaturesRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_PRODUCERS => {
                let data = DescribeProducersRequestData::read(readable, api_version)?;
                Ok(Self::DescribeProducers(DescribeProducersRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_TRANSACTIONS => {
                let data = DescribeTransactionsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeTransactions(DescribeTransactionsRequest::new(data, api_version)))
            },
            ApiKeys::INIT_PRODUCER_ID => {
                let data = InitProducerIdRequestData::read(readable, api_version)?;
                Ok(Self::InitProducerId(InitProducerIdRequest::new(data, api_version)))
            },
            ApiKeys::ADD_PARTITIONS_TO_TXN => {
                let data = AddPartitionsToTxnRequestData::read(readable, api_version)?;
                Ok(Self::AddPartitionsToTxn(AddPartitionsToTxnRequest::new(data, api_version)))
            },
            ApiKeys::ADD_OFFSETS_TO_TXN => {
                let data = AddOffsetsToTxnRequestData::read(readable, api_version)?;
                Ok(Self::AddOffsetsToTxn(AddOffsetsToTxnRequest::new(data, api_version)))
            },
            ApiKeys::END_TXN => {
                let data = EndTxnRequestData::read(readable, api_version)?;
                Ok(Self::EndTxn(EndTxnRequest::new(data, api_version)))
            },
            ApiKeys::TXN_OFFSET_COMMIT => {
                let data = TxnOffsetCommitRequestData::read(readable, api_version)?;
                Ok(Self::TxnOffsetCommit(TxnOffsetCommitRequest::new(data, api_version)))
            },
            ApiKeys::WRITE_TXN_MARKERS => {
                let data = WriteTxnMarkersRequestData::read(readable, api_version)?;
                Ok(Self::WriteTxnMarkers(WriteTxnMarkersRequest::new(data, api_version)))
            },
            ApiKeys::LIST_TRANSACTIONS => {
                let data = ListTransactionsRequestData::read(readable, api_version)?;
                Ok(Self::ListTransactions(ListTransactionsRequest::new(data, api_version)))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "ApiKey {api_key} is not currently handled in `parse_request`, the code should be updated to do so."
                ),
            )),
        }
    }
}

impl std::fmt::Display for ConcreteRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiVersions(r) => write!(f, "{r}"),
            Self::Metadata(r) => write!(f, "{r}"),
            Self::Produce(r) => write!(f, "{r}"),
            Self::Fetch(r) => write!(f, "{r}"),
            Self::SaslHandshake(r) => write!(f, "{r}"),
            Self::SaslAuthenticate(r) => write!(f, "{r}"),
            Self::FindCoordinator(r) => write!(f, "{r}"),
            Self::ListGroups(r) => write!(f, "{r}"),
            Self::DescribeGroups(r) => write!(f, "{r}"),
            Self::ConsumerGroupDescribe(r) => write!(f, "{r}"),
            Self::ListOffsets(r) => write!(f, "{r}"),
            Self::OffsetsForLeaderEpoch(r) => write!(f, "{r}"),
            Self::ConsumerGroupHeartbeat(r) => write!(f, "{r}"),
            Self::OffsetCommit(r) => write!(f, "{r}"),
            Self::DeleteGroups(r) => write!(f, "{r}"),
            Self::LeaveGroup(r) => write!(f, "{r}"),
            Self::OffsetDelete(r) => write!(f, "{r}"),
            Self::OffsetFetch(r) => write!(f, "{r}"),
            Self::InitProducerId(r) => write!(f, "{r}"),
            Self::AddPartitionsToTxn(r) => write!(f, "{r}"),
            Self::AddOffsetsToTxn(r) => write!(f, "{r}"),
            Self::EndTxn(r) => write!(f, "{r}"),
            Self::TxnOffsetCommit(r) => write!(f, "{r}"),
            Self::CreateTopics(r) => write!(f, "{r}"),
            Self::DeleteTopics(r) => write!(f, "{r}"),
            Self::CreatePartitions(r) => write!(f, "{r}"),
            Self::DeleteRecords(r) => write!(f, "{r}"),
            Self::DescribeConfigs(r) => write!(f, "{r}"),
            Self::IncrementalAlterConfigs(r) => write!(f, "{r}"),
            Self::ListConfigResources(r) => write!(f, "{r}"),
            Self::DescribeCluster(r) => write!(f, "{r}"),
            Self::DescribeLogDirs(r) => write!(f, "{r}"),
            Self::AlterReplicaLogDirs(r) => write!(f, "{r}"),
            Self::ElectLeaders(r) => write!(f, "{r}"),
            Self::AlterPartitionReassignments(r) => write!(f, "{r}"),
            Self::ListPartitionReassignments(r) => write!(f, "{r}"),
            Self::DescribeAcls(r) => write!(f, "{r}"),
            Self::CreateAcls(r) => write!(f, "{r}"),
            Self::DeleteAcls(r) => write!(f, "{r}"),
            Self::DescribeClientQuotas(r) => write!(f, "{r}"),
            Self::AlterClientQuotas(r) => write!(f, "{r}"),
            Self::DescribeUserScramCredentials(r) => write!(f, "{r}"),
            Self::AlterUserScramCredentials(r) => write!(f, "{r}"),
            Self::CreateDelegationToken(r) => write!(f, "{r}"),
            Self::RenewDelegationToken(r) => write!(f, "{r}"),
            Self::ExpireDelegationToken(r) => write!(f, "{r}"),
            Self::DescribeDelegationToken(r) => write!(f, "{r}"),
            Self::UpdateFeatures(r) => write!(f, "{r}"),
            Self::DescribeProducers(r) => write!(f, "{r}"),
            Self::DescribeTransactions(r) => write!(f, "{r}"),
            Self::WriteTxnMarkers(r) => write!(f, "{r}"),
            Self::ListTransactions(r) => write!(f, "{r}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::RequestHeaderOptionsBuilder;

    /// Java's `AbstractRequest.serializeWithHeader` builds the message by
    /// concatenating the two `ApiKeys` values (`AbstractRequest.java:117`).
    /// `ApiKeys` overrides no `toString()`, so each renders as its enum
    /// **constant**, bare — not quoted, and not the specification spelling
    /// carried by the public `name` field.
    #[test]
    fn test_serialize_with_header_api_key_mismatch_message() {
        let mut request = ConcreteRequest::Metadata(MetadataRequest::new(MetadataRequestData::new(), 12));
        let header = RequestHeader::new_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::PRODUCE)
                .set_request_version(12)
                .set_client_id("client")
                .set_correlation_id(1)
                .build(),
        )
        .expect("valid header");

        let error = match request.serialize_with_header(&header) {
            Err(error) => error,
            Ok(_) => panic!("api keys differ"),
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "Could not build request METADATA with header api key PRODUCE"
        );
    }

    /// Java's version mismatch branch (`AbstractRequest.java:120`) interpolates
    /// only the two versions, so it is unaffected by the `ApiKeys` rendering —
    /// pinned here so the pair of messages stays covered together.
    #[test]
    fn test_serialize_with_header_version_mismatch_message() {
        let mut request = ConcreteRequest::Metadata(MetadataRequest::new(MetadataRequestData::new(), 12));
        let header = RequestHeader::new_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::METADATA)
                .set_request_version(9)
                .set_client_id("client")
                .set_correlation_id(1)
                .build(),
        )
        .expect("valid header");

        let error = match request.serialize_with_header(&header) {
            Err(error) => error,
            Ok(_) => panic!("versions differ"),
        };
        assert_eq!(error.to_string(), "Could not build request version 12 with header version 9");
    }

    /// Java's `parseRequest` default arm throws an `AssertionError` whose text
    /// interpolates the `ApiKeys` value — the enum constant — and ends with the
    /// "code should be updated" clause (`AbstractRequest.java:358-359`). The
    /// method name is snake_cased per CLAUDE.md §2; the rest is verbatim.
    #[test]
    fn test_parse_request_unhandled_api_key_message() {
        let mut readable = ByteBufferAccessor::from_bytes(Vec::new());
        let error = ConcreteRequest::parse_request(&ApiKeys::VOTE, 0, &mut readable)
            .expect_err("VOTE is a broker-only api with no client-side parser");
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "ApiKey VOTE is not currently handled in `parse_request`, the code should be updated to do so."
        );
    }
}
