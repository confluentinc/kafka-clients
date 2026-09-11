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

//! Abstract response framework for Kafka protocol responses.
//!
//! Corresponds to `org.apache.kafka.common.requests.AbstractResponse`.
//!
//! Java uses an abstract class with per-type subclasses. In Rust we use an enum
//! (`ConcreteResponse`) with a variant for each supported response type. Currently
//! only ApiVersions and Metadata are supported; other variants will be added as
//! their request/response types are translated.

use std::collections::HashMap;
use std::io;

use crate::common::network::ByteBufferSend;
use crate::common::protocol::Message;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors, Readable};

use super::CorrelationIdMismatchError;
use super::correlation_id_mismatch_error::correlation_id_mismatch_io_error;

use super::AddOffsetsToTxnResponse;
use super::AddPartitionsToTxnResponse;
use super::AlterClientQuotasResponse;
use super::AlterPartitionReassignmentsResponse;
use super::AlterReplicaLogDirsResponse;
use super::AlterUserScramCredentialsResponse;
use super::ApiVersionsResponse;
use super::ConsumerGroupDescribeResponse;
use super::ConsumerGroupHeartbeatResponse;
use super::CreateAclsResponse;
use super::CreateDelegationTokenResponse;
use super::CreatePartitionsResponse;
use super::CreateTopicsResponse;
use super::DeleteAclsResponse;
use super::DeleteGroupsResponse;
use super::DeleteRecordsResponse;
use super::DeleteTopicsResponse;
use super::DescribeAclsResponse;
use super::DescribeClientQuotasResponse;
use super::DescribeClusterResponse;
use super::DescribeConfigsResponse;
use super::DescribeDelegationTokenResponse;
use super::DescribeGroupsResponse;
use super::DescribeLogDirsResponse;
use super::DescribeProducersResponse;
use super::DescribeTransactionsResponse;
use super::DescribeUserScramCredentialsResponse;
use super::ElectLeadersResponse;
use super::EndTxnResponse;
use super::ExpireDelegationTokenResponse;
use super::FetchResponse;
use super::FindCoordinatorResponse;
use super::IncrementalAlterConfigsResponse;
use super::InitProducerIdResponse;
use super::LeaveGroupResponse;
use super::ListConfigResourcesResponse;
use super::ListGroupsResponse;
use super::ListOffsetsResponse;
use super::ListPartitionReassignmentsResponse;
use super::ListTransactionsResponse;
use super::MetadataResponse;
use super::OffsetCommitResponse;
use super::OffsetDeleteResponse;
use super::OffsetFetchResponse;
use super::OffsetsForLeaderEpochResponse;
use super::ProduceResponse;
use super::RenewDelegationTokenResponse;
use super::RequestHeader;
use super::ResponseHeader;
use super::SaslAuthenticateResponse;
use super::SaslHandshakeResponse;
use super::SendBuilder;
use super::TxnOffsetCommitResponse;
use super::UpdateFeaturesResponse;
use super::WriteTxnMarkersResponse;

/// Default throttle time in milliseconds.
pub const DEFAULT_THROTTLE_TIME: i32 = 0;

/// Enum dispatch for all supported Kafka response types.
///
/// Each variant wraps a concrete response struct. Common methods are dispatched
/// via `match` on the variant.
///
/// Variants will be added as response types are translated.
#[derive(Debug, Clone)]
pub enum ConcreteResponse {
    /// An ApiVersions response.
    ApiVersions(ApiVersionsResponse),
    /// A Metadata response.
    Metadata(MetadataResponse),
    /// A Produce response.
    Produce(ProduceResponse),
    /// A Fetch response (consumer fetch loop).
    Fetch(FetchResponse),
    /// A SASL handshake response.
    SaslHandshake(SaslHandshakeResponse),
    /// A SASL authenticate response.
    SaslAuthenticate(SaslAuthenticateResponse),
    /// A FindCoordinator response.
    FindCoordinator(FindCoordinatorResponse),
    /// A ListGroups response.
    ListGroups(ListGroupsResponse),
    /// A DescribeGroups response.
    DescribeGroups(DescribeGroupsResponse),
    /// A ConsumerGroupDescribe response.
    ConsumerGroupDescribe(ConsumerGroupDescribeResponse),
    /// A ListOffsets response.
    ListOffsets(ListOffsetsResponse),
    /// An OffsetsForLeaderEpoch response.
    OffsetsForLeaderEpoch(OffsetsForLeaderEpochResponse),
    /// A ConsumerGroupHeartbeat response (KIP-848).
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponse),
    /// An OffsetCommit response.
    OffsetCommit(OffsetCommitResponse),
    /// A DeleteGroups response.
    DeleteGroups(DeleteGroupsResponse),
    /// A LeaveGroup response.
    LeaveGroup(LeaveGroupResponse),
    /// An OffsetDelete response.
    OffsetDelete(OffsetDeleteResponse),
    /// An OffsetFetch response.
    OffsetFetch(OffsetFetchResponse),
    /// An InitProducerId response.
    InitProducerId(InitProducerIdResponse),
    /// An AddPartitionsToTxn response.
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    /// An AddOffsetsToTxn response.
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    /// An EndTxn response.
    EndTxn(EndTxnResponse),
    /// A TxnOffsetCommit response.
    TxnOffsetCommit(TxnOffsetCommitResponse),
    /// A CreateTopics response.
    CreateTopics(CreateTopicsResponse),
    /// A DeleteTopics response.
    DeleteTopics(DeleteTopicsResponse),
    /// A CreatePartitions response.
    CreatePartitions(CreatePartitionsResponse),
    /// A DeleteRecords response.
    DeleteRecords(DeleteRecordsResponse),
    /// A DescribeConfigs response.
    DescribeConfigs(DescribeConfigsResponse),
    /// An IncrementalAlterConfigs response.
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    /// A ListConfigResources response.
    ListConfigResources(ListConfigResourcesResponse),
    /// A DescribeCluster response.
    DescribeCluster(DescribeClusterResponse),
    /// A DescribeLogDirs response.
    DescribeLogDirs(DescribeLogDirsResponse),
    /// An AlterReplicaLogDirs response.
    AlterReplicaLogDirs(AlterReplicaLogDirsResponse),
    /// An ElectLeaders response.
    ElectLeaders(ElectLeadersResponse),
    /// An AlterPartitionReassignments response.
    AlterPartitionReassignments(AlterPartitionReassignmentsResponse),
    /// A ListPartitionReassignments response.
    ListPartitionReassignments(ListPartitionReassignmentsResponse),
    /// A DescribeAcls response.
    DescribeAcls(DescribeAclsResponse),
    /// A CreateAcls response.
    CreateAcls(CreateAclsResponse),
    /// A DeleteAcls response.
    DeleteAcls(DeleteAclsResponse),
    /// A DescribeClientQuotas response.
    DescribeClientQuotas(DescribeClientQuotasResponse),
    /// An AlterClientQuotas response.
    AlterClientQuotas(AlterClientQuotasResponse),
    /// A DescribeUserScramCredentials response.
    DescribeUserScramCredentials(DescribeUserScramCredentialsResponse),
    /// An AlterUserScramCredentials response.
    AlterUserScramCredentials(AlterUserScramCredentialsResponse),
    /// A CreateDelegationToken response.
    CreateDelegationToken(CreateDelegationTokenResponse),
    /// A RenewDelegationToken response.
    RenewDelegationToken(RenewDelegationTokenResponse),
    /// An ExpireDelegationToken response.
    ExpireDelegationToken(ExpireDelegationTokenResponse),
    /// A DescribeDelegationToken response.
    DescribeDelegationToken(DescribeDelegationTokenResponse),
    /// An UpdateFeatures response.
    UpdateFeatures(UpdateFeaturesResponse),
    /// A DescribeProducers response.
    DescribeProducers(DescribeProducersResponse),
    /// A DescribeTransactions response.
    DescribeTransactions(DescribeTransactionsResponse),
    /// An InitProducerId response.
    /// A WriteTxnMarkers response.
    WriteTxnMarkers(WriteTxnMarkersResponse),
    /// A ListTransactions response.
    ListTransactions(ListTransactionsResponse),
}

impl ConcreteResponse {
    /// Returns the API key for this response.
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
    /// Corresponds to `AbstractResponse.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&mut self, header: &ResponseHeader, version: i16) -> io::Result<ByteBufferSend> {
        match self {
            Self::ApiVersions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Metadata(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Produce(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Fetch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::SaslHandshake(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::SaslAuthenticate(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::FindCoordinator(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListGroups(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeGroups(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ConsumerGroupDescribe(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListOffsets(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetsForLeaderEpoch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ConsumerGroupHeartbeat(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetCommit(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteGroups(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::LeaveGroup(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetDelete(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetFetch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::InitProducerId(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AddPartitionsToTxn(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AddOffsetsToTxn(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::EndTxn(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::TxnOffsetCommit(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreateTopics(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteTopics(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreatePartitions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteRecords(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeConfigs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::IncrementalAlterConfigs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListConfigResources(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeCluster(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeLogDirs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AlterReplicaLogDirs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ElectLeaders(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AlterPartitionReassignments(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListPartitionReassignments(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeAcls(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreateAcls(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteAcls(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeClientQuotas(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AlterClientQuotas(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeUserScramCredentials(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::AlterUserScramCredentials(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreateDelegationToken(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::RenewDelegationToken(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ExpireDelegationToken(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeDelegationToken(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::UpdateFeatures(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeProducers(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeTransactions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::WriteTxnMarkers(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListTransactions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
        }
    }

    /// Serializes header and body without a size prefix.
    ///
    /// Corresponds to `AbstractResponse.serializeWithHeader` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize_with_header(&mut self, header: &ResponseHeader, version: i16) -> io::Result<ByteBufferAccessor> {
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

    /// Serializes just the response body (no header, no size prefix).
    ///
    /// Corresponds to `AbstractResponse.serialize` in Java (visible for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize(&mut self, version: i16) -> io::Result<ByteBufferAccessor> {
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

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        match self {
            Self::ApiVersions(r) => r.error_counts(),
            Self::Metadata(r) => r.error_counts(),
            Self::Produce(r) => r.error_counts(),
            Self::Fetch(r) => r.error_counts(),
            Self::SaslHandshake(r) => r.error_counts(),
            Self::SaslAuthenticate(r) => r.error_counts(),
            Self::FindCoordinator(r) => r.error_counts(),
            Self::ListGroups(r) => r.error_counts(),
            Self::DescribeGroups(r) => r.error_counts(),
            Self::ConsumerGroupDescribe(r) => r.error_counts(),
            Self::ListOffsets(r) => r.error_counts(),
            Self::OffsetsForLeaderEpoch(r) => r.error_counts(),
            Self::ConsumerGroupHeartbeat(r) => r.error_counts(),
            Self::OffsetCommit(r) => r.error_counts(),
            Self::DeleteGroups(r) => r.error_counts(),
            Self::LeaveGroup(r) => r.error_counts(),
            Self::OffsetDelete(r) => r.error_counts(),
            Self::OffsetFetch(r) => r.error_counts(),
            Self::InitProducerId(r) => r.error_counts(),
            Self::AddPartitionsToTxn(r) => r.error_counts(),
            Self::AddOffsetsToTxn(r) => r.error_counts(),
            Self::EndTxn(r) => r.error_counts(),
            Self::TxnOffsetCommit(r) => r.error_counts(),
            Self::CreateTopics(r) => r.error_counts(),
            Self::DeleteTopics(r) => r.error_counts(),
            Self::CreatePartitions(r) => r.error_counts(),
            Self::DeleteRecords(r) => r.error_counts(),
            Self::DescribeConfigs(r) => r.error_counts(),
            Self::IncrementalAlterConfigs(r) => r.error_counts(),
            Self::ListConfigResources(r) => r.error_counts(),
            Self::DescribeCluster(r) => r.error_counts(),
            Self::DescribeLogDirs(r) => r.error_counts(),
            Self::AlterReplicaLogDirs(r) => r.error_counts(),
            Self::ElectLeaders(r) => r.error_counts(),
            Self::AlterPartitionReassignments(r) => r.error_counts(),
            Self::ListPartitionReassignments(r) => r.error_counts(),
            Self::DescribeAcls(r) => r.error_counts(),
            Self::CreateAcls(r) => r.error_counts(),
            Self::DeleteAcls(r) => r.error_counts(),
            Self::DescribeClientQuotas(r) => r.error_counts(),
            Self::AlterClientQuotas(r) => r.error_counts(),
            Self::DescribeUserScramCredentials(r) => r.error_counts(),
            Self::AlterUserScramCredentials(r) => r.error_counts(),
            Self::CreateDelegationToken(r) => r.error_counts(),
            Self::RenewDelegationToken(r) => r.error_counts(),
            Self::ExpireDelegationToken(r) => r.error_counts(),
            Self::DescribeDelegationToken(r) => r.error_counts(),
            Self::UpdateFeatures(r) => r.error_counts(),
            Self::DescribeProducers(r) => r.error_counts(),
            Self::DescribeTransactions(r) => r.error_counts(),
            Self::WriteTxnMarkers(r) => r.error_counts(),
            Self::ListTransactions(r) => r.error_counts(),
        }
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// Returns 0 if the response schema does not support this field.
    pub fn throttle_time_ms(&self) -> i32 {
        match self {
            Self::ApiVersions(r) => r.throttle_time_ms(),
            Self::Metadata(r) => r.throttle_time_ms(),
            Self::Produce(r) => r.throttle_time_ms(),
            Self::Fetch(r) => r.throttle_time_ms(),
            Self::SaslHandshake(r) => r.throttle_time_ms(),
            Self::SaslAuthenticate(r) => r.throttle_time_ms(),
            Self::FindCoordinator(r) => r.throttle_time_ms(),
            Self::ListGroups(r) => r.throttle_time_ms(),
            Self::DescribeGroups(r) => r.throttle_time_ms(),
            Self::ConsumerGroupDescribe(r) => r.throttle_time_ms(),
            Self::ListOffsets(r) => r.throttle_time_ms(),
            Self::OffsetsForLeaderEpoch(r) => r.throttle_time_ms(),
            Self::ConsumerGroupHeartbeat(r) => r.throttle_time_ms(),
            Self::OffsetCommit(r) => r.throttle_time_ms(),
            Self::DeleteGroups(r) => r.throttle_time_ms(),
            Self::LeaveGroup(r) => r.throttle_time_ms(),
            Self::OffsetDelete(r) => r.throttle_time_ms(),
            Self::OffsetFetch(r) => r.throttle_time_ms(),
            Self::InitProducerId(r) => r.throttle_time_ms(),
            Self::AddPartitionsToTxn(r) => r.throttle_time_ms(),
            Self::AddOffsetsToTxn(r) => r.throttle_time_ms(),
            Self::EndTxn(r) => r.throttle_time_ms(),
            Self::TxnOffsetCommit(r) => r.throttle_time_ms(),
            Self::CreateTopics(r) => r.throttle_time_ms(),
            Self::DeleteTopics(r) => r.throttle_time_ms(),
            Self::CreatePartitions(r) => r.throttle_time_ms(),
            Self::DeleteRecords(r) => r.throttle_time_ms(),
            Self::DescribeConfigs(r) => r.throttle_time_ms(),
            Self::IncrementalAlterConfigs(r) => r.throttle_time_ms(),
            Self::ListConfigResources(r) => r.throttle_time_ms(),
            Self::DescribeCluster(r) => r.throttle_time_ms(),
            Self::DescribeLogDirs(r) => r.throttle_time_ms(),
            Self::AlterReplicaLogDirs(r) => r.throttle_time_ms(),
            Self::ElectLeaders(r) => r.throttle_time_ms(),
            Self::AlterPartitionReassignments(r) => r.throttle_time_ms(),
            Self::ListPartitionReassignments(r) => r.throttle_time_ms(),
            Self::DescribeAcls(r) => r.throttle_time_ms(),
            Self::CreateAcls(r) => r.throttle_time_ms(),
            Self::DeleteAcls(r) => r.throttle_time_ms(),
            Self::DescribeClientQuotas(r) => r.throttle_time_ms(),
            Self::AlterClientQuotas(r) => r.throttle_time_ms(),
            Self::DescribeUserScramCredentials(r) => r.throttle_time_ms(),
            Self::AlterUserScramCredentials(r) => r.throttle_time_ms(),
            Self::CreateDelegationToken(r) => r.throttle_time_ms(),
            Self::RenewDelegationToken(r) => r.throttle_time_ms(),
            Self::ExpireDelegationToken(r) => r.throttle_time_ms(),
            Self::DescribeDelegationToken(r) => r.throttle_time_ms(),
            Self::UpdateFeatures(r) => r.throttle_time_ms(),
            Self::DescribeProducers(r) => r.throttle_time_ms(),
            Self::DescribeTransactions(r) => r.throttle_time_ms(),
            Self::WriteTxnMarkers(r) => r.throttle_time_ms(),
            Self::ListTransactions(r) => r.throttle_time_ms(),
        }
    }

    /// Sets the throttle time in the response if the schema supports it.
    /// Otherwise, this is a no-op.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        match self {
            Self::ApiVersions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Metadata(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Produce(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Fetch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::SaslHandshake(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::SaslAuthenticate(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::FindCoordinator(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListGroups(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeGroups(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ConsumerGroupDescribe(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListOffsets(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetsForLeaderEpoch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ConsumerGroupHeartbeat(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetCommit(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteGroups(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::LeaveGroup(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetDelete(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetFetch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::InitProducerId(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AddPartitionsToTxn(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AddOffsetsToTxn(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::EndTxn(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::TxnOffsetCommit(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreateTopics(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteTopics(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreatePartitions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteRecords(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeConfigs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::IncrementalAlterConfigs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListConfigResources(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeCluster(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeLogDirs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AlterReplicaLogDirs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ElectLeaders(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AlterPartitionReassignments(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListPartitionReassignments(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeAcls(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreateAcls(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteAcls(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeClientQuotas(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AlterClientQuotas(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeUserScramCredentials(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::AlterUserScramCredentials(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreateDelegationToken(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::RenewDelegationToken(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ExpireDelegationToken(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeDelegationToken(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::UpdateFeatures(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeProducers(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeTransactions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::WriteTxnMarkers(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListTransactions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
        }
    }

    /// Returns whether the client should throttle upon receiving this response.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        match self {
            Self::ApiVersions(r) => r.should_client_throttle(version),
            Self::Metadata(r) => r.should_client_throttle(version),
            Self::Produce(r) => r.should_client_throttle(version),
            Self::Fetch(r) => r.should_client_throttle(version),
            Self::SaslHandshake(r) => r.should_client_throttle(version),
            Self::SaslAuthenticate(r) => r.should_client_throttle(version),
            Self::FindCoordinator(r) => r.should_client_throttle(version),
            Self::ListGroups(r) => r.should_client_throttle(version),
            Self::DescribeGroups(r) => r.should_client_throttle(version),
            Self::ConsumerGroupDescribe(r) => r.should_client_throttle(version),
            Self::ListOffsets(r) => r.should_client_throttle(version),
            Self::OffsetsForLeaderEpoch(r) => r.should_client_throttle(version),
            Self::ConsumerGroupHeartbeat(r) => r.should_client_throttle(version),
            Self::OffsetCommit(r) => r.should_client_throttle(version),
            Self::DeleteGroups(r) => r.should_client_throttle(version),
            Self::LeaveGroup(r) => r.should_client_throttle(version),
            Self::OffsetDelete(r) => r.should_client_throttle(version),
            Self::OffsetFetch(r) => r.should_client_throttle(version),
            Self::InitProducerId(r) => r.should_client_throttle(version),
            Self::AddPartitionsToTxn(r) => r.should_client_throttle(version),
            Self::AddOffsetsToTxn(r) => r.should_client_throttle(version),
            Self::EndTxn(r) => r.should_client_throttle(version),
            Self::TxnOffsetCommit(r) => r.should_client_throttle(version),
            Self::CreateTopics(r) => r.should_client_throttle(version),
            Self::DeleteTopics(r) => r.should_client_throttle(version),
            Self::CreatePartitions(r) => r.should_client_throttle(version),
            Self::DeleteRecords(r) => r.should_client_throttle(version),
            Self::DescribeConfigs(r) => r.should_client_throttle(version),
            Self::IncrementalAlterConfigs(r) => r.should_client_throttle(version),
            Self::ListConfigResources(r) => r.should_client_throttle(version),
            Self::DescribeCluster(r) => r.should_client_throttle(version),
            Self::DescribeLogDirs(r) => r.should_client_throttle(version),
            Self::AlterReplicaLogDirs(r) => r.should_client_throttle(version),
            Self::ElectLeaders(r) => r.should_client_throttle(version),
            Self::AlterPartitionReassignments(r) => r.should_client_throttle(version),
            Self::ListPartitionReassignments(r) => r.should_client_throttle(version),
            Self::DescribeAcls(r) => r.should_client_throttle(version),
            Self::CreateAcls(r) => r.should_client_throttle(version),
            Self::DeleteAcls(r) => r.should_client_throttle(version),
            Self::DescribeClientQuotas(r) => r.should_client_throttle(version),
            Self::AlterClientQuotas(r) => r.should_client_throttle(version),
            Self::DescribeUserScramCredentials(r) => r.should_client_throttle(version),
            Self::AlterUserScramCredentials(r) => r.should_client_throttle(version),
            Self::CreateDelegationToken(r) => r.should_client_throttle(version),
            Self::RenewDelegationToken(r) => r.should_client_throttle(version),
            Self::ExpireDelegationToken(r) => r.should_client_throttle(version),
            Self::DescribeDelegationToken(r) => r.should_client_throttle(version),
            Self::UpdateFeatures(r) => r.should_client_throttle(version),
            Self::DescribeProducers(r) => r.should_client_throttle(version),
            Self::DescribeTransactions(r) => r.should_client_throttle(version),
            Self::WriteTxnMarkers(r) => r.should_client_throttle(version),
            Self::ListTransactions(r) => r.should_client_throttle(version),
        }
    }

    /// Parses a response from a buffer that contains both the response header and the
    /// response body.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The response header cannot be parsed
    /// - The correlation id in the response does not match the request
    /// - The response body cannot be parsed
    pub fn parse_response(buffer: &mut dyn Readable, request_header: &RequestHeader) -> io::Result<Self> {
        let api_key = request_header.api_key();
        let api_version = request_header.api_version();

        let response_header = ResponseHeader::parse(buffer, api_key.response_header_version(api_version))?;

        if request_header.correlation_id() != response_header.correlation_id() {
            // Java throws `CorrelationIdMismatchException`
            // (`AbstractResponse.java:105`), which `NetworkClient.parseResponse`
            // catches **by type** and either converts to a `SchemaException` or
            // rethrows (`NetworkClient.java:829-838`). An untyped
            // `ErrorKind::InvalidData` made that dispatch unexpressible, so the
            // typed value travels in the `io::Error` payload — the crate's
            // documented mechanism for a Java exception class crossing an
            // `io::Result` boundary (see `correlation_id_mismatch_io_error`).
            return Err(correlation_id_mismatch_io_error(CorrelationIdMismatchError::new(
                format!(
                    "Correlation id for response ({}) does not match request ({}), request header: {}",
                    response_header.correlation_id(),
                    request_header.correlation_id(),
                    request_header
                ),
                request_header.correlation_id(),
                response_header.correlation_id(),
            )));
        }

        Self::parse(api_key, buffer, api_version)
    }

    /// Parses a response body from the buffer for the given API key and version.
    ///
    /// For ApiVersions, the readable must be a `ByteBufferAccessor` to support
    /// the fallback-to-v0 parsing logic.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not supported or parsing fails.
    pub fn parse(api_key: &ApiKeys, readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        match *api_key {
            ApiKeys::API_VERSIONS => {
                // ApiVersionsResponse.parse requires a ByteBufferAccessor for snapshot_remaining
                // Since we receive &mut dyn Readable, we read remaining bytes and construct one.
                let remaining = readable.remaining();
                let bytes = readable.read_array(remaining)?;
                let mut buf = ByteBufferAccessor::from_bytes(bytes);
                let response = ApiVersionsResponse::parse(&mut buf, version)?;
                Ok(Self::ApiVersions(response))
            },
            ApiKeys::METADATA => {
                let response = MetadataResponse::parse(readable, version)?;
                Ok(Self::Metadata(response))
            },
            ApiKeys::PRODUCE => {
                let response = ProduceResponse::parse(readable, version)?;
                Ok(Self::Produce(response))
            },
            ApiKeys::FETCH => {
                let data = crate::fetch_response_data::FetchResponseData::read(readable, version)?;
                Ok(Self::Fetch(FetchResponse::new(data)))
            },
            ApiKeys::SASL_HANDSHAKE => {
                let response = SaslHandshakeResponse::parse(readable, version)?;
                Ok(Self::SaslHandshake(response))
            },
            ApiKeys::SASL_AUTHENTICATE => {
                let response = SaslAuthenticateResponse::parse(readable, version)?;
                Ok(Self::SaslAuthenticate(response))
            },
            ApiKeys::FIND_COORDINATOR => {
                let response = FindCoordinatorResponse::parse(readable, version)?;
                Ok(Self::FindCoordinator(response))
            },
            ApiKeys::LIST_GROUPS => {
                let response = ListGroupsResponse::parse(readable, version)?;
                Ok(Self::ListGroups(response))
            },
            ApiKeys::DESCRIBE_GROUPS => {
                let response = DescribeGroupsResponse::parse(readable, version)?;
                Ok(Self::DescribeGroups(response))
            },
            ApiKeys::CONSUMER_GROUP_DESCRIBE => {
                let response = ConsumerGroupDescribeResponse::parse(readable, version)?;
                Ok(Self::ConsumerGroupDescribe(response))
            },
            ApiKeys::LIST_OFFSETS => {
                let response = ListOffsetsResponse::parse(readable, version)?;
                Ok(Self::ListOffsets(response))
            },
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => {
                let response = OffsetsForLeaderEpochResponse::parse(readable, version)?;
                Ok(Self::OffsetsForLeaderEpoch(response))
            },
            ApiKeys::CONSUMER_GROUP_HEARTBEAT => {
                let response = ConsumerGroupHeartbeatResponse::parse(readable, version)?;
                Ok(Self::ConsumerGroupHeartbeat(response))
            },
            ApiKeys::OFFSET_COMMIT => {
                let response = OffsetCommitResponse::parse(readable, version)?;
                Ok(Self::OffsetCommit(response))
            },
            ApiKeys::DELETE_GROUPS => {
                let response = DeleteGroupsResponse::parse(readable, version)?;
                Ok(Self::DeleteGroups(response))
            },
            ApiKeys::LEAVE_GROUP => {
                let response = LeaveGroupResponse::parse(readable, version)?;
                Ok(Self::LeaveGroup(response))
            },
            ApiKeys::OFFSET_DELETE => {
                let response = OffsetDeleteResponse::parse(readable, version)?;
                Ok(Self::OffsetDelete(response))
            },
            ApiKeys::OFFSET_FETCH => {
                let response = OffsetFetchResponse::parse(readable, version)?;
                Ok(Self::OffsetFetch(response))
            },
            ApiKeys::CREATE_TOPICS => {
                let response = CreateTopicsResponse::parse(readable, version)?;
                Ok(Self::CreateTopics(response))
            },
            ApiKeys::DELETE_TOPICS => {
                let response = DeleteTopicsResponse::parse(readable, version)?;
                Ok(Self::DeleteTopics(response))
            },
            ApiKeys::CREATE_PARTITIONS => {
                let response = CreatePartitionsResponse::parse(readable, version)?;
                Ok(Self::CreatePartitions(response))
            },
            ApiKeys::DELETE_RECORDS => {
                let response = DeleteRecordsResponse::parse(readable, version)?;
                Ok(Self::DeleteRecords(response))
            },
            ApiKeys::DESCRIBE_CONFIGS => {
                let response = DescribeConfigsResponse::parse(readable, version)?;
                Ok(Self::DescribeConfigs(response))
            },
            ApiKeys::INCREMENTAL_ALTER_CONFIGS => {
                let response = IncrementalAlterConfigsResponse::parse(readable, version)?;
                Ok(Self::IncrementalAlterConfigs(response))
            },
            ApiKeys::LIST_CONFIG_RESOURCES => {
                let response = ListConfigResourcesResponse::parse(readable, version)?;
                Ok(Self::ListConfigResources(response))
            },
            ApiKeys::DESCRIBE_CLUSTER => {
                let response = DescribeClusterResponse::parse(readable, version)?;
                Ok(Self::DescribeCluster(response))
            },
            ApiKeys::DESCRIBE_LOG_DIRS => {
                let response = DescribeLogDirsResponse::parse(readable, version)?;
                Ok(Self::DescribeLogDirs(response))
            },
            ApiKeys::ALTER_REPLICA_LOG_DIRS => {
                let response = AlterReplicaLogDirsResponse::parse(readable, version)?;
                Ok(Self::AlterReplicaLogDirs(response))
            },
            ApiKeys::ELECT_LEADERS => {
                let response = ElectLeadersResponse::parse(readable, version)?;
                Ok(Self::ElectLeaders(response))
            },
            ApiKeys::ALTER_PARTITION_REASSIGNMENTS => {
                let response = AlterPartitionReassignmentsResponse::parse(readable, version)?;
                Ok(Self::AlterPartitionReassignments(response))
            },
            ApiKeys::LIST_PARTITION_REASSIGNMENTS => {
                let response = ListPartitionReassignmentsResponse::parse(readable, version)?;
                Ok(Self::ListPartitionReassignments(response))
            },
            ApiKeys::DESCRIBE_ACLS => {
                let response = DescribeAclsResponse::parse(readable, version)?;
                Ok(Self::DescribeAcls(response))
            },
            ApiKeys::CREATE_ACLS => {
                let response = CreateAclsResponse::parse(readable, version)?;
                Ok(Self::CreateAcls(response))
            },
            ApiKeys::DELETE_ACLS => {
                let response = DeleteAclsResponse::parse(readable, version)?;
                Ok(Self::DeleteAcls(response))
            },
            ApiKeys::DESCRIBE_CLIENT_QUOTAS => {
                let response = DescribeClientQuotasResponse::parse(readable, version)?;
                Ok(Self::DescribeClientQuotas(response))
            },
            ApiKeys::ALTER_CLIENT_QUOTAS => {
                let response = AlterClientQuotasResponse::parse(readable, version)?;
                Ok(Self::AlterClientQuotas(response))
            },
            ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS => {
                let response = DescribeUserScramCredentialsResponse::parse(readable, version)?;
                Ok(Self::DescribeUserScramCredentials(response))
            },
            ApiKeys::ALTER_USER_SCRAM_CREDENTIALS => {
                let response = AlterUserScramCredentialsResponse::parse(readable, version)?;
                Ok(Self::AlterUserScramCredentials(response))
            },
            ApiKeys::CREATE_DELEGATION_TOKEN => {
                let response = CreateDelegationTokenResponse::parse(readable, version)?;
                Ok(Self::CreateDelegationToken(response))
            },
            ApiKeys::RENEW_DELEGATION_TOKEN => {
                let response = RenewDelegationTokenResponse::parse(readable, version)?;
                Ok(Self::RenewDelegationToken(response))
            },
            ApiKeys::EXPIRE_DELEGATION_TOKEN => {
                let response = ExpireDelegationTokenResponse::parse(readable, version)?;
                Ok(Self::ExpireDelegationToken(response))
            },
            ApiKeys::DESCRIBE_DELEGATION_TOKEN => {
                let response = DescribeDelegationTokenResponse::parse(readable, version)?;
                Ok(Self::DescribeDelegationToken(response))
            },
            ApiKeys::UPDATE_FEATURES => {
                let response = UpdateFeaturesResponse::parse(readable, version)?;
                Ok(Self::UpdateFeatures(response))
            },
            ApiKeys::DESCRIBE_PRODUCERS => {
                let response = DescribeProducersResponse::parse(readable, version)?;
                Ok(Self::DescribeProducers(response))
            },
            ApiKeys::DESCRIBE_TRANSACTIONS => {
                let response = DescribeTransactionsResponse::parse(readable, version)?;
                Ok(Self::DescribeTransactions(response))
            },
            ApiKeys::INIT_PRODUCER_ID => {
                let response = InitProducerIdResponse::parse(readable, version)?;
                Ok(Self::InitProducerId(response))
            },
            ApiKeys::ADD_PARTITIONS_TO_TXN => {
                let response = AddPartitionsToTxnResponse::parse(readable, version)?;
                Ok(Self::AddPartitionsToTxn(response))
            },
            ApiKeys::ADD_OFFSETS_TO_TXN => {
                let response = AddOffsetsToTxnResponse::parse(readable, version)?;
                Ok(Self::AddOffsetsToTxn(response))
            },
            ApiKeys::END_TXN => {
                let response = EndTxnResponse::parse(readable, version)?;
                Ok(Self::EndTxn(response))
            },
            ApiKeys::TXN_OFFSET_COMMIT => {
                let response = TxnOffsetCommitResponse::parse(readable, version)?;
                Ok(Self::TxnOffsetCommit(response))
            },
            ApiKeys::WRITE_TXN_MARKERS => {
                let response = WriteTxnMarkersResponse::parse(readable, version)?;
                Ok(Self::WriteTxnMarkers(response))
            },
            ApiKeys::LIST_TRANSACTIONS => {
                let response = ListTransactionsResponse::parse(readable, version)?;
                Ok(Self::ListTransactions(response))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "ApiKey {api_key} is not currently handled in `parse_response`, the code should be updated to do so."
                ),
            )),
        }
    }
}

impl std::fmt::Display for ConcreteResponse {
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

/// Helper: counts a single error, returning a map with one entry.
pub fn single_error_count(error: Errors) -> HashMap<Errors, i32> {
    let mut map = HashMap::new();
    map.insert(error, 1);
    map
}

/// Helper: increments the count for the given error in the map.
pub fn update_error_counts(error_counts: &mut HashMap<Errors, i32>, error: Errors) {
    *error_counts.entry(error).or_insert(0) += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's `parseResponse` default arm throws an `AssertionError` whose text
    /// interpolates the `ApiKeys` value — the enum constant — and ends with the
    /// "code should be updated" clause (`AbstractResponse.java:295-296`). The
    /// method name is snake_cased per CLAUDE.md §2; the rest is verbatim.
    #[test]
    fn test_parse_response_unhandled_api_key_message() {
        let mut readable = ByteBufferAccessor::from_bytes(Vec::new());
        let error = ConcreteResponse::parse(&ApiKeys::VOTE, &mut readable, 0)
            .expect_err("VOTE is a broker-only api with no client-side parser");
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "ApiKey VOTE is not currently handled in `parse_response`, the code should be updated to do so."
        );
    }
}
