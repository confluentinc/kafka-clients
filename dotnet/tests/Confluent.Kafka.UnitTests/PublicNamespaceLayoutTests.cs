// Copyright 2026 Confluent Inc.
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

using Confluent.Kafka.Common;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Each exported type sits in the namespace of its Java class's package (a JDK class in the root
/// namespace), or, for a .NET-only type (a <c>=</c> row), in that of the type it pairs with, and nests
/// where its Java class nests; every Java class the table names exists in the Kafka sources, or, for a
/// JDK class, in the error generator's table. The error classes' rows come from the generator.
/// </summary>
public sealed class PublicNamespaceLayoutTests
{
    private const string RootNamespace = "Confluent.Kafka";
    private const string JavaPrefix = "org.apache.kafka.";
    private const string ClientsSegment = "clients.";
    private const string JdkPrefix = "java.";

    private static readonly Type[] s_exported = typeof(KafkaException).Assembly.GetExportedTypes();

    // The JDK exception classes, the only public types in the root namespace, as Python keeps them at
    // confluent_kafka.
    private static readonly string[] s_jdkClasses =
    {
        "ConcurrentModificationException",
        "ExecutionException",
        "IllegalArgumentException",
        "IllegalStateException",
        "IndexOutOfBoundsException",
        "NoSuchElementException",
        "TimeoutException",
        "UnsupportedOperationException",
    };

    private static IReadOnlyList<(string DotNet, string Java)> JavaCounterparts { get; } =
        HandWrittenRows.Concat(ErrorClassRows.Rows).ToArray();

    private static IReadOnlyList<(string DotNet, string Java)> HandWrittenRows => new[]
    {
        ("AbortTransactionOptions", "org.apache.kafka.clients.admin.AbortTransactionOptions"),
        ("AbortTransactionResult", "org.apache.kafka.clients.admin.AbortTransactionResult"),
        ("AbortTransactionSpec", "org.apache.kafka.clients.admin.AbortTransactionSpec"),
        ("AdminClientConfig", "org.apache.kafka.clients.admin.AdminClientConfig"),
        ("AlterClientQuotasOptions", "org.apache.kafka.clients.admin.AlterClientQuotasOptions"),
        ("AlterClientQuotasResult", "org.apache.kafka.clients.admin.AlterClientQuotasResult"),
        ("AlterConfigOp", "org.apache.kafka.clients.admin.AlterConfigOp"),
        ("AlterConfigOpType", "org.apache.kafka.clients.admin.AlterConfigOp.OpType"),
        ("AlterConfigsOptions", "org.apache.kafka.clients.admin.AlterConfigsOptions"),
        ("AlterConfigsResult", "org.apache.kafka.clients.admin.AlterConfigsResult"),
        ("AlterConsumerGroupOffsetsOptions", "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions"),
        ("AlterConsumerGroupOffsetsResult", "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult"),
        ("AlterPartitionReassignmentsOptions", "org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions"),
        ("AlterPartitionReassignmentsResult", "org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult"),
        ("AlterReplicaLogDirsOptions", "org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions"),
        ("AlterReplicaLogDirsResult", "org.apache.kafka.clients.admin.AlterReplicaLogDirsResult"),
        ("AlterUserScramCredentialsOptions", "org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions"),
        ("AlterUserScramCredentialsResult", "org.apache.kafka.clients.admin.AlterUserScramCredentialsResult"),
        ("AsyncAbortTransactionResult", "=AbortTransactionResult"),
        ("AsyncAlterClientQuotasResult", "=AlterClientQuotasResult"),
        ("AsyncAlterConfigsResult", "=AlterConfigsResult"),
        ("AsyncAlterConsumerGroupOffsetsResult", "=AlterConsumerGroupOffsetsResult"),
        ("AsyncAlterPartitionReassignmentsResult", "=AlterPartitionReassignmentsResult"),
        ("AsyncAlterReplicaLogDirsResult", "=AlterReplicaLogDirsResult"),
        ("AsyncAlterUserScramCredentialsResult", "=AlterUserScramCredentialsResult"),
        ("AsyncCreateAclsResult", "=CreateAclsResult"),
        ("AsyncCreateDelegationTokenResult", "=CreateDelegationTokenResult"),
        ("AsyncCreatePartitionsResult", "=CreatePartitionsResult"),
        ("AsyncCreateTopicsResult", "=CreateTopicsResult"),
        ("AsyncDeleteAclsResult", "=DeleteAclsResult"),
        ("AsyncDeleteConsumerGroupOffsetsResult", "=DeleteConsumerGroupOffsetsResult"),
        ("AsyncDeleteConsumerGroupsResult", "=DeleteConsumerGroupsResult"),
        ("AsyncDeleteRecordsResult", "=DeleteRecordsResult"),
        ("AsyncDeleteTopicsResult", "=DeleteTopicsResult"),
        ("AsyncDescribeAclsResult", "=DescribeAclsResult"),
        ("AsyncDescribeClassicGroupsResult", "=DescribeClassicGroupsResult"),
        ("AsyncDescribeClientQuotasResult", "=DescribeClientQuotasResult"),
        ("AsyncDescribeClusterResult", "=DescribeClusterResult"),
        ("AsyncDescribeConfigsResult", "=DescribeConfigsResult"),
        ("AsyncDescribeConsumerGroupsResult", "=DescribeConsumerGroupsResult"),
        ("AsyncDescribeDelegationTokenResult", "=DescribeDelegationTokenResult"),
        ("AsyncDescribeFeaturesResult", "=DescribeFeaturesResult"),
        ("AsyncDescribeLogDirsResult", "=DescribeLogDirsResult"),
        ("AsyncDescribeProducersResult", "=DescribeProducersResult"),
        ("AsyncDescribeReplicaLogDirsResult", "=DescribeReplicaLogDirsResult"),
        ("AsyncDescribeTopicsResult", "=DescribeTopicsResult"),
        ("AsyncDescribeTransactionsResult", "=DescribeTransactionsResult"),
        ("AsyncDescribeUserScramCredentialsResult", "=DescribeUserScramCredentialsResult"),
        ("AsyncElectLeadersResult", "=ElectLeadersResult"),
        ("AsyncExpireDelegationTokenResult", "=ExpireDelegationTokenResult"),
        ("AsyncFenceProducersResult", "=FenceProducersResult"),
        ("AsyncKafkaAdminClient", "=KafkaAdminClient"),
        ("AsyncListConfigResourcesResult", "=ListConfigResourcesResult"),
        ("AsyncListConsumerGroupOffsetsResult", "=ListConsumerGroupOffsetsResult"),
        ("AsyncListGroupsResult", "=ListGroupsResult"),
        ("AsyncListOffsetsResult", "=ListOffsetsResult"),
        ("AsyncListPartitionReassignmentsResult", "=ListPartitionReassignmentsResult"),
        ("AsyncListTopicsResult", "=ListTopicsResult"),
        ("AsyncListTransactionsResult", "=ListTransactionsResult"),
        ("AsyncMockAdminClient", "=MockAdminClient"),
        ("AsyncMockAdminClient+Builder", "=MockAdminClient+Builder"),
        ("AsyncRemoveMembersFromConsumerGroupResult", "=RemoveMembersFromConsumerGroupResult"),
        ("AsyncRenewDelegationTokenResult", "=RenewDelegationTokenResult"),
        ("AsyncTerminateTransactionResult", "=TerminateTransactionResult"),
        ("AsyncUnregisterControllerResult", "=UnregisterControllerResult"),
        ("AsyncUpdateFeaturesResult", "=UpdateFeaturesResult"),
        ("ClassicGroupDescription", "org.apache.kafka.clients.admin.ClassicGroupDescription"),
        ("Config", "org.apache.kafka.clients.admin.Config"),
        ("ConfigEntry", "org.apache.kafka.clients.admin.ConfigEntry"),
        ("ConfigEntry+ConfigSource", "org.apache.kafka.clients.admin.ConfigEntry.ConfigSource"),
        ("ConfigEntry+ConfigSynonym", "org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym"),
        ("ConfigEntry+ConfigType", "org.apache.kafka.clients.admin.ConfigEntry.ConfigType"),
        ("ConsumerGroupDescription", "org.apache.kafka.clients.admin.ConsumerGroupDescription"),
        ("CreateAclsOptions", "org.apache.kafka.clients.admin.CreateAclsOptions"),
        ("CreateAclsResult", "org.apache.kafka.clients.admin.CreateAclsResult"),
        ("CreateDelegationTokenOptions", "org.apache.kafka.clients.admin.CreateDelegationTokenOptions"),
        ("CreateDelegationTokenResult", "org.apache.kafka.clients.admin.CreateDelegationTokenResult"),
        ("CreatePartitionsOptions", "org.apache.kafka.clients.admin.CreatePartitionsOptions"),
        ("CreatePartitionsResult", "org.apache.kafka.clients.admin.CreatePartitionsResult"),
        ("CreateTopicsOptions", "org.apache.kafka.clients.admin.CreateTopicsOptions"),
        ("CreateTopicsResult", "org.apache.kafka.clients.admin.CreateTopicsResult"),
        ("CreateTopicsResult+TopicMetadataAndConfig", "org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig"),
        ("DeleteAclsOptions", "org.apache.kafka.clients.admin.DeleteAclsOptions"),
        ("DeleteAclsResult", "org.apache.kafka.clients.admin.DeleteAclsResult"),
        ("DeleteAclsResult+FilterResult", "org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult"),
        ("DeleteAclsResult+FilterResults", "org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults"),
        ("DeleteConsumerGroupOffsetsOptions", "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions"),
        ("DeleteConsumerGroupOffsetsResult", "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult"),
        ("DeleteConsumerGroupsOptions", "org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions"),
        ("DeleteConsumerGroupsResult", "org.apache.kafka.clients.admin.DeleteConsumerGroupsResult"),
        ("DeleteRecordsOptions", "org.apache.kafka.clients.admin.DeleteRecordsOptions"),
        ("DeleteRecordsResult", "org.apache.kafka.clients.admin.DeleteRecordsResult"),
        ("DeleteTopicsOptions", "org.apache.kafka.clients.admin.DeleteTopicsOptions"),
        ("DeleteTopicsResult", "org.apache.kafka.clients.admin.DeleteTopicsResult"),
        ("DeletedRecords", "org.apache.kafka.clients.admin.DeletedRecords"),
        ("DescribeAclsOptions", "org.apache.kafka.clients.admin.DescribeAclsOptions"),
        ("DescribeAclsResult", "org.apache.kafka.clients.admin.DescribeAclsResult"),
        ("DescribeClassicGroupsOptions", "org.apache.kafka.clients.admin.DescribeClassicGroupsOptions"),
        ("DescribeClassicGroupsResult", "org.apache.kafka.clients.admin.DescribeClassicGroupsResult"),
        ("DescribeClientQuotasOptions", "org.apache.kafka.clients.admin.DescribeClientQuotasOptions"),
        ("DescribeClientQuotasResult", "org.apache.kafka.clients.admin.DescribeClientQuotasResult"),
        ("DescribeClusterOptions", "org.apache.kafka.clients.admin.DescribeClusterOptions"),
        ("DescribeClusterResult", "org.apache.kafka.clients.admin.DescribeClusterResult"),
        ("DescribeConfigsOptions", "org.apache.kafka.clients.admin.DescribeConfigsOptions"),
        ("DescribeConfigsResult", "org.apache.kafka.clients.admin.DescribeConfigsResult"),
        ("DescribeConsumerGroupsOptions", "org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions"),
        ("DescribeConsumerGroupsResult", "org.apache.kafka.clients.admin.DescribeConsumerGroupsResult"),
        ("DescribeDelegationTokenOptions", "org.apache.kafka.clients.admin.DescribeDelegationTokenOptions"),
        ("DescribeDelegationTokenResult", "org.apache.kafka.clients.admin.DescribeDelegationTokenResult"),
        ("DescribeFeaturesOptions", "org.apache.kafka.clients.admin.DescribeFeaturesOptions"),
        ("DescribeFeaturesResult", "org.apache.kafka.clients.admin.DescribeFeaturesResult"),
        ("DescribeLogDirsOptions", "org.apache.kafka.clients.admin.DescribeLogDirsOptions"),
        ("DescribeLogDirsResult", "org.apache.kafka.clients.admin.DescribeLogDirsResult"),
        ("DescribeProducersOptions", "org.apache.kafka.clients.admin.DescribeProducersOptions"),
        ("DescribeProducersResult", "org.apache.kafka.clients.admin.DescribeProducersResult"),
        ("DescribeProducersResult+PartitionProducerState", "org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState"),
        ("DescribeReplicaLogDirsOptions", "org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions"),
        ("DescribeReplicaLogDirsResult", "org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult"),
        ("DescribeReplicaLogDirsResult+ReplicaLogDirInfo", "org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo"),
        ("DescribeTopicsOptions", "org.apache.kafka.clients.admin.DescribeTopicsOptions"),
        ("DescribeTopicsResult", "org.apache.kafka.clients.admin.DescribeTopicsResult"),
        ("DescribeTransactionsOptions", "org.apache.kafka.clients.admin.DescribeTransactionsOptions"),
        ("DescribeTransactionsResult", "org.apache.kafka.clients.admin.DescribeTransactionsResult"),
        ("DescribeUserScramCredentialsOptions", "org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions"),
        ("DescribeUserScramCredentialsResult", "org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult"),
        ("ElectLeadersOptions", "org.apache.kafka.clients.admin.ElectLeadersOptions"),
        ("ElectLeadersResult", "org.apache.kafka.clients.admin.ElectLeadersResult"),
        ("ExpireDelegationTokenOptions", "org.apache.kafka.clients.admin.ExpireDelegationTokenOptions"),
        ("ExpireDelegationTokenResult", "org.apache.kafka.clients.admin.ExpireDelegationTokenResult"),
        ("FeatureMetadata", "org.apache.kafka.clients.admin.FeatureMetadata"),
        ("FeatureUpdate", "org.apache.kafka.clients.admin.FeatureUpdate"),
        ("FenceProducersOptions", "org.apache.kafka.clients.admin.FenceProducersOptions"),
        ("FenceProducersResult", "org.apache.kafka.clients.admin.FenceProducersResult"),
        ("FinalizedVersionRange", "org.apache.kafka.clients.admin.FinalizedVersionRange"),
        ("GroupListing", "org.apache.kafka.clients.admin.GroupListing"),
        ("IAdmin", "org.apache.kafka.clients.admin.Admin"),
        ("IAsyncAdmin", "=IAdmin"),
        ("KafkaAdminClient", "org.apache.kafka.clients.admin.KafkaAdminClient"),
        ("ListConfigResourcesOptions", "org.apache.kafka.clients.admin.ListConfigResourcesOptions"),
        ("ListConfigResourcesResult", "org.apache.kafka.clients.admin.ListConfigResourcesResult"),
        ("ListConsumerGroupOffsetsOptions", "org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions"),
        ("ListConsumerGroupOffsetsResult", "org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult"),
        ("ListConsumerGroupOffsetsSpec", "org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec"),
        ("ListGroupsOptions", "org.apache.kafka.clients.admin.ListGroupsOptions"),
        ("ListGroupsResult", "org.apache.kafka.clients.admin.ListGroupsResult"),
        ("ListOffsetsOptions", "org.apache.kafka.clients.admin.ListOffsetsOptions"),
        ("ListOffsetsResult", "org.apache.kafka.clients.admin.ListOffsetsResult"),
        ("ListOffsetsResult+ListOffsetsResultInfo", "org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo"),
        ("ListPartitionReassignmentsOptions", "org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions"),
        ("ListPartitionReassignmentsResult", "org.apache.kafka.clients.admin.ListPartitionReassignmentsResult"),
        ("ListTopicsOptions", "org.apache.kafka.clients.admin.ListTopicsOptions"),
        ("ListTopicsResult", "org.apache.kafka.clients.admin.ListTopicsResult"),
        ("ListTransactionsOptions", "org.apache.kafka.clients.admin.ListTransactionsOptions"),
        ("ListTransactionsResult", "org.apache.kafka.clients.admin.ListTransactionsResult"),
        ("LogDirDescription", "org.apache.kafka.clients.admin.LogDirDescription"),
        ("MemberAssignment", "org.apache.kafka.clients.admin.MemberAssignment"),
        ("MemberDescription", "org.apache.kafka.clients.admin.MemberDescription"),
        ("MemberToRemove", "org.apache.kafka.clients.admin.MemberToRemove"),
        ("MockAdminClient", "org.apache.kafka.clients.admin.MockAdminClient"),
        ("MockAdminClient+Builder", "org.apache.kafka.clients.admin.MockAdminClient.Builder"),
        ("NewPartitionReassignment", "org.apache.kafka.clients.admin.NewPartitionReassignment"),
        ("NewPartitions", "org.apache.kafka.clients.admin.NewPartitions"),
        ("NewTopic", "org.apache.kafka.clients.admin.NewTopic"),
        ("OffsetSpec", "org.apache.kafka.clients.admin.OffsetSpec"),
        ("OffsetSpec+EarliestLocalSpec", "org.apache.kafka.clients.admin.OffsetSpec.EarliestLocalSpec"),
        ("OffsetSpec+EarliestPendingUploadSpec", "org.apache.kafka.clients.admin.OffsetSpec.EarliestPendingUploadSpec"),
        ("OffsetSpec+EarliestSpec", "org.apache.kafka.clients.admin.OffsetSpec.EarliestSpec"),
        ("OffsetSpec+LatestSpec", "org.apache.kafka.clients.admin.OffsetSpec.LatestSpec"),
        ("OffsetSpec+LatestTieredSpec", "org.apache.kafka.clients.admin.OffsetSpec.LatestTieredSpec"),
        ("OffsetSpec+MaxTimestampSpec", "org.apache.kafka.clients.admin.OffsetSpec.MaxTimestampSpec"),
        ("OffsetSpec+TimestampSpec", "org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec"),
        ("PartitionReassignment", "org.apache.kafka.clients.admin.PartitionReassignment"),
        ("ProducerState", "org.apache.kafka.clients.admin.ProducerState"),
        ("RecordsToDelete", "org.apache.kafka.clients.admin.RecordsToDelete"),
        ("RemoveMembersFromConsumerGroupOptions", "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions"),
        ("RemoveMembersFromConsumerGroupResult", "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult"),
        ("RenewDelegationTokenOptions", "org.apache.kafka.clients.admin.RenewDelegationTokenOptions"),
        ("RenewDelegationTokenResult", "org.apache.kafka.clients.admin.RenewDelegationTokenResult"),
        ("ReplicaInfo", "org.apache.kafka.clients.admin.ReplicaInfo"),
        ("ScramCredentialInfo", "org.apache.kafka.clients.admin.ScramCredentialInfo"),
        ("ScramMechanism", "org.apache.kafka.clients.admin.ScramMechanism"),
        ("ScramMechanisms", "=ScramMechanism"),
        ("SupportedVersionRange", "org.apache.kafka.clients.admin.SupportedVersionRange"),
        ("TerminateTransactionOptions", "org.apache.kafka.clients.admin.TerminateTransactionOptions"),
        ("TerminateTransactionResult", "org.apache.kafka.clients.admin.TerminateTransactionResult"),
        ("TopicDescription", "org.apache.kafka.clients.admin.TopicDescription"),
        ("TopicListing", "org.apache.kafka.clients.admin.TopicListing"),
        ("TransactionDescription", "org.apache.kafka.clients.admin.TransactionDescription"),
        ("TransactionListing", "org.apache.kafka.clients.admin.TransactionListing"),
        ("TransactionState", "org.apache.kafka.clients.admin.TransactionState"),
        ("UnregisterControllerOptions", "org.apache.kafka.clients.admin.UnregisterControllerOptions"),
        ("UnregisterControllerResult", "org.apache.kafka.clients.admin.UnregisterControllerResult"),
        ("UpdateFeaturesOptions", "org.apache.kafka.clients.admin.UpdateFeaturesOptions"),
        ("UpdateFeaturesResult", "org.apache.kafka.clients.admin.UpdateFeaturesResult"),
        ("UpgradeType", "org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType"),
        ("UpgradeTypes", "=UpgradeType"),
        ("UserScramCredentialAlteration", "org.apache.kafka.clients.admin.UserScramCredentialAlteration"),
        ("UserScramCredentialDeletion", "org.apache.kafka.clients.admin.UserScramCredentialDeletion"),
        ("UserScramCredentialUpsertion", "org.apache.kafka.clients.admin.UserScramCredentialUpsertion"),
        ("UserScramCredentialsDescription", "org.apache.kafka.clients.admin.UserScramCredentialsDescription"),
        ("AsyncKafkaFuture", "=KafkaFuture"),
        ("AsyncKafkaFuture`1", "=KafkaFuture`1"),
        ("ClassicGroupState", "org.apache.kafka.common.ClassicGroupState"),
        ("ElectionType", "org.apache.kafka.common.ElectionType"),
        ("GroupState", "org.apache.kafka.common.GroupState"),
        ("GroupStateExtensions", "=GroupState"),
        ("GroupType", "org.apache.kafka.common.GroupType"),
        ("IMetric", "org.apache.kafka.common.Metric"),
        ("IsolationLevel", "org.apache.kafka.common.IsolationLevel"),
        ("KafkaFuture", "org.apache.kafka.common.KafkaFuture"),
        ("KafkaFuture+BaseFunction`2", "org.apache.kafka.common.KafkaFuture.BaseFunction"),
        ("KafkaFuture+BiConsumer`2", "org.apache.kafka.common.KafkaFuture.BiConsumer"),
        ("KafkaFuture`1", "org.apache.kafka.common.KafkaFuture"),
        ("MetricName", "org.apache.kafka.common.MetricName"),
        ("Node", "org.apache.kafka.common.Node"),
        ("PartitionInfo", "org.apache.kafka.common.PartitionInfo"),
        ("TopicCollection", "org.apache.kafka.common.TopicCollection"),
        ("TopicCollection+TopicIdCollection", "org.apache.kafka.common.TopicCollection.TopicIdCollection"),
        ("TopicCollection+TopicNameCollection", "org.apache.kafka.common.TopicCollection.TopicNameCollection"),
        ("TopicPartition", "org.apache.kafka.common.TopicPartition"),
        ("TopicPartitionInfo", "org.apache.kafka.common.TopicPartitionInfo"),
        ("TopicPartitionReplica", "org.apache.kafka.common.TopicPartitionReplica"),
        ("Uuid", "org.apache.kafka.common.Uuid"),
        ("AccessControlEntry", "org.apache.kafka.common.acl.AccessControlEntry"),
        ("AccessControlEntryFilter", "org.apache.kafka.common.acl.AccessControlEntryFilter"),
        ("AclBinding", "org.apache.kafka.common.acl.AclBinding"),
        ("AclBindingFilter", "org.apache.kafka.common.acl.AclBindingFilter"),
        ("AclOperation", "org.apache.kafka.common.acl.AclOperation"),
        ("AclPermissionType", "org.apache.kafka.common.acl.AclPermissionType"),
        ("ConfigResource", "org.apache.kafka.common.config.ConfigResource"),
        ("ConfigResourceType", "org.apache.kafka.common.config.ConfigResource.Type"),
        ("LogLevelConfig", "org.apache.kafka.common.config.LogLevelConfig"),
        ("SaslConfigs", "org.apache.kafka.common.config.SaslConfigs"),
        ("SecurityConfig", "org.apache.kafka.common.config.SecurityConfig"),
        ("SslClientAuth", "org.apache.kafka.common.config.SslClientAuth"),
        ("SslClientAuths", "=SslClientAuth"),
        ("SslConfigs", "org.apache.kafka.common.config.SslConfigs"),
        ("TopicConfig", "org.apache.kafka.common.config.TopicConfig"),
        ("RecordDeserializationException+DeserializationExceptionOrigin", "org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin"),
        ("Header", "org.apache.kafka.common.header.Header"),
        ("KafkaMetric", "org.apache.kafka.common.metrics.KafkaMetric"),
        ("Headers", "org.apache.kafka.common.header.Headers"),
        ("ClientQuotaAlteration", "org.apache.kafka.common.quota.ClientQuotaAlteration"),
        ("ClientQuotaAlteration+Op", "org.apache.kafka.common.quota.ClientQuotaAlteration.Op"),
        ("ClientQuotaEntity", "org.apache.kafka.common.quota.ClientQuotaEntity"),
        ("ClientQuotaFilter", "org.apache.kafka.common.quota.ClientQuotaFilter"),
        ("ClientQuotaFilterComponent", "org.apache.kafka.common.quota.ClientQuotaFilterComponent"),
        ("ClientQuotaMatchType", "=ClientQuotaFilterComponent"),
        ("TimestampType", "org.apache.kafka.common.record.TimestampType"),
        ("PatternType", "org.apache.kafka.common.resource.PatternType"),
        ("ResourcePattern", "org.apache.kafka.common.resource.ResourcePattern"),
        ("ResourcePatternFilter", "org.apache.kafka.common.resource.ResourcePatternFilter"),
        ("ResourceType", "org.apache.kafka.common.resource.ResourceType"),
        ("KafkaPrincipal", "org.apache.kafka.common.security.auth.KafkaPrincipal"),
        ("SecurityProtocol", "org.apache.kafka.common.security.auth.SecurityProtocol"),
        ("SecurityProtocols", "=SecurityProtocol"),
        ("DelegationToken", "org.apache.kafka.common.security.token.delegation.DelegationToken"),
        ("TokenInformation", "org.apache.kafka.common.security.token.delegation.TokenInformation"),
        ("BooleanDeserializer", "org.apache.kafka.common.serialization.BooleanDeserializer"),
        ("BooleanSerializer", "org.apache.kafka.common.serialization.BooleanSerializer"),
        ("ByteArrayDeserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
        ("ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
        ("ByteBufferDeserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer"),
        ("ByteBufferSerializer", "org.apache.kafka.common.serialization.ByteBufferSerializer"),
        ("DeserializerExtensions", "=IDeserializer`1"),
        ("DoubleDeserializer", "org.apache.kafka.common.serialization.DoubleDeserializer"),
        ("DoubleSerializer", "org.apache.kafka.common.serialization.DoubleSerializer"),
        ("FloatDeserializer", "org.apache.kafka.common.serialization.FloatDeserializer"),
        ("FloatSerializer", "org.apache.kafka.common.serialization.FloatSerializer"),
        ("IDeserializer`1", "org.apache.kafka.common.serialization.Deserializer"),
        ("IDeserializerWithConfigure`1", "=IDeserializer`1"),
        ("IDeserializerWithHeaders`1", "=IDeserializer`1"),
        ("ISerde`1", "org.apache.kafka.common.serialization.Serde"),
        ("ISerdeWithConfigure`1", "=ISerde`1"),
        ("ISerializer`1", "org.apache.kafka.common.serialization.Serializer"),
        ("ISerializerWithConfigure`1", "=ISerializer`1"),
        ("ISerializerWithHeaders`1", "=ISerializer`1"),
        ("IntegerDeserializer", "org.apache.kafka.common.serialization.IntegerDeserializer"),
        ("IntegerSerializer", "org.apache.kafka.common.serialization.IntegerSerializer"),
        ("LongDeserializer", "org.apache.kafka.common.serialization.LongDeserializer"),
        ("LongSerializer", "org.apache.kafka.common.serialization.LongSerializer"),
        ("SerdeExtensions", "=ISerde`1"),
        ("Serdes", "org.apache.kafka.common.serialization.Serdes"),
        ("Serdes+BooleanSerde", "org.apache.kafka.common.serialization.Serdes.BooleanSerde"),
        ("Serdes+ByteArraySerde", "org.apache.kafka.common.serialization.Serdes.ByteArraySerde"),
        ("Serdes+ByteBufferSerde", "org.apache.kafka.common.serialization.Serdes.ByteBufferSerde"),
        ("Serdes+DoubleSerde", "org.apache.kafka.common.serialization.Serdes.DoubleSerde"),
        ("Serdes+FloatSerde", "org.apache.kafka.common.serialization.Serdes.FloatSerde"),
        ("Serdes+IntegerSerde", "org.apache.kafka.common.serialization.Serdes.IntegerSerde"),
        ("Serdes+LongSerde", "org.apache.kafka.common.serialization.Serdes.LongSerde"),
        ("Serdes+ShortSerde", "org.apache.kafka.common.serialization.Serdes.ShortSerde"),
        ("Serdes+StringSerde", "org.apache.kafka.common.serialization.Serdes.StringSerde"),
        ("Serdes+UUIDSerde", "org.apache.kafka.common.serialization.Serdes.UUIDSerde"),
        ("Serdes+VoidSerde", "org.apache.kafka.common.serialization.Serdes.VoidSerde"),
        ("Serdes+WrapperSerde`1", "org.apache.kafka.common.serialization.Serdes.WrapperSerde"),
        ("SerializerExtensions", "=ISerializer`1"),
        ("ShortDeserializer", "org.apache.kafka.common.serialization.ShortDeserializer"),
        ("ShortSerializer", "org.apache.kafka.common.serialization.ShortSerializer"),
        ("StringDeserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
        ("StringSerializer", "org.apache.kafka.common.serialization.StringSerializer"),
        ("UUIDDeserializer", "org.apache.kafka.common.serialization.UUIDDeserializer"),
        ("UUIDSerializer", "org.apache.kafka.common.serialization.UUIDSerializer"),
        ("VoidDeserializer", "org.apache.kafka.common.serialization.VoidDeserializer"),
        ("VoidSerializer", "org.apache.kafka.common.serialization.VoidSerializer"),
        ("AsyncConsumerRebalanceListenerBase", "=IAsyncConsumerRebalanceListener"),
        ("AsyncKafkaConsumer`2", "=KafkaConsumer`2"),
        ("AsyncMockConsumer`2", "=MockConsumer`2"),
        ("CloseOptions", "org.apache.kafka.clients.consumer.CloseOptions"),
        ("ConsumerConfig", "org.apache.kafka.clients.consumer.ConsumerConfig"),
        ("ConsumerGroupMetadata", "org.apache.kafka.clients.consumer.ConsumerGroupMetadata"),
        ("ConsumerHandle", "=IConsumer`2"),
        ("ConsumerRebalanceListenerBase", "=IConsumerRebalanceListener"),
        ("ConsumerRecord`2", "org.apache.kafka.clients.consumer.ConsumerRecord"),
        ("ConsumerRecords`2", "org.apache.kafka.clients.consumer.ConsumerRecords"),
        ("GroupMembershipOperation", "org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation"),
        ("GroupProtocol", "org.apache.kafka.clients.consumer.GroupProtocol"),
        ("GroupProtocols", "=GroupProtocol"),
        ("IAsyncConsumerRebalanceListener", "=IConsumerRebalanceListener"),
        ("IAsyncConsumer`2", "=IConsumer`2"),
        ("IAsyncOffsetCommitCallback", "=IOffsetCommitCallback"),
        ("IConsumerCommon", "=IConsumer`2"),
        ("IConsumerRebalanceListener", "org.apache.kafka.clients.consumer.ConsumerRebalanceListener"),
        ("IConsumer`2", "org.apache.kafka.clients.consumer.Consumer"),
        ("IOffsetCommitCallback", "org.apache.kafka.clients.consumer.OffsetCommitCallback"),
        ("KafkaConsumer`2", "org.apache.kafka.clients.consumer.KafkaConsumer"),
        ("MockConsumer`2", "org.apache.kafka.clients.consumer.MockConsumer"),
        ("OffsetAndMetadata", "org.apache.kafka.clients.consumer.OffsetAndMetadata"),
        ("OffsetAndTimestamp", "org.apache.kafka.clients.consumer.OffsetAndTimestamp"),
        ("SubscriptionPattern", "org.apache.kafka.clients.consumer.SubscriptionPattern"),
        ("AsyncKafkaProducer`2", "=KafkaProducer`2"),
        ("AsyncMockProducer`2", "=MockProducer`2"),
        ("IAsyncCallback", "=ICallback"),
        ("IAsyncProducer`2", "=IProducer`2"),
        ("ICallback", "org.apache.kafka.clients.producer.Callback"),
        ("IProducer`2", "org.apache.kafka.clients.producer.Producer"),
        ("KafkaProducer`2", "org.apache.kafka.clients.producer.KafkaProducer"),
        ("MockProducer`2", "org.apache.kafka.clients.producer.MockProducer"),
        ("ProducerConfig", "org.apache.kafka.clients.producer.ProducerConfig"),
        ("ProducerRecord`2", "org.apache.kafka.clients.producer.ProducerRecord"),
        ("RecordMetadata", "org.apache.kafka.clients.producer.RecordMetadata"),
    };

    private static IReadOnlyCollection<string> JavaNestedTypesKeptTopLevel { get; } = new[]
    {
        "GroupMembershipOperation", "ConfigResourceType", "AlterConfigOpType", "UpgradeType",
    };

    public static TheoryData<string, string> AllRows => ToTheoryData(JavaCounterparts);

    public static TheoryData<string, string> KafkaBackedRows =>
        ToTheoryData(JavaCounterparts.Where(row => !IsDotNetOnly(row.Java) && !IsJdk(row.Java)));

    public static TheoryData<string, string> JdkBackedRows => ToTheoryData(JavaCounterparts.Where(row => IsJdk(row.Java)));

    [Fact]
    public void EveryExportedType_HasARow_AndEveryRowNamesAnExportedType()
    {
        string[] exported = s_exported.Select(type => type.FullName!).ToArray();
        string[] rows = JavaCounterparts.Select(ExpectedFullName).ToArray();

        var problems = new List<string>();
        AddProblem(problems, "Exported types without a row", exported.Except(rows, StringComparer.Ordinal));
        AddProblem(problems, "Rows naming no exported type", rows.Except(exported, StringComparer.Ordinal));
        AddProblem(problems, "Rows listed twice", Duplicates(rows));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void NoPublicType_IsInTheRootNamespace()
    {
        string[] root = s_exported
            .Where(type => type.Namespace == RootNamespace)
            .Select(type => type.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(s_jdkClasses.OrderBy(name => name, StringComparer.Ordinal), root);
    }

    [Theory]
    [MemberData(nameof(AllRows))]
    public void TheNamespace_IsTheJavaPackages_OrThePairedTypes(string dotnet, string java)
    {
        if (IsDotNetOnly(java))
        {
            string pair = java.Substring(1);
            Assert.True(
                JavaCounterparts.Count(row => row.DotNet == pair) == 1,
                $"{dotnet} pairs with {pair}, which has no row or several.");
        }
        else
        {
            Assert.True(
                java.StartsWith(JavaPrefix, StringComparison.Ordinal) || IsJdk(java),
                $"{dotnet}: {java} is neither a Kafka class nor a JDK class.");
        }

        string expected = ExpectedFullName((dotnet, java));
        Type type = Exported(expected);
        Assert.Equal(expected.Substring(0, expected.Length - dotnet.Length - 1), type.Namespace);
    }

    [Fact]
    public void AJavaNestedType_IsNestedInTheTypeOfItsOuterClass()
    {
        Dictionary<string, string> javaOf = JavaCounterparts.ToDictionary(ExpectedFullName, row => row.Java);
        var problems = new List<string>();

        foreach ((string dotnet, string java) in JavaCounterparts.Where(row => !IsDotNetOnly(row.Java)))
        {
            Type type = Exported(ExpectedFullName((dotnet, java)));
            string? declaring = type.DeclaringType?.FullName;
            bool javaNested = JavaClassLevels(java).Length > 1;

            if (!javaNested || JavaNestedTypesKeptTopLevel.Contains(dotnet))
            {
                if (declaring is not null)
                {
                    problems.Add($"{dotnet} is nested in {declaring}, but {java} is not nested in .NET.");
                }

                continue;
            }

            string outer = java.Substring(0, java.LastIndexOf('.'));
            if (declaring is null || javaOf[declaring] != outer)
            {
                problems.Add($"{dotnet} ({java}) is nested in {declaring ?? "nothing"}, not in the type of {outer}.");
            }
        }

        foreach (string kept in JavaNestedTypesKeptTopLevel)
        {
            string? java = JavaCounterparts.Where(row => row.DotNet == kept).Select(row => row.Java).FirstOrDefault();
            if (java is null || IsDotNetOnly(java) || JavaClassLevels(java).Length == 1)
            {
                problems.Add($"{kept} is listed as a Java nested type kept top-level, but its row names none.");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(KafkaBackedRows))]
    public void TheJavaClass_ExistsInTheKafkaSources(string dotnet, string java)
    {
        string root = KafkaSourceRoot();
        if (!Directory.Exists(Path.Combine(root, "main", "java")))
        {
            Assert.Fail($"The Kafka sources are not at {root}; run `git submodule update --init kafka`.");
        }

        string[] classes = JavaClassLevels(java);
        string relative = Path.Combine(JavaPackage(java).Split('.').Concat(new[] { classes[0] + ".java" }).ToArray());
        string? file = new[] { "main", "testFixtures" }
            .Select(sourceSet => Path.Combine(root, sourceSet, "java", relative))
            .FirstOrDefault(File.Exists);
        Assert.True(file is not null, $"{dotnet}: {java} has no {relative} under {root}/{{main,testFixtures}}/java.");

        string source = File.ReadAllText(file!);
        foreach (string name in classes)
        {
            Assert.True(
                Regex.IsMatch(source, $@"\b(?:class|interface|enum|record)\s+{name}\b"),
                $"{dotnet}: {file} declares no class, interface, enum or record {name} ({java}).");
        }
    }

    [Theory]
    [MemberData(nameof(JdkBackedRows))]
    public void TheJdkClass_IsInTheErrorGeneratorsTable(string dotnet, string java)
    {
        string generator = Path.Combine(KafkaSourceRoot(), "..", "..", "..", "rust", "xtask", "src", "error_hierarchy.rs");
        Assert.True(File.Exists(generator), $"The error generator is not at {generator}.");
        Assert.True(
            File.ReadAllText(generator).Contains($"\"{java}\"", StringComparison.Ordinal),
            $"{dotnet}: {java} is not in the JDK table of {generator}.");
    }

    private static TheoryData<string, string> ToTheoryData(IEnumerable<(string DotNet, string Java)> rows)
    {
        var data = new TheoryData<string, string>();
        foreach ((string dotnet, string java) in rows)
        {
            data.Add(dotnet, java);
        }

        return data;
    }

    private static void AddProblem(List<string> problems, string what, IEnumerable<string> names)
    {
        string[] sorted = names.OrderBy(name => name, StringComparer.Ordinal).ToArray();
        if (sorted.Length > 0)
        {
            problems.Add($"{what}: {string.Join(", ", sorted)}.");
        }
    }

    private static IEnumerable<string> Duplicates(IEnumerable<string> names) =>
        names.GroupBy(name => name, StringComparer.Ordinal).Where(group => group.Count() > 1).Select(group => group.Key);

    private static bool IsDotNetOnly(string java) => java.StartsWith("=", StringComparison.Ordinal);

    private static bool IsJdk(string java) => java.StartsWith(JdkPrefix, StringComparison.Ordinal);

    private static string ExpectedFullName((string DotNet, string Java) row)
    {
        if (!IsDotNetOnly(row.Java))
        {
            return NamespaceOf(JavaPackage(row.Java)) + "." + row.DotNet;
        }

        string pair = row.Java.Substring(1);
        (string DotNet, string Java)[] paired = JavaCounterparts.Where(other => other.DotNet == pair).ToArray();
        if (paired.Length != 1)
        {
            return "(no single row " + pair + ")." + row.DotNet;
        }

        string pairName = ExpectedFullName(paired[0]);
        return pairName.Substring(0, pairName.Length - pair.Length) + row.DotNet;
    }

    internal static Type Exported(string fullName)
    {
        Type? type = s_exported.SingleOrDefault(candidate => candidate.FullName == fullName);
        Assert.True(type is not null, $"{fullName} is not an exported type.");
        return type!;
    }

    internal static string JavaPackage(string java) =>
        string.Join(".", java.Split('.').TakeWhile(segment => char.IsLower(segment[0])));

    private static string[] JavaClassLevels(string java) =>
        java.Split('.').SkipWhile(segment => char.IsLower(segment[0])).ToArray();

    internal static string NamespaceOf(string javaPackage)
    {
        if (javaPackage.StartsWith(JdkPrefix, StringComparison.Ordinal))
        {
            return RootNamespace;
        }

        string below = javaPackage.Substring(JavaPrefix.Length);
        if (below.StartsWith(ClientsSegment, StringComparison.Ordinal))
        {
            below = below.Substring(ClientsSegment.Length);
        }

        return RootNamespace + "."
            + string.Join(".", below.Split('.').Select(segment => char.ToUpperInvariant(segment[0]) + segment.Substring(1)));
    }

    internal static string KafkaSourceRoot() =>
        typeof(PublicNamespaceLayoutTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(attribute => attribute.Key == "KafkaSourceRoot").Value!;
}
