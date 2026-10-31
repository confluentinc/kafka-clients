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
/// Each exported type sits in the namespace of its Java class's package, or, for a .NET-only type
/// (a <c>=</c> row), in that of the type it pairs with, and nests where its Java class nests; every
/// Java class the table names exists in the Kafka sources.
/// </summary>
public sealed class PublicNamespaceLayoutTests
{
    private const string RootNamespace = "Confluent.Kafka";
    private const string JavaPrefix = "org.apache.kafka.";
    private const string ClientsSegment = "clients.";

    private static readonly Type[] s_exported = typeof(KafkaException).Assembly.GetExportedTypes();

    private static IReadOnlyList<(string DotNet, string Java)> JavaCounterparts { get; } = new[]
    {
        ("AbortTransactionOptions", "org.apache.kafka.clients.admin.AbortTransactionOptions"),
        ("AbortTransactionResult", "org.apache.kafka.clients.admin.AbortTransactionResult"),
        ("AbortTransactionSpec", "org.apache.kafka.clients.admin.AbortTransactionSpec"),
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
        ("FeatureUpdate+UpgradeType", "org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType"),
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
        ("UpgradeTypes", "=FeatureUpdate"),
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
        ("KafkaException", "org.apache.kafka.common.KafkaException"),
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
        ("RecordDeserializationException", "org.apache.kafka.common.errors.RecordDeserializationException"),
        ("RecordDeserializationException+DeserializationExceptionOrigin", "org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin"),
        ("SerializationException", "org.apache.kafka.common.errors.SerializationException"),
        ("Header", "org.apache.kafka.common.header.Header"),
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
        ("DelegationToken", "org.apache.kafka.common.security.token.delegation.DelegationToken"),
        ("TokenInformation", "org.apache.kafka.common.security.token.delegation.TokenInformation"),
        ("IDeserializer`1", "org.apache.kafka.common.serialization.Deserializer"),
        ("ISerde`1", "org.apache.kafka.common.serialization.Serde"),
        ("ISerializer`1", "org.apache.kafka.common.serialization.Serializer"),
        ("Serdes", "org.apache.kafka.common.serialization.Serdes"),
        ("AsyncConsumerRebalanceListenerBase", "=IAsyncConsumerRebalanceListener"),
        ("AsyncKafkaConsumer`2", "=KafkaConsumer`2"),
        ("AsyncMockConsumer`2", "=MockConsumer`2"),
        ("CloseOptions", "org.apache.kafka.clients.consumer.CloseOptions"),
        ("ConsumerGroupMetadata", "org.apache.kafka.clients.consumer.ConsumerGroupMetadata"),
        ("ConsumerHandle", "=IConsumer`2"),
        ("ConsumerRebalanceListenerBase", "=IConsumerRebalanceListener"),
        ("ConsumerRecord`2", "org.apache.kafka.clients.consumer.ConsumerRecord"),
        ("ConsumerRecords`2", "org.apache.kafka.clients.consumer.ConsumerRecords"),
        ("GroupMembershipOperation", "org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation"),
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
        ("AsyncKafkaProducer`2", "=KafkaProducer`2"),
        ("AsyncMockProducer`2", "=MockProducer`2"),
        ("IAsyncDeliveryCallback", "=IDeliveryCallback"),
        ("IAsyncProducer`2", "=IProducer`2"),
        ("IDeliveryCallback", "org.apache.kafka.clients.producer.Callback"),
        ("IProducer`2", "org.apache.kafka.clients.producer.Producer"),
        ("KafkaProducer`2", "org.apache.kafka.clients.producer.KafkaProducer"),
        ("MockProducer`2", "org.apache.kafka.clients.producer.MockProducer"),
        ("ProducerRecord`2", "org.apache.kafka.clients.producer.ProducerRecord"),
        ("RecordMetadata", "org.apache.kafka.clients.producer.RecordMetadata"),
    };

    private static IReadOnlyCollection<string> JavaNestedTypesKeptTopLevel { get; } = new[]
    {
        "GroupMembershipOperation", "ConfigResourceType", "AlterConfigOpType",
    };

    public static TheoryData<string, string> AllRows => ToTheoryData(JavaCounterparts);

    public static TheoryData<string, string> JavaBackedRows =>
        ToTheoryData(JavaCounterparts.Where(row => !IsDotNetOnly(row.Java)));

    [Fact]
    public void EveryExportedType_HasARow_AndEveryRowNamesAnExportedType()
    {
        string[] exported = s_exported.Select(NameBelowNamespace).ToArray();
        string[] rows = JavaCounterparts.Select(row => row.DotNet).ToArray();

        var problems = new List<string>();
        AddProblem(problems, "Exported types without a row", exported.Except(rows, StringComparer.Ordinal));
        AddProblem(problems, "Rows naming no exported type", rows.Except(exported, StringComparer.Ordinal));
        AddProblem(problems, "Exported names below two namespaces", Duplicates(exported));
        AddProblem(problems, "Rows listed twice", Duplicates(rows));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void NoPublicType_IsInTheRootNamespace() =>
        Assert.DoesNotContain(s_exported, type => type.Namespace == RootNamespace);

    [Theory]
    [MemberData(nameof(AllRows))]
    public void TheNamespace_IsTheJavaPackages_OrThePairedTypes(string dotnet, string java)
    {
        Type type = Exported(dotnet);

        string expected;
        if (IsDotNetOnly(java))
        {
            string pair = java.Substring(1);
            Assert.True(
                JavaCounterparts.Any(row => row.DotNet == pair),
                $"{dotnet} pairs with {pair}, which has no row.");
            expected = Exported(pair).Namespace!;
        }
        else
        {
            Assert.StartsWith(JavaPrefix, java, StringComparison.Ordinal);
            expected = NamespaceOf(JavaPackage(java));
        }

        Assert.Equal(expected, type.Namespace);
    }

    [Fact]
    public void AJavaNestedType_IsNestedInTheTypeOfItsOuterClass()
    {
        Dictionary<string, string> javaOf = JavaCounterparts.ToDictionary(row => row.DotNet, row => row.Java);
        var problems = new List<string>();

        foreach ((string dotnet, string java) in JavaCounterparts.Where(row => !IsDotNetOnly(row.Java)))
        {
            Type type = Exported(dotnet);
            string? declaring = type.DeclaringType is null ? null : NameBelowNamespace(type.DeclaringType);
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
            if (!javaOf.TryGetValue(kept, out string? java) || IsDotNetOnly(java) || JavaClassLevels(java).Length == 1)
            {
                problems.Add($"{kept} is listed as a Java nested type kept top-level, but its row names none.");
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(JavaBackedRows))]
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

    private static string NameBelowNamespace(Type type) => type.FullName!.Substring(type.Namespace!.Length + 1);

    private static Type Exported(string name)
    {
        Type? type = s_exported.SingleOrDefault(candidate => NameBelowNamespace(candidate) == name);
        Assert.True(type is not null, $"{name} is not an exported type.");
        return type!;
    }

    private static string JavaPackage(string java) =>
        string.Join(".", java.Split('.').TakeWhile(segment => char.IsLower(segment[0])));

    private static string[] JavaClassLevels(string java) =>
        java.Split('.').SkipWhile(segment => char.IsLower(segment[0])).ToArray();

    private static string NamespaceOf(string javaPackage)
    {
        string below = javaPackage.Substring(JavaPrefix.Length);
        if (below.StartsWith(ClientsSegment, StringComparison.Ordinal))
        {
            below = below.Substring(ClientsSegment.Length);
        }

        return RootNamespace + "."
            + string.Join(".", below.Split('.').Select(segment => char.ToUpperInvariant(segment[0]) + segment.Substring(1)));
    }

    private static string KafkaSourceRoot() =>
        typeof(PublicNamespaceLayoutTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(attribute => attribute.Key == "KafkaSourceRoot").Value!;
}
