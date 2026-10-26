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

using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

internal static partial class NativeMethods
{
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_topics_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListTopicsWithOptions(SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_names_to_listings_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTopicsResultNamesToListings(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTopicsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTopicsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTopicsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_set_list_internal_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTopicsOptionsSetListInternal(IntPtr self, sbyte listInternal, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTopicsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_cluster_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeClusterWithOptions(SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_nodes_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultNodes(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_controller_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultController(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_cluster_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultClusterId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterResultAuthorizedOperations(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClusterOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_set_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterOptionsSetIncludeAuthorizedOperations(
        IntPtr self, sbyte includeAuthorizedOperations, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_set_include_fenced_brokers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterOptionsSetIncludeFencedBrokers(IntPtr self, sbyte includeFencedBrokers, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClusterOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicListingName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_topic_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicListingTopicId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicListing_is_internal_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte TopicListingIsInternal(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Uuid_most_significant_bits_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long UuidMostSignificantBits(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Uuid_least_significant_bits_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long UuidLeastSignificantBits(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Uuid_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UuidDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_create_topics_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminCreateTopicsWithOptions(SafeAdminClientHandle self, IntPtr newTopics, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_config_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultConfig(IntPtr self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_topic_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultTopicId(IntPtr self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_num_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultNumPartitions(IntPtr self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_replication_factor_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsResultReplicationFactor(IntPtr self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreateTopicsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_set_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsOptionsSetValidateOnly(IntPtr self, sbyte validateOnly, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_set_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsOptionsSetRetryOnQuotaViolation(IntPtr self, sbyte retryOnQuotaViolation, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreateTopicsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreateTopicsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_with_num_partitions_replication_factor_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewTopicWithNumPartitionsReplicationFactor(IntPtr name, int numPartitions, short replicationFactor, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_with_replicas_assignments_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewTopicWithReplicasAssignments(IntPtr name, IntPtr replicasAssignments, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_set_configs_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewTopicSetConfigs(IntPtr self, IntPtr configs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewTopic_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewTopicDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Config_entries_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntries(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_source_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySource(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_is_sensitive_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ConfigEntryIsSensitive(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_is_read_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ConfigEntryIsReadOnly(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_synonyms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntrySynonyms(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_documentation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigEntryDocumentation(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSynonym_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigSynonymName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSynonym_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigSynonymValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSynonym_source_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConfigSynonymSource(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConfigSourceEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConfigTypeEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_delete_topics_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDeleteTopicsWithOptions(SafeAdminClientHandle self, IntPtr topics, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_topic_id_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteTopicsResultTopicIdValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_topic_name_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteTopicsResultTopicNameValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteTopicsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteTopicsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteTopicsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_set_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteTopicsOptionsSetRetryOnQuotaViolation(IntPtr self, sbyte retryOnQuotaViolation, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteTopicsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteTopicsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_topics_with_topic_names_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeTopicsWithTopicNamesOptions(SafeAdminClientHandle self, IntPtr topicNames, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_topics_with_topics_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeTopicsWithTopicsOptions(SafeAdminClientHandle self, IntPtr topics, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_topic_id_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsResultTopicIdValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_topic_name_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsResultTopicNameValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTopicsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_set_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsOptionsSetIncludeAuthorizedOperations(IntPtr self, sbyte includeAuthorizedOperations, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_set_partition_size_limit_per_response_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsOptionsSetPartitionSizeLimitPerResponse(IntPtr self, int partitionSizeLimitPerResponse, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTopicsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTopicsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_is_internal_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte TopicDescriptionIsInternal(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_topic_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionTopicId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionPartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TopicDescription_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicDescriptionAuthorizedOperations(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoNew(
        int partition, IntPtr leader, IntPtr replicas, IntPtr isr, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_with_elr_last_known_elr_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoWithElrLastKnownElr(
        int partition, IntPtr leader, IntPtr replicas, IntPtr isr, IntPtr elr, IntPtr lastKnownElr, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicPartitionInfoDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionInfoPartition(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_leader_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoLeader(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_isr_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoIsr(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_elr_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoElr(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionInfo_last_known_elr_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionInfoLastKnownElr(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_acl_AclOperation_code_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte AclOperationCode(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicCollection_of_topic_names_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicCollectionOfTopicNames(IntPtr topics, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicCollection_of_topic_ids_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicCollectionOfTopicIds(IntPtr topics, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicCollection_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicCollectionDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Uuid_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UuidNew(long mostSigBits, long leastSigBits, ref IntPtr outErr);
}
