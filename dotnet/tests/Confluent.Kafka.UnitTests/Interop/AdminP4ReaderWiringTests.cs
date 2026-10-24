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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins which ABI symbol each admin static is bound to, the one axis no behavioural test can reach.
/// </summary>
/// <remarks>
/// The accessors are identified by their ABI <c>EntryPoint</c>, not by their C# name: the entry point is the contract
/// with the core.
/// </remarks>
public sealed class AdminP4ReaderWiringTests
{
    [Fact]
    public void EveryAdminStaticBoundToAnAbiSymbol_HasExactlyOneRow()
    {
        string[] bound = typeof(NativeAdminClient)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(field => field.GetValue(null) is Delegate value
                && value.Method.GetCustomAttribute<DllImportAttribute>() is not null)
            .Select(field => field.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
        string[] rows = typeof(AdminP4ReaderWiringTests)
            .GetMethods()
            .SelectMany(method => method.GetCustomAttributes<InlineDataAttribute>()
                .SelectMany(data => data.GetData(method)))
            .Select(row => (string)row[0])
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(bound);
        Assert.Equal(bound, rows);
    }

    [Theory]
    [InlineData("s_createAclsValues", "kafka_admin_CreateAclsResult_values_err")]
    [InlineData("s_deleteAclsValues", "kafka_admin_DeleteAclsResult_values_err")]
    [InlineData("s_describeAclsValues", "kafka_admin_DescribeAclsResult_values_err")]
    [InlineData("s_newCreateAclsOptions", "kafka_admin_CreateAclsOptions_new_err")]
    [InlineData("s_destroyCreateAclsOptions", "kafka_admin_CreateAclsOptions_destroy_err")]
    [InlineData("s_destroyCreateAclsResult", "kafka_admin_CreateAclsResult_destroy_err")]
    [InlineData("s_newDescribeAclsOptions", "kafka_admin_DescribeAclsOptions_new_err")]
    [InlineData("s_destroyDescribeAclsOptions", "kafka_admin_DescribeAclsOptions_destroy_err")]
    [InlineData("s_destroyDescribeAclsResult", "kafka_admin_DescribeAclsResult_destroy_err")]
    [InlineData("s_newDeleteAclsOptions", "kafka_admin_DeleteAclsOptions_new_err")]
    [InlineData("s_destroyDeleteAclsOptions", "kafka_admin_DeleteAclsOptions_destroy_err")]
    [InlineData("s_destroyDeleteAclsResult", "kafka_admin_DeleteAclsResult_destroy_err")]
    public void EachAclRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_alterConfigsValues", "kafka_admin_AlterConfigsResult_values_err")]
    [InlineData("s_describeConfigsValues", "kafka_admin_DescribeConfigsResult_values_err")]
    [InlineData("s_listConfigResourcesAll", "kafka_admin_ListConfigResourcesResult_all_err")]
    [InlineData("s_newDescribeConfigsOptions", "kafka_admin_DescribeConfigsOptions_new_err")]
    [InlineData("s_destroyDescribeConfigsOptions", "kafka_admin_DescribeConfigsOptions_destroy_err")]
    [InlineData("s_destroyDescribeConfigsResult", "kafka_admin_DescribeConfigsResult_destroy_err")]
    [InlineData("s_newAlterConfigsOptions", "kafka_admin_AlterConfigsOptions_new_err")]
    [InlineData("s_destroyAlterConfigsOptions", "kafka_admin_AlterConfigsOptions_destroy_err")]
    [InlineData("s_destroyAlterConfigsResult", "kafka_admin_AlterConfigsResult_destroy_err")]
    [InlineData("s_newListConfigResourcesOptions", "kafka_admin_ListConfigResourcesOptions_new_err")]
    [InlineData("s_destroyListConfigResourcesOptions", "kafka_admin_ListConfigResourcesOptions_destroy_err")]
    [InlineData("s_destroyListConfigResourcesResult", "kafka_admin_ListConfigResourcesResult_destroy_err")]
    public void EachConfigRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_newUnregisterControllerOptions", "kafka_admin_UnregisterControllerOptions_new_err")]
    [InlineData("s_destroyUnregisterControllerOptions", "kafka_admin_UnregisterControllerOptions_destroy_err")]
    [InlineData("s_destroyUnregisterControllerResult", "kafka_admin_UnregisterControllerResult_destroy_err")]
    [InlineData("s_unregisterControllerAll", "kafka_admin_UnregisterControllerResult_all_err")]
    public void EachControllerRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_describeFeaturesFeatureMetadata", "kafka_admin_DescribeFeaturesResult_feature_metadata_err")]
    [InlineData("s_updateFeaturesValues", "kafka_admin_UpdateFeaturesResult_values_err")]
    [InlineData("s_destroyDescribeFeaturesResult", "kafka_admin_DescribeFeaturesResult_destroy_err")]
    [InlineData("s_destroyUpdateFeaturesResult", "kafka_admin_UpdateFeaturesResult_destroy_err")]
    [InlineData("s_newDescribeFeaturesOptions", "kafka_admin_DescribeFeaturesOptions_new_err")]
    [InlineData("s_newUpdateFeaturesOptions", "kafka_admin_UpdateFeaturesOptions_new_err")]
    [InlineData("s_destroyDescribeFeaturesOptions", "kafka_admin_DescribeFeaturesOptions_destroy_err")]
    [InlineData("s_destroyUpdateFeaturesOptions", "kafka_admin_UpdateFeaturesOptions_destroy_err")]
    public void EachFeatureRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_adminDeleteConsumerGroupsWithOptions", "kafka_admin_Admin_delete_consumer_groups_with_options_err")]
    [InlineData("s_alterConsumerGroupOffsetsAll", "kafka_admin_AlterConsumerGroupOffsetsResult_all_err")]
    [InlineData(
        "s_alterConsumerGroupOffsetsPartitionResult",
        "kafka_admin_AlterConsumerGroupOffsetsResult_partition_result_err")]
    [InlineData("s_deleteConsumerGroupOffsetsAll", "kafka_admin_DeleteConsumerGroupOffsetsResult_all_err")]
    [InlineData(
        "s_deleteConsumerGroupOffsetsPartitionResult",
        "kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result")]
    [InlineData("s_deleteConsumerGroupsDeletedGroups", "kafka_admin_DeleteConsumerGroupsResult_deleted_groups_err")]
    [InlineData(
        "s_listConsumerGroupOffsetsPartitionsToOffsetAndMetadataWithGroupId",
        "kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id")]
    [InlineData("s_removeMembersFromConsumerGroupAll", "kafka_admin_RemoveMembersFromConsumerGroupResult_all_err")]
    [InlineData(
        "s_removeMembersFromConsumerGroupMemberResult",
        "kafka_admin_RemoveMembersFromConsumerGroupResult_member_result")]
    [InlineData("s_newListConsumerGroupOffsetsOptions", "kafka_admin_ListConsumerGroupOffsetsOptions_new_err")]
    [InlineData("s_destroyListConsumerGroupOffsetsOptions", "kafka_admin_ListConsumerGroupOffsetsOptions_destroy_err")]
    [InlineData("s_destroyListConsumerGroupOffsetsResult", "kafka_admin_ListConsumerGroupOffsetsResult_destroy_err")]
    [InlineData("s_newAlterConsumerGroupOffsetsOptions", "kafka_admin_AlterConsumerGroupOffsetsOptions_new_err")]
    [InlineData(
        "s_destroyAlterConsumerGroupOffsetsOptions",
        "kafka_admin_AlterConsumerGroupOffsetsOptions_destroy_err")]
    [InlineData("s_destroyAlterConsumerGroupOffsetsResult", "kafka_admin_AlterConsumerGroupOffsetsResult_destroy_err")]
    [InlineData("s_newDeleteConsumerGroupOffsetsOptions", "kafka_admin_DeleteConsumerGroupOffsetsOptions_new_err")]
    [InlineData(
        "s_destroyDeleteConsumerGroupOffsetsOptions",
        "kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy_err")]
    [InlineData(
        "s_destroyDeleteConsumerGroupOffsetsResult",
        "kafka_admin_DeleteConsumerGroupOffsetsResult_destroy_err")]
    [InlineData("s_newDeleteConsumerGroupsOptions", "kafka_admin_DeleteConsumerGroupsOptions_new_err")]
    [InlineData("s_destroyDeleteConsumerGroupsOptions", "kafka_admin_DeleteConsumerGroupsOptions_destroy_err")]
    [InlineData("s_destroyDeleteConsumerGroupsResult", "kafka_admin_DeleteConsumerGroupsResult_destroy_err")]
    [InlineData(
        "s_destroyRemoveMembersFromConsumerGroupOptions",
        "kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy_err")]
    [InlineData(
        "s_destroyRemoveMembersFromConsumerGroupResult",
        "kafka_admin_RemoveMembersFromConsumerGroupResult_destroy_err")]
    public void EachGroupOffsetsRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData(
        "s_adminDescribeClassicGroupsWithOptions",
        "kafka_admin_Admin_describe_classic_groups_with_options_err")]
    [InlineData(
        "s_adminDescribeConsumerGroupsWithOptions",
        "kafka_admin_Admin_describe_consumer_groups_with_options_err")]
    [InlineData(
        "s_describeClassicGroupsDescribedGroups",
        "kafka_admin_DescribeClassicGroupsResult_described_groups_err")]
    [InlineData(
        "s_describeConsumerGroupsDescribedGroups",
        "kafka_admin_DescribeConsumerGroupsResult_described_groups_err")]
    [InlineData("s_listGroupsOptionsInGroupStates", "kafka_admin_ListGroupsOptions_in_group_states_err")]
    [InlineData("s_listGroupsOptionsWithTypes", "kafka_admin_ListGroupsOptions_with_types_err")]
    [InlineData("s_listGroupsAll", "kafka_admin_ListGroupsResult_all_err")]
    [InlineData("s_listGroupsErrors", "kafka_admin_ListGroupsResult_errors_err")]
    [InlineData("s_listGroupsValid", "kafka_admin_ListGroupsResult_valid_err")]
    [InlineData("s_newListGroupsOptions", "kafka_admin_ListGroupsOptions_new_err")]
    [InlineData("s_destroyListGroupsOptions", "kafka_admin_ListGroupsOptions_destroy_err")]
    [InlineData("s_destroyListGroupsResult", "kafka_admin_ListGroupsResult_destroy_err")]
    [InlineData("s_newDescribeConsumerGroupsOptions", "kafka_admin_DescribeConsumerGroupsOptions_new_err")]
    [InlineData("s_destroyDescribeConsumerGroupsOptions", "kafka_admin_DescribeConsumerGroupsOptions_destroy_err")]
    [InlineData("s_destroyDescribeConsumerGroupsResult", "kafka_admin_DescribeConsumerGroupsResult_destroy_err")]
    [InlineData("s_newDescribeClassicGroupsOptions", "kafka_admin_DescribeClassicGroupsOptions_new_err")]
    [InlineData("s_destroyDescribeClassicGroupsOptions", "kafka_admin_DescribeClassicGroupsOptions_destroy_err")]
    [InlineData("s_destroyDescribeClassicGroupsResult", "kafka_admin_DescribeClassicGroupsResult_destroy_err")]
    public void EachGroupRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_alterReplicaLogDirsValues", "kafka_admin_AlterReplicaLogDirsResult_values_err")]
    [InlineData("s_describeLogDirsDescriptions", "kafka_admin_DescribeLogDirsResult_descriptions_err")]
    [InlineData("s_describeReplicaLogDirsValues", "kafka_admin_DescribeReplicaLogDirsResult_values_err")]
    [InlineData("s_newDescribeLogDirsOptions", "kafka_admin_DescribeLogDirsOptions_new_err")]
    [InlineData("s_destroyDescribeLogDirsOptions", "kafka_admin_DescribeLogDirsOptions_destroy_err")]
    [InlineData("s_destroyDescribeLogDirsResult", "kafka_admin_DescribeLogDirsResult_destroy_err")]
    [InlineData("s_newAlterReplicaLogDirsOptions", "kafka_admin_AlterReplicaLogDirsOptions_new_err")]
    [InlineData("s_destroyAlterReplicaLogDirsOptions", "kafka_admin_AlterReplicaLogDirsOptions_destroy_err")]
    [InlineData("s_destroyAlterReplicaLogDirsResult", "kafka_admin_AlterReplicaLogDirsResult_destroy_err")]
    [InlineData("s_newDescribeReplicaLogDirsOptions", "kafka_admin_DescribeReplicaLogDirsOptions_new_err")]
    [InlineData("s_destroyDescribeReplicaLogDirsOptions", "kafka_admin_DescribeReplicaLogDirsOptions_destroy_err")]
    [InlineData("s_destroyDescribeReplicaLogDirsResult", "kafka_admin_DescribeReplicaLogDirsResult_destroy_err")]
    public void EachLogDirRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_createPartitionsValues", "kafka_admin_CreatePartitionsResult_values_err")]
    [InlineData("s_deleteRecordsLowWatermarks", "kafka_admin_DeleteRecordsResult_low_watermarks_err")]
    [InlineData("s_listOffsetsPartitionResult", "kafka_admin_ListOffsetsResult_partition_result")]
    [InlineData("s_newCreatePartitionsOptions", "kafka_admin_CreatePartitionsOptions_new_err")]
    [InlineData("s_destroyCreatePartitionsOptions", "kafka_admin_CreatePartitionsOptions_destroy_err")]
    [InlineData("s_destroyCreatePartitionsResult", "kafka_admin_CreatePartitionsResult_destroy_err")]
    [InlineData("s_newDeleteRecordsOptions", "kafka_admin_DeleteRecordsOptions_new_err")]
    [InlineData("s_destroyDeleteRecordsOptions", "kafka_admin_DeleteRecordsOptions_destroy_err")]
    [InlineData("s_destroyDeleteRecordsResult", "kafka_admin_DeleteRecordsResult_destroy_err")]
    [InlineData("s_newListOffsetsOptions", "kafka_admin_ListOffsetsOptions_new_err")]
    [InlineData("s_destroyListOffsetsOptions", "kafka_admin_ListOffsetsOptions_destroy_err")]
    [InlineData("s_destroyListOffsetsResult", "kafka_admin_ListOffsetsResult_destroy_err")]
    public void EachPartitionRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_alterClientQuotasValues", "kafka_admin_AlterClientQuotasResult_values_err")]
    [InlineData("s_describeClientQuotasEntities", "kafka_admin_DescribeClientQuotasResult_entities_err")]
    [InlineData("s_newDescribeClientQuotasOptions", "kafka_admin_DescribeClientQuotasOptions_new_err")]
    [InlineData("s_destroyDescribeClientQuotasOptions", "kafka_admin_DescribeClientQuotasOptions_destroy_err")]
    [InlineData("s_destroyDescribeClientQuotasResult", "kafka_admin_DescribeClientQuotasResult_destroy_err")]
    [InlineData("s_newAlterClientQuotasOptions", "kafka_admin_AlterClientQuotasOptions_new_err")]
    [InlineData("s_destroyAlterClientQuotasOptions", "kafka_admin_AlterClientQuotasOptions_destroy_err")]
    [InlineData("s_destroyAlterClientQuotasResult", "kafka_admin_AlterClientQuotasResult_destroy_err")]
    public void EachQuotaRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_alterPartitionReassignmentsValues", "kafka_admin_AlterPartitionReassignmentsResult_values_err")]
    [InlineData("s_electLeadersPartitions", "kafka_admin_ElectLeadersResult_partitions_err")]
    [InlineData(
        "s_listPartitionReassignmentsReassignments",
        "kafka_admin_ListPartitionReassignmentsResult_reassignments_err")]
    [InlineData("s_newElectLeadersOptions", "kafka_admin_ElectLeadersOptions_new_err")]
    [InlineData("s_destroyElectLeadersOptions", "kafka_admin_ElectLeadersOptions_destroy_err")]
    [InlineData("s_destroyElectLeadersResult", "kafka_admin_ElectLeadersResult_destroy_err")]
    [InlineData("s_newAlterPartitionReassignmentsOptions", "kafka_admin_AlterPartitionReassignmentsOptions_new_err")]
    [InlineData(
        "s_destroyAlterPartitionReassignmentsOptions",
        "kafka_admin_AlterPartitionReassignmentsOptions_destroy_err")]
    [InlineData(
        "s_destroyAlterPartitionReassignmentsResult",
        "kafka_admin_AlterPartitionReassignmentsResult_destroy_err")]
    [InlineData("s_newListPartitionReassignmentsOptions", "kafka_admin_ListPartitionReassignmentsOptions_new_err")]
    [InlineData(
        "s_destroyListPartitionReassignmentsOptions",
        "kafka_admin_ListPartitionReassignmentsOptions_destroy_err")]
    [InlineData(
        "s_destroyListPartitionReassignmentsResult",
        "kafka_admin_ListPartitionReassignmentsResult_destroy_err")]
    public void EachReassignmentRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_alterUserScramCredentialsValues", "kafka_admin_AlterUserScramCredentialsResult_values_err")]
    [InlineData("s_describeUserScramCredentialsAll", "kafka_admin_DescribeUserScramCredentialsResult_all_err")]
    [InlineData(
        "s_describeUserScramCredentialsDescription",
        "kafka_admin_DescribeUserScramCredentialsResult_description_err")]
    [InlineData("s_describeUserScramCredentialsUsers", "kafka_admin_DescribeUserScramCredentialsResult_users_err")]
    [InlineData("s_newDescribeUserScramCredentialsOptions", "kafka_admin_DescribeUserScramCredentialsOptions_new_err")]
    [InlineData(
        "s_destroyDescribeUserScramCredentialsOptions",
        "kafka_admin_DescribeUserScramCredentialsOptions_destroy_err")]
    [InlineData(
        "s_destroyDescribeUserScramCredentialsResult",
        "kafka_admin_DescribeUserScramCredentialsResult_destroy_err")]
    [InlineData("s_newAlterUserScramCredentialsOptions", "kafka_admin_AlterUserScramCredentialsOptions_new_err")]
    [InlineData(
        "s_destroyAlterUserScramCredentialsOptions",
        "kafka_admin_AlterUserScramCredentialsOptions_destroy_err")]
    [InlineData("s_destroyAlterUserScramCredentialsResult", "kafka_admin_AlterUserScramCredentialsResult_destroy_err")]
    public void EachScramRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_createDelegationTokenToken", "kafka_admin_CreateDelegationTokenResult_delegation_token_err")]
    [InlineData("s_renewDelegationTokenExpiryTimestamp", "kafka_admin_RenewDelegationTokenResult_expiry_timestamp_err")]
    [InlineData(
        "s_expireDelegationTokenExpiryTimestamp",
        "kafka_admin_ExpireDelegationTokenResult_expiry_timestamp_err")]
    [InlineData("s_describeDelegationTokenTokens", "kafka_admin_DescribeDelegationTokenResult_delegation_tokens_err")]
    [InlineData("s_destroyCreateDelegationTokenResult", "kafka_admin_CreateDelegationTokenResult_destroy_err")]
    [InlineData("s_destroyRenewDelegationTokenResult", "kafka_admin_RenewDelegationTokenResult_destroy_err")]
    [InlineData("s_destroyExpireDelegationTokenResult", "kafka_admin_ExpireDelegationTokenResult_destroy_err")]
    [InlineData("s_destroyDescribeDelegationTokenResult", "kafka_admin_DescribeDelegationTokenResult_destroy_err")]
    [InlineData(
        "s_createDelegationTokenOptionsSetRenewers",
        "kafka_admin_CreateDelegationTokenOptions_set_renewers_err")]
    [InlineData(
        "s_describeDelegationTokenOptionsSetOwners",
        "kafka_admin_DescribeDelegationTokenOptions_set_owners_err")]
    [InlineData("s_newCreateDelegationTokenOptions", "kafka_admin_CreateDelegationTokenOptions_new_err")]
    [InlineData("s_destroyCreateDelegationTokenOptions", "kafka_admin_CreateDelegationTokenOptions_destroy_err")]
    [InlineData("s_newRenewDelegationTokenOptions", "kafka_admin_RenewDelegationTokenOptions_new_err")]
    [InlineData("s_destroyRenewDelegationTokenOptions", "kafka_admin_RenewDelegationTokenOptions_destroy_err")]
    [InlineData("s_newExpireDelegationTokenOptions", "kafka_admin_ExpireDelegationTokenOptions_new_err")]
    [InlineData("s_destroyExpireDelegationTokenOptions", "kafka_admin_ExpireDelegationTokenOptions_destroy_err")]
    [InlineData("s_newDescribeDelegationTokenOptions", "kafka_admin_DescribeDelegationTokenOptions_new_err")]
    [InlineData("s_destroyDescribeDelegationTokenOptions", "kafka_admin_DescribeDelegationTokenOptions_destroy_err")]
    public void EachDelegationTokenRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_adminDescribeClusterWithOptions", "kafka_admin_Admin_describe_cluster_with_options_err")]
    [InlineData("s_adminListTopicsWithOptions", "kafka_admin_Admin_list_topics_with_options_err")]
    [InlineData("s_createTopicsValues", "kafka_admin_CreateTopicsResult_values_err")]
    [InlineData("s_deleteTopicsTopicIdValues", "kafka_admin_DeleteTopicsResult_topic_id_values_err")]
    [InlineData("s_deleteTopicsTopicNameValues", "kafka_admin_DeleteTopicsResult_topic_name_values_err")]
    [InlineData("s_describeClusterAuthorizedOperations", "kafka_admin_DescribeClusterResult_authorized_operations_err")]
    [InlineData("s_describeClusterClusterId", "kafka_admin_DescribeClusterResult_cluster_id_err")]
    [InlineData("s_describeClusterController", "kafka_admin_DescribeClusterResult_controller_err")]
    [InlineData("s_describeClusterNodes", "kafka_admin_DescribeClusterResult_nodes_err")]
    [InlineData("s_describeTopicsTopicIdValues", "kafka_admin_DescribeTopicsResult_topic_id_values_err")]
    [InlineData("s_describeTopicsTopicNameValues", "kafka_admin_DescribeTopicsResult_topic_name_values_err")]
    [InlineData("s_listTopicsNamesToListings", "kafka_admin_ListTopicsResult_names_to_listings_err")]
    [InlineData("s_newListTopicsOptions", "kafka_admin_ListTopicsOptions_new_err")]
    [InlineData("s_destroyListTopicsOptions", "kafka_admin_ListTopicsOptions_destroy_err")]
    [InlineData("s_destroyListTopicsResult", "kafka_admin_ListTopicsResult_destroy_err")]
    [InlineData("s_newCreateTopicsOptions", "kafka_admin_CreateTopicsOptions_new_err")]
    [InlineData("s_destroyCreateTopicsOptions", "kafka_admin_CreateTopicsOptions_destroy_err")]
    [InlineData("s_destroyCreateTopicsResult", "kafka_admin_CreateTopicsResult_destroy_err")]
    [InlineData("s_newDeleteTopicsOptions", "kafka_admin_DeleteTopicsOptions_new_err")]
    [InlineData("s_destroyDeleteTopicsOptions", "kafka_admin_DeleteTopicsOptions_destroy_err")]
    [InlineData("s_destroyDeleteTopicsResult", "kafka_admin_DeleteTopicsResult_destroy_err")]
    [InlineData("s_newDescribeTopicsOptions", "kafka_admin_DescribeTopicsOptions_new_err")]
    [InlineData("s_destroyDescribeTopicsOptions", "kafka_admin_DescribeTopicsOptions_destroy_err")]
    [InlineData("s_destroyDescribeTopicsResult", "kafka_admin_DescribeTopicsResult_destroy_err")]
    [InlineData("s_newDescribeClusterOptions", "kafka_admin_DescribeClusterOptions_new_err")]
    [InlineData("s_destroyDescribeClusterOptions", "kafka_admin_DescribeClusterOptions_destroy_err")]
    [InlineData("s_destroyDescribeClusterResult", "kafka_admin_DescribeClusterResult_destroy_err")]
    public void EachTopicRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_adminDescribeTransactionsWithOptions", "kafka_admin_Admin_describe_transactions_with_options_err")]
    [InlineData("s_adminFenceProducersWithOptions", "kafka_admin_Admin_fence_producers_with_options_err")]
    [InlineData("s_adminListTransactionsWithOptions", "kafka_admin_Admin_list_transactions_with_options_err")]
    [InlineData("s_describeProducersPartitionResult", "kafka_admin_DescribeProducersResult_partition_result")]
    [InlineData("s_describeTransactionsDescription", "kafka_admin_DescribeTransactionsResult_description")]
    [InlineData("s_fenceProducersEpochId", "kafka_admin_FenceProducersResult_epoch_id")]
    [InlineData("s_fenceProducersProducerId", "kafka_admin_FenceProducersResult_producer_id")]
    [InlineData("s_listTransactionsOptionsFilterStates", "kafka_admin_ListTransactionsOptions_filter_states_err")]
    [InlineData("s_newDescribeProducersOptions", "kafka_admin_DescribeProducersOptions_new_err")]
    [InlineData("s_destroyDescribeProducersOptions", "kafka_admin_DescribeProducersOptions_destroy_err")]
    [InlineData("s_destroyDescribeProducersResult", "kafka_admin_DescribeProducersResult_destroy_err")]
    [InlineData("s_newDescribeTransactionsOptions", "kafka_admin_DescribeTransactionsOptions_new_err")]
    [InlineData("s_destroyDescribeTransactionsOptions", "kafka_admin_DescribeTransactionsOptions_destroy_err")]
    [InlineData("s_destroyDescribeTransactionsResult", "kafka_admin_DescribeTransactionsResult_destroy_err")]
    [InlineData("s_newAbortTransactionOptions", "kafka_admin_AbortTransactionOptions_new_err")]
    [InlineData("s_destroyAbortTransactionOptions", "kafka_admin_AbortTransactionOptions_destroy_err")]
    [InlineData("s_destroyAbortTransactionResult", "kafka_admin_AbortTransactionResult_destroy_err")]
    [InlineData("s_abortTransactionAll", "kafka_admin_AbortTransactionResult_all_err")]
    [InlineData("s_newFenceProducersOptions", "kafka_admin_FenceProducersOptions_new_err")]
    [InlineData("s_destroyFenceProducersOptions", "kafka_admin_FenceProducersOptions_destroy_err")]
    [InlineData("s_destroyFenceProducersResult", "kafka_admin_FenceProducersResult_destroy_err")]
    [InlineData("s_newTerminateTransactionOptions", "kafka_admin_TerminateTransactionOptions_new_err")]
    [InlineData("s_destroyTerminateTransactionOptions", "kafka_admin_TerminateTransactionOptions_destroy_err")]
    [InlineData("s_destroyTerminateTransactionResult", "kafka_admin_TerminateTransactionResult_destroy_err")]
    [InlineData("s_terminateTransactionResult", "kafka_admin_TerminateTransactionResult_result_err")]
    [InlineData("s_newListTransactionsOptions", "kafka_admin_ListTransactionsOptions_new_err")]
    [InlineData("s_destroyListTransactionsOptions", "kafka_admin_ListTransactionsOptions_destroy_err")]
    [InlineData("s_destroyListTransactionsResult", "kafka_admin_ListTransactionsResult_destroy_err")]
    [InlineData("s_listTransactionsByBrokerId", "kafka_admin_ListTransactionsResult_by_broker_id_err")]
    public void EachTransactionRpc_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    [Theory]
    [InlineData("s_mockAdminClientUpdateBeginningOffsets", "kafka_admin_MockAdminClient_update_beginning_offsets_err")]
    [InlineData(
        "s_mockAdminClientUpdateConsumerGroupOffsets",
        "kafka_admin_MockAdminClient_update_consumer_group_offsets_err")]
    [InlineData("s_mockAdminClientUpdateEndOffsets", "kafka_admin_MockAdminClient_update_end_offsets_err")]
    public void EachMockSeeder_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        AssertBinding(fieldName, entryPoint);

    private static void AssertBinding(string fieldName, string entryPoint) =>
        Assert.Equal(
            entryPoint,
            EntryPointOf(
                (Delegate)typeof(NativeAdminClient)
                    .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)!
                    .GetValue(null)!));

    private static string EntryPointOf(Delegate accessor) =>
        accessor.Method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint
        ?? throw new Xunit.Sdk.XunitException(
            "the bundle member is not a direct P/Invoke, so its ABI symbol is no longer "
            + "readable here — this assertion needs a different mechanism, not deleting");
}
