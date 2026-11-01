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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_groups_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListGroupsWithOptions(SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_consumer_groups_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeConsumerGroupsWithOptions(
        SafeAdminClientHandle self, IntPtr groupIds, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_classic_groups_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeClassicGroupsWithOptions(
        SafeAdminClientHandle self, IntPtr groupIds, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListGroupsResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsResult_valid_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListGroupsResultValid(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsResult_errors_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListGroupsResultErrors(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsResult_described_groups_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConsumerGroupsResultDescribedGroups(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConsumerGroupsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsResult_described_groups_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClassicGroupsResultDescribedGroups(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClassicGroupsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListGroupsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_in_group_states_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsOptionsInGroupStates(IntPtr self, IntPtr list, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_with_protocol_types_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsOptionsWithProtocolTypes(IntPtr self, IntPtr list, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_with_types_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsOptionsWithTypes(IntPtr self, IntPtr list, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListGroupsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListGroupsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeConsumerGroupsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConsumerGroupsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConsumerGroupsOptionsSetIncludeAuthorizedOperations(IntPtr self, sbyte value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConsumerGroupsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeConsumerGroupsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeClassicGroupsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClassicGroupsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_set_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClassicGroupsOptionsSetIncludeAuthorizedOperations(IntPtr self, sbyte value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClassicGroupsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeClassicGroupsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_group_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupListingGroupId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupListingType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_protocol_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupListingProtocol(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_GroupListing_group_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupListingGroupState(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_group_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionGroupId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_is_simple_consumer_group_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ConsumerGroupDescriptionIsSimpleConsumerGroup(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_members_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionMembers(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_partition_assignor_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionPartitionAssignor(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_group_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionGroupState(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_coordinator_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionCoordinator(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupDescriptionAuthorizedOperations(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_group_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerGroupDescriptionGroupEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ConsumerGroupDescription_target_assignment_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerGroupDescriptionTargetAssignmentEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_group_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionGroupId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_protocol_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionProtocol(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_protocol_data_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionProtocolData(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_members_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionMembers(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionState(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_coordinator_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionCoordinator(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ClassicGroupDescription_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClassicGroupDescriptionAuthorizedOperations(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_consumer_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionConsumerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_group_instance_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionGroupInstanceId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_rack_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionRackId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_client_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionClientId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_host_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionHost(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_assignment_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionAssignment(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_target_assignment_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberDescriptionTargetAssignment(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_member_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MemberDescriptionMemberEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberDescription_upgraded_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte MemberDescriptionUpgraded(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberAssignment_topic_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberAssignmentTopicPartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_unknown_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateUnknown(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_preparing_rebalance_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStatePreparingRebalance(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_completing_rebalance_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateCompletingRebalance(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_stable_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateStable(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_dead_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateDead(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_empty_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateEmpty(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_assigning_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateAssigning(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_reconciling_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateReconciling(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState_not_ready_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupStateNotReady(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupState__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int GroupStateEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType_unknown_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupTypeUnknown(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType_consumer_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupTypeConsumer(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType_classic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupTypeClassic(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType_share_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupTypeShare(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType_streams_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupTypeStreams(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupType__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int GroupTypeEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ClassicGroupStateEnum(IntPtr self, ref IntPtr outErr);
}
