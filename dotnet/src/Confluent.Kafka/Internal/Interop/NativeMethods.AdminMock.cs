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
    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_create_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientCreate(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_with_brokers_controller", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientWithBrokersController(IntPtr brokers, IntPtr controller, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient__as_Admin_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientAsAdmin(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_update_beginning_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientUpdateBeginningOffsets(IntPtr self, IntPtr newOffsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_update_end_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientUpdateEndOffsets(IntPtr self, IntPtr newOffsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_update_consumer_group_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientUpdateConsumerGroupOffsets(IntPtr self, IntPtr newOffsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_add_topic", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientAddTopic(
        IntPtr self, sbyte @internal, IntPtr name, IntPtr partitions, IntPtr configs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_mark_topic_for_deletion", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientMarkTopicForDeletion(IntPtr self, IntPtr name, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_timeout_next_request_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientTimeoutNextRequest(IntPtr self, int numberOfRequests, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_set_controller", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientSetController(IntPtr self, IntPtr controller, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_set_fetches_remaining_until_visible", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientSetFetchesRemainingUntilVisible(
        IntPtr self, IntPtr topicName, int fetchesRemainingUntilVisible, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientBuilderNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_cluster_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetClusterId(
        SafeMockAdminClientBuilderHandle self, IntPtr clusterId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_brokers", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientBuilderSetBrokers(
        SafeMockAdminClientBuilderHandle self, IntPtr brokers, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_num_brokers", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientBuilderSetNumBrokers(
        SafeMockAdminClientBuilderHandle self, int numBrokers, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_controller", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientBuilderSetController(
        SafeMockAdminClientBuilderHandle self, int index, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_broker_log_dirs_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetBrokerLogDirs(
        SafeMockAdminClientBuilderHandle self, IntPtr brokerLogDirs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_default_replication_factor_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetDefaultReplicationFactor(
        SafeMockAdminClientBuilderHandle self, int defaultReplicationFactor, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_using_raft_controller_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetUsingRaftController(
        SafeMockAdminClientBuilderHandle self, sbyte usingRaftController, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_default_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetDefaultPartitions(
        SafeMockAdminClientBuilderHandle self, short numPartitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_feature_levels_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetFeatureLevels(
        SafeMockAdminClientBuilderHandle self, IntPtr featureLevels, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_min_supported_feature_levels_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetMinSupportedFeatureLevels(
        SafeMockAdminClientBuilderHandle self, IntPtr minSupportedFeatureLevels, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_max_supported_feature_levels_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetMaxSupportedFeatureLevels(
        SafeMockAdminClientBuilderHandle self, IntPtr maxSupportedFeatureLevels, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_set_default_group_configs_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderSetDefaultGroupConfigs(
        SafeMockAdminClientBuilderHandle self, IntPtr defaultGroupConfigs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_build", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockAdminClientBuilderBuild(SafeMockAdminClientBuilderHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_Builder_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockAdminClientBuilderDestroy(IntPtr self, ref IntPtr outErr);
}
