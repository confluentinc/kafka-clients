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

using System.Collections.Generic;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The real admin clients' only handling of a null options: an RPC whose arguments send no request never reads its
/// options, so there a null becomes new options; any other null reaches the core, which rejects it.
/// </summary>
internal static class AdminEarlyReturns
{
    internal static CreateTopicsOptions? CreateTopics(ref IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options)
    {
        if (options is not null || newTopics is null)
        {
            return options;
        }

        List<NewTopic> topics = new List<NewTopic>(newTopics);
        newTopics = topics;
        return topics.Exists(topic => topic is not null && !string.IsNullOrEmpty(topic.Name()))
            ? null
            : new CreateTopicsOptions();
    }

    internal static DeleteTopicsOptions? DeleteTopics(IReadOnlyCollection<string> topics, DeleteTopicsOptions? options) =>
        options ?? (topics is not null && !AnyTopicName(topics) ? new DeleteTopicsOptions() : null);

    internal static DeleteTopicsOptions? DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options) =>
        options ?? ((topics is TopicCollection.TopicNameCollection names && !AnyTopicName(names.TopicNames()))
            || (topics is TopicCollection.TopicIdCollection ids && !AnyTopicId(ids.TopicIds()))
                ? new DeleteTopicsOptions()
                : null);

    internal static DescribeTopicsOptions? DescribeTopics(
        IReadOnlyCollection<string> topicNames, DescribeTopicsOptions? options) =>
        options ?? (topicNames is not null && !AnyTopicName(topicNames) ? new DescribeTopicsOptions() : null);

    internal static DescribeTopicsOptions? DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options) =>
        options ?? (topics is TopicCollection.TopicNameCollection names && !AnyTopicName(names.TopicNames())
            ? new DescribeTopicsOptions()
            : null);

    internal static DescribeAclsOptions? DescribeAcls(AclBindingFilter filter, DescribeAclsOptions? options) =>
        options ?? (filter is not null && filter.IsUnknown() ? new DescribeAclsOptions() : null);

    internal static DescribeConfigsOptions? DescribeConfigs(
        IReadOnlyCollection<ConfigResource> resources, DescribeConfigsOptions? options) =>
        options ?? (resources is { Count: 0 } ? new DescribeConfigsOptions() : null);

    internal static AlterConfigsOptions? IncrementalAlterConfigs(
        IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs, AlterConfigsOptions? options) =>
        options ?? (configs is { Count: 0 } ? new AlterConfigsOptions() : null);

    internal static DescribeLogDirsOptions? DescribeLogDirs(
        IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options) =>
        options ?? (brokers is { Count: 0 } ? new DescribeLogDirsOptions() : null);

    internal static AlterReplicaLogDirsOptions? AlterReplicaLogDirs(
        IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment, AlterReplicaLogDirsOptions? options) =>
        options ?? (replicaAssignment is { Count: 0 } ? new AlterReplicaLogDirsOptions() : null);

    internal static DescribeReplicaLogDirsOptions? DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions? options) =>
        options ?? (replicas is { Count: 0 } ? new DescribeReplicaLogDirsOptions() : null);

    internal static CreatePartitionsOptions? CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options) =>
        options ?? (newPartitions is { Count: 0 } ? new CreatePartitionsOptions() : null);

    internal static ListPartitionReassignmentsOptions? ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions, ListPartitionReassignmentsOptions? options) =>
        options ?? (partitions is not null && AnyInvalidPartition(partitions)
            ? new ListPartitionReassignmentsOptions()
            : null);

    private static bool AnyTopicName(IEnumerable<string>? names)
    {
        if (names is null)
        {
            return true;
        }

        foreach (string? name in names)
        {
            if (!string.IsNullOrEmpty(name))
            {
                return true;
            }
        }

        return false;
    }

    private static bool AnyTopicId(IEnumerable<Uuid>? ids)
    {
        if (ids is null)
        {
            return true;
        }

        foreach (Uuid id in ids)
        {
            if (!id.Equals(Uuid.ZERO_UUID))
            {
                return true;
            }
        }

        return false;
    }

    private static bool AnyInvalidPartition(IEnumerable<TopicPartition> partitions)
    {
        foreach (TopicPartition partition in partitions)
        {
            if (string.IsNullOrEmpty(partition.Topic()) || partition.Partition() < 0)
            {
                return true;
            }
        }

        return false;
    }
}
