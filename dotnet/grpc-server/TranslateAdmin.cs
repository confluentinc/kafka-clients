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
using System.Collections.Generic;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Google.Protobuf;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Admin-direction translation helpers for <see cref="AdminServiceImpl"/> and
/// <see cref="AsyncAdminServiceImpl"/>, the C# port of Python's <c>_admin_*</c> helpers in
/// <c>grpc_translate.py</c>.
/// </summary>
internal static class TranslateAdmin
{
    private const int LocalIllegalArgumentCode = -3;

    private static readonly Dictionary<string, TransactionState> s_transactionStatesByName =
        BuildTransactionStateTable();

    // Case-insensitive, unlike s_transactionStatesByName: GroupState.parse lower-cases its input,
    // TransactionState.parse does not.
    private static readonly Dictionary<string, GroupState> s_groupStatesByName =
        BuildNameTable<GroupState>();

    private static readonly Dictionary<string, GroupType> s_groupTypesByName =
        BuildNameTable<GroupType>();

    internal static bool SelectsMock(IReadOnlyDictionary<string, string> config)
    {
        foreach (string value in config.Values)
        {
            if (!string.IsNullOrEmpty(value))
            {
                return false;
            }
        }

        return true;
    }

    internal static Proto.KafkaError ConstructorError(Exception error) =>
        error is KafkaException
            ? Translate.ToProto(error)
            : new Proto.KafkaError
            {
                Code = LocalIllegalArgumentCode,
                Message = $"dotnet server: {error.GetType().Name}: {error.Message}",
            };

    internal static int? Timeout(bool hasTimeoutMs, int timeoutMs) => hasTimeoutMs ? timeoutMs : (int?)null;

    internal static bool RetryOnQuota(bool hasRetryOnQuotaViolation, bool retryOnQuotaViolation) =>
        !hasRetryOnQuotaViolation || retryOnQuotaViolation;

    internal static Proto.ResultKey NameKey(string name) => new Proto.ResultKey { Name = name };

    internal static Proto.ResultKey TopicIdKey(Uuid topicId) => new Proto.ResultKey { TopicId = topicId.ToString() };

    internal static Proto.ResultKey PartitionKey(TopicPartition partition) =>
        new Proto.ResultKey { Partition = Translate.TpToProto(partition) };

    internal static Proto.ResultKey BrokerIdKey(int brokerId) => new Proto.ResultKey { BrokerId = brokerId };

    internal static Proto.ResultKey ConfigResourceKey(ConfigResource resource) =>
        new Proto.ResultKey { ConfigResource = ConfigResourceToProto(resource) };

    internal static Proto.ResultKey ReplicaKey(TopicPartitionReplica replica) =>
        new Proto.ResultKey { Replica = ReplicaToProto(replica) };

    internal static async Task<(TValue Value, Proto.KafkaError? Error)> Resolve<TValue>(AsyncKafkaFuture<TValue> future)
    {
        try
        {
            return (await future.Get().ConfigureAwait(false), null);
        }
        catch (Exception ex)
        {
            return (default!, Translate.ToProto(ex));
        }
    }

    internal static (TValue Value, Proto.KafkaError? Error) Resolve<TValue>(KafkaFuture<TValue> future)
    {
        try
        {
            return (future.Get(), null);
        }
        catch (Exception ex)
        {
            return (default!, Translate.ToProto(ex));
        }
    }

    internal static async Task<Proto.KafkaError?> ResolveVoid(AsyncKafkaFuture future)
    {
        try
        {
            await future.Get().ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            return Translate.ToProto(ex);
        }
    }

    internal static Proto.KafkaError? ResolveVoid(KafkaFuture future)
    {
        try
        {
            future.Get();
            return null;
        }
        catch (Exception ex)
        {
            return Translate.ToProto(ex);
        }
    }

    internal static async Task<Proto.VoidKeyedResponse> VoidResponse<TKey>(
        IReadOnlyDictionary<TKey, AsyncKafkaFuture> futures, Func<TKey, Proto.ResultKey> keyFn)
    {
        Proto.VoidKeyedResponse response = new Proto.VoidKeyedResponse();
        foreach (KeyValuePair<TKey, AsyncKafkaFuture> pair in futures)
        {
            Proto.ResultKey key = keyFn(pair.Key);
            response.Entries.Add(VoidEntry(key, await ResolveVoid(pair.Value).ConfigureAwait(false)));
        }

        return response;
    }

    internal static Proto.VoidKeyedResponse VoidResponse<TKey>(
        IReadOnlyDictionary<TKey, KafkaFuture> futures, Func<TKey, Proto.ResultKey> keyFn)
    {
        Proto.VoidKeyedResponse response = new Proto.VoidKeyedResponse();
        foreach (KeyValuePair<TKey, KafkaFuture> pair in futures)
        {
            Proto.ResultKey key = keyFn(pair.Key);
            response.Entries.Add(VoidEntry(key, ResolveVoid(pair.Value)));
        }

        return response;
    }

    private static Proto.VoidResultEntry VoidEntry(Proto.ResultKey key, Proto.KafkaError? error)
    {
        Proto.VoidResultEntry entry = new Proto.VoidResultEntry { Key = key };
        if (error is not null)
        {
            entry.Error = error;
        }

        return entry;
    }

    // An unset oneof is an empty name collection, as Python reads an unset message field as a
    // default instance.
    internal static TopicCollection TopicCollectionOf(bool byIds, Proto.StringList? names, Proto.StringList? topicIds)
    {
        if (byIds)
        {
            List<Uuid> ids = new List<Uuid>();
            if (topicIds is not null)
            {
                foreach (string value in topicIds.Values)
                {
                    ids.Add(Uuid.Parse(value));
                }
            }

            return TopicCollection.OfTopicIds(ids);
        }

        return TopicCollection.OfTopicNames(
            names is null ? Array.Empty<string>() : (IEnumerable<string>)names.Values);
    }

    internal static List<NewTopic> NewTopics(IEnumerable<Proto.NewTopic> protos)
    {
        List<NewTopic> topics = new List<NewTopic>();
        foreach (Proto.NewTopic proto in protos)
        {
            NewTopic topic;
            if (proto.ReplicasAssignments.Count > 0)
            {
                Dictionary<int, IReadOnlyList<int>> assignments = new Dictionary<int, IReadOnlyList<int>>();
                foreach (Proto.ReplicaAssignment assignment in proto.ReplicasAssignments)
                {
                    assignments[assignment.Partition] = new List<int>(assignment.BrokerIds);
                }

                topic = new NewTopic(proto.Name, assignments);
            }
            else
            {
                topic = new NewTopic(
                    proto.Name,
                    proto.NumPartitions < 0 ? (int?)null : proto.NumPartitions,
                    proto.ReplicationFactor < 0 ? (short?)null : (short)proto.ReplicationFactor);
            }

            topic.Configs = new Dictionary<string, string>(proto.Configs);
            topics.Add(topic);
        }

        return topics;
    }

    // Presence of new_assignments, not its length, is the discriminant: increaseTo(n) and
    // increaseTo(n, []) are different broker requests.
    internal static Dictionary<string, NewPartitions> NewPartitionsMap(IEnumerable<Proto.NewPartitions> protos)
    {
        Dictionary<string, NewPartitions> map = new Dictionary<string, NewPartitions>(StringComparer.Ordinal);
        foreach (Proto.NewPartitions proto in protos)
        {
            if (proto.NewAssignments is null)
            {
                map[proto.Topic] = NewPartitions.IncreaseTo(proto.TotalCount);
                continue;
            }

            List<IReadOnlyList<int>> assignments = new List<IReadOnlyList<int>>();
            foreach (Proto.BrokerIdList list in proto.NewAssignments.Assignments)
            {
                assignments.Add(new List<int>(list.BrokerIds));
            }

            map[proto.Topic] = NewPartitions.IncreaseTo(proto.TotalCount, assignments);
        }

        return map;
    }

    internal static Dictionary<TopicPartition, RecordsToDelete> RecordsToDeleteMap(
        IEnumerable<Proto.RecordsToDelete> protos)
    {
        Dictionary<TopicPartition, RecordsToDelete> map = new Dictionary<TopicPartition, RecordsToDelete>();
        foreach (Proto.RecordsToDelete proto in protos)
        {
            map[Translate.Tp(proto.Partition)] = RecordsToDelete.BeforeOffset(proto.BeforeOffset);
        }

        return map;
    }

    // Five fields only: createTopics' response carries no source, type, documentation or synonyms.
    internal static Proto.ConfigEntry ConfigEntryToProto(ConfigEntry entry)
    {
        Proto.ConfigEntry proto = new Proto.ConfigEntry
        {
            Name = entry.Name,
            IsDefault = entry.IsDefault,
            IsSensitive = entry.IsSensitive,
            IsReadOnly = entry.IsReadOnly,
        };
        if (entry.Value is not null)
        {
            proto.Value = entry.Value;
        }

        return proto;
    }

    internal static Proto.ConfigEntry FullConfigEntryToProto(ConfigEntry entry)
    {
        Proto.ConfigEntry proto = ConfigEntryToProto(entry);
        proto.Source = ConfigSourceName(entry.Source);
        proto.ConfigType = ConfigTypeName(entry.Type);
        if (entry.Documentation is not null)
        {
            proto.Documentation = entry.Documentation;
        }

        // Java's precedence order is meaningful — do not sort.
        foreach (ConfigEntry.ConfigSynonym synonym in entry.Synonyms)
        {
            Proto.ConfigSynonym protoSynonym = new Proto.ConfigSynonym
            {
                Name = synonym.Name,
                Source = ConfigSourceName(synonym.Source),
            };
            if (synonym.Value is not null)
            {
                protoSynonym.Value = synonym.Value;
            }

            proto.Synonyms.Add(protoSynonym);
        }

        return proto;
    }

    internal static Proto.AdminConfig ConfigToProto(Config config)
    {
        Proto.AdminConfig proto = new Proto.AdminConfig();
        foreach (ConfigEntry entry in config.Entries)
        {
            proto.Entries.Add(FullConfigEntryToProto(entry));
        }

        return proto;
    }

    internal static Proto.ConfigResource ConfigResourceToProto(ConfigResource resource) =>
        new Proto.ConfigResource { ResourceType = (int)resource.Type, Name = resource.Name };

    internal static List<ConfigResource> ConfigResources(IEnumerable<Proto.ConfigResource> protos)
    {
        List<ConfigResource> resources = new List<ConfigResource>();
        foreach (Proto.ConfigResource proto in protos)
        {
            resources.Add(new ConfigResource((ConfigResourceType)proto.ResourceType, proto.Name));
        }

        return resources;
    }

    internal static Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> AlterConfigsMap(
        IEnumerable<Proto.ConfigResourceOps> protos)
    {
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> map =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>();
        foreach (Proto.ConfigResourceOps proto in protos)
        {
            List<AlterConfigOp> ops = new List<AlterConfigOp>();
            foreach (Proto.AlterConfigOp op in proto.Ops)
            {
                ops.Add(new AlterConfigOp(
                    new ConfigEntry(op.Name, op.HasValue ? op.Value : null),
                    (AlterConfigOpType)op.OpType));
            }

            map[new ConfigResource((ConfigResourceType)proto.Resource.ResourceType, proto.Resource.Name)] = ops;
        }

        return map;
    }

    // An empty list is Java's empty set ("every supported type"): passed through, not rejected.
    internal static List<ConfigResourceType> ConfigResourceTypes(IEnumerable<int> ids)
    {
        List<ConfigResourceType> types = new List<ConfigResourceType>();
        foreach (int id in ids)
        {
            types.Add((ConfigResourceType)id);
        }

        return types;
    }

    internal static Proto.TopicPartitionReplica ReplicaToProto(TopicPartitionReplica replica) =>
        new Proto.TopicPartitionReplica
        {
            Topic = replica.Topic,
            Partition = replica.Partition,
            BrokerId = replica.BrokerId,
        };

    internal static List<TopicPartitionReplica> Replicas(IEnumerable<Proto.TopicPartitionReplica> protos)
    {
        List<TopicPartitionReplica> replicas = new List<TopicPartitionReplica>();
        foreach (Proto.TopicPartitionReplica proto in protos)
        {
            replicas.Add(new TopicPartitionReplica(proto.Topic, proto.Partition, proto.BrokerId));
        }

        return replicas;
    }

    internal static Dictionary<TopicPartitionReplica, string> ReplicaLogDirAssignments(
        IEnumerable<Proto.ReplicaLogDirAssignment> protos)
    {
        Dictionary<TopicPartitionReplica, string> map = new Dictionary<TopicPartitionReplica, string>();
        foreach (Proto.ReplicaLogDirAssignment proto in protos)
        {
            map[new TopicPartitionReplica(proto.Replica.Topic, proto.Replica.Partition, proto.Replica.BrokerId)] =
                proto.LogDir;
        }

        return map;
    }

    internal static Proto.LogDirDescription LogDirDescriptionToProto(LogDirDescription description)
    {
        Proto.LogDirDescription proto = new Proto.LogDirDescription { IsCordoned = description.IsCordoned };
        if (description.Error is not null)
        {
            proto.Error = Translate.ToProto(description.Error);
        }

        if (description.TotalBytes.HasValue)
        {
            proto.TotalBytes = description.TotalBytes.Value;
        }

        if (description.UsableBytes.HasValue)
        {
            proto.UsableBytes = description.UsableBytes.Value;
        }

        foreach (KeyValuePair<TopicPartition, ReplicaInfo> pair in description.ReplicaInfos)
        {
            proto.ReplicaInfos.Add(new Proto.ReplicaInfoEntry
            {
                Partition = Translate.TpToProto(pair.Key),
                Size = pair.Value.Size,
                OffsetLag = pair.Value.OffsetLag,
                IsFuture = pair.Value.IsFuture,
            });
        }

        return proto;
    }

    internal static string ConfigSourceName(ConfigEntry.ConfigSource source) => source switch
    {
        ConfigEntry.ConfigSource.DynamicTopicConfig => "DYNAMIC_TOPIC_CONFIG",
        ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig => "DYNAMIC_BROKER_LOGGER_CONFIG",
        ConfigEntry.ConfigSource.DynamicBrokerConfig => "DYNAMIC_BROKER_CONFIG",
        ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ConfigEntry.ConfigSource.DynamicClientMetricsConfig => "DYNAMIC_CLIENT_METRICS_CONFIG",
        ConfigEntry.ConfigSource.DynamicGroupConfig => "DYNAMIC_GROUP_CONFIG",
        ConfigEntry.ConfigSource.StaticBrokerConfig => "STATIC_BROKER_CONFIG",
        ConfigEntry.ConfigSource.DefaultConfig => "DEFAULT_CONFIG",
        _ => "UNKNOWN",
    };

    internal static string ConfigTypeName(ConfigEntry.ConfigType type) => type switch
    {
        ConfigEntry.ConfigType.Boolean => "BOOLEAN",
        ConfigEntry.ConfigType.String => "STRING",
        ConfigEntry.ConfigType.Int => "INT",
        ConfigEntry.ConfigType.Short => "SHORT",
        ConfigEntry.ConfigType.Long => "LONG",
        ConfigEntry.ConfigType.Double => "DOUBLE",
        ConfigEntry.ConfigType.List => "LIST",
        ConfigEntry.ConfigType.Class => "CLASS",
        ConfigEntry.ConfigType.Password => "PASSWORD",
        _ => "UNKNOWN",
    };

    internal static Proto.TopicPartitionInfo PartitionInfoToProto(TopicPartitionInfo info)
    {
        Proto.TopicPartitionInfo proto = new Proto.TopicPartitionInfo { Partition = info.Partition };
        if (info.Leader is not null)
        {
            proto.Leader = Translate.NodeToProto(info.Leader);
        }

        foreach (Node node in info.Replicas)
        {
            proto.Replicas.Add(Translate.NodeToProto(node));
        }

        foreach (Node node in info.InSyncReplicas)
        {
            proto.Isr.Add(Translate.NodeToProto(node));
        }

        // Absent (not reported) is not the same as reported-empty.
        if (info.Elr is not null)
        {
            proto.Elr = NodeListToProto(info.Elr);
        }

        if (info.LastKnownElr is not null)
        {
            proto.LastKnownElr = NodeListToProto(info.LastKnownElr);
        }

        return proto;
    }

    internal static Proto.TopicDescription DescriptionToProto(TopicDescription description)
    {
        Proto.TopicDescription proto = new Proto.TopicDescription
        {
            Name = description.Name,
            TopicId = description.TopicId.ToString(),
            IsInternal = description.IsInternal,
        };
        foreach (TopicPartitionInfo info in description.Partitions)
        {
            proto.Partitions.Add(PartitionInfoToProto(info));
        }

        // Absent (not reported) is not the same as reported-empty.
        if (description.AuthorizedOperations is not null)
        {
            proto.AuthorizedOperations = AclOperationsToProto(description.AuthorizedOperations);
        }

        return proto;
    }

    internal static Proto.AdminTopicListing ListingToProto(TopicListing listing) => new Proto.AdminTopicListing
    {
        Name = listing.Name,
        TopicId = listing.TopicId.ToString(),
        IsInternal = listing.IsInternal,
    };

    internal static Proto.KafkaError RequestError(string message) => new Proto.KafkaError
    {
        Code = LocalIllegalArgumentCode,
        Message = $"dotnet server: AdminRequestError: {message}",
    };

    internal static IReadOnlyCollection<TopicPartition>? OptionalPartitions(Proto.TopicPartitionList? partitions)
    {
        if (partitions is null)
        {
            return null;
        }

        List<TopicPartition> selection = new List<TopicPartition>(partitions.Partitions.Count);
        foreach (Proto.TopicPartition partition in partitions.Partitions)
        {
            selection.Add(Translate.Tp(partition));
        }

        return selection;
    }

    internal static Dictionary<TopicPartition, NewPartitionReassignment?> Reassignments(
        IEnumerable<Proto.PartitionReassignmentSpec> protos)
    {
        Dictionary<TopicPartition, NewPartitionReassignment?> map =
            new Dictionary<TopicPartition, NewPartitionReassignment?>();
        foreach (Proto.PartitionReassignmentSpec proto in protos)
        {
            map[Translate.Tp(proto.Partition)] = proto.Reassignment is null
                ? null
                : new NewPartitionReassignment(new List<int>(proto.Reassignment.TargetReplicas));
        }

        return map;
    }

    internal static Proto.PartitionReassignment ReassignmentToProto(PartitionReassignment reassignment)
    {
        Proto.PartitionReassignment proto = new Proto.PartitionReassignment();
        proto.Replicas.AddRange(reassignment.Replicas);
        proto.AddingReplicas.AddRange(reassignment.AddingReplicas);
        proto.RemovingReplicas.AddRange(reassignment.RemovingReplicas);
        return proto;
    }

    // KIND_UNSPECIFIED, or FOR_TIMESTAMP without a timestamp, fails the call: a dropped kind must
    // not silently become earliest().
    internal static Dictionary<TopicPartition, OffsetSpec>? OffsetSpecs(
        IEnumerable<Proto.OffsetSpecEntry> protos, out string? invalid)
    {
        Dictionary<TopicPartition, OffsetSpec> map = new Dictionary<TopicPartition, OffsetSpec>();
        foreach (Proto.OffsetSpecEntry proto in protos)
        {
            TopicPartition partition = Translate.Tp(proto.Partition);
            Proto.OffsetSpec spec = proto.Spec;
            switch (spec.Kind)
            {
                case Proto.OffsetSpec.Types.Kind.Earliest:
                    map[partition] = OffsetSpec.Earliest();
                    break;
                case Proto.OffsetSpec.Types.Kind.Latest:
                    map[partition] = OffsetSpec.Latest();
                    break;
                case Proto.OffsetSpec.Types.Kind.MaxTimestamp:
                    map[partition] = OffsetSpec.MaxTimestamp();
                    break;
                case Proto.OffsetSpec.Types.Kind.EarliestLocal:
                    map[partition] = OffsetSpec.EarliestLocal();
                    break;
                case Proto.OffsetSpec.Types.Kind.LatestTiered:
                    map[partition] = OffsetSpec.LatestTiered();
                    break;
                case Proto.OffsetSpec.Types.Kind.EarliestPendingUpload:
                    map[partition] = OffsetSpec.EarliestPendingUpload();
                    break;
                case Proto.OffsetSpec.Types.Kind.ForTimestamp:
                    if (!spec.HasTimestamp)
                    {
                        invalid = $"OffsetSpec FOR_TIMESTAMP for {partition} carries no timestamp";
                        return null;
                    }

                    map[partition] = OffsetSpec.ForTimestamp(spec.Timestamp);
                    break;
                default:
                    invalid = $"OffsetSpec for {partition} has kind {(int)spec.Kind}, not a Java OffsetSpec variant";
                    return null;
            }
        }

        invalid = null;
        return map;
    }

    internal static Proto.ListOffsetsResultInfo OffsetInfoToProto(ListOffsetsResult.ListOffsetsResultInfo info)
    {
        Proto.ListOffsetsResultInfo proto = new Proto.ListOffsetsResultInfo
        {
            Offset = info.Offset,
            Timestamp = info.Timestamp,
        };
        if (info.LeaderEpoch.HasValue)
        {
            proto.LeaderEpoch = info.LeaderEpoch.Value;
        }

        return proto;
    }

    internal static Proto.ProducerState ProducerStateToProto(ProducerState state)
    {
        Proto.ProducerState proto = new Proto.ProducerState
        {
            ProducerId = state.ProducerId,
            ProducerEpoch = state.ProducerEpoch,
            LastSequence = state.LastSequence,
            LastTimestamp = state.LastTimestamp,
        };
        if (state.CoordinatorEpoch.HasValue)
        {
            proto.CoordinatorEpoch = state.CoordinatorEpoch.Value;
        }

        if (state.CurrentTransactionStartOffset.HasValue)
        {
            proto.CurrentTransactionStartOffset = state.CurrentTransactionStartOffset.Value;
        }

        return proto;
    }

    internal static Proto.PartitionProducerState PartitionProducerStateToProto(
        DescribeProducersResult.PartitionProducerState value)
    {
        Proto.PartitionProducerState proto = new Proto.PartitionProducerState();
        foreach (ProducerState state in value.ActiveProducers)
        {
            proto.ActiveProducers.Add(ProducerStateToProto(state));
        }

        return proto;
    }

    internal static Proto.TransactionDescription TransactionDescriptionToProto(TransactionDescription description)
    {
        Proto.TransactionDescription proto = new Proto.TransactionDescription
        {
            CoordinatorId = description.CoordinatorId,
            State = TransactionStateName(description.State),
            ProducerId = description.ProducerId,
            ProducerEpoch = description.ProducerEpoch,
            TransactionTimeoutMs = description.TransactionTimeoutMs,
        };
        if (description.TransactionStartTimeMs.HasValue)
        {
            proto.TransactionStartTimeMs = description.TransactionStartTimeMs.Value;
        }

        foreach (TopicPartition partition in description.TopicPartitions)
        {
            proto.TopicPartitions.Add(Translate.TpToProto(partition));
        }

        return proto;
    }

    internal static Proto.TransactionListing TransactionListingToProto(TransactionListing listing) =>
        new Proto.TransactionListing
        {
            TransactionalId = listing.TransactionalId,
            ProducerId = listing.ProducerId,
            State = TransactionStateName(listing.State),
        };

    internal static List<TransactionState> TransactionStates(IEnumerable<string> names)
    {
        List<TransactionState> states = new List<TransactionState>();
        foreach (string name in names)
        {
            states.Add(s_transactionStatesByName.TryGetValue(name, out TransactionState state)
                ? state
                : TransactionState.Unknown);
        }

        return states;
    }

    internal static AbortTransactionSpec? AbortSpec(Proto.AbortTransactionRequest request, out string? invalid)
    {
        if (request.TopicPartition is null)
        {
            invalid = "abort_transaction requires a topic_partition";
            return null;
        }

        if (request.ProducerEpoch < short.MinValue || request.ProducerEpoch > short.MaxValue)
        {
            invalid = $"abort_transaction producer_epoch {request.ProducerEpoch} is outside int16";
            return null;
        }

        invalid = null;
        return new AbortTransactionSpec(
            Translate.Tp(request.TopicPartition),
            request.ProducerId,
            (short)request.ProducerEpoch,
            request.CoordinatorEpoch);
    }

    internal static Proto.AclOperationList AclOperationsToProto(IEnumerable<AclOperation> operations)
    {
        Proto.AclOperationList list = new Proto.AclOperationList();
        foreach (AclOperation operation in operations)
        {
            list.Operations.Add((int)operation);
        }

        return list;
    }

    internal static List<GroupState> GroupStates(IEnumerable<string> names) =>
        ParseNames(names, s_groupStatesByName, GroupState.Unknown);

    internal static List<GroupType> GroupTypes(IEnumerable<string> names) =>
        ParseNames(names, s_groupTypesByName, GroupType.Unknown);

    internal static Proto.GroupListing GroupListingToProto(GroupListing listing)
    {
        Proto.GroupListing proto = new Proto.GroupListing
        {
            GroupId = listing.GroupId,
            Protocol = listing.Protocol,
            IsSimpleConsumerGroup = listing.IsSimpleConsumerGroup,
        };
        if (listing.Type.HasValue)
        {
            proto.GroupType = listing.Type.Value.ToString();
        }

        if (listing.GroupState.HasValue)
        {
            proto.GroupState = listing.GroupState.Value.ToString();
        }

        return proto;
    }

    internal static Proto.MemberAssignment MemberAssignmentToProto(MemberAssignment assignment)
    {
        Proto.MemberAssignment proto = new Proto.MemberAssignment();
        foreach (TopicPartition partition in assignment.TopicPartitions)
        {
            proto.TopicPartitions.Add(Translate.TpToProto(partition));
        }

        return proto;
    }

    internal static Proto.MemberDescription MemberDescriptionToProto(MemberDescription member)
    {
        Proto.MemberDescription proto = new Proto.MemberDescription
        {
            ConsumerId = member.ConsumerId,
            ClientId = member.ClientId,
            Host = member.Host,
            Assignment = MemberAssignmentToProto(member.Assignment),
        };
        if (member.GroupInstanceId is not null)
        {
            proto.GroupInstanceId = member.GroupInstanceId;
        }

        if (member.RackId is not null)
        {
            proto.RackId = member.RackId;
        }

        if (member.TargetAssignment is not null)
        {
            proto.TargetAssignment = MemberAssignmentToProto(member.TargetAssignment);
        }

        if (member.MemberEpoch.HasValue)
        {
            proto.MemberEpoch = member.MemberEpoch.Value;
        }

        if (member.Upgraded.HasValue)
        {
            proto.Upgraded = member.Upgraded.Value;
        }

        return proto;
    }

    internal static Proto.ConsumerGroupDescription ConsumerGroupDescriptionToProto(
        ConsumerGroupDescription description)
    {
        Proto.ConsumerGroupDescription proto = new Proto.ConsumerGroupDescription
        {
            GroupId = description.GroupId,
            IsSimpleConsumerGroup = description.IsSimpleConsumerGroup,
            PartitionAssignor = description.PartitionAssignor,
            GroupType = description.Type.ToString(),
            GroupState = description.GroupState.ToString(),
        };
        foreach (MemberDescription member in description.Members)
        {
            proto.Members.Add(MemberDescriptionToProto(member));
        }

        if (description.Coordinator is not null)
        {
            proto.Coordinator = Translate.NodeToProto(description.Coordinator);
        }

        // Absent (not reported) is not the same as reported-empty.
        if (description.AuthorizedOperations is not null)
        {
            proto.AuthorizedOperations = AclOperationsToProto(description.AuthorizedOperations);
        }

        if (description.GroupEpoch.HasValue)
        {
            proto.GroupEpoch = description.GroupEpoch.Value;
        }

        if (description.TargetAssignmentEpoch.HasValue)
        {
            proto.TargetAssignmentEpoch = description.TargetAssignmentEpoch.Value;
        }

        return proto;
    }

    // A null protocol faults into the whole-call catch, as Java's isSimpleConsumerGroup does;
    // Python sends "" instead.
    internal static Proto.ClassicGroupDescription ClassicGroupDescriptionToProto(
        ClassicGroupDescription description)
    {
        Proto.ClassicGroupDescription proto = new Proto.ClassicGroupDescription
        {
            GroupId = description.GroupId,
            Protocol = description.Protocol!,
            ProtocolData = description.ProtocolData,
            IsSimpleConsumerGroup = description.IsSimpleConsumerGroup,
            State = description.State.ToString(),
        };
        foreach (MemberDescription member in description.Members)
        {
            proto.Members.Add(MemberDescriptionToProto(member));
        }

        if (description.Coordinator is not null)
        {
            proto.Coordinator = Translate.NodeToProto(description.Coordinator);
        }

        if (description.AuthorizedOperations is not null)
        {
            proto.AuthorizedOperations = AclOperationsToProto(description.AuthorizedOperations);
        }

        return proto;
    }

    // A repeated group_id is rejected, not de-duplicated, as the C entry point refuses it.
    internal static Dictionary<string, ListConsumerGroupOffsetsSpec>? GroupOffsetSpecs(
        IEnumerable<Proto.ListConsumerGroupOffsetsSpec> protos, out string? invalid)
    {
        Dictionary<string, ListConsumerGroupOffsetsSpec> map =
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal);
        foreach (Proto.ListConsumerGroupOffsetsSpec proto in protos)
        {
            if (map.ContainsKey(proto.GroupId))
            {
                invalid = $"group id `{proto.GroupId}` appears more than once in listConsumerGroupOffsets";
                return null;
            }

            map[proto.GroupId] = new ListConsumerGroupOffsetsSpec
            {
                TopicPartitions = OptionalPartitions(proto.TopicPartitions),
            };
        }

        invalid = null;
        return map;
    }

    internal static Dictionary<TopicPartition, OffsetAndMetadata> GroupOffsetCommits(
        IEnumerable<Proto.GroupOffsetCommit> protos)
    {
        Dictionary<TopicPartition, OffsetAndMetadata> map = new Dictionary<TopicPartition, OffsetAndMetadata>();
        foreach (Proto.GroupOffsetCommit proto in protos)
        {
            int? leaderEpoch = proto.Offset.HasLeaderEpoch ? proto.Offset.LeaderEpoch : (int?)null;
            map[Translate.Tp(proto.Partition)] =
                new OffsetAndMetadata(proto.Offset.Offset, proto.Offset.Metadata, leaderEpoch);
        }

        return map;
    }

    internal static List<MemberToRemove>? MembersToRemove(Proto.MemberToRemoveList? members)
    {
        if (members is null)
        {
            return null;
        }

        List<MemberToRemove> selection = new List<MemberToRemove>(members.Members.Count);
        foreach (Proto.MemberToRemove member in members.Members)
        {
            selection.Add(new MemberToRemove(member.GroupInstanceId));
        }

        // Java's options constructor collapses duplicates into a Set, and `memberResult` is a
        // keyed lookup, so a repeated instance id must not emit two identical entries.
        return DistinctKeys(selection);
    }

    internal static List<T> DistinctKeys<T>(IEnumerable<T> keys)
    {
        List<T> ordered = new List<T>();
        HashSet<T> seen = new HashSet<T>();
        foreach (T key in keys)
        {
            if (seen.Add(key))
            {
                ordered.Add(key);
            }
        }

        return ordered;
    }

    internal static Proto.ResultKey AclBindingKey(AclBinding binding) =>
        new Proto.ResultKey { AclBinding = AclBindingToProto(binding) };

    internal static Proto.ResultKey AclFilterKey(AclBindingFilter filter) =>
        new Proto.ResultKey { AclBindingFilter = AclFilterToProto(filter) };

    internal static Proto.ResultKey QuotaEntityKey(ClientQuotaEntity entity) =>
        new Proto.ResultKey { ClientQuotaEntity = QuotaEntityToProto(entity) };

    internal static List<AclBinding> AclBindings(IEnumerable<Proto.AclBinding> protos)
    {
        List<AclBinding> bindings = new List<AclBinding>();
        foreach (Proto.AclBinding proto in protos)
        {
            bindings.Add(new AclBinding(
                new ResourcePattern(
                    (ResourceType)proto.ResourceType, proto.ResourceName, (PatternType)proto.PatternType),
                new AccessControlEntry(
                    proto.Principal,
                    proto.Host,
                    (AclOperation)proto.Operation,
                    (AclPermissionType)proto.PermissionType)));
        }

        return bindings;
    }

    internal static AclBindingFilter AclFilter(Proto.AclBindingFilter proto) =>
        new AclBindingFilter(
            new ResourcePatternFilter(
                (ResourceType)proto.ResourceType,
                proto.HasResourceName ? proto.ResourceName : null,
                (PatternType)proto.PatternType),
            new AccessControlEntryFilter(
                proto.HasPrincipal ? proto.Principal : null,
                proto.HasHost ? proto.Host : null,
                (AclOperation)proto.Operation,
                (AclPermissionType)proto.PermissionType));

    internal static List<AclBindingFilter> AclFilters(IEnumerable<Proto.AclBindingFilter> protos)
    {
        List<AclBindingFilter> filters = new List<AclBindingFilter>();
        foreach (Proto.AclBindingFilter proto in protos)
        {
            filters.Add(AclFilter(proto));
        }

        return filters;
    }

    internal static Proto.AclBinding AclBindingToProto(AclBinding binding) => new Proto.AclBinding
    {
        ResourceType = (int)binding.Pattern.ResourceType,
        ResourceName = binding.Pattern.Name,
        PatternType = (int)binding.Pattern.PatternType,
        Principal = binding.Entry.Principal,
        Host = binding.Entry.Host,
        Operation = (int)binding.Entry.Operation,
        PermissionType = (int)binding.Entry.PermissionType,
    };

    internal static Proto.AclBindingFilter AclFilterToProto(AclBindingFilter filter)
    {
        Proto.AclBindingFilter proto = new Proto.AclBindingFilter
        {
            ResourceType = (int)filter.PatternFilter.ResourceType,
            PatternType = (int)filter.PatternFilter.PatternType,
            Operation = (int)filter.EntryFilter.Operation,
            PermissionType = (int)filter.EntryFilter.PermissionType,
        };
        if (filter.PatternFilter.Name is not null)
        {
            proto.ResourceName = filter.PatternFilter.Name;
        }

        if (filter.EntryFilter.Principal is not null)
        {
            proto.Principal = filter.EntryFilter.Principal;
        }

        if (filter.EntryFilter.Host is not null)
        {
            proto.Host = filter.EntryFilter.Host;
        }

        return proto;
    }

    internal static Proto.ClientQuotaEntity QuotaEntityToProto(ClientQuotaEntity entity)
    {
        Proto.ClientQuotaEntity proto = new Proto.ClientQuotaEntity();
        foreach (KeyValuePair<string, string?> pair in entity.Entries)
        {
            Proto.ClientQuotaEntityEntry entry = new Proto.ClientQuotaEntityEntry { EntityType = pair.Key };
            if (pair.Value is not null)
            {
                entry.EntityName = pair.Value;
            }

            proto.Entries.Add(entry);
        }

        return proto;
    }

    internal static ClientQuotaEntity QuotaEntity(Proto.ClientQuotaEntity? proto)
    {
        Dictionary<string, string?> entries = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (proto is not null)
        {
            foreach (Proto.ClientQuotaEntityEntry entry in proto.Entries)
            {
                entries[entry.EntityType] = entry.HasEntityName ? entry.EntityName : null;
            }
        }

        return new ClientQuotaEntity(entries);
    }

    // MATCH_KIND_UNSPECIFIED is rejected, not defaulted: a defaulted EXACT would make a dropped
    // field a valid filter.
    internal static ClientQuotaFilter? QuotaFilter(
        Proto.DescribeClientQuotasRequest request, out string? invalid)
    {
        List<ClientQuotaFilterComponent> components = new List<ClientQuotaFilterComponent>();
        foreach (Proto.ClientQuotaFilterComponent component in request.Components)
        {
            switch (component.MatchKind)
            {
                case Proto.ClientQuotaMatchKind.MatchKindExact:
                    if (!component.HasMatchName)
                    {
                        invalid = "ClientQuotaFilterComponent with MATCH_KIND_EXACT carries no match_name";
                        return null;
                    }

                    components.Add(ClientQuotaFilterComponent.OfEntity(component.EntityType, component.MatchName));
                    break;
                case Proto.ClientQuotaMatchKind.MatchKindDefault:
                    components.Add(ClientQuotaFilterComponent.OfDefaultEntity(component.EntityType));
                    break;
                case Proto.ClientQuotaMatchKind.MatchKindAny:
                    components.Add(ClientQuotaFilterComponent.OfEntityType(component.EntityType));
                    break;
                default:
                    invalid = $"ClientQuotaFilterComponent has no match_kind (got {(int)component.MatchKind})";
                    return null;
            }
        }

        invalid = null;
        return request.Strict
            ? ClientQuotaFilter.ContainsOnly(components)
            : ClientQuotaFilter.Contains(components);
    }

    internal static List<ClientQuotaAlteration> QuotaAlterations(
        IEnumerable<Proto.ClientQuotaAlteration> protos)
    {
        List<ClientQuotaAlteration> alterations = new List<ClientQuotaAlteration>();
        foreach (Proto.ClientQuotaAlteration proto in protos)
        {
            List<ClientQuotaAlteration.Op> ops = new List<ClientQuotaAlteration.Op>();
            foreach (Proto.ClientQuotaOp op in proto.Ops)
            {
                ops.Add(new ClientQuotaAlteration.Op(op.Key, op.HasValue ? op.Value : (double?)null));
            }

            alterations.Add(new ClientQuotaAlteration(QuotaEntity(proto.Entity), ops));
        }

        return alterations;
    }

    internal static Proto.ScramCredentialInfo ScramInfoToProto(ScramCredentialInfo info) =>
        new Proto.ScramCredentialInfo
        {
            Mechanism = (int)info.Mechanism,
            Iterations = info.Iterations,
        };

    internal static Proto.UserScramCredentialsDescription ScramDescriptionToProto(
        UserScramCredentialsDescription description)
    {
        Proto.UserScramCredentialsDescription proto =
            new Proto.UserScramCredentialsDescription { Name = description.Name };
        foreach (ScramCredentialInfo info in description.CredentialInfos)
        {
            proto.CredentialInfos.Add(ScramInfoToProto(info));
        }

        return proto;
    }

    internal static List<UserScramCredentialAlteration> ScramAlterations(
        IEnumerable<Proto.UserScramCredentialAlteration> protos)
    {
        List<UserScramCredentialAlteration> alterations = new List<UserScramCredentialAlteration>();
        foreach (Proto.UserScramCredentialAlteration proto in protos)
        {
            if (proto.IsDeletion)
            {
                alterations.Add(new UserScramCredentialDeletion(proto.User, (ScramMechanism)proto.Mechanism));
                continue;
            }

            alterations.Add(new UserScramCredentialUpsertion(
                proto.User,
                new ScramCredentialInfo((ScramMechanism)proto.Mechanism, proto.Iterations),
                proto.HasPassword ? proto.Password.ToByteArray() : Array.Empty<byte>(),
                proto.HasSalt ? proto.Salt.ToByteArray() : null));
        }

        return alterations;
    }

    internal static List<KafkaPrincipal> Principals(IEnumerable<Proto.KafkaPrincipal> protos)
    {
        List<KafkaPrincipal> principals = new List<KafkaPrincipal>();
        foreach (Proto.KafkaPrincipal proto in protos)
        {
            principals.Add(new KafkaPrincipal(proto.PrincipalType, proto.Name, proto.TokenAuthenticated));
        }

        return principals;
    }

    internal static Proto.KafkaPrincipal PrincipalToProto(KafkaPrincipal principal) =>
        new Proto.KafkaPrincipal
        {
            PrincipalType = principal.PrincipalType,
            Name = principal.Name,
            TokenAuthenticated = principal.TokenAuthenticated,
        };

    internal static IReadOnlyList<KafkaPrincipal>? TokenOwners(Proto.KafkaPrincipalList? owners) =>
        owners is null ? null : Principals(owners.Principals);

    internal static Proto.DelegationToken DelegationTokenToProto(DelegationToken token)
    {
        TokenInformation info = token.TokenInfo;
        Proto.TokenInformation protoInfo = new Proto.TokenInformation
        {
            TokenId = info.TokenId,
            Owner = PrincipalToProto(info.Owner),
            TokenRequester = PrincipalToProto(info.TokenRequester),
            IssueTimestamp = info.IssueTimestamp,
            MaxTimestamp = info.MaxTimestamp,
            ExpiryTimestamp = info.ExpiryTimestamp,
        };
        foreach (KafkaPrincipal renewer in info.Renewers)
        {
            protoInfo.Renewers.Add(PrincipalToProto(renewer));
        }

        return new Proto.DelegationToken
        {
            TokenInformation = protoInfo,
            Hmac = ByteString.CopyFrom(token.Hmac),
            HmacAsBase64 = token.HmacAsBase64String,
        };
    }

    internal static Proto.FeatureMetadata FeatureMetadataToProto(FeatureMetadata metadata)
    {
        Proto.FeatureMetadata proto = new Proto.FeatureMetadata();
        foreach (KeyValuePair<string, FinalizedVersionRange> pair in metadata.FinalizedFeatures)
        {
            proto.FinalizedFeatures[pair.Key] = new Proto.FinalizedVersionRange
            {
                MinVersionLevel = pair.Value.MinVersionLevel,
                MaxVersionLevel = pair.Value.MaxVersionLevel,
            };
        }

        foreach (KeyValuePair<string, SupportedVersionRange> pair in metadata.SupportedFeatures)
        {
            proto.SupportedFeatures[pair.Key] = new Proto.SupportedVersionRange
            {
                MinVersion = pair.Value.MinVersion,
                MaxVersion = pair.Value.MaxVersion,
            };
        }

        if (metadata.FinalizedFeaturesEpoch.HasValue)
        {
            proto.FinalizedFeaturesEpoch = metadata.FinalizedFeaturesEpoch.Value;
        }

        return proto;
    }

    internal static Dictionary<string, FeatureUpdate>? FeatureUpdates(
        IDictionary<string, Proto.FeatureUpdate> updates, out string? invalid)
    {
        Dictionary<string, FeatureUpdate> map = new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, Proto.FeatureUpdate> pair in updates)
        {
            if (pair.Value.MaxVersionLevel < short.MinValue || pair.Value.MaxVersionLevel > short.MaxValue)
            {
                invalid = $"FeatureUpdate for `{pair.Key}` has max_version_level " +
                    $"{pair.Value.MaxVersionLevel}, outside int16";
                return null;
            }

            map[pair.Key] = new FeatureUpdate(
                (short)pair.Value.MaxVersionLevel, UpgradeTypes.FromCode(pair.Value.UpgradeType));
        }

        invalid = null;
        return map;
    }

    // The enum members are spelled as Java's toString(), so Enum.ToString() is the wire spelling.
    private static string TransactionStateName(TransactionState state) => state.ToString();

    private static Dictionary<string, TransactionState> BuildTransactionStateTable()
    {
        Dictionary<string, TransactionState> table =
            new Dictionary<string, TransactionState>(StringComparer.Ordinal);
        foreach (TransactionState state in (TransactionState[])Enum.GetValues(typeof(TransactionState)))
        {
            table[TransactionStateName(state)] = state;
        }

        return table;
    }

    private static Dictionary<string, TEnum> BuildNameTable<TEnum>()
        where TEnum : struct, Enum
    {
        Dictionary<string, TEnum> table = new Dictionary<string, TEnum>(StringComparer.OrdinalIgnoreCase);
        foreach (TEnum value in (TEnum[])Enum.GetValues(typeof(TEnum)))
        {
            table[value.ToString()!] = value;
        }

        return table;
    }

    private static List<TEnum> ParseNames<TEnum>(
        IEnumerable<string> names, Dictionary<string, TEnum> table, TEnum fallback)
        where TEnum : struct, Enum
    {
        List<TEnum> values = new List<TEnum>();
        foreach (string name in names)
        {
            values.Add(table.TryGetValue(name, out TEnum value) ? value : fallback);
        }

        return values;
    }

    private static Proto.NodeList NodeListToProto(IEnumerable<Node> nodes)
    {
        Proto.NodeList list = new Proto.NodeList();
        foreach (Node node in nodes)
        {
            list.Nodes.Add(Translate.NodeToProto(node));
        }

        return list;
    }
}
