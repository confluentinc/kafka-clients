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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Admin-direction translation helpers for <see cref="AdminServiceImpl"/> — the C# port of
/// <c>grpc_translate.py</c>'s <c>_admin_*</c> family. Kept out of <see cref="Translate"/>
/// (producer/consumer) so the admin slices can grow additively.
/// </summary>
internal static class TranslateAdmin
{
    /// <summary>
    /// <c>kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT</c>, the code the proto's
    /// constructor-rejection rule mandates (<c>_error_code.py:168</c>).
    /// </summary>
    private const int LocalIllegalArgumentCode = -3;

    /// <summary>Java's <c>NAME_TO_ENUM</c>, keyed by the <c>toString()</c> spelling.</summary>
    private static readonly Dictionary<string, TransactionState> s_transactionStatesByName =
        BuildTransactionStateTable();

    /// <summary>
    /// Java's <c>GroupState.NAME_TO_ENUM</c>. Case-<b>insensitive</b>, unlike
    /// <see cref="s_transactionStatesByName"/>: <c>GroupState.parse</c> lower-cases its input,
    /// <c>TransactionState.parse</c> does not.
    /// </summary>
    private static readonly Dictionary<string, GroupState> s_groupStatesByName =
        BuildNameTable<GroupState>();

    /// <summary>Java's <c>GroupType.NAME_TO_ENUM</c>, also case-insensitive.</summary>
    private static readonly Dictionary<string, GroupType> s_groupTypesByName =
        BuildNameTable<GroupType>();

    /// <summary>
    /// The normative mock-selection rule of <c>admin_service.proto</c>'s
    /// <c>CreateAdminRequest</c>: an empty config, or one whose every value is empty, selects
    /// <see cref="MockAdminClient"/>. Port of <c>_admin_selects_mock</c>.
    /// </summary>
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

    /// <summary>
    /// Translates a <em>constructor</em> failure for <c>CreateAdminResponse.error</c> — port of
    /// <c>_admin_constructor_error</c>. A genuine <see cref="KafkaException"/> is forwarded
    /// verbatim; anything else (the mock's <c>numBrokers &lt; 1</c>) is the proto's
    /// constructor-rejection variant and crosses as <c>LOCAL_ILLEGAL_ARGUMENT</c>, matching
    /// Java's <c>IllegalArgumentException</c>.
    /// </summary>
    internal static Proto.KafkaError ConstructorError(Exception error) =>
        error is KafkaException
            ? Translate.ToProto(error)
            : new Proto.KafkaError
            {
                Code = LocalIllegalArgumentCode,
                Message = $"dotnet server: {error.GetType().Name}: {error.Message}",
            };

    /// <summary><c>optional int32 timeout_ms</c> -&gt; an options object's <c>TimeoutMs</c>.</summary>
    internal static int? Timeout(bool hasTimeoutMs, int timeoutMs) => hasTimeoutMs ? timeoutMs : (int?)null;

    /// <summary>
    /// <c>optional bool retry_on_quota_violation</c> -&gt; Java's default of <c>true</c> when
    /// absent (port of <c>_admin_retry_on_quota</c>).
    /// </summary>
    internal static bool RetryOnQuota(bool hasRetryOnQuotaViolation, bool retryOnQuotaViolation) =>
        !hasRetryOnQuotaViolation || retryOnQuotaViolation;

    /// <summary>A <c>name</c>-keyed <c>ResultKey</c> (<c>_admin_name_key</c>).</summary>
    internal static Proto.ResultKey NameKey(string name) => new Proto.ResultKey { Name = name };

    /// <summary>A <c>topic_id</c>-keyed <c>ResultKey</c> (<c>_admin_topic_id_key</c>).</summary>
    internal static Proto.ResultKey TopicIdKey(Uuid topicId) => new Proto.ResultKey { TopicId = topicId.ToString() };

    /// <summary>A <c>partition</c>-keyed <c>ResultKey</c> (<c>_admin_partition_key</c>).</summary>
    internal static Proto.ResultKey PartitionKey(TopicPartition partition) =>
        new Proto.ResultKey { Partition = Translate.TpToProto(partition) };

    /// <summary>A <c>broker_id</c>-keyed <c>ResultKey</c>.</summary>
    internal static Proto.ResultKey BrokerIdKey(int brokerId) => new Proto.ResultKey { BrokerId = brokerId };

    /// <summary>
    /// A <c>config_resource</c>-keyed <c>ResultKey</c> (<c>_admin_config_resource_key</c>).
    /// </summary>
    internal static Proto.ResultKey ConfigResourceKey(ConfigResource resource) =>
        new Proto.ResultKey { ConfigResource = ConfigResourceToProto(resource) };

    /// <summary>A <c>replica</c>-keyed <c>ResultKey</c> (<c>_admin_replica_key</c>).</summary>
    internal static Proto.ResultKey ReplicaKey(TopicPartitionReplica replica) =>
        new Proto.ResultKey { Replica = ReplicaToProto(replica) };

    /// <summary>
    /// Awaits one per-key future — the C# shape of <c>_resolve_admin_futures</c>. A
    /// <see cref="KafkaException"/> is <em>that key's</em> outcome; any other exception
    /// propagates to the handler's whole-call <c>catch</c>, which is where a failure with no
    /// per-key future belongs.
    /// </summary>
    internal static async Task<(TValue Value, Proto.KafkaError? Error)> Resolve<TValue>(Task<TValue> future)
    {
        try
        {
            return (await future.ConfigureAwait(false), null);
        }
        catch (KafkaException ex)
        {
            return (default!, Translate.ToProto(ex));
        }
    }

    /// <summary>
    /// The void-valued twin of <see cref="Resolve{TValue}"/>: <see langword="null"/> means that
    /// key succeeded, there being no value to distinguish success by.
    /// </summary>
    internal static async Task<Proto.KafkaError?> ResolveVoid(Task future)
    {
        try
        {
            await future.ConfigureAwait(false);
            return null;
        }
        catch (KafkaException ex)
        {
            return Translate.ToProto(ex);
        }
    }

    /// <summary>
    /// <c>{key: Task}</c> -&gt; <c>VoidKeyedResponse</c>, the response every per-key-void admin
    /// RPC answers with (port of <c>_admin_void_response</c>).
    /// </summary>
    internal static async Task<Proto.VoidKeyedResponse> VoidResponse<TKey>(
        IReadOnlyDictionary<TKey, Task> futures, Func<TKey, Proto.ResultKey> keyFn)
    {
        Proto.VoidKeyedResponse response = new Proto.VoidKeyedResponse();
        foreach (KeyValuePair<TKey, Task> pair in futures)
        {
            Proto.VoidResultEntry entry = new Proto.VoidResultEntry { Key = keyFn(pair.Key) };
            Proto.KafkaError? error = await ResolveVoid(pair.Value).ConfigureAwait(false);
            if (error is not null)
            {
                entry.Error = error;
            }

            response.Entries.Add(entry);
        }

        return response;
    }

    /// <summary>
    /// Java's <c>TopicCollection</c> from a request's <c>oneof topics</c>. An unset oneof is an
    /// empty name collection — proto3 in Python reads an unset message field as a default
    /// instance, where C# reads <see langword="null"/>.
    /// </summary>
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

    /// <summary>
    /// Proto <c>NewTopic</c>s -&gt; binding <see cref="NewTopic"/>s (port of
    /// <c>_admin_new_topics</c>). <c>-1</c> is the wire's "absent" for
    /// <c>num_partitions</c> / <c>replication_factor</c>; a non-empty
    /// <c>replicas_assignments</c> selects Java's assignment constructor.
    /// </summary>
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

    /// <summary>
    /// Proto <c>NewPartitions</c> -&gt; <c>{topic: NewPartitions}</c> (port of
    /// <c>_admin_new_partitions</c>). Presence of <c>new_assignments</c>, NOT its length, is the
    /// discriminant: <c>increaseTo(n)</c> and <c>increaseTo(n, [])</c> are different broker
    /// requests.
    /// </summary>
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

    /// <summary>
    /// Proto <c>RecordsToDelete</c>s -&gt; the map <see cref="IAdmin.DeleteRecords"/> takes
    /// (port of <c>_admin_records_to_delete</c>).
    /// </summary>
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

    /// <summary>
    /// Binding <see cref="ConfigEntry"/> -&gt; proto <c>ConfigEntry</c>, <b>five fields only</b>
    /// (port of <c>_admin_config_entry_to_proto</c>). Fields 6-9 (<c>source</c> /
    /// <c>config_type</c> / <c>documentation</c> / <c>synonyms</c>) belong to
    /// <c>describeConfigs</c>: <c>createTopics</c>' broker response carries none of them, and
    /// emitting them here would let a <c>createTopics</c> entry claim a source it never carried.
    /// </summary>
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

    // -- Cluster, configs & log dirs (slice G2) -------------------------------------------

    /// <summary>
    /// Binding <see cref="ConfigEntry"/> -&gt; proto <c>ConfigEntry</c>, <b>all nine fields</b>
    /// (port of <c>_admin_full_config_entry_to_proto</c>). The <c>describeConfigs</c> twin of
    /// <see cref="ConfigEntryToProto"/>. <c>source</c> / <c>config_type</c> cross as Java's
    /// enum constant names — neither enum has a numeric id.
    /// </summary>
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

    /// <summary>Binding <see cref="Config"/> -&gt; proto <c>AdminConfig</c>.</summary>
    internal static Proto.AdminConfig ConfigToProto(Config config)
    {
        Proto.AdminConfig proto = new Proto.AdminConfig();
        foreach (ConfigEntry entry in config.Entries)
        {
            proto.Entries.Add(FullConfigEntryToProto(entry));
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="ConfigResource"/> -&gt; proto <c>ConfigResource</c>. The enum's values
    /// <em>are</em> Java's <c>ConfigResource.Type.id()</c>, which is what the wire carries.
    /// </summary>
    internal static Proto.ConfigResource ConfigResourceToProto(ConfigResource resource) =>
        new Proto.ConfigResource { ResourceType = (int)resource.Type, Name = resource.Name };

    /// <summary>Proto <c>ConfigResource</c>s -&gt; binding ones (<c>_admin_config_resources</c>).</summary>
    internal static List<ConfigResource> ConfigResources(IEnumerable<Proto.ConfigResource> protos)
    {
        List<ConfigResource> resources = new List<ConfigResource>();
        foreach (Proto.ConfigResource proto in protos)
        {
            resources.Add(new ConfigResource((ConfigResourceType)proto.ResourceType, proto.Name));
        }

        return resources;
    }

    /// <summary>
    /// Proto <c>ConfigResourceOps</c>s -&gt; the map <see cref="IAdmin.IncrementalAlterConfigs"/>
    /// takes (port of <c>_admin_alter_configs</c>). An absent op value is Java's null, which is
    /// what a DELETE carries.
    /// </summary>
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

    /// <summary>
    /// Proto <c>resource_types</c> (Java <c>Type.id()</c> codes) -&gt; the filter
    /// <see cref="IAdmin.ListConfigResources"/> takes. An empty list is Java's empty set —
    /// "every supported type" — and is passed through, not turned into a rejection.
    /// </summary>
    internal static List<ConfigResourceType> ConfigResourceTypes(IEnumerable<int> ids)
    {
        List<ConfigResourceType> types = new List<ConfigResourceType>();
        foreach (int id in ids)
        {
            types.Add((ConfigResourceType)id);
        }

        return types;
    }

    /// <summary>Binding <see cref="TopicPartitionReplica"/> -&gt; proto (<c>_admin_replica_to_proto</c>).</summary>
    internal static Proto.TopicPartitionReplica ReplicaToProto(TopicPartitionReplica replica) =>
        new Proto.TopicPartitionReplica
        {
            Topic = replica.Topic,
            Partition = replica.Partition,
            BrokerId = replica.BrokerId,
        };

    /// <summary>Proto <c>TopicPartitionReplica</c>s -&gt; binding ones (<c>_admin_replicas</c>).</summary>
    internal static List<TopicPartitionReplica> Replicas(IEnumerable<Proto.TopicPartitionReplica> protos)
    {
        List<TopicPartitionReplica> replicas = new List<TopicPartitionReplica>();
        foreach (Proto.TopicPartitionReplica proto in protos)
        {
            replicas.Add(new TopicPartitionReplica(proto.Topic, proto.Partition, proto.BrokerId));
        }

        return replicas;
    }

    /// <summary>
    /// Proto <c>ReplicaLogDirAssignment</c>s -&gt; the map
    /// <see cref="IAdmin.AlterReplicaLogDirs"/> takes
    /// (<c>_admin_replica_log_dir_assignments</c>).
    /// </summary>
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

    /// <summary>
    /// Binding <see cref="LogDirDescription"/> -&gt; proto (port of
    /// <c>_admin_log_dir_description_to_proto</c>). <c>error</c> is this <em>directory's</em>
    /// own error (offline / unreadable) — envelope exception 3 — never the per-broker error,
    /// which arrives as the entry's error arm instead of a value.
    /// </summary>
    internal static Proto.LogDirDescription LogDirDescriptionToProto(LogDirDescription description)
    {
        Proto.LogDirDescription proto = new Proto.LogDirDescription { IsCordoned = description.IsCordoned };
        if (description.Error is not null)
        {
            proto.Error = Translate.ToProto(description.Error);
        }

        // OptionalLong in Java: absent means the broker did not report it.
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

    /// <summary>
    /// Java's <c>ConfigEntry.ConfigSource</c> constant name. The enum has no numeric id, so the
    /// name is the contract at the C boundary.
    /// </summary>
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

    /// <summary>Java's <c>ConfigEntry.ConfigType</c> constant name. Same reasoning as
    /// <see cref="ConfigSourceName"/>.</summary>
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

    /// <summary>Binding <see cref="TopicPartitionInfo"/> -&gt; proto <c>TopicPartitionInfo</c>.</summary>
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

        // elr / last_known_elr are nullable in Java, hence the NodeList wrapper: absent is not
        // the same as reported-empty.
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

    /// <summary>Binding <see cref="TopicDescription"/> -&gt; proto <c>TopicDescription</c>.</summary>
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

        // Nullable: absent means the broker did not report the operations, which is not the same
        // as reporting that none are authorized.
        if (description.AuthorizedOperations is not null)
        {
            proto.AuthorizedOperations = AclOperationsToProto(description.AuthorizedOperations);
        }

        return proto;
    }

    /// <summary>Binding <see cref="TopicListing"/> -&gt; proto <c>AdminTopicListing</c>.</summary>
    internal static Proto.AdminTopicListing ListingToProto(TopicListing listing) => new Proto.AdminTopicListing
    {
        Name = listing.Name,
        TopicId = listing.TopicId.ToString(),
        IsInternal = listing.IsInternal,
    };

    // -- Elections, reassignments & offsets (slice G3) -------------------------------------

    /// <summary>
    /// A malformed <em>request</em> error — Python's <c>AdminRequestError</c>, which crosses as
    /// <c>LOCAL_ILLEGAL_ARGUMENT</c> rather than the generic branch's
    /// <c>LOCAL_ILLEGAL_STATE</c>: <c>code</c> is the field the Rust client matches on.
    /// </summary>
    internal static Proto.KafkaError RequestError(string message) => new Proto.KafkaError
    {
        Code = LocalIllegalArgumentCode,
        Message = $"dotnet server: AdminRequestError: {message}",
    };

    /// <summary>
    /// <c>optional TopicPartitionList partitions</c> -&gt; Java's <b>nullable</b>
    /// <c>Set&lt;TopicPartition&gt;</c> (port of <c>_admin_optional_partitions</c>). An absent
    /// message is null — every partition in the cluster; present-but-empty is an empty
    /// selection, a different broker request. Emptiness of the repeated field must not be the
    /// discriminant.
    /// </summary>
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

    /// <summary>
    /// Proto <c>PartitionReassignmentSpec</c>s -&gt; the map
    /// <see cref="IAdmin.AlterPartitionReassignments"/> takes (<c>_admin_reassignments</c>). An
    /// absent <c>reassignment</c> is Java's empty <c>Optional</c>, which <em>cancels</em> the
    /// partition's ongoing move; it must not become a present wrapper over an empty replica
    /// list, which Java rejects outright.
    /// </summary>
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

    /// <summary>Binding <see cref="PartitionReassignment"/> -&gt; proto.</summary>
    internal static Proto.PartitionReassignment ReassignmentToProto(PartitionReassignment reassignment)
    {
        Proto.PartitionReassignment proto = new Proto.PartitionReassignment();
        proto.Replicas.AddRange(reassignment.Replicas);
        proto.AddingReplicas.AddRange(reassignment.AddingReplicas);
        proto.RemovingReplicas.AddRange(reassignment.RemovingReplicas);
        return proto;
    }

    /// <summary>
    /// Proto <c>OffsetSpecEntry</c>s -&gt; the map <see cref="IAdmin.ListOffsets"/> takes (port
    /// of <c>_admin_offset_specs</c>). Each variant is reached through the binding's <b>named
    /// factory</b>, so the six <c>ListOffsets</c> sentinels come from this binding's own table
    /// rather than from one written in the harness.
    /// </summary>
    /// <returns>
    /// The map, or <see langword="null"/> with <paramref name="invalid"/> set.
    /// <c>KIND_UNSPECIFIED</c> and <c>FOR_TIMESTAMP</c> without a timestamp are protocol
    /// errors, never a defaulted variant: a dropped <c>kind</c> must fail the call rather than
    /// silently become <c>earliest()</c> and pass.
    /// </returns>
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

    /// <summary>
    /// Binding <see cref="ListOffsetsResult.ListOffsetsResultInfo"/> -&gt; proto. Java reports
    /// <c>-1</c> for a timestamp the broker did not send, and that sentinel crosses as-is;
    /// <c>leaderEpoch</c> is a real <c>Optional</c>, so absent is not epoch 0.
    /// </summary>
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

    // -- Producers & transactions (slice G6) ----------------------------------------------

    /// <summary>
    /// Binding <see cref="ProducerState"/> -&gt; proto. Both <c>Optional</c> columns stay
    /// absent when null: every <see langword="long"/> / <see langword="int"/> — 0 and -1
    /// included — is a legal coordinator epoch or transaction start offset.
    /// </summary>
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

    /// <summary>
    /// One partition's <c>activeProducers()</c> -&gt; proto. An <b>empty</b> list is a
    /// successful description of a partition with no producer state — not an error and not an
    /// absent value.
    /// </summary>
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

    /// <summary>
    /// Binding <see cref="TransactionDescription"/> -&gt; proto. <c>state</c> crosses as Java's
    /// <c>toString()</c> spelling (<see cref="TransactionStateName"/>);
    /// <c>transactionStartTimeMs</c> stays absent for a transaction that is not in progress,
    /// which is not start time 0.
    /// </summary>
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

    /// <summary>Binding <see cref="TransactionListing"/> -&gt; proto.</summary>
    internal static Proto.TransactionListing TransactionListingToProto(TransactionListing listing) =>
        new Proto.TransactionListing
        {
            TransactionalId = listing.TransactionalId,
            ProducerId = listing.ProducerId,
            State = TransactionStateName(listing.State),
        };

    /// <summary>
    /// <c>repeated string states</c> -&gt; <see cref="ListTransactionsOptions.FilteredStates"/>
    /// (port of <c>_admin_transaction_states</c>). Java's own default is an empty set meaning
    /// "every state", so an empty list needs no null form. Matching is <b>case-sensitive</b> and
    /// an unrecognised name decodes to <see cref="TransactionState.Unknown"/>, exactly as Java's
    /// <c>TransactionState.parse</c> does.
    /// </summary>
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

    /// <summary>
    /// <c>AbortTransactionRequest</c> -&gt; <see cref="AbortTransactionSpec"/> (port of
    /// <c>_admin_abort_transaction_spec</c>).
    /// </summary>
    /// <returns>
    /// The spec, or <see langword="null"/> with <paramref name="invalid"/> set. The partition is
    /// required — Java's <c>TopicPartition</c> has no null-topic form — and a
    /// <c>producer_epoch</c> outside <see langword="short"/> is a protocol error rather than
    /// something to truncate.
    /// </returns>
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

    // -- Groups (slice G4) ----------------------------------------------------------------

    /// <summary>
    /// A nullable <c>authorizedOperations</c> -&gt; proto <c>AclOperationList</c>. The enum's
    /// values <em>are</em> Java's <c>AclOperation.code()</c>, which is what the wire carries.
    /// </summary>
    internal static Proto.AclOperationList AclOperationsToProto(IEnumerable<AclOperation> operations)
    {
        Proto.AclOperationList list = new Proto.AclOperationList();
        foreach (AclOperation operation in operations)
        {
            list.Operations.Add((int)operation);
        }

        return list;
    }

    /// <summary>
    /// <c>repeated string group_states</c> -&gt; the filter the list options take. Matching is
    /// <b>case-insensitive</b> and an unrecognised name decodes to
    /// <see cref="GroupState.Unknown"/>, exactly as Java's <c>GroupState.parse</c>. An empty
    /// list is Java's empty set — the filter left unset — and needs no null form.
    /// </summary>
    internal static List<GroupState> GroupStates(IEnumerable<string> names) =>
        ParseNames(names, s_groupStatesByName, GroupState.Unknown);

    /// <summary>
    /// <c>repeated string types</c> -&gt; the group-type filter, on the same terms as
    /// <see cref="GroupStates"/> (Java's <c>GroupType.parse</c>).
    /// </summary>
    internal static List<GroupType> GroupTypes(IEnumerable<string> names) =>
        ParseNames(names, s_groupTypesByName, GroupType.Unknown);

    /// <summary>
    /// Binding <see cref="GroupListing"/> -&gt; proto. <c>group_type</c> / <c>group_state</c>
    /// are Java <c>Optional</c>s and stay absent when null — "the broker did not report a
    /// state" is not the empty string. <c>protocol</c> is the lower-case wire protocol-type
    /// string, unrelated to the type.
    /// </summary>
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

    // Java deprecates the listing type and its state enum; mirrored, not avoided.
#pragma warning disable CS0618
    /// <summary>
    /// Binding <see cref="ConsumerGroupListing"/> -&gt; proto. Both <c>group_state</c> and the
    /// deprecated <c>state</c> cross although Java derives the second from the first: both
    /// bindings expose both, so a dropped one is a finding.
    /// </summary>
    internal static Proto.ConsumerGroupListing ConsumerGroupListingToProto(ConsumerGroupListing listing)
    {
        Proto.ConsumerGroupListing proto = new Proto.ConsumerGroupListing
        {
            GroupId = listing.GroupId,
            IsSimpleConsumerGroup = listing.IsSimpleConsumerGroup,
        };
        if (listing.GroupState.HasValue)
        {
            proto.GroupState = listing.GroupState.Value.ToString();
        }

        if (listing.State.HasValue)
        {
            proto.State = listing.State.Value.ToString();
        }

        if (listing.Type.HasValue)
        {
            proto.GroupType = listing.Type.Value.ToString();
        }

        return proto;
    }
#pragma warning restore CS0618

    /// <summary>Binding <see cref="MemberAssignment"/> -&gt; proto.</summary>
    internal static Proto.MemberAssignment MemberAssignmentToProto(MemberAssignment assignment)
    {
        Proto.MemberAssignment proto = new Proto.MemberAssignment();
        foreach (TopicPartition partition in assignment.TopicPartitions)
        {
            proto.TopicPartitions.Add(Translate.TpToProto(partition));
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="MemberDescription"/> -&gt; proto. <c>group_instance_id</c> /
    /// <c>rack_id</c> / <c>target_assignment</c> / <c>member_epoch</c> / <c>upgraded</c> are
    /// Java <c>Optional</c>s and stay absent when null: a static member with an empty instance
    /// id is not a dynamic member, and an absent epoch is not epoch 0.
    /// </summary>
    /// <remarks>
    /// <c>assignment</c> is never absent here. Java's constructor coalesces a null assignment
    /// to an empty one (<c>MemberDescription.java:55</c>) and this binding mirrors that, so
    /// there is no null to leave absent — unlike Python, whose holder can carry one.
    /// </remarks>
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

    // Java deprecates state(); mirrored, not avoided.
#pragma warning disable CS0618
    /// <summary>
    /// Binding <see cref="ConsumerGroupDescription"/> -&gt; proto. The coordinator must carry
    /// the broker's real host and port, not a placeholder — the field this milestone exists
    /// for; a null coordinator stays absent.
    /// </summary>
    internal static Proto.ConsumerGroupDescription ConsumerGroupDescriptionToProto(
        ConsumerGroupDescription description)
    {
        Proto.ConsumerGroupDescription proto = new Proto.ConsumerGroupDescription
        {
            GroupId = description.GroupId,
            IsSimpleConsumerGroup = description.IsSimpleConsumerGroup,
            PartitionAssignor = description.PartitionAssignor,
            GroupType = description.Type.ToString(),
            State = description.State.ToString(),
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

        // Absent means the broker did not report the operations at all, which is not the same
        // as reporting that none are authorized.
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
#pragma warning restore CS0618

    /// <summary>
    /// Binding <see cref="ClassicGroupDescription"/> -&gt; proto. <c>protocol</c> (the
    /// protocol type) and <c>protocol_data</c> (the selected assignment strategy) are two
    /// different response fields; both cross so a transposition is detectable.
    /// </summary>
    /// <remarks>
    /// Java's <c>protocol()</c> is nullable and <c>isSimpleConsumerGroup()</c> dereferences it
    /// without a guard, so a null faults into the handler's whole-call <c>catch</c> rather
    /// than being invented as <c>""</c> — which is what the Python server does for the same
    /// input.
    /// </remarks>
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

    /// <summary>
    /// Proto <c>ListConsumerGroupOffsetsSpec</c>s -&gt; the map
    /// <see cref="IAdmin.ListConsumerGroupOffsets(IReadOnlyDictionary{string, ListConsumerGroupOffsetsSpec}, ListConsumerGroupOffsetsOptions)"/>
    /// takes (port of <c>_admin_group_offset_specs</c>). An absent <c>topic_partitions</c> is
    /// Java's <b>unset</b> collection — every partition the group has committed offsets for —
    /// and present-but-empty selects nothing; emptiness is never the discriminant.
    /// </summary>
    /// <returns>
    /// The map, or <see langword="null"/> with <paramref name="invalid"/> set. A repeated
    /// <c>group_id</c> is <b>rejected</b>, not silently de-duplicated: keying a dictionary by
    /// the wire field would let a second entry replace the first, which is exactly what the C
    /// entry point refuses.
    /// </returns>
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

    /// <summary>
    /// Proto <c>GroupOffsetCommit</c>s -&gt; the map
    /// <see cref="IAdmin.AlterConsumerGroupOffsets"/> takes (port of
    /// <c>_admin_group_offset_commits</c>). An absent <c>leader_epoch</c> is Java's empty
    /// <c>Optional</c> and must not become 0.
    /// </summary>
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

    /// <summary>
    /// <c>optional MemberToRemoveList members</c> -&gt; the selection
    /// <see cref="RemoveMembersFromConsumerGroupOptions"/> takes (port of
    /// <c>_admin_members_to_remove</c>).
    /// </summary>
    /// <returns>
    /// <see langword="null"/> for an absent list — Java's no-argument constructor, i.e.
    /// removeAll — and otherwise the (de-duplicated) selection, <b>including an empty one</b>,
    /// which is the collection constructor Java rejects. Presence is the discriminant, never
    /// emptiness.
    /// </returns>
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

    /// <summary>
    /// The de-duplicated request keys, in request order: the per-key result accessors
    /// (<c>PartitionResult</c> / <c>Description</c>) are keyed lookups, so a duplicated request
    /// key would otherwise emit a duplicated entry where Python's resolved map emits one.
    /// </summary>
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

    /// <summary>
    /// Java's <c>TransactionState.toString()</c> display spelling, which is the wire value. The
    /// enum members are spelled to match, so <see cref="Enum.ToString()"/> is that table.
    /// </summary>
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

    /// <summary>
    /// The <c>toString()</c>-spelling lookup for an enum whose members are spelled to match
    /// Java's display names, keyed case-insensitively (Java's <c>parse</c> lower-cases).
    /// </summary>
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

    /// <summary>
    /// Java's <c>parse</c> over a list of names: an unrecognised name becomes
    /// <paramref name="fallback"/> (the enum's <c>UNKNOWN</c>) rather than failing the call.
    /// </summary>
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
