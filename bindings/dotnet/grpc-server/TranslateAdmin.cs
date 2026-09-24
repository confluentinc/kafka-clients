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
            Proto.AclOperationList operations = new Proto.AclOperationList();
            foreach (AclOperation operation in description.AuthorizedOperations)
            {
                operations.Operations.Add((int)operation);
            }

            proto.AuthorizedOperations = operations;
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
