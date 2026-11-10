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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Google.Protobuf;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Value &lt;-&gt; proto translation helpers shared by the gRPC servicers, the C# port of Python's
/// <c>grpc_translate.py</c>.
/// </summary>
internal static class Translate
{
    // The kind strings are a cross-backend wire contract (producer_service.proto's
    // CallbackLogEntry): every server emits the same lowercase values.
    internal const string KindAssigned = "assigned";

    internal const string KindRevoked = "revoked";

    internal const string KindLost = "lost";

    internal const string KindCommit = "commit";

    internal const string KindDelivery = "delivery";

    internal static string OffsetKey(string topic, int partition) => $"{topic}-{partition}";

    internal static Dictionary<string, object?> ConfigsFromProto(IReadOnlyDictionary<string, string> config)
    {
        var configs = new Dictionary<string, object?>(config.Count);
        foreach (KeyValuePair<string, string> entry in config)
        {
            configs.Add(entry.Key, entry.Value);
        }

        return configs;
    }

    internal static Proto.CallbackLogPartition CallbackLogPartitionToProto(TopicPartition partition) =>
        new Proto.CallbackLogPartition
        {
            Topic = partition.Topic(),
            Partition = partition.Partition(),
        };

    internal static Dictionary<TopicPartition, OffsetAndMetadata> ProtoOffsetsToDictionary(
        IEnumerable<Proto.OffsetMapEntry> offsets)
    {
        Dictionary<TopicPartition, OffsetAndMetadata> result = new Dictionary<TopicPartition, OffsetAndMetadata>();
        foreach (Proto.OffsetMapEntry entry in offsets)
        {
            int? leaderEpoch = entry.Offset.HasLeaderEpoch ? entry.Offset.LeaderEpoch : (int?)null;
            result[Tp(entry.Partition)] = new OffsetAndMetadata(entry.Offset.Offset, entry.Offset.Metadata, leaderEpoch);
        }

        return result;
    }

    /// <summary>
    /// A transaction's offsets: an absent metadata is the empty string and an absent leader epoch none,
    /// as Python's <c>_proto_offset_entries_to_dict</c> reads them.
    /// </summary>
    internal static Dictionary<TopicPartition, OffsetAndMetadata> OffsetEntriesToDictionary(
        IEnumerable<Proto.OffsetEntry> offsets)
    {
        Dictionary<TopicPartition, OffsetAndMetadata> result = new Dictionary<TopicPartition, OffsetAndMetadata>();
        foreach (Proto.OffsetEntry entry in offsets)
        {
            int? leaderEpoch = entry.HasLeaderEpoch ? entry.LeaderEpoch : (int?)null;
            string metadata = entry.HasMetadata ? entry.Metadata : string.Empty;
            result[new TopicPartition(entry.Topic, entry.Partition)] = new OffsetAndMetadata(entry.Offset, metadata, leaderEpoch);
        }

        return result;
    }

    internal const string PatternSubscribeUnsupported = "pattern subscribe is not supported by the .NET binding yet";

    internal static TimeSpan? ConsumerTimeout(bool hasTimeoutMs, long timeoutMs) =>
        hasTimeoutMs ? TimeSpan.FromMilliseconds(timeoutMs) : null;

    internal static Proto.ConsumerGroupMetadata GroupMetadataToProto(ConsumerGroupMetadata groupMetadata)
    {
        Proto.ConsumerGroupMetadata proto = new Proto.ConsumerGroupMetadata
        {
            GroupId = groupMetadata.GroupId(),
            GenerationId = groupMetadata.GenerationId(),
            MemberId = groupMetadata.MemberId(),
        };

        if (groupMetadata.GroupInstanceId() is not null)
        {
            proto.GroupInstanceId = groupMetadata.GroupInstanceId();
        }

        return proto;
    }

    internal static CloseOptions? CloseOptionsFromProto(Proto.ConsumerCloseRequest request)
    {
        if (!request.HasGroupMembershipOperation)
        {
            return null;
        }

        CloseOptions options = CloseOptions.GroupMembershipOperation(GroupMembershipOperationFromName(request.GroupMembershipOperation));
        return request.HasCloseTimeoutMs ? options.WithTimeout(TimeSpan.FromMilliseconds(request.CloseTimeoutMs)) : options;
    }

    internal static Proto.CurrentLagResponse CurrentLagResponse(long? lag) =>
        lag.HasValue ? new Proto.CurrentLagResponse { Lag = lag.Value } : new Proto.CurrentLagResponse();

    private const int LocalIllegalArgumentCode = -3;

    private const int LocalIllegalStateCode = -4;

    private const int RequestTimedOutCode = 7;

    // As Python's _kafka_error_to_proto sends each class's _ffi_id: a class with an id (a KafkaException,
    // a Java-named class, CorrelationIdMismatchException) sends it; a .NET-only ArgumentException
    // precondition -3. Any other exception has no id, Python's TypeError (ArgumentNullException) and the
    // Java-named classes without one included, and takes the server-made -4 fallback. A future's Get
    // throws Java's ExecutionException around the failure; the failure is what is sent, as Python's
    // _unwrapped sends an ExecutionError's cause.
    internal static Proto.KafkaError ToProto(Exception error)
    {
        Exception sent = Unwrapped(error);
        return KafkaException.FfiIdOf(sent) switch
        {
            int code => Coded(code, sent),
            null => sent switch
            {
                ArgumentNullException => ServerMade(sent),
                IndexOutOfBoundsException => ServerMade(sent),
                ArgumentException => Coded(LocalIllegalArgumentCode, sent),
                _ => ServerMade(sent),
            },
        };
    }

    // The error behind a future's Get failure: Java's ExecutionException wraps the cause the future
    // failed with. Any other error is itself.
    internal static Exception Unwrapped(Exception error) =>
        error is ExecutionException { InnerException: { } cause } ? cause : error;

    private static GroupMembershipOperation GroupMembershipOperationFromName(string name) => name switch
    {
        "LEAVE_GROUP" => GroupMembershipOperation.LEAVE_GROUP,
        "REMAIN_IN_GROUP" => GroupMembershipOperation.REMAIN_IN_GROUP,
        "DEFAULT" => GroupMembershipOperation.DEFAULT,
        _ => throw new KeyNotFoundException($"'{name}'"),
    };

    private static Proto.KafkaError Coded(int code, Exception error) => new Proto.KafkaError
    {
        Code = code,
        Message = WithoutDotNetSuffix(error),
    };

    // The message without what .NET appends to it, so the text is Java's, as Python sends it. The suffix
    // is read from a probe of the same type, so it matches this runtime and culture.
    private static string WithoutDotNetSuffix(Exception error)
    {
        string message = error.Message ?? string.Empty;
        string suffix = error switch
        {
            ArgumentOutOfRangeException e => new ArgumentOutOfRangeException(e.ParamName, e.ActualValue, string.Empty).Message,
            ArgumentException e => new ArgumentException(string.Empty, e.ParamName).Message,
            _ => string.Empty,
        };

        return suffix.Length > 0 && message.EndsWith(suffix, StringComparison.Ordinal)
            ? message.Substring(0, message.Length - suffix.Length)
            : message;
    }

    private static Proto.KafkaError ServerMade(Exception error) => new Proto.KafkaError
    {
        Code = LocalIllegalStateCode,
        Message = $"dotnet server: {error.GetType().Name}: {error.Message}",
    };

    internal static Proto.KafkaError UnknownConsumer(ulong consumerId) => new Proto.KafkaError
    {
        Code = LocalIllegalStateCode,
        Message = $"unknown consumer_id {consumerId}",
    };

    internal static Proto.KafkaError UnknownProducer(ulong producerId) => new Proto.KafkaError
    {
        Code = LocalIllegalStateCode,
        Message = $"unknown producer_id {producerId}",
    };

    internal static Proto.KafkaError UnknownGroupMetadata(ulong groupMetadataId) => new Proto.KafkaError
    {
        Code = LocalIllegalStateCode,
        Message = $"unknown group_metadata_id {groupMetadataId}",
    };

    internal static Proto.KafkaError SendTimedOut() => new Proto.KafkaError
    {
        Code = RequestTimedOutCode,
        Message = "dotnet server: producer future timed out after 120s",
    };

    internal static Proto.KafkaError UnknownAdmin(ulong adminId) => new Proto.KafkaError
    {
        Code = LocalIllegalStateCode,
        Message = $"unknown admin_id {adminId}",
    };

    internal static TopicPartition Tp(Proto.TopicPartition partition) =>
        new TopicPartition(partition.Topic, partition.Partition);

    internal static Proto.TopicPartition TpToProto(TopicPartition partition) => new Proto.TopicPartition
    {
        Topic = partition.Topic(),
        Partition = partition.Partition(),
    };

    internal static Proto.OffsetAndMetadata OamToProto(OffsetAndMetadata oam)
    {
        Proto.OffsetAndMetadata proto = new Proto.OffsetAndMetadata
        {
            Offset = oam.Offset(),
            Metadata = oam.Metadata() ?? string.Empty,
        };
        if (oam.LeaderEpoch() is int leaderEpoch)
        {
            proto.LeaderEpoch = leaderEpoch;
        }

        return proto;
    }

    internal static Proto.OffsetAndTimestamp OatToProto(OffsetAndTimestamp oat)
    {
        Proto.OffsetAndTimestamp proto = new Proto.OffsetAndTimestamp
        {
            Offset = oat.Offset(),
            Timestamp = oat.Timestamp(),
        };
        if (oat.LeaderEpoch() is int leaderEpoch)
        {
            proto.LeaderEpoch = leaderEpoch;
        }

        return proto;
    }

    internal static Proto.Node NodeToProto(Node node)
    {
        Proto.Node proto = new Proto.Node
        {
            Id = node.Id(),
            Host = node.Host(),
            Port = node.Port(),
            IsFenced = node.IsFenced(),
        };
        if (node.Rack() is not null)
        {
            proto.Rack = node.Rack();
        }

        return proto;
    }

    internal static Proto.PartitionInfo PartitionInfoToProto(PartitionInfo info)
    {
        Proto.PartitionInfo proto = new Proto.PartitionInfo
        {
            Topic = info.Topic(),
            Partition = info.Partition(),
        };
        if (info.Leader() is { } leader)
        {
            proto.Leader = NodeToProto(leader);
        }

        foreach (Node node in info.Replicas())
        {
            proto.Replicas.Add(NodeToProto(node));
        }

        foreach (Node node in info.InSyncReplicas())
        {
            proto.InSyncReplicas.Add(NodeToProto(node));
        }

        foreach (Node node in info.OfflineReplicas())
        {
            proto.OfflineReplicas.Add(NodeToProto(node));
        }

        return proto;
    }

    internal static Proto.ConsumerRecord RecordToProto(ConsumerRecord<byte[], byte[]> record)
    {
        Proto.ConsumerRecord proto = new Proto.ConsumerRecord
        {
            Topic = record.Topic(),
            Partition = record.Partition(),
            Offset = record.Offset(),
            Timestamp = record.Timestamp(),
            TimestampType = (int)record.TimestampType(),
        };

        if (record.Key() is not null)
        {
            proto.Key = ByteString.CopyFrom(record.Key());
        }

        if (record.Value() is not null)
        {
            proto.Value = ByteString.CopyFrom(record.Value());
        }

        if (record.LeaderEpoch() is int le)
        {
            proto.LeaderEpoch = le;
        }

        foreach (Header header in record.Headers())
        {
            proto.Headers.Add(new Proto.Header
            {
                Key = header.Key(),
                Value = ByteString.CopyFrom(header.Value() ?? Array.Empty<byte>()),
            });
        }

        return proto;
    }

    internal static ProducerRecord<byte[], byte[]> ProducerRecordFromProto(Proto.ProducerRecord proto)
    {
        byte[]? key = proto.HasKey ? proto.Key.ToByteArray() : null;
        byte[]? value = proto.HasValue ? proto.Value.ToByteArray() : null;
        int? partition = proto.HasPartition ? proto.Partition : (int?)null;
        long? timestamp = proto.HasTimestamp ? proto.Timestamp : (long?)null;

        // The headers are sent, as C's server sends them; Python's drops them. A proto header's value is never null, so
        // an empty value is sent empty.
        Header[]? headers = null;
        if (proto.Headers.Count > 0)
        {
            headers = new Header[proto.Headers.Count];
            for (int i = 0; i < headers.Length; i++)
            {
                Proto.Header header = proto.Headers[i];
                headers[i] = new Header(header.Key, header.Value.ToByteArray());
            }
        }

        return new ProducerRecord<byte[], byte[]>(proto.Topic, value, key, partition, timestamp, headers);
    }

    internal static Proto.RecordMetadata MetadataToProto(RecordMetadata metadata) => new Proto.RecordMetadata
    {
        Offset = metadata.Offset(),
        Timestamp = metadata.Timestamp(),
        SerializedKeySize = metadata.SerializedKeySize(),
        SerializedValueSize = metadata.SerializedValueSize(),
        Topic = metadata.Topic(),
        Partition = metadata.Partition(),
    };

    internal static Proto.Metric MetricToProto(MetricName name, IMetric metric)
    {
        Proto.Metric proto = new Proto.Metric
        {
            Name = name.Name(),
            Group = name.Group(),
            Description = name.Description(),
        };

        foreach (KeyValuePair<string, string> tag in name.Tags())
        {
            proto.Tags.Add(tag.Key, tag.Value);
        }

        switch (metric.MetricValue())
        {
            case double doubleValue:
                proto.DoubleValue = doubleValue;
                break;
            case string stringValue:
                proto.StringValue = stringValue;
                break;
            case long longValue:
                proto.LongValue = longValue;
                break;
            case int intValue:
                proto.IntValue = intValue;
                break;
            default:
                throw new InvalidOperationException(
                    $"unexpected metric value type '{metric.MetricValue()?.GetType().FullName ?? "null"}' for metric '{name.Name()}'");
        }

        return proto;
    }
}
