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

using Google.Protobuf;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Pure value &lt;-&gt; proto translation helpers for the .NET consumer gRPC backend — the
/// C# port of <c>bindings/python/grpc_translate.py</c>. Converts between the generated
/// protobuf messages and the binding's public value types, and infers a proto
/// <c>KafkaError.Variant</c> from a flat <see cref="KafkaException"/> message.
/// </summary>
/// <remarks>
/// The central non-trivial piece is <see cref="GuessVariant"/>: the flat
/// <see cref="KafkaException"/> carries no variant discriminator (only
/// <see cref="KafkaException.Code"/> / <see cref="KafkaException.IsRetriable"/> /
/// <see cref="KafkaException.IsFatal"/> / message), but the proto <c>KafkaError</c> has a
/// <c>variant</c> enum the Rust client <c>matches!</c> on — so it must be sniffed from the
/// message string, a faithful, ordered, first-match-wins port of
/// <c>grpc_translate.py</c>'s <c>_guess_variant</c> (case-insensitive).
/// </remarks>
internal static class Translate
{
    /// <summary>
    /// <c>CallbackLogEntry.kind</c> for <c>IConsumerRebalanceListener.OnPartitionsAssigned</c>.
    /// </summary>
    /// <remarks>
    /// The <c>kind</c> values are a cross-backend wire contract — <c>producer_service.proto</c>'s
    /// <c>CallbackLogEntry</c> table (<c>:214-222</c>): every server must emit the identical
    /// lowercase string or the one shared Rust test body fails on this backend alone. This
    /// assembly hosts <em>both</em> gRPC services (Program.cs), so every value in that table is
    /// defined here: the rebalance kinds and <see cref="KindCommit"/> are emitted by the consumer
    /// servicers, and <see cref="KindDelivery"/> by the producer servicers (M14/P2).
    /// <para>
    /// This remark used to say <c>"delivery"</c> was "deliberately not defined here" because
    /// "there is no .NET producer backend". That stopped being true when M12/P1 added the two
    /// producer servicers, and it stayed stale until M14/P2 gave them a real
    /// <see cref="IDeliveryCallback"/> to log.
    /// </para>
    /// </remarks>
    internal const string KindAssigned = "assigned";

    /// <summary>
    /// <c>CallbackLogEntry.kind</c> for <c>IConsumerRebalanceListener.OnPartitionsRevoked</c>.
    /// </summary>
    internal const string KindRevoked = "revoked";

    /// <summary>
    /// <c>CallbackLogEntry.kind</c> for <c>IConsumerRebalanceListener.OnPartitionsLost</c>.
    /// </summary>
    internal const string KindLost = "lost";

    /// <summary>
    /// <c>CallbackLogEntry.kind</c> for <c>IOffsetCommitCallback.OnComplete</c>.
    /// </summary>
    internal const string KindCommit = "commit";

    /// <summary>
    /// <c>CallbackLogEntry.kind</c> for <see cref="IDeliveryCallback.OnCompletion"/> — the
    /// producer's delivery callback, emitted by both producer servicers when
    /// <c>SendRequest.with_callback</c> is set (M14/P2).
    /// </summary>
    internal const string KindDelivery = "delivery";

    /// <summary>
    /// The <c>CallbackLogEntry.offsets</c> key for a partition:
    /// <c>"&lt;topic&gt;-&lt;partition&gt;"</c> with a plain hyphen (e.g. <c>"my-topic-0"</c>) —
    /// the port of <c>grpc_translate.py</c>'s <c>_offset_key</c> (<c>:282-284</c>) and C's
    /// <c>offset_key</c> (<c>server.cc:209-211</c>). Part of the cross-backend wire contract; the
    /// Rust harness rebuilds the same key in <c>CallbackLogEntry::offset_key</c>.
    /// </summary>
    internal static string OffsetKey(string topic, int partition) => $"{topic}-{partition}";

    /// <summary>
    /// Binding <see cref="TopicPartition"/> -&gt; proto <c>CallbackLogPartition</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ Deliberately <b>not</b> <see cref="TpToProto"/>: a <c>CallbackLogEntry</c> carries
    /// <c>CallbackLogPartition</c>, a distinct message declared in <c>producer_service.proto</c>
    /// rather than <c>consumer_service.proto</c>'s <c>TopicPartition</c>. The two are
    /// field-identical but not interchangeable — the entry types are shared by both services and
    /// live in the imported proto, which cannot reference the importing one (that would be a
    /// cycle).
    /// </remarks>
    internal static Proto.CallbackLogPartition CallbackLogPartitionToProto(TopicPartition partition) =>
        new Proto.CallbackLogPartition
        {
            Topic = partition.Topic,
            Partition = partition.Partition,
        };

    /// <summary>
    /// Proto <c>OffsetMapEntry</c> list -&gt; the binding's commit-offsets dictionary — the port
    /// of <c>grpc_translate.py</c>'s <c>_proto_offsets_to_dict</c>. <c>leader_epoch</c> uses
    /// proto3 optional-presence: forwarded when present, <see langword="null"/> when absent.
    /// </summary>
    /// <remarks>
    /// Shared by <c>CommitSync</c> and <c>CommitAsync</c> in both servicers (Python shares the
    /// same helper between its two commit handlers). It was inlined in <c>CommitSync</c> until
    /// M9/P9 hoisted it here rather than copy-pasting it into a third and fourth site.
    /// </remarks>
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
    /// Infers the proto <c>KafkaError.Variant</c> from a <see cref="KafkaException"/>
    /// message — an exact, ordered, first-match-wins port of
    /// <c>grpc_translate.py</c>'s <c>_guess_variant</c> over
    /// <c>message.ToLowerInvariant()</c>. <c>ILLEGAL_ARGUMENT</c> is defined by the proto
    /// but never emitted (Python never emits it either — kept that way deliberately).
    /// </summary>
    internal static Proto.KafkaError.Types.Variant GuessVariant(string? message)
    {
        if (string.IsNullOrEmpty(message))
        {
            return Proto.KafkaError.Types.Variant.Generic;
        }

        string lowered = message!.ToLowerInvariant();

        if (Has(lowered, "max.request.size") || Has(lowered, "is larger than") || Has(lowered, "too large"))
        {
            return Proto.KafkaError.Types.Variant.RecordTooLarge;
        }

        if (Has(lowered, "buffer is full") || Has(lowered, "buffer.memory"))
        {
            return Proto.KafkaError.Types.Variant.BufferExhausted;
        }

        if (Has(lowered, "timed out") || Has(lowered, "expired") || Has(lowered, "not present in metadata"))
        {
            return Proto.KafkaError.Types.Variant.Timeout;
        }

        if (Has(lowered, "topic authorization"))
        {
            return Proto.KafkaError.Types.Variant.TopicAuthorization;
        }

        if (Has(lowered, "invalid topic"))
        {
            return Proto.KafkaError.Types.Variant.InvalidTopic;
        }

        if (Has(lowered, "group authorization"))
        {
            return Proto.KafkaError.Types.Variant.GroupAuthorization;
        }

        if (Has(lowered, "illegal state") || Has(lowered, "already been closed"))
        {
            return Proto.KafkaError.Types.Variant.IllegalState;
        }

        if (Has(lowered, "serialization") || Has(lowered, "failed to serialize"))
        {
            return Proto.KafkaError.Types.Variant.Serialization;
        }

        return Proto.KafkaError.Types.Variant.Generic;
    }

    /// <summary>
    /// Translates a binding <see cref="KafkaException"/> (or any other
    /// <see cref="Exception"/>) into a proto <c>KafkaError</c> — the port of
    /// <c>grpc_translate.py</c>'s <c>_kafka_error_to_proto</c>. A <see cref="KafkaException"/>
    /// keeps its code / retriable / fatal flags and gets a sniffed variant; any other
    /// exception surfaces as <c>ILLEGAL_STATE</c> with a <c>"dotnet server: &lt;Type&gt;:
    /// &lt;msg&gt;"</c> message so the Rust client sees a clear server-side signal.
    /// </summary>
    internal static Proto.KafkaError ToProto(Exception error)
    {
        if (error is KafkaException ke)
        {
            string message = ke.Message ?? string.Empty;
            return new Proto.KafkaError
            {
                Variant = GuessVariant(message),
                Code = ke.Code,
                Message = message,
                IsRetriable = ke.IsRetriable,
                IsFatal = ke.IsFatal,
            };
        }

        return new Proto.KafkaError
        {
            Variant = Proto.KafkaError.Types.Variant.IllegalState,
            Code = -1,
            Message = $"dotnet server: {error.GetType().Name}: {error.Message}",
            IsRetriable = false,
            IsFatal = true,
        };
    }

    /// <summary>
    /// Builds the hand-crafted <c>ILLEGAL_STATE</c> error returned when an RPC names an
    /// unknown <c>consumer_id</c> (Python parity — <c>grpc_server.py</c> returns the same
    /// <c>{variant=ILLEGAL_STATE, code=-1, "unknown consumer_id N", retriable=false,
    /// fatal=true}</c>).
    /// </summary>
    internal static Proto.KafkaError UnknownConsumer(ulong consumerId) => new Proto.KafkaError
    {
        Variant = Proto.KafkaError.Types.Variant.IllegalState,
        Code = -1,
        Message = $"unknown consumer_id {consumerId}",
        IsRetriable = false,
        IsFatal = true,
    };

    /// <summary>
    /// The producer sibling of <see cref="UnknownConsumer"/>: the hand-crafted
    /// <c>ILLEGAL_STATE</c> error returned when a producer RPC names an unknown
    /// <c>producer_id</c> (Python parity — <c>grpc_server.py</c>'s <c>ProducerService</c>
    /// returns the same <c>{variant=ILLEGAL_STATE, code=-1, "unknown producer_id N",
    /// retriable=false, fatal=true}</c>).
    /// </summary>
    internal static Proto.KafkaError UnknownProducer(ulong producerId) => new Proto.KafkaError
    {
        Variant = Proto.KafkaError.Types.Variant.IllegalState,
        Code = -1,
        Message = $"unknown producer_id {producerId}",
        IsRetriable = false,
        IsFatal = true,
    };

    /// <summary>Proto <c>TopicPartition</c> -&gt; binding <see cref="TopicPartition"/>.</summary>
    internal static TopicPartition Tp(Proto.TopicPartition partition) =>
        new TopicPartition(partition.Topic, partition.Partition);

    /// <summary>Binding <see cref="TopicPartition"/> -&gt; proto <c>TopicPartition</c>.</summary>
    internal static Proto.TopicPartition TpToProto(TopicPartition partition) => new Proto.TopicPartition
    {
        Topic = partition.Topic,
        Partition = partition.Partition,
    };

    /// <summary>
    /// Binding <see cref="OffsetAndMetadata"/> -&gt; proto <c>OffsetAndMetadata</c>.
    /// <c>leader_epoch</c> uses proto3 optional-presence: set only when non-null.
    /// </summary>
    internal static Proto.OffsetAndMetadata OamToProto(OffsetAndMetadata oam)
    {
        Proto.OffsetAndMetadata proto = new Proto.OffsetAndMetadata
        {
            Offset = oam.Offset,
            Metadata = oam.Metadata ?? string.Empty,
        };
        if (oam.LeaderEpoch is int leaderEpoch)
        {
            proto.LeaderEpoch = leaderEpoch;
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="OffsetAndTimestamp"/> -&gt; proto <c>OffsetAndTimestamp</c>.
    /// <c>leader_epoch</c> uses proto3 optional-presence: set only when non-null.
    /// </summary>
    internal static Proto.OffsetAndTimestamp OatToProto(OffsetAndTimestamp oat)
    {
        Proto.OffsetAndTimestamp proto = new Proto.OffsetAndTimestamp
        {
            Offset = oat.Offset,
            Timestamp = oat.Timestamp,
        };
        if (oat.LeaderEpoch is int leaderEpoch)
        {
            proto.LeaderEpoch = leaderEpoch;
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="Node"/> -&gt; proto <c>Node</c>. <c>rack</c> uses proto3
    /// optional-presence: set only when non-null.
    /// </summary>
    internal static Proto.Node NodeToProto(Node node)
    {
        Proto.Node proto = new Proto.Node
        {
            Id = node.Id,
            Host = node.Host,
            Port = node.Port,
        };
        if (node.Rack is not null)
        {
            proto.Rack = node.Rack;
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="PartitionInfo"/> -&gt; proto <c>PartitionInfo</c>. The leader is
    /// optional (absent when the partition has no leader); replica lists are never-null
    /// node lists.
    /// </summary>
    internal static Proto.PartitionInfo PartitionInfoToProto(PartitionInfo info)
    {
        Proto.PartitionInfo proto = new Proto.PartitionInfo
        {
            Topic = info.Topic,
            Partition = info.Partition,
        };
        if (info.Leader is not null)
        {
            proto.Leader = NodeToProto(info.Leader);
        }

        foreach (Node node in info.Replicas)
        {
            proto.Replicas.Add(NodeToProto(node));
        }

        foreach (Node node in info.InSyncReplicas)
        {
            proto.InSyncReplicas.Add(NodeToProto(node));
        }

        foreach (Node node in info.OfflineReplicas)
        {
            proto.OfflineReplicas.Add(NodeToProto(node));
        }

        return proto;
    }

    /// <summary>
    /// Binding <see cref="ConsumerRecord{TKey, TValue}"/> (bytes/bytes) -&gt; proto
    /// <c>ConsumerRecord</c>. <c>key</c> / <c>value</c> use proto3 optional-presence: absent
    /// (null) key/value is omitted, a present-empty payload is a zero-length
    /// <see cref="ByteString"/>. <c>leader_epoch</c> is forwarded when present and omitted
    /// when absent (the M9/P1 accessor closed the gap that PLAN §2.1 recorded — Python
    /// parity; do not fabricate a value). A null header value maps to an empty byte string
    /// (Python parity). The serialized key/value sizes are binding-only — the consumer proto
    /// has no fields for them, so they are not forwarded.
    /// </summary>
    internal static Proto.ConsumerRecord RecordToProto(ConsumerRecord<byte[], byte[]> record)
    {
        Proto.ConsumerRecord proto = new Proto.ConsumerRecord
        {
            Topic = record.Topic,
            Partition = record.Partition,
            Offset = record.Offset,
            Timestamp = record.Timestamp,
            TimestampType = (int)record.TimestampType,
        };

        if (record.Key is not null)
        {
            proto.Key = ByteString.CopyFrom(record.Key);
        }

        if (record.Value is not null)
        {
            proto.Value = ByteString.CopyFrom(record.Value);
        }

        if (record.LeaderEpoch is int le)
        {
            proto.LeaderEpoch = le;
        }

        foreach (Header header in record.Headers)
        {
            proto.Headers.Add(new Proto.Header
            {
                Key = header.Key,
                Value = ByteString.CopyFrom(header.Value ?? Array.Empty<byte>()),
            });
        }

        return proto;
    }

    /// <summary>
    /// Proto <c>ProducerRecord</c> -&gt; binding <see cref="ProducerRecord{TKey, TValue}"/>
    /// (bytes/bytes) — the C# port of <c>grpc_translate.py</c>'s
    /// <c>_proto_to_producer_record</c>. <c>partition</c> / <c>timestamp</c> / <c>key</c> /
    /// <c>value</c> are three-state via proto3 optional-presence: absent maps to
    /// <see langword="null"/> (producer chooses / no key / tombstone), a present payload maps
    /// to its value (a present-empty <see cref="ByteString"/> becomes an empty
    /// <c>byte[]</c>). This is more faithful than Python for <c>value</c> (Python's wrapper
    /// rejects a null value and substitutes an empty payload); the .NET generic
    /// <see cref="ProducerRecord{TKey, TValue}"/> models a null-value tombstone directly.
    /// Incoming proto headers are DROPPED — the .NET <see cref="ProducerRecord{TKey, TValue}"/>
    /// has no headers today (Python parity — its wrapper drops them too), and no
    /// <c>multilanguage_test!</c> scenario sends headers.
    /// </summary>
    internal static ProducerRecord<byte[], byte[]> ProducerRecordFromProto(Proto.ProducerRecord proto)
    {
        byte[]? key = proto.HasKey ? proto.Key.ToByteArray() : null;
        byte[]? value = proto.HasValue ? proto.Value.ToByteArray() : null;
        int? partition = proto.HasPartition ? proto.Partition : (int?)null;
        long? timestamp = proto.HasTimestamp ? proto.Timestamp : (long?)null;
        return new ProducerRecord<byte[], byte[]>(proto.Topic, value, key, partition, timestamp);
    }

    /// <summary>
    /// Binding <see cref="RecordMetadata"/> -&gt; proto <c>RecordMetadata</c> — the C# port of
    /// <c>grpc_translate.py</c>'s <c>_record_metadata_to_proto</c>. The serialized key/value
    /// sizes are emitted as <c>-1</c> (Python parity — the .NET <see cref="RecordMetadata"/>
    /// exposes no serialized-size accessors today, and <c>-1</c> lets the Rust client's
    /// <c>RecordMetadata::new</c> construct validly).
    /// </summary>
    internal static Proto.RecordMetadata MetadataToProto(RecordMetadata metadata) => new Proto.RecordMetadata
    {
        Offset = metadata.Offset,
        Timestamp = metadata.Timestamp,
        SerializedKeySize = -1,
        SerializedValueSize = -1,
        Topic = metadata.Topic,
        Partition = metadata.Partition,
    };

    /// <summary>
    /// One entry of a <c>metrics()</c> snapshot
    /// (<c>(MetricName, IMetric)</c>) -&gt; proto <c>Metric</c> — the C# port of
    /// <c>grpc_translate.py</c>'s <c>_metric_to_proto</c> and the C++ server's <c>Metrics</c>
    /// value switch (<c>bindings/c/grpc_server/server.cc</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Value oneof dispatched on the boxed CLR runtime type</b> of <see cref="IMetric.Value"/>:
    /// <see cref="double"/> -&gt; <c>double_value</c>, <see cref="string"/> -&gt;
    /// <c>string_value</c>, <see cref="long"/> -&gt; <c>long_value</c>, <see cref="int"/> -&gt;
    /// <c>int_value</c>. The binding preserves the <c>Long</c>/<c>Int</c> distinction in the
    /// boxed type (<see cref="IMetric"/> "the boxed type is the kind" — no <c>Kind</c> member),
    /// so no explicit discriminator is needed (unlike Python's single <c>int</c> + <c>kind</c>).
    /// </para>
    /// <para>
    /// <b>Unmatched type -&gt; explicit failure (D2).</b> If <see cref="IMetric.Value"/> is none
    /// of those four, this throws an <see cref="InvalidOperationException"/> naming the
    /// unexpected type rather than silently defaulting to <c>double_value</c> — a future core
    /// <c>MetricValue</c> variant divergence surfaces loudly (the servicer's <c>catch</c> routes
    /// it through <see cref="ToProto(Exception)"/> to a proto <c>KafkaError</c>).
    /// </para>
    /// </remarks>
    internal static Proto.Metric MetricToProto(MetricName name, IMetric metric)
    {
        Proto.Metric proto = new Proto.Metric
        {
            Name = name.Name,
            Group = name.Group,
            Description = name.Description,
        };

        foreach (KeyValuePair<string, string> tag in name.Tags)
        {
            proto.Tags.Add(tag.Key, tag.Value);
        }

        switch (metric.Value)
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
                    $"unexpected metric value type '{metric.Value?.GetType().FullName ?? "null"}' for metric '{name.Name}'");
        }

        return proto;
    }

    private static bool Has(string haystack, string needle) =>
        haystack.Contains(needle, StringComparison.Ordinal);
}
