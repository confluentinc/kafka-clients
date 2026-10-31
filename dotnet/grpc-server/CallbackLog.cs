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
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Thread-safe, per-client-id log of callback invocations, the C# port of Python's <c>CallbackLog</c>.
/// Each servicer owns one and reads it back in <c>GetCallbackLog</c>; entries are never evicted, so
/// they survive <c>Close</c>, as in Python and C.
/// </summary>
internal sealed class CallbackLog
{
    // Its own lock, not a consumer servicer's per-entry Gate: a callback that fires inside a Poll
    // must not wait for that Poll, and GetCallbackLog must not either.
    private readonly object _gate = new object();
    private readonly Dictionary<ulong, List<Proto.CallbackLogEntry>> _entries =
        new Dictionary<ulong, List<Proto.CallbackLogEntry>>();

    internal void Append(
        ulong clientId,
        string kind,
        IEnumerable<TopicPartition>? partitions = null,
        IReadOnlyDictionary<string, long>? offsets = null,
        string? error = null)
    {
        Proto.CallbackLogEntry entry = new Proto.CallbackLogEntry
        {
            Kind = kind,
            Error = error ?? string.Empty,
        };

        if (partitions is not null)
        {
            foreach (TopicPartition partition in partitions)
            {
                entry.Partitions.Add(Translate.CallbackLogPartitionToProto(partition));
            }
        }

        if (offsets is not null)
        {
            foreach (KeyValuePair<string, long> pair in offsets)
            {
                // Indexer, not Add: nothing here may throw inside a callback, where it would
                // propagate out of the consumer call that ran it.
                entry.Offsets[pair.Key] = pair.Value;
            }
        }

        lock (_gate)
        {
            if (!_entries.TryGetValue(clientId, out List<Proto.CallbackLogEntry>? list))
            {
                list = new List<Proto.CallbackLogEntry>();
                _entries[clientId] = list;
            }

            list.Add(entry);
        }
    }

    internal Proto.CallbackLogResponse Response(ulong clientId)
    {
        Proto.CallbackLogResponse response = new Proto.CallbackLogResponse();
        lock (_gate)
        {
            if (_entries.TryGetValue(clientId, out List<Proto.CallbackLogEntry>? list))
            {
                response.Entries.Add(list);
            }
        }

        return response;
    }
}

/// <summary>
/// An <see cref="IConsumerRebalanceListener"/> that logs each call in a <see cref="CallbackLog"/>, the
/// C# port of Python's <c>LoggingRebalanceListener</c>. It implements the interface, not
/// <see cref="ConsumerRebalanceListenerBase"/>, so a lost partition is logged as <c>lost</c>.
/// </summary>
internal sealed class LoggingRebalanceListener : IConsumerRebalanceListener
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;

    internal LoggingRebalanceListener(CallbackLog log, ulong consumerId)
    {
        _log = log;
        _consumerId = consumerId;
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindAssigned, partitions);

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindRevoked, partitions);

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindLost, partitions);
}

/// <summary>
/// An <see cref="IOffsetCommitCallback"/> that logs each commit's offsets and error in a
/// <see cref="CallbackLog"/>, the C# port of Python's <c>make_logging_commit_callback</c>.
/// </summary>
internal sealed class LoggingCommitCallback : IOffsetCommitCallback
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;

    internal LoggingCommitCallback(CallbackLog log, ulong consumerId)
    {
        _log = log;
        _consumerId = consumerId;
    }

    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> applied =
            offsets ?? new Dictionary<TopicPartition, OffsetAndMetadata>();
        List<TopicPartition> partitions = new List<TopicPartition>(applied.Count);
        Dictionary<string, long> committed = new Dictionary<string, long>(applied.Count);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> pair in applied)
        {
            partitions.Add(pair.Key);
            committed[Translate.OffsetKey(pair.Key.Topic, pair.Key.Partition)] = pair.Value.Offset;
        }

        _log.Append(_consumerId, Translate.KindCommit, partitions, committed, exception?.Message);
    }
}

/// <summary>
/// An <see cref="IDeliveryCallback"/> that logs each record's metadata and error in a
/// <see cref="CallbackLog"/>, the C# port of Python's <c>make_logging_delivery_callback</c>.
/// </summary>
internal sealed class LoggingDeliveryCallback : IDeliveryCallback
{
    private readonly CallbackLog _log;
    private readonly ulong _producerId;

    internal LoggingDeliveryCallback(CallbackLog log, ulong producerId)
    {
        _log = log;
        _producerId = producerId;
    }

    public void OnCompletion(RecordMetadata metadata, Exception? exception)
    {
        List<TopicPartition> partitions = new List<TopicPartition>(1)
        {
            new TopicPartition(metadata.Topic, metadata.Partition),
        };

        Dictionary<string, long> offsets = new Dictionary<string, long>(1)
        {
            [Translate.OffsetKey(metadata.Topic, metadata.Partition)] = metadata.Offset,
        };

        _log.Append(_producerId, Translate.KindDelivery, partitions, offsets, exception?.Message);
    }
}
