// Copyright 2026 Confluent Inc.
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
using System.Globalization;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// Record encoding, record checking and <c>WorkloadEvent</c> construction for both chaos flavours, the
/// .NET twin of Python's <c>grpc_chaos.py</c> helpers. The <see cref="TryCheckRecord"/> texts are the
/// harness's, character for character, because the verifier compares them.
/// </summary>
internal static class ChaosEvents
{
    internal const int MaxBatch = 4096;

    internal const int LocalIllegalArgumentCode = -3;

    internal const int LocalIllegalStateCode = -4;

    internal const int KeyLength = 8;

    internal static readonly TimeSpan PollErrorBackoff = TimeSpan.FromMilliseconds(100);

    private static readonly long s_unixEpochTicks = DateTime.UnixEpoch.Ticks;

    internal static byte[] Key(ulong index)
    {
        byte[] key = new byte[KeyLength];
        WriteIndex(index, key);
        return key;
    }

    internal static byte[] Value(ulong index, uint msgSize)
    {
        byte[] value = new byte[checked((int)msgSize)];
        if (msgSize >= KeyLength)
        {
            WriteIndex(index, value);
        }
        else
        {
            byte[] key = Key(index);
            Array.Copy(key, value, value.Length);
        }

        return value;
    }

    // Serdes.ByteArray keeps absent and empty apart: null is an absent key or value, an empty array
    // a present empty one.
    internal static bool TryCheckRecord(byte[]? key, byte[]? value, uint msgSize, out ulong index, out string? detail)
    {
        index = 0;
        if (key is null)
        {
            detail = "key is missing (expected the 8-byte index)";
            return false;
        }

        if (key.Length != KeyLength)
        {
            detail = string.Format(
                CultureInfo.InvariantCulture, "key is {0} byte(s), expected the 8-byte index", key.Length);
            return false;
        }

        index = ReadIndex(key);
        if (value is null)
        {
            if (msgSize == 0)
            {
                detail = null;
                return true;
            }

            detail = string.Format(
                CultureInfo.InvariantCulture,
                "value is missing (expected {0} byte(s) encoding index {1})",
                msgSize,
                index);
            return false;
        }

        if (!Matches(value, index, msgSize))
        {
            detail = string.Format(
                CultureInfo.InvariantCulture,
                "value of {0} byte(s) does not match the producer's encoding of index {1} ({2} byte(s))",
                value.Length,
                index,
                msgSize);
            return false;
        }

        detail = null;
        return true;
    }

    internal static Proto.WorkloadEvent Sent(ulong index) =>
        new Proto.WorkloadEvent { Sent = new Proto.Sent { Index = index } };

    // The binding never passes a null metadata, so the "neither" branch is unreachable from it; it
    // is kept to match Python's _outcome on every input.
    internal static Proto.WorkloadEvent Outcome(ulong index, RecordMetadata? metadata, Exception? exception)
    {
        if (exception is not null)
        {
            return SendFailed(index, exception);
        }

        if (metadata is null)
        {
            return new Proto.WorkloadEvent
            {
                SendFailed = new Proto.SendFailed
                {
                    Index = index,
                    Error = new Proto.KafkaError
                    {
                        Code = LocalIllegalStateCode,
                        Message = "dotnet server: delivery callback fired with neither metadata nor error",
                    },
                },
            };
        }

        return new Proto.WorkloadEvent
        {
            Delivered = new Proto.Delivered { Index = index, Partition = metadata.Partition, Offset = metadata.Offset },
        };
    }

    internal static Proto.WorkloadEvent SendFailed(ulong index, Exception error) =>
        new Proto.WorkloadEvent
        {
            SendFailed = new Proto.SendFailed { Index = index, Error = Translate.ToProto(error) },
        };

    internal static Proto.WorkloadEvent Stats(ulong sent, double elapsedSeconds) =>
        new Proto.WorkloadEvent
        {
            ProducerStats = new Proto.ProducerStats { Sent = sent, ElapsedSeconds = elapsedSeconds },
        };

    internal static Proto.WorkloadEvent ConsumedOrCorrupted(ConsumerRecord<byte[], byte[]> record, uint msgSize)
    {
        if (TryCheckRecord(record.Key, record.Value, msgSize, out ulong index, out string? detail))
        {
            return new Proto.WorkloadEvent
            {
                Consumed = new Proto.Consumed
                {
                    Index = index,
                    Topic = record.Topic,
                    Partition = record.Partition,
                    Offset = record.Offset,
                },
            };
        }

        return new Proto.WorkloadEvent
        {
            Corrupted = new Proto.Corrupted
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Detail = detail ?? string.Empty,
            },
        };
    }

    internal static Proto.WorkloadEvent Rebalance(
        Proto.RebalanceKind kind, IReadOnlyCollection<TopicPartition> partitions, long observedAtUnixNanos)
    {
        List<TopicPartition> sorted = new List<TopicPartition>(partitions);
        sorted.Sort(static (a, b) =>
        {
            int byTopic = string.CompareOrdinal(a.Topic, b.Topic);
            return byTopic != 0 ? byTopic : a.Partition.CompareTo(b.Partition);
        });

        Proto.Rebalance rebalance = new Proto.Rebalance { Kind = kind, ObservedAtUnixNanos = observedAtUnixNanos };
        foreach (TopicPartition tp in sorted)
        {
            rebalance.Partitions.Add(new Proto.TopicPartitionRef { Topic = tp.Topic, Partition = tp.Partition });
        }

        return new Proto.WorkloadEvent { Rebalance = rebalance };
    }

    internal static long UnixNanosNow() => (DateTime.UtcNow.Ticks - s_unixEpochTicks) * 100;

    internal static void EmitCommitted(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets, Action<Proto.WorkloadEvent> emit)
    {
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata?> entry in offsets)
        {
            if (entry.Value is null)
            {
                continue;
            }

            emit(new Proto.WorkloadEvent
            {
                Committed = new Proto.Committed
                {
                    Topic = entry.Key.Topic,
                    Partition = entry.Key.Partition,
                    Offset = entry.Value.Offset,
                },
            });
        }
    }

    internal static Proto.WorkloadEvent ConsumerError(Proto.ConsumerOp op, Exception error) =>
        new Proto.WorkloadEvent
        {
            ConsumerError = new Proto.ConsumerError { Op = op, Error = Translate.ToProto(error) },
        };

    internal static Proto.WorkloadEvent Marker(ulong marker) =>
        new Proto.WorkloadEvent { Marker = new Proto.Marker { Marker_ = marker } };

    internal static Proto.WorkloadEvent Closing() =>
        new Proto.WorkloadEvent { ConsumerClosing = new Proto.ConsumerClosing() };

    internal static Proto.WorkloadEvent Closed() =>
        new Proto.WorkloadEvent { ConsumerClosed = new Proto.ConsumerClosed() };

    internal static Proto.WorkloadEvent Finished() =>
        new Proto.WorkloadEvent { Finished = new Proto.Finished() };

    internal static Proto.WorkloadEvent Failed(Exception error) =>
        new Proto.WorkloadEvent { Failed = new Proto.Failed { Error = Translate.ToProto(error) } };

    internal static bool IsTerminal(Proto.WorkloadEvent workloadEvent) =>
        workloadEvent.EventCase is Proto.WorkloadEvent.EventOneofCase.Finished
            or Proto.WorkloadEvent.EventOneofCase.Failed;

    internal static Proto.WorkloadEventBatch DuplicateId(string workloadId)
    {
        Proto.WorkloadEventBatch batch = new Proto.WorkloadEventBatch();
        batch.Events.Add(new Proto.WorkloadEvent
        {
            Failed = new Proto.Failed
            {
                Error = new Proto.KafkaError
                {
                    Code = LocalIllegalArgumentCode,
                    Message = $"dotnet server: workload_id '{workloadId}' is already running",
                },
            },
        });
        return batch;
    }

    internal static void Log(string message)
    {
        try
        {
            Console.Error.WriteLine($"dotnet server: {message}");
        }
        catch (Exception)
        {
            // Diagnostics must never fail the workload or unwind into a binding thread.
        }
    }

    private static void WriteIndex(ulong index, byte[] destination)
    {
        for (int i = 0; i < KeyLength; i++)
        {
            destination[i] = (byte)(index >> (8 * (KeyLength - 1 - i)));
        }
    }

    private static ulong ReadIndex(byte[] key)
    {
        ulong index = 0;
        for (int i = 0; i < KeyLength; i++)
        {
            index = (index << 8) | key[i];
        }

        return index;
    }

    private static bool Matches(byte[] value, ulong index, uint msgSize)
    {
        if (value.Length != msgSize)
        {
            return false;
        }

        byte[] expected = Value(index, msgSize);
        for (int i = 0; i < expected.Length; i++)
        {
            if (value[i] != expected[i])
            {
                return false;
            }
        }

        return true;
    }
}
