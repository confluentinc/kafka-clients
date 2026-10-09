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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// Record encoding, record checking and <c>WorkloadEvent</c> construction for the chaos
/// workloads, shared by both flavours (M18/P1 §5.1). The .NET twin of the "Record encoding and
/// event construction" half of <c>python/grpc_chaos.py</c> (<c>_key</c>, <c>_value</c>,
/// <c>_check_record</c>, <c>_outcome</c>, <c>_rebalance</c>, ..., <c>_duplicate_id</c>,
/// <c>_is_terminal</c>), which in turn mirrors the Rust harness's in-process workload
/// (<c>rust/tests/chaos/workload.rs</c> <c>build_value</c> / <c>check_record</c>).
/// </summary>
/// <remarks>
/// Test-server scaffolding with no Java class (DoD §7): every builder here is a named Python
/// helper, and the strings in <see cref="TryCheckRecord"/> are the harness's, character for
/// character (PLAN §3.6), because the verifier compares them.
/// </remarks>
internal static class ChaosEvents
{
    /// <summary>Upper bound on events per stream message (Python <c>_MAX_BATCH</c>).</summary>
    internal const int MaxBatch = 4096;

    /// <summary>
    /// <c>kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT</c> — the duplicate-<c>workload_id</c>
    /// code (Python <c>ec.LOCAL_ILLEGAL_ARGUMENT</c>).
    /// </summary>
    internal const int LocalIllegalArgumentCode = -3;

    /// <summary>
    /// <c>kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE</c> — the code of the server-made
    /// "neither metadata nor error" outcome (Python <c>ec.LOCAL_ILLEGAL_STATE</c>), and the
    /// one <see cref="Translate.ToProto"/> stamps on any non-Kafka exception.
    /// </summary>
    internal const int LocalIllegalStateCode = -4;

    /// <summary>The record key and the leading bytes of its value: an 8-byte index.</summary>
    internal const int KeyLength = 8;

    /// <summary>
    /// Pause after a failed poll before polling again, so a persistent error does not spin at
    /// full CPU (Python <c>_POLL_ERROR_BACKOFF_S</c>, <c>workload.rs</c> <c>POLL_ERROR_BACKOFF</c>).
    /// </summary>
    internal static readonly TimeSpan PollErrorBackoff = TimeSpan.FromMilliseconds(100);

    private static readonly long s_unixEpochTicks = DateTime.UnixEpoch.Ticks;

    /// <summary>The record key: its 8-byte big-endian logical index (Python <c>_key</c>).</summary>
    /// <param name="index">The record's logical index.</param>
    /// <returns>A fresh 8-byte array.</returns>
    internal static byte[] Key(ulong index)
    {
        byte[] key = new byte[KeyLength];
        WriteIndex(index, key);
        return key;
    }

    /// <summary>
    /// The record value: the index's 8 bytes zero-padded to <paramref name="msgSize"/>, or the
    /// first <paramref name="msgSize"/> of those bytes when smaller (Python <c>_value</c>,
    /// <c>workload.rs</c> <c>build_value</c>). <paramref name="msgSize"/> 0 gives an empty,
    /// <b>present</b> value, as Rust's empty <c>Vec</c> does.
    /// </summary>
    /// <remarks>
    /// A fresh array per call (PLAN §5.5.4): the async send path borrows the caller's buffers
    /// until delivery (ffi §A4), so a value is never reused or mutated after <c>Send</c>.
    /// </remarks>
    /// <param name="index">The record's logical index.</param>
    /// <param name="msgSize">The value size in bytes.</param>
    /// <returns>A fresh array of exactly <paramref name="msgSize"/> bytes.</returns>
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

    /// <summary>
    /// Whether a consumed record is what a producer wrote: an 8-byte big-endian index key and
    /// that index's <see cref="Value"/> at <paramref name="msgSize"/>. Python
    /// <c>_check_record</c> / Rust <c>check_record</c>, messages included.
    /// </summary>
    /// <remarks>
    /// <b>Absent versus empty (PLAN §5.6, T14c).</b> <paramref name="key"/> and
    /// <paramref name="value"/> are the record's <c>byte[]</c> as <see cref="Serdes.ByteArray"/>
    /// delivers them, and that path keeps the two apart: an absent key or value (the ABI's
    /// <c>len &lt; 0</c> sentinel) becomes <see langword="null"/> without the deserializer being
    /// called, and a present-but-empty one becomes a zero-length array
    /// (<c>ConsumerRecordsMarshal</c>, and the library's own
    /// <c>PublicSyncConsumerRoundTripTests.Poll_TombstoneAndAbsentKey_MapToNull</c> /
    /// <c>Poll_EmptyKeyAndValue_MapToEmptyNonNull</c>). So <see langword="null"/> here is
    /// "missing" and an empty array is "0 byte(s)", exactly as in Rust; the server needs no
    /// workaround. The gRPC server unit tests pin it end to end through the consumer loop.
    /// </remarks>
    /// <param name="key">The record key, or <see langword="null"/> when absent.</param>
    /// <param name="value">The record value, or <see langword="null"/> when absent.</param>
    /// <param name="msgSize">The producers' value size.</param>
    /// <param name="index">The record's index when it checks out.</param>
    /// <param name="detail">What is wrong with it otherwise.</param>
    /// <returns><see langword="true"/> when the record is what a producer wrote.</returns>
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

    /// <summary>Python <c>_sent</c>: record <paramref name="index"/> is about to be handed over.</summary>
    internal static Proto.WorkloadEvent Sent(ulong index) =>
        new Proto.WorkloadEvent { Sent = new Proto.Sent { Index = index } };

    /// <summary>
    /// Python <c>_outcome</c>: the event settling record <paramref name="index"/> from its
    /// delivery callback — <c>SendFailed</c> on an error, the server-made
    /// <c>SendFailed{-4, …}</c> when the callback carries neither, <c>Delivered</c> otherwise.
    /// </summary>
    /// <remarks>
    /// The binding's <see cref="IDeliveryCallback"/> never passes a <see langword="null"/>
    /// metadata (the <c>-1</c> placeholder on failure, CLAUDE.md §4), so the "neither" branch is
    /// unreachable from it; it is kept so the outcome matches the anchor's on every input.
    /// </remarks>
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

    /// <summary>Python <c>_send_failed</c>.</summary>
    internal static Proto.WorkloadEvent SendFailed(ulong index, Exception error) =>
        new Proto.WorkloadEvent
        {
            SendFailed = new Proto.SendFailed { Index = index, Error = Translate.ToProto(error) },
        };

    /// <summary>Python <c>_stats</c>.</summary>
    internal static Proto.WorkloadEvent Stats(ulong sent, double elapsedSeconds) =>
        new Proto.WorkloadEvent
        {
            ProducerStats = new Proto.ProducerStats { Sent = sent, ElapsedSeconds = elapsedSeconds },
        };

    /// <summary>
    /// One record of a poll's batch (Python <c>_consumed_events</c>): <c>Consumed</c> when it is
    /// what a producer wrote, <c>Corrupted</c> with the <see cref="TryCheckRecord"/> text otherwise.
    /// </summary>
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

    /// <summary>
    /// Python <c>_rebalance</c>: the partitions sorted by (topic, partition) — topic by ordinal
    /// comparison, which is Python's code-point order on these ASCII names — stamped with
    /// <paramref name="observedAtUnixNanos"/>, the wall clock the caller read at callback entry.
    /// </summary>
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

    /// <summary>
    /// The wall clock in nanoseconds since the Unix epoch (Python <c>time.time_ns()</c>), at
    /// the 100 ns resolution <see cref="DateTime"/> has.
    /// </summary>
    internal static long UnixNanosNow() => (DateTime.UtcNow.Ticks - s_unixEpochTicks) * 100;

    /// <summary>Python <c>_committed_events</c>: one <c>Committed</c> per read-back entry.</summary>
    internal static void EmitCommitted(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, Action<Proto.WorkloadEvent> emit)
    {
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
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

    /// <summary>Python <c>_consumer_error</c>.</summary>
    internal static Proto.WorkloadEvent ConsumerError(Proto.ConsumerOp op, Exception error) =>
        new Proto.WorkloadEvent
        {
            ConsumerError = new Proto.ConsumerError { Op = op, Error = Translate.ToProto(error) },
        };

    /// <summary>Python <c>_marker</c>.</summary>
    internal static Proto.WorkloadEvent Marker(ulong marker) =>
        new Proto.WorkloadEvent { Marker = new Proto.Marker { Marker_ = marker } };

    /// <summary>Python <c>_closing</c>.</summary>
    internal static Proto.WorkloadEvent Closing() =>
        new Proto.WorkloadEvent { ConsumerClosing = new Proto.ConsumerClosing() };

    /// <summary>Python <c>_closed</c>.</summary>
    internal static Proto.WorkloadEvent Closed() =>
        new Proto.WorkloadEvent { ConsumerClosed = new Proto.ConsumerClosed() };

    /// <summary>Python <c>_finished</c>.</summary>
    internal static Proto.WorkloadEvent Finished() =>
        new Proto.WorkloadEvent { Finished = new Proto.Finished() };

    /// <summary>Python <c>_failed</c>.</summary>
    internal static Proto.WorkloadEvent Failed(Exception error) =>
        new Proto.WorkloadEvent { Failed = new Proto.Failed { Error = Translate.ToProto(error) } };

    /// <summary>Python <c>_is_terminal</c>: <c>Finished</c> or <c>Failed</c> ends the stream.</summary>
    internal static bool IsTerminal(Proto.WorkloadEvent workloadEvent) =>
        workloadEvent.EventCase is Proto.WorkloadEvent.EventOneofCase.Finished
            or Proto.WorkloadEvent.EventOneofCase.Failed;

    /// <summary>
    /// Python <c>_duplicate_id</c>: the one batch a second <c>Run*</c> with a running
    /// <paramref name="workloadId"/> gets.
    /// </summary>
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

    /// <summary>
    /// Writes a diagnostic line to STDERR, where the harness collects the server's output
    /// (Python logs through <c>logging</c>, which also lands on STDERR). Never throws: callers
    /// include the binding's callback threads.
    /// </summary>
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
