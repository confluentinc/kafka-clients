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

namespace Confluent.Kafka;

/// <summary>
/// A single record read from a poll — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerRecord</c>, clipped to today's ABI.
/// Every field is an <b>owned copy</b> made during the receive-path copy-out on the
/// dispatcher thread (ffi-marshalling.md §6.4): the topic is an owned
/// <see cref="string"/>, the key / value / header values are owned <c>byte[]</c>.
/// <b>Nothing native-backed escapes</b> — the record stays valid after the underlying
/// batch is destroyed, sidestepping the §B4 use-after-free hazard.
/// </summary>
/// <remarks>
/// <b>Poll-output-only this phase (PLAN decision 4).</b> There is no public constructor:
/// a <see cref="ConsumerRecord"/> is produced only by <c>IAsyncConsumer.Poll(...)</c>.
/// A public constructor (and the fuller Java field set — leader epoch, serialized
/// sizes, and Java's <c>addRecord(ConsumerRecord)</c> mock helper) are deferred to a
/// later additive phase.
///
/// <b>Key/Value are <c>byte[]?</c> (deviation from the CLAUDE.md §3
/// <see cref="System.ReadOnlyMemory{T}"/> sketch, PLAN micro-decision A).</b> Java's
/// raw-bytes interim and confluent-kafka-dotnet both use <c>byte[]</c>; unifying on
/// <c>byte[]?</c> matches <see cref="Header.Value"/> and removes the
/// <see cref="System.ReadOnlyMemory{T}"/> wrap the internal type used (the copy-out
/// already allocates the owned array), so it is a simplification, not a new copy.
/// </remarks>
public sealed class ConsumerRecord
{
    /// <summary>The timestamp value for a record with no timestamp (ABI sentinel).</summary>
    internal const long NoTimestamp = -1;

    /// <summary>
    /// Initializes an owned record from values already copied out of the (borrowed)
    /// native batch. Internal because a <see cref="ConsumerRecord"/> is poll-output-only
    /// this phase (no public constructor, PLAN decision 4); callers must pass owned
    /// copies — no borrowed pointer may be captured (the copy-out contract, ffi §6.4).
    /// </summary>
    internal ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        byte[]? key,
        byte[]? value,
        Headers headers)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        TimestampType = timestampType;
        Key = key;
        Value = value;
        Headers = headers;
    }

    /// <summary>The topic name (an owned copy of the length-delimited slice, §B3).</summary>
    public string Topic { get; }

    /// <summary>The partition the record was read from.</summary>
    public int Partition { get; }

    /// <summary>The record's offset within its partition.</summary>
    public long Offset { get; }

    /// <summary>
    /// The record timestamp in milliseconds since the epoch, or
    /// <see cref="NoTimestamp"/> (<c>-1</c>) when absent.
    /// </summary>
    public long Timestamp { get; }

    /// <summary>The kind of <see cref="Timestamp"/> the record carries.</summary>
    public TimestampType TimestampType { get; }

    /// <summary>
    /// The key as an owned <c>byte[]</c>, or <see langword="null"/> when the key is
    /// absent.
    /// </summary>
    public byte[]? Key { get; }

    /// <summary>
    /// The value as an owned <c>byte[]</c>, or <see langword="null"/> when the value is
    /// absent (a tombstone).
    /// </summary>
    public byte[]? Value { get; }

    /// <summary>The record headers; empty when the record has none.</summary>
    public Headers Headers { get; }
}
