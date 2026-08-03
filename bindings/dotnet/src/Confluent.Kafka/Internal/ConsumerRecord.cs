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

namespace Confluent.Kafka.Internal;

/// <summary>
/// A single record copied out of a poll batch — the .NET realization of Java's
/// <c>ConsumerRecord</c>, clipped to today's ABI (ffi-marshalling.md §6.4). Every
/// field is an <b>owned copy</b> made on the dispatcher thread during the poll
/// callback's copy-out (see <c>ConsumerRecordsMarshal</c>): the topic is an owned
/// <see cref="string"/>, the key / value / header values are owned
/// <c>byte[]</c>-backed <see cref="ReadOnlyMemory{T}"/>. <b>Nothing native-backed
/// escapes</b> — the record stays valid after the batch is destroyed, sidestepping the
/// §B4 use-after-free hazard.
/// </summary>
/// <remarks>
/// <b>Internal this phase (PLAN decision 1).</b> The public <c>ConsumerRecord</c> /
/// <c>ConsumerRecords</c> (and public <c>Headers</c> / <c>TimestampType</c> /
/// <c>TopicPartition</c>) land with the first public client. Here the type proves the
/// owned-handle receive-path copy-out shape end to end; it uses a bare
/// <see cref="long"/> <see cref="Timestamp"/> and a plain <see cref="int"/>
/// <see cref="TimestampType"/> id (no public enum), and an internal
/// <see cref="RecordHeader"/> list (no public <c>Headers</c> type).
/// </remarks>
internal sealed class ConsumerRecord
{
    /// <summary>The timestamp value for a record with no timestamp (ABI sentinel).</summary>
    internal const long NoTimestamp = -1;

    /// <summary>
    /// Initializes an owned record from values already copied out of the (borrowed)
    /// native batch. Callers must pass owned copies — no borrowed pointer may be
    /// captured (that is the copy-out contract, ffi §6.4).
    /// </summary>
    internal ConsumerRecord(
        string topic,
        int partition,
        long offset,
        long timestamp,
        int timestampType,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value,
        IReadOnlyList<RecordHeader> headers)
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

    /// <summary>
    /// The timestamp type as its numeric id (<c>-1</c> NoTimestampType /
    /// <c>0</c> CreateTime / <c>1</c> LogAppendTime). A plain <see cref="int"/> this
    /// phase — the public <c>TimestampType</c> enum lands with the public client
    /// (PLAN decision 1).
    /// </summary>
    public int TimestampType { get; }

    /// <summary>
    /// The key as an owned <c>byte[]</c>-backed <see cref="ReadOnlyMemory{T}"/>, or
    /// <see langword="null"/> when the key is absent.
    /// </summary>
    public ReadOnlyMemory<byte>? Key { get; }

    /// <summary>
    /// The value as an owned <c>byte[]</c>-backed <see cref="ReadOnlyMemory{T}"/>, or
    /// <see langword="null"/> when the value is absent (a tombstone).
    /// </summary>
    public ReadOnlyMemory<byte>? Value { get; }

    /// <summary>
    /// The record headers, each an owned <see cref="RecordHeader"/> (internal only
    /// this phase, PLAN decision 2). Empty when the record has no headers.
    /// </summary>
    public IReadOnlyList<RecordHeader> Headers { get; }
}

/// <summary>
/// One record header, copied out of the batch: an owned <see cref="string"/> key
/// (from the length-delimited slice, §B3) and an owned <c>byte[]</c>-backed value (or
/// <see langword="null"/>). A <c>readonly struct</c> — small, immutable, no extra heap
/// allocation per header beyond the key string and value array it already owns.
/// Internal this phase; the public <c>Headers</c> shape lands with the public client
/// (PLAN decision 2).
/// </summary>
internal readonly struct RecordHeader
{
    /// <summary>Initializes an owned header from copied-out values.</summary>
    internal RecordHeader(string key, ReadOnlyMemory<byte>? value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>The header key (an owned copy of the length-delimited slice, §B3).</summary>
    public string Key { get; }

    /// <summary>
    /// The header value as an owned <c>byte[]</c>-backed
    /// <see cref="ReadOnlyMemory{T}"/>, or <see langword="null"/> when the value is
    /// null.
    /// </summary>
    public ReadOnlyMemory<byte>? Value { get; }
}
