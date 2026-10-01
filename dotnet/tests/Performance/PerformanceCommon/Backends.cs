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

namespace Confluent.Kafka.Performance;

/// <summary>
/// The client-agnostic acknowledgement metadata a producer backend returns per send — the fields the
/// shared engine's verify step checks (Python <c>verify_record_metadata</c>: offset / partition / topic
/// / timestamp all valid). Client-free so <c>PerformanceCommon</c> carries no dependency on our binding's
/// (or ckd's) <c>RecordMetadata</c>.
/// </summary>
public readonly struct PerfRecordMetadata
{
    /// <summary>Constructs the acknowledgement metadata for one produced record.</summary>
    public PerfRecordMetadata(string topic, int partition, long offset, long timestamp)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
    }

    /// <summary>The topic the record was published to.</summary>
    public string Topic { get; }

    /// <summary>The partition the record was published to.</summary>
    public int Partition { get; }

    /// <summary>The offset assigned in the partition.</summary>
    public long Offset { get; }

    /// <summary>The record timestamp in milliseconds since epoch.</summary>
    public long Timestamp { get; }
}

/// <summary>
/// One polled record normalized to the consumer engine's measurement inputs (Python's consumer backends
/// yield <c>(timestamp_ms, nbytes)</c>): the record's <see cref="TimestampMs"/> (for the e2e-latency
/// <c>now - timestamp</c>) and <see cref="NBytes"/> (value + key length).
/// </summary>
public readonly struct PolledRecord
{
    /// <summary>Constructs a normalized polled record.</summary>
    public PolledRecord(long timestampMs, int nbytes)
    {
        TimestampMs = timestampMs;
        NBytes = nbytes;
    }

    /// <summary>The record timestamp in ms (<c>-1</c> / <c>&lt;= 0</c> when absent — the engine skips those).</summary>
    public long TimestampMs { get; }

    /// <summary>The record's payload size: value length + key length.</summary>
    public int NBytes { get; }
}

// The producer backend is split into a sync and an async interface because the .NET sync
// IProducer.Send returns RecordMetadata directly (a serial blocking measurement) while the async
// IAsyncProducer.Send returns Task<RecordMetadata> (pipelined) — the key .NET-specific deviation from
// Python, which pipelines both (PLAN §5.1 / D5). Each per-client exe implements the one it drives for a
// given ASYNC mode; a single unified interface would force every backend to implement the unused shape.

/// <summary>The synchronous (serial-blocking) producer backend — <see cref="Send"/> blocks and returns metadata.</summary>
public interface IProducerBackend : IDisposable
{
    /// <summary>Serializes then blocks until the record is acknowledged, returning its metadata (Java <c>send(record).get()</c>).</summary>
    PerfRecordMetadata Send(string topic, byte[]? key, byte[]? value);

    /// <summary>Flushes and closes the producer, surfacing a close failure.</summary>
    void Close();
}

/// <summary>The asynchronous (pipelined) producer backend — <see cref="Send"/> returns a delivery task.</summary>
public interface IAsyncProducerBackend : IDisposable, IAsyncDisposable
{
    /// <summary>Serializes and enqueues the record, returning a task that completes with its metadata on delivery.</summary>
    Task<PerfRecordMetadata> Send(string topic, byte[]? key, byte[]? value);

    /// <summary>Flushes and closes the producer, surfacing a close failure.</summary>
    Task Close();
}

/// <summary>The synchronous consumer backend — blocking <see cref="PollBatch"/> yields normalized records.</summary>
public interface IConsumerBackend : IDisposable
{
    /// <summary>Subscribes to <paramref name="topic"/>.</summary>
    void Subscribe(string topic);

    /// <summary>Whether the consumer currently has a partition assignment (Java <c>assignment()</c> non-empty).</summary>
    bool Assigned();

    /// <summary>Polls once and returns the batch's records normalized to <see cref="PolledRecord"/>.</summary>
    IReadOnlyList<PolledRecord> PollBatch();

    /// <summary>
    /// <c>POLL_SINGLE</c> path: the binding exposes no single-message poll, so this delegates to
    /// <see cref="PollBatch"/> (documented in <c>consumer_performance_test.py</c>; the flag is kept for env parity).
    /// </summary>
    IReadOnlyList<PolledRecord> PollSingle();

    /// <summary>Closes the consumer.</summary>
    void Close();
}

/// <summary>The asynchronous consumer backend — blocking-in-Java ops are tasks; the assignment read stays sync.</summary>
public interface IAsyncConsumerBackend : IDisposable, IAsyncDisposable
{
    /// <summary>Subscribes to <paramref name="topic"/>.</summary>
    Task Subscribe(string topic);

    /// <summary>Whether the consumer currently has a partition assignment (Java <c>assignment()</c> is a sync read).</summary>
    bool Assigned();

    /// <summary>Polls once and returns the batch's records normalized to <see cref="PolledRecord"/>.</summary>
    Task<IReadOnlyList<PolledRecord>> PollBatch();

    /// <summary><c>POLL_SINGLE</c> path — delegates to <see cref="PollBatch"/> (see <see cref="IConsumerBackend.PollSingle"/>).</summary>
    Task<IReadOnlyList<PolledRecord>> PollSingle();

    /// <summary>Closes the consumer.</summary>
    Task Close();
}
