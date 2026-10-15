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
using System.Threading;
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

/// <summary>
/// The handle a sync producer backend's <see cref="IProducerBackend.Send"/> returns for one record: Python's
/// <c>concurrent.futures.Future</c> from <c>producer.send(record)</c>, Java's <c>Future&lt;RecordMetadata&gt;</c>.
/// <see cref="Get"/> blocks until the record is acknowledged and returns its metadata, or rethrows its failure
/// (<c>future.result()</c> / <c>future.get()</c>). Client-free like <see cref="PerfRecordMetadata"/>: a backend
/// passes its client's future as the state, with a function that waits it.
/// </summary>
public readonly struct PerfSendHandle
{
    private readonly Func<object, PerfRecordMetadata>? _get;
    private readonly object? _state;

    /// <summary>Wraps a client's future: <see cref="Get"/> returns <c>get(state)</c>.</summary>
    /// <param name="get">
    /// Blocks until <paramref name="state"/> completes, then returns its metadata or throws its failure. Pass a
    /// static (non-capturing) function, so that a send allocates no closure for it.
    /// </param>
    /// <param name="state">The client's future, handed back to <paramref name="get"/>.</param>
    public PerfSendHandle(Func<object, PerfRecordMetadata> get, object state)
    {
        _get = get ?? throw new ArgumentNullException(nameof(get));
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    /// <summary>Blocks until the record is acknowledged and returns its metadata; a delivery failure is rethrown.</summary>
    /// <exception cref="InvalidOperationException">The handle is <c>default</c>, so there is no send to wait for.</exception>
    public PerfRecordMetadata Get() =>
        _get is null
            ? throw new InvalidOperationException("default(PerfSendHandle) has no send to wait for.")
            : _get(_state!);
}

/// <summary>
/// The engine's delivery notification — Java's producer <c>Callback.onCompletion(metadata, exception)</c>, the
/// callback Python passes to <c>producer.send(record, callback)</c>. The engine stamps a record's latency here,
/// when the client reports its acknowledgement, not when some other thread later gets round to its future, so
/// a backlog on the engine's side is never reported as client latency.
/// </summary>
/// <remarks>
/// A backend calls <see cref="OnCompletion"/> exactly once for every record whose callback-taking send
/// returned normally, on whichever thread its client reports the delivery, and calls it for none whose send
/// threw. One instance serves every send of a run, so a backend hands back the <c>sendStartMs</c> it was given
/// instead of the engine allocating a callback object per send. Implementations must not throw.
/// </remarks>
public interface IPerfDeliveryCallback
{
    /// <summary>Reports one record's delivery: <paramref name="exception"/> is <see langword="null"/> on success.</summary>
    /// <param name="sendStartMs">The value the engine passed to the send, handed back unchanged.</param>
    /// <param name="metadata">The record's acknowledgement metadata; unspecified when <paramref name="exception"/> is set.</param>
    /// <param name="exception">The delivery failure, or <see langword="null"/> when the record was acknowledged.</param>
    void OnCompletion(long sendStartMs, PerfRecordMetadata metadata, Exception? exception);
}

// The producer backend is split into a sync and an async interface, one per Python loop. Each has two sends.
// The callback-taking one carries the measured interval: it returns once the client has accepted the record
// and reports the delivery to an IPerfDeliveryCallback (Python's send(record, callback) in main, and the
// add_done_callback on the send's future in async_main). The plain one is the warmup's: it hands back the
// delivery to wait inline. Each per-client exe implements the interface it drives for a given ASYNC mode; a
// single unified interface would force every backend to implement the unused shape.

/// <summary>
/// The synchronous producer backend — each <c>Send</c> returns once the client has accepted the record.
/// </summary>
public interface IProducerBackend : IDisposable
{
    /// <summary>
    /// Serializes and sends the record, returning once the client has accepted it (Java <c>send(record)</c>,
    /// Python <c>producer.send(record)</c>), with the handle whose <see cref="PerfSendHandle.Get"/> waits for
    /// its delivery (Java <c>future.get()</c>). A send that fails before it is accepted throws. The engine
    /// uses this only for warmup.
    /// </summary>
    PerfSendHandle Send(string topic, byte[]? key, byte[]? value);

    /// <summary>
    /// Serializes and sends the record, returning once the client has accepted it, and reports its delivery to
    /// <paramref name="callback"/> (Java <c>send(record, callback)</c>). A send that fails before it is
    /// accepted throws, and then <paramref name="callback"/> is not called for it.
    /// </summary>
    void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs);

    /// <summary>
    /// Drives every record sent so far to completion (Java <c>flush()</c>). Delivery callbacks may still be
    /// running when it returns. A backend whose client takes no token ignores <paramref name="cancellationToken"/>.
    /// </summary>
    void Flush(CancellationToken cancellationToken);

    /// <summary>Flushes and closes the producer, surfacing a close failure.</summary>
    void Close();
}

/// <summary>The asynchronous (pipelined) producer backend — each <c>Send</c> is two-stage (accepted, then delivered).</summary>
public interface IAsyncProducerBackend : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Serializes and sends the record. The returned <see cref="ValueTask{TResult}"/> completes once the
    /// client has accepted the record (Java's <c>send()</c> returning), and holds the task that completes
    /// with its metadata on delivery. A send that fails before it is accepted throws, from the call or from
    /// awaiting the returned value. The engine uses this only for warmup.
    /// </summary>
    ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value);

    /// <summary>
    /// Serializes and sends the record, and reports its delivery to <paramref name="callback"/>. The returned
    /// <see cref="ValueTask"/> completes once the client has accepted the record. A send that fails before it
    /// is accepted throws, from the call or from awaiting the returned value, and then
    /// <paramref name="callback"/> is not called for it.
    /// </summary>
    ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs);

    /// <summary>Drives every record sent so far to completion (Java <c>flush()</c>). Delivery callbacks may still be running when it completes.</summary>
    Task Flush(CancellationToken cancellationToken);

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
