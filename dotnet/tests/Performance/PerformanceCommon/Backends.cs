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
/// The client-free acknowledgement metadata a producer backend returns per send: the topic, partition, offset and
/// timestamp the engine's verify step checks, as Python's <c>verify_record_metadata</c>.
/// </summary>
public readonly struct PerfRecordMetadata
{
    public PerfRecordMetadata(string topic, int partition, long offset, long timestamp)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
    }

    public string Topic { get; }

    public int Partition { get; }

    public long Offset { get; }

    public long Timestamp { get; }
}

/// <summary>
/// One polled record reduced to the consumer engine's inputs, as Python's consumer backends yield
/// <c>(timestamp_ms, nbytes)</c>: the record's timestamp in milliseconds and its key + value length.
/// </summary>
public readonly struct PolledRecord
{
    public PolledRecord(long timestampMs, int nbytes)
    {
        TimestampMs = timestampMs;
        NBytes = nbytes;
    }

    public long TimestampMs { get; }

    public int NBytes { get; }
}

/// <summary>
/// The handle a sync backend's plain <c>Send</c> returns, Java's <c>Future&lt;RecordMetadata&gt;</c>:
/// <see cref="Get"/> blocks until the record is acknowledged and returns its metadata, or rethrows its failure.
/// </summary>
public readonly struct PerfSendHandle
{
    private readonly Func<object, PerfRecordMetadata>? _get;
    private readonly object? _state;

    public PerfSendHandle(Func<object, PerfRecordMetadata> get, object state)
    {
        _get = get ?? throw new ArgumentNullException(nameof(get));
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    public PerfRecordMetadata Get() =>
        _get is null
            ? throw new InvalidOperationException("default(PerfSendHandle) has no send to wait for.")
            : _get(_state!);
}

/// <summary>
/// The engine's delivery notification, Java's <c>Callback.onCompletion</c>: a backend calls it once for each record
/// whose callback-taking send returned, on its client's delivery thread, and for none whose send threw. It must not
/// throw.
/// </summary>
public interface IPerfDeliveryCallback
{
    void OnCompletion(long sendStartMs, PerfRecordMetadata metadata, Exception? exception);
}

/// <summary>
/// The synchronous producer backend (Python's <c>main</c> loop): each <c>Send</c> returns once the client has
/// accepted the record; the callback-taking one is the measured send, the plain one the warmup's.
/// </summary>
public interface IProducerBackend : IDisposable
{
    PerfSendHandle Send(string topic, byte[]? key, byte[]? value);

    void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs);

    void Flush(CancellationToken cancellationToken);

    void Close();
}

/// <summary>
/// The asynchronous producer backend (Python's <c>async_main</c> loop): each <c>Send</c> completes once the client
/// has accepted the record; the callback-taking one is the measured send, the plain one the warmup's.
/// </summary>
public interface IAsyncProducerBackend : IDisposable, IAsyncDisposable
{
    ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value);

    ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs);

    Task Flush(CancellationToken cancellationToken);

    Task Close();
}

/// <summary>The synchronous consumer backend, whose blocking polls yield <see cref="PolledRecord"/>s.</summary>
public interface IConsumerBackend : IDisposable
{
    void Subscribe(string topic);

    bool Assigned();

    IReadOnlyList<PolledRecord> PollBatch();

    IReadOnlyList<PolledRecord> PollSingle();

    void Close();
}

/// <summary>The asynchronous consumer backend: its operations are tasks, except the assignment read.</summary>
public interface IAsyncConsumerBackend : IDisposable, IAsyncDisposable
{
    Task Subscribe(string topic);

    bool Assigned();

    Task<IReadOnlyList<PolledRecord>> PollBatch();

    Task<IReadOnlyList<PolledRecord>> PollSingle();

    Task Close();
}
