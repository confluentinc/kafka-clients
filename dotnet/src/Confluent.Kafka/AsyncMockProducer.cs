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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// A broker-free asynchronous producer for tests, Java's <c>org.apache.kafka.clients.producer.MockProducer&lt;K, V&gt;</c>:
/// it serializes the key and value like <see cref="AsyncKafkaProducer{TKey, TValue}"/> and counts the records it sends.
/// </summary>
/// <remarks>
/// With <c>autoComplete: false</c>, a send's future stays pending until <see cref="CompleteNext"/> or
/// <see cref="ErrorNext"/> resolves it, and its first stage does not wait for that. <see cref="Dispose"/> and
/// <see cref="DisposeAsync"/> first flush, completing the pending sends, unless a close has begun. <c>Close</c>
/// leaves a pending send pending.
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
{
    private readonly NativeProducer _native;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;

    /// <summary>
    /// Creates a broker-free mock producer with the key and value serializers, as Java's <c>MockProducer</c> takes them.
    /// </summary>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), each send completes at once. When <see langword="false"/>, a
    /// send's future stays pending until <see cref="CompleteNext"/> or <see cref="ErrorNext"/> resolves it;
    /// <see cref="Dispose"/> and <see cref="DisposeAsync"/> first flush, completing the pending sends, unless a
    /// close has begun, and <c>Close</c> leaves a pending send pending.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null.
    /// </exception>
    public AsyncMockProducer(
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool autoComplete = true)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.CreateMock(autoComplete, NativeProducer.PumpThreadName);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        return SendValidated(record, callback, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<TKey, TValue> record,
        IAsyncDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        return SendValidated(record, null, cancellationToken, callback);
    }

    // Not async, so a serializer throw stays synchronous.
    private ValueTask<AsyncKafkaFuture<RecordMetadata>> SendValidated(
        ProducerRecord<TKey, TValue> record,
        IDeliveryCallback? callback,
        CancellationToken cancellationToken,
        IAsyncDeliveryCallback? asyncCallback = null)
    {
        // Java's doSend checks for a closed producer before it serializes.
        _native.ThrowIfClosed();

        SerializedProducerRecord serialized =
            SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer);

        return _native.SendViaPump(serialized, callback, cancellationToken, asyncCallback);
    }

    /// <inheritdoc/>
    public Task Flush(CancellationToken cancellationToken = default) =>
        _native.FlushWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default) =>
        _native.PartitionsForWithCallback(topic, cancellationToken);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        _native.CloseWithCallback(timeout, cancellationToken);

    /// <summary>
    /// Completes the earliest uncompleted send successfully (Java <c>MockProducer.completeNext()</c>), for a mock
    /// created with <c>autoComplete: false</c>.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Fails the earliest uncompleted send with <paramref name="exception"/> (Java <c>MockProducer.errorNext</c>); its
    /// delivery callback and its future's <see cref="AsyncKafkaFuture{T}.Get()"/> receive the same instance.
    /// </summary>
    /// <param name="exception">The exception to fail the send with.</param>
    /// <returns><see langword="true"/> if a pending send was failed; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public bool ErrorNext(Exception exception) => _native.MockErrorNext(exception);

    /// <summary>
    /// The number of records in the sent history (Java <c>MockProducer.history().size()</c>); the native mock
    /// exposes the count only.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public int HistoryCount() => _native.MockHistoryCount();

    /// <summary>
    /// Clears the sent history and any pending completions (Java <c>MockProducer.clear()</c>).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
