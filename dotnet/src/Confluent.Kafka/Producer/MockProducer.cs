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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Producer;

/// <summary>
/// A broker-free synchronous producer for tests, Java's <c>org.apache.kafka.clients.producer.MockProducer&lt;K, V&gt;</c>:
/// it serializes the key and value like <see cref="KafkaProducer{TKey, TValue}"/> and counts the records it sends.
/// </summary>
/// <remarks>
/// With <c>autoComplete: false</c>, <c>Send</c> still returns at once, and the future's
/// <see cref="KafkaFuture{T}.Get()"/> blocks until <see cref="CompleteNext"/> or <see cref="ErrorNext"/> resolves the
/// send, which the sending thread may call itself. <see cref="Dispose"/> first flushes, completing the pending
/// sends, unless a close has begun. <c>Close</c> leaves a pending send pending.
/// </remarks>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class MockProducer<TKey, TValue> : IProducer<TKey, TValue>
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
    /// When <see langword="true"/> (the default), each send completes at once. When <see langword="false"/>,
    /// <c>Send</c> still returns at once, and the future's <see cref="KafkaFuture{T}.Get()"/> blocks until
    /// <see cref="CompleteNext"/> or <see cref="ErrorNext"/> resolves the send, which the sending thread may call
    /// itself; <see cref="Dispose"/> first flushes, completing the pending sends, unless a close has begun, and
    /// <c>Close</c> leaves a pending send pending.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null.
    /// </exception>
    public MockProducer(
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool autoComplete = true)
    {
        _keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        _native = NativeProducer.CreateMock(autoComplete);
    }

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        return SendValidated(record, callback: null);
    }

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, ICallback callback)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        return SendValidated(record, callback);
    }

    private KafkaFuture<RecordMetadata> SendValidated(ProducerRecord<TKey, TValue> record, ICallback? callback)
    {
        // Java's doSend checks for a closed producer before it serializes.
        _native.ThrowIfClosed();

        // Java's MockProducer calls the serializers with no try, so every throw leaves Send unchanged.
        SerializedProducerRecord serialized =
            SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer, wrapInvalidCast: false);

        return _native.Send(serialized, callback);
    }

    /// <inheritdoc/>
    public void Flush() => _native.Flush();

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _native.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _native.Metrics();

    /// <inheritdoc/>
    public void Close() => _native.Close();

    /// <inheritdoc/>
    public void Close(TimeSpan timeout) => _native.Close(timeout);

    /// <summary>
    /// Completes the earliest uncompleted send successfully (Java <c>MockProducer.completeNext()</c>), for a mock
    /// created with <c>autoComplete: false</c>. The send's callback and its future complete on the producer's
    /// completion thread just after this returns, where Java's <c>completeNext</c> does both before it returns, so
    /// call the future's <see cref="KafkaFuture{T}.Get()"/> before asserting on a callback.
    /// </summary>
    /// <returns><see langword="true"/> if a pending completion was resolved; otherwise <see langword="false"/>.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool CompleteNext() => _native.MockCompleteNext();

    /// <summary>
    /// Fails the earliest uncompleted send with <paramref name="e"/> (Java <c>MockProducer.errorNext</c>); its
    /// delivery callback receives the same instance, and its future's <see cref="KafkaFuture{T}.Get()"/> throws an
    /// <see cref="Confluent.Kafka.ExecutionException"/> around it.
    /// </summary>
    /// <param name="e">
    /// The exception to fail the send with; <see langword="null"/> completes it successfully, as
    /// <see cref="CompleteNext"/> does.
    /// </param>
    /// <returns><see langword="true"/> if a pending send was completed; otherwise <see langword="false"/>.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool ErrorNext(Exception? e) => _native.MockErrorNext(e);

    /// <summary>
    /// The number of records in the sent history (Java <c>MockProducer.history().size()</c>); the native mock
    /// exposes the count only.
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public int HistoryCount() => _native.MockHistoryCount();

    /// <summary>
    /// Clears the sent history and any pending completions (Java <c>MockProducer.clear()</c>).
    /// </summary>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public void Clear() => _native.MockClear();

    /// <summary>Whether the producer has been closed (Java <c>MockProducer.closed()</c>).</summary>
    /// <returns><see langword="true"/> once the producer is closed or disposed.</returns>
    public bool Closed() => _native.MockClosed();

    /// <summary>
    /// Whether every sent record has completed, none still waiting for <c>CompleteNext</c> or <c>ErrorNext</c> (Java
    /// <c>MockProducer.flushed()</c>).
    /// </summary>
    /// <returns><see langword="true"/> when no completion is pending.</returns>
    /// <exception cref="IllegalStateException">The producer is closed.</exception>
    public bool Flushed() => _native.MockFlushed();

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
