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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Producer;

/// <summary>
/// The real asynchronous Kafka producer, Java's <c>org.apache.kafka.clients.producer.KafkaProducer&lt;K, V&gt;</c>:
/// it serializes the key and value, awaiting a serializer that waits, and forwards the bytes to the native producer.
/// </summary>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
{
    /// <summary>The name prefix of the producer's network thread, Java's <c>KafkaProducer.NETWORK_THREAD_PREFIX</c>.</summary>
    public const string NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";

    /// <summary>The group of the producer's metrics, Java's <c>KafkaProducer.PRODUCER_METRIC_GROUP_NAME</c>.</summary>
    public const string PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private readonly NativeProducer _native;
    private readonly AsyncSerializerCall<TKey> _key;
    private readonly AsyncSerializerCall<TValue> _value;
    private readonly bool _awaits;

    /// <summary>
    /// Creates a producer from a configuration map of Java's dotted keys and the key and value serializers
    /// (Java <c>KafkaProducer(Map, Serializer&lt;K&gt;, Serializer&lt;V&gt;)</c>). <c>Send</c> awaits the serializers
    /// unless both are built-in ones (<see cref="AsyncStringSerializer"/>, ...), which never wait; the remarks of
    /// <see cref="IAsyncProducer{TKey, TValue}"/> describe both cases.
    /// </summary>
    /// <param name="configs">
    /// The producer configuration, keyed by Java's dotted names: each value of the type Java's <c>ConfigDef</c>
    /// accepts for its key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the
    /// key's validator refuses <c>null</c>. The serializer keys (<c>key.serializer</c>, <c>value.serializer</c>,
    /// <c>serializer.encoding</c>, <c>key.serializer.encoding</c>, <c>value.serializer.encoding</c>) are ignored: the
    /// serializers are the arguments, which the producer never configures, as Java never configures a serializer it is
    /// given.
    /// </param>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null: Java then reads the
    /// <c>key.serializer</c> / <c>value.serializer</c> config, which .NET config cannot set.
    /// </exception>
    /// <exception cref="IllegalArgumentException">
    /// A serializer declares a public <c>DisposeAsync()</c> but is not <see cref="IAsyncDisposable"/>, a public
    /// <c>Dispose()</c> but is not <see cref="IDisposable"/>, or a public <c>Close()</c> and is neither: the producer
    /// would never call that method.
    /// </exception>
    /// <exception cref="ConfigException">
    /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
    /// <c>null</c>, or the client refuses a value.
    /// </exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    /// <remarks>
    /// Each serializer is disposed once, key then value, through <see cref="IAsyncDisposable"/> when it has it and
    /// otherwise through <see cref="IDisposable"/>, when the producer is closed or disposed, or when the core fails to
    /// build it with the <see cref="KafkaException"/> "Failed to construct kafka
    /// producer", but not for a configuration the core refuses with another error, nor when this constructor throws
    /// <see cref="ArgumentNullException"/> or refuses a serializer, a value's type or a <c>null</c> with
    /// <see cref="ConfigException"/> or a serializer with <see cref="IllegalArgumentException"/>. So a serializer
    /// shared with another client, or used after the close, sees it
    /// disposed. A close or a <c>DisposeAsync()</c> of the producer awaits a serializer's <c>DisposeAsync()</c>; a
    /// <c>Dispose()</c> of the producer and this constructor wait for it on the calling thread. A serializer's
    /// <c>DisposeAsync</c> or <c>Dispose</c> that throws is written to <see cref="System.Diagnostics.Trace"/> and not
    /// rethrown.
    /// </remarks>
    public AsyncKafkaProducer(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncSerializer<TKey> keySerializer,
        IAsyncSerializer<TValue> valueSerializer)
    {
        // Java copies the config map first, so a null one fails before a missing serializer.
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        IAsyncSerializer<TKey> key = NativeProducer.RequireSerializer(keySerializer, "key.serializer");
        IAsyncSerializer<TValue> value = NativeProducer.RequireSerializer(valueSerializer, "value.serializer");
        SerdeGuard.Check(key, DefaultMethodGuard.Producer, disposes: true);
        SerdeGuard.Check(value, DefaultMethodGuard.Producer, disposes: true);
        _key = new AsyncSerializerCall<TKey>(key);
        _value = new AsyncSerializerCall<TValue>(value);
        _awaits = _key.Inner is null || _value.Inner is null;
        _native = NativeProducer.Create(
            configs, NativeProducer.PumpThreadName, SerdeDisposal.OfAsyncSerdes(key, value));
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
        ICallback callback,
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
        IAsyncCallback callback,
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

    // Not async, so with two built-in serializers a serializer throw stays synchronous and no state machine runs.
    private ValueTask<AsyncKafkaFuture<RecordMetadata>> SendValidated(
        ProducerRecord<TKey, TValue> record,
        ICallback? callback,
        CancellationToken cancellationToken,
        IAsyncCallback? asyncCallback = null)
    {
        // Java's doSend checks for a closed producer before it serializes.
        _native.ThrowIfClosed();

        if (_awaits)
        {
            return SendAwaited(record, callback, cancellationToken, asyncCallback);
        }

        SerializedProducerRecord serialized;
        try
        {
            serialized = SerializedProducerRecord.Serialize(
                record, _key.Inner!, _key.InnerWithHeaders, _value.Inner!, _value.InnerWithHeaders, mock: false);
        }
        catch (ApiException error)
        {
            return FutureFailure.SendAsync(
                record.Topic(), record.Partition(), error, callback, asyncCallback, cancellationToken,
                _native.FailureCallbacks);
        }

        // As Java's doSend, the headers turn read-only once the serializers ran; the mocks leave them writable.
        record.Headers().SetReadOnly();

        return _native.SendViaPump(serialized, callback, cancellationToken, asyncCallback);
    }

    // The record enters the producer when its serialization completes, so two sends in flight enter in that order.
    private async ValueTask<AsyncKafkaFuture<RecordMetadata>> SendAwaited(
        ProducerRecord<TKey, TValue> record,
        ICallback? callback,
        CancellationToken cancellationToken,
        IAsyncCallback? asyncCallback)
    {
        SerializedProducerRecord serialized;
        try
        {
            serialized = await SerializedProducerRecord.SerializeAsync(record, _key, _value, mock: false, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ApiException error)
        {
            return await FutureFailure.SendAsync(
                    record.Topic(), record.Partition(), error, callback, asyncCallback, cancellationToken,
                    _native.FailureCallbacks)
                .ConfigureAwait(false);
        }

        record.Headers().SetReadOnly();

        return await _native.SendViaPump(serialized, callback, cancellationToken, asyncCallback).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task InitTransactions(CancellationToken cancellationToken = default) =>
        _native.InitTransactionsWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task BeginTransaction(CancellationToken cancellationToken = default) =>
        _native.BeginTransactionWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata,
        CancellationToken cancellationToken = default) =>
        _native.SendOffsetsToTransactionWithCallback(offsets, groupMetadata, cancellationToken);

    /// <inheritdoc/>
    public Task CommitTransaction(CancellationToken cancellationToken = default) =>
        _native.CommitTransactionWithCallback(cancellationToken);

    /// <inheritdoc/>
    public Task AbortTransaction(CancellationToken cancellationToken = default) =>
        _native.AbortTransactionWithCallback(cancellationToken);

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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
