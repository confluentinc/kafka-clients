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
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Producer;

/// <summary>
/// The real synchronous Kafka producer, Java's <c>org.apache.kafka.clients.producer.KafkaProducer&lt;K, V&gt;</c>:
/// it serializes the key and value on the caller's thread and forwards the bytes to the native producer.
/// </summary>
/// <typeparam name="TKey">The key type serialized on the send path.</typeparam>
/// <typeparam name="TValue">The value type serialized on the send path.</typeparam>
public sealed class KafkaProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly NativeProducer _native;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;

    /// <summary>
    /// Creates a producer from a configuration map of Java's dotted keys and the key and value serializers
    /// (Java <c>KafkaProducer(Map, Serializer&lt;K&gt;, Serializer&lt;V&gt;)</c>).
    /// </summary>
    /// <param name="configs">
    /// The producer configuration, keyed by Java's dotted names: each value of the type Java's <c>ConfigDef</c>
    /// accepts for its key, or the string Java would parse. A <c>null</c> value leaves its key unset, unless the
    /// key's validator refuses <c>null</c>. The serializer keys (<c>key.serializer</c>, <c>value.serializer</c>,
    /// <c>serializer.encoding</c>, <c>key.serializer.encoding</c>, <c>value.serializer.encoding</c>) are ignored: the
    /// serializers are the arguments.
    /// </param>
    /// <param name="keySerializer">The serializer for record keys.</param>
    /// <param name="valueSerializer">The serializer for record values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configs"/> is null.</exception>
    /// <exception cref="ConfigException">
    /// <paramref name="keySerializer"/> or <paramref name="valueSerializer"/> is null: Java then reads the
    /// <c>key.serializer</c> / <c>value.serializer</c> config, which .NET config cannot set.
    /// </exception>
    /// <exception cref="ConfigException">
    /// A value is not of a type its key accepts, a <c>null</c> value is one its key's validator refuses, a key is
    /// <c>null</c>, or the client refuses a value.
    /// </exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    /// <remarks>
    /// Each serializer that is <see cref="IDisposable"/> is disposed once, key then value, when the producer is closed
    /// or disposed, or when the core cannot build it, but not when this constructor throws
    /// <see cref="ArgumentNullException"/> or refuses a serializer, a value's type or a <c>null</c> with
    /// <see cref="ConfigException"/>. So a serializer shared with another client, or used after the close, sees it
    /// disposed. A <c>Dispose</c> that throws is written to <see cref="System.Diagnostics.Trace"/> and not rethrown.
    /// A serializer that is only <see cref="IAsyncDisposable"/> is not disposed.
    /// </remarks>
    public KafkaProducer(
        IReadOnlyDictionary<string, object?> configs,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        // Java copies the config map first, so a null one fails before a missing serializer.
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        _keySerializer = NativeProducer.RequireSerializer(keySerializer, "key.serializer");
        _valueSerializer = NativeProducer.RequireSerializer(valueSerializer, "value.serializer");
        _native = NativeProducer.Create(configs, serdes: new SerdeDisposal(keySerializer, valueSerializer));
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
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback)
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

    private KafkaFuture<RecordMetadata> SendValidated(ProducerRecord<TKey, TValue> record, IDeliveryCallback? callback)
    {
        // Java's doSend checks for a closed producer before it serializes.
        _native.ThrowIfClosed();

        SerializedProducerRecord serialized;
        try
        {
            serialized = SerializedProducerRecord.Serialize(record, _keySerializer, _valueSerializer, wrapInvalidCast: true);
        }
        catch (ApiException error)
        {
            return FutureFailure.Send(record.Topic, record.Partition, error, callback);
        }

        // As Java's doSend, the headers turn read-only once the serializers ran; the mocks leave them writable.
        record.Headers.SetReadOnly();

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

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();
}
