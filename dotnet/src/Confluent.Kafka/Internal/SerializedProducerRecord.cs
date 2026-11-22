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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The serialized key and value of a <see cref="ProducerRecord{TKey, TValue}"/> with its topic, partition, timestamp
/// and headers, as the native send reads them; a <see langword="null"/> key or value is an absent field.
/// </summary>
internal readonly struct SerializedProducerRecord
{
    internal SerializedProducerRecord(
        string topic,
        int? partition,
        long? timestamp,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value,
        RecordHeaders? headers = null)
    {
        Topic = topic;
        Partition = partition;
        Timestamp = timestamp;
        Key = key;
        Value = value;
        Headers = headers;
    }

    internal string Topic { get; }

    internal int? Partition { get; }

    internal long? Timestamp { get; }

    internal ReadOnlyMemory<byte>? Key { get; }

    internal ReadOnlyMemory<byte>? Value { get; }

    internal RecordHeaders? Headers { get; }

    // mock is false for KafkaProducer and AsyncKafkaProducer, as Java's doSend: a serializer's headers form gets the
    // record's own headers, and a ClassCastException is wrapped in a SerializationException. It is true for the mocks,
    // as Java's MockProducer on its empty cluster: each headers form gets a new, empty RecordHeaders that is then dropped,
    // and every throw leaves send unchanged, since the serializers are called with no try.
    internal static SerializedProducerRecord Serialize<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        ISerializer<TKey> keySerializer,
        ISerializerWithHeaders<TKey>? keyWithHeaders,
        ISerializer<TValue> valueSerializer,
        ISerializerWithHeaders<TValue>? valueWithHeaders,
        bool mock)
    {
        RecordHeaders headers = record.OwnHeaders;
        ReadOnlyMemory<byte>? keyBytes = mock
            ? SerializeOnMock(keySerializer, keyWithHeaders, record.Topic(), record.Key())
            : SerializeField(keySerializer, keyWithHeaders, record.Topic(), headers, record.Key(), isKey: true);
        ReadOnlyMemory<byte>? valueBytes = mock
            ? SerializeOnMock(valueSerializer, valueWithHeaders, record.Topic(), record.Value())
            : SerializeField(valueSerializer, valueWithHeaders, record.Topic(), headers, record.Value(), isKey: false);

        return new SerializedProducerRecord(
            record.Topic(),
            record.Partition(),
            record.Timestamp(),
            keyBytes,
            valueBytes,
            headers);
    }

    // The asynchronous producers' form when a serializer is awaited: the key, then the value. A built-in side is
    // called through its synchronous class. mock is as on Serialize: an awaited serializer's headers form gets a new,
    // empty RecordHeaders on a mock and the record's own on a client, and its form without headers gets none.
    internal static async ValueTask<SerializedProducerRecord> SerializeAsync<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        AsyncSerializerCall<TKey> key,
        AsyncSerializerCall<TValue> value,
        bool mock,
        CancellationToken cancellationToken)
    {
        RecordHeaders headers = record.OwnHeaders;
        ReadOnlyMemory<byte>? keyBytes =
            await SerializeFieldAsync(key, record.Topic(), headers, record.Key(), isKey: true, mock, cancellationToken)
                .ConfigureAwait(false);
        ReadOnlyMemory<byte>? valueBytes =
            await SerializeFieldAsync(value, record.Topic(), headers, record.Value(), isKey: false, mock, cancellationToken)
                .ConfigureAwait(false);

        return new SerializedProducerRecord(
            record.Topic(),
            record.Partition(),
            record.Timestamp(),
            keyBytes,
            valueBytes,
            headers);
    }

    private static ValueTask<ReadOnlyMemory<byte>?> SerializeFieldAsync<T>(
        AsyncSerializerCall<T> call,
        string topic,
        IHeaders headers,
        T? data,
        bool isKey,
        bool mock,
        CancellationToken cancellationToken)
    {
        if (call.Inner is { } inner)
        {
            return new ValueTask<ReadOnlyMemory<byte>?>(mock
                ? SerializeOnMock(inner, call.InnerWithHeaders, topic, data)
                : SerializeField(inner, call.InnerWithHeaders, topic, headers, data, isKey));
        }

        if (mock)
        {
            return call.WithHeaders is null
                ? call.Serializer.Serialize(topic, data, cancellationToken)
                : call.WithHeaders.Serialize(topic, new RecordHeaders(), data, cancellationToken);
        }

        return AwaitField(call, topic, headers, data, isKey, cancellationToken);
    }

    private static async ValueTask<ReadOnlyMemory<byte>?> AwaitField<T>(
        AsyncSerializerCall<T> call,
        string topic,
        IHeaders headers,
        T? data,
        bool isKey,
        CancellationToken cancellationToken)
    {
        try
        {
            return await (call.WithHeaders is null
                    ? call.Serializer.Serialize(topic, data, cancellationToken)
                    : call.WithHeaders.Serialize(topic, headers, data, cancellationToken))
                .ConfigureAwait(false);
        }
        catch (InvalidCastException exception)
        {
            throw CannotConvert(call.Serializer, data, isKey, exception);
        }
    }

    // The serializer is called also for a null key or value, as Java's doSend and MockProducer.send call it.
    private static ReadOnlyMemory<byte>? SerializeOnMock<T>(
        ISerializer<T> serializer, ISerializerWithHeaders<T>? withHeaders, string topic, T? data) =>
        withHeaders is null
            ? serializer.Serialize(topic, data)
            : withHeaders.Serialize(topic, new RecordHeaders(), data);

    private static ReadOnlyMemory<byte>? SerializeField<T>(
        ISerializer<T> serializer, ISerializerWithHeaders<T>? withHeaders, string topic, IHeaders headers, T? data, bool isKey)
    {
        try
        {
            return withHeaders is null ? serializer.Serialize(topic, data) : withHeaders.Serialize(topic, headers, data);
        }
        catch (InvalidCastException exception)
        {
            throw CannotConvert(serializer, data, isKey, exception);
        }
    }

    // Java's doSend wraps only a ClassCastException; any other throw, an ApiException included, leaves the serializer
    // unchanged.
    private static SerializationException CannotConvert<T>(
        object serializer, T? data, bool isKey, InvalidCastException exception)
    {
        string field = isKey ? "key" : "value";
        return new SerializationException(
            $"Can't convert {field} of class {ClassName(data?.GetType())} to class "
            + $"{ClassName(serializer.GetType())} specified in {field}.serializer",
            exception);
    }

    private static string ClassName(Type? type) => type is null ? "null" : type.FullName ?? type.Name;
}
