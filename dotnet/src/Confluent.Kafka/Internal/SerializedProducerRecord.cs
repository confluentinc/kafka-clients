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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
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
        Headers? headers = null)
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

    internal Headers? Headers { get; }

    // mock is false for KafkaProducer and AsyncKafkaProducer, as Java's doSend: a serializer's headers form gets the
    // record's own headers, and a ClassCastException is wrapped in a SerializationException. It is true for the mocks,
    // as Java's MockProducer on its empty cluster: each headers form gets a new, empty Headers that is then dropped, and
    // every throw leaves send unchanged, since the serializers are called with no try.
    internal static SerializedProducerRecord Serialize<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        ISerializer<TKey> keySerializer,
        ISerializerWithHeaders<TKey>? keyWithHeaders,
        ISerializer<TValue> valueSerializer,
        ISerializerWithHeaders<TValue>? valueWithHeaders,
        bool mock)
    {
        Headers headers = record.Headers();
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

    // The serializer is called also for a null key or value, as Java's doSend and MockProducer.send call it.
    private static ReadOnlyMemory<byte>? SerializeOnMock<T>(
        ISerializer<T> serializer, ISerializerWithHeaders<T>? withHeaders, string topic, T? data) =>
        withHeaders is null ? serializer.Serialize(topic, data) : withHeaders.Serialize(topic, new Headers(), data);

    private static ReadOnlyMemory<byte>? SerializeField<T>(
        ISerializer<T> serializer, ISerializerWithHeaders<T>? withHeaders, string topic, Headers headers, T? data, bool isKey)
    {
        try
        {
            return withHeaders is null ? serializer.Serialize(topic, data) : withHeaders.Serialize(topic, headers, data);
        }
        catch (InvalidCastException exception)
        {
            // Java's doSend wraps only a ClassCastException; any other throw, an ApiException included, leaves
            // the serializer unchanged.
            string field = isKey ? "key" : "value";
            throw new SerializationException(
                $"Can't convert {field} of class {ClassName(data?.GetType())} to class "
                + $"{ClassName(serializer.GetType())} specified in {field}.serializer",
                exception);
        }
    }

    private static string ClassName(Type? type) => type is null ? "null" : type.FullName ?? type.Name;
}
