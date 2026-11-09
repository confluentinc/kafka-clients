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

    // wrapInvalidCast is true for KafkaProducer and AsyncKafkaProducer, whose Java doSend wraps a
    // ClassCastException in a SerializationException, and false for the mocks: Java's MockProducer calls the
    // serializers with no try, so every throw leaves send unchanged.
    internal static SerializedProducerRecord Serialize<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        bool wrapInvalidCast)
    {
        ReadOnlyMemory<byte>? keyBytes = wrapInvalidCast
            ? SerializeField(keySerializer, record.Topic(), record.Key(), isKey: true)
            : keySerializer.Serialize(record.Topic(), record.Key()!);
        ReadOnlyMemory<byte>? valueBytes = wrapInvalidCast
            ? SerializeField(valueSerializer, record.Topic(), record.Value(), isKey: false)
            : valueSerializer.Serialize(record.Topic(), record.Value()!);

        return new SerializedProducerRecord(
            record.Topic(),
            record.Partition(),
            record.Timestamp(),
            keyBytes,
            valueBytes,
            record.Headers());
    }

    private static ReadOnlyMemory<byte>? SerializeField<T>(ISerializer<T> serializer, string topic, T? data, bool isKey)
    {
        try
        {
            // The serializer is called also for a null key or value, as Java's doSend calls it.
            return serializer.Serialize(topic, data!);
        }
        catch (InvalidCastException exception)
        {
            // Java's doSend wraps only a ClassCastException; any other throw, an ApiException included, leaves
            // the serializer unchanged.
            string field = isKey ? "key" : "value";
            throw new SerializationException(
                $"Can't convert {field} of class {ClassName(data?.GetType())} to class "
                + $"{ClassName(serializer.GetType())} specified as the {field} serializer",
                exception);
        }
    }

    private static string ClassName(Type? type) => type is null ? "null" : type.FullName ?? type.Name;
}
