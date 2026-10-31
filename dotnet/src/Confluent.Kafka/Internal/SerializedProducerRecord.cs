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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The serialized key and value of a <see cref="ProducerRecord{TKey, TValue}"/> with its topic, partition and
/// timestamp, as the native send reads them; a <see langword="null"/> key or value is an absent field.
/// </summary>
internal readonly struct SerializedProducerRecord
{
    internal SerializedProducerRecord(
        string topic,
        int? partition,
        long? timestamp,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value)
    {
        Topic = topic;
        Partition = partition;
        Timestamp = timestamp;
        Key = key;
        Value = value;
    }

    internal string Topic { get; }

    internal int? Partition { get; }

    internal long? Timestamp { get; }

    internal ReadOnlyMemory<byte>? Key { get; }

    internal ReadOnlyMemory<byte>? Value { get; }

    internal static SerializedProducerRecord Serialize<TKey, TValue>(
        ProducerRecord<TKey, TValue> record,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer)
    {
        byte[]? keyBytes = SerializeField(keySerializer, record.Topic, record.Key, isKey: true);
        byte[]? valueBytes = SerializeField(valueSerializer, record.Topic, record.Value, isKey: false);

        return new SerializedProducerRecord(
            record.Topic,
            record.Partition,
            record.Timestamp,
            keyBytes is null ? (ReadOnlyMemory<byte>?)null : keyBytes,
            valueBytes is null ? (ReadOnlyMemory<byte>?)null : valueBytes);
    }

    private static byte[]? SerializeField<T>(ISerializer<T> serializer, string topic, T? data, bool isKey)
    {
        try
        {
            // The serializer is called also for a null key or value, as Java's doSend calls it.
            return serializer.Serialize(topic, data!);
        }
        catch (Exception exception)
        {
            // Every throw is wrapped, a SerializationException included, so each failure names the topic.
            throw new SerializationException(
                $"Error serializing {(isKey ? "key" : "value")} for topic '{topic}'.",
                exception);
        }
    }
}
