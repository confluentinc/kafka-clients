// Copyright 2026 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading;

using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The binding's decode tests' consumers: the real <see cref="KafkaConsumer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> over the C mock, built by their internal <c>CreateOverMock</c>,
/// which checks, builds and owns the deserializers as <c>Create</c> does. A test adds byte records to the core and
/// drives its mock-only helpers through the extension methods here, named as the mock's own helpers, which call
/// <see cref="NativeConsumer"/> as the mock does; the consumer's <c>Poll</c> then decodes them as fetched records.
/// </summary>
internal static class DecodingConsumers
{
    private static readonly IReadOnlyDictionary<string, object?> s_noConfigs = new Dictionary<string, object?>();

    internal static KafkaConsumer<TKey, TValue> Create<TKey, TValue>(
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer,
        string? autoOffsetReset = NativeConsumer.DefaultOffsetResetStrategy) =>
        KafkaConsumer<TKey, TValue>.CreateOverMock(s_noConfigs, keyDeserializer, valueDeserializer, autoOffsetReset);

    internal static KafkaConsumer<TKey, TValue> Create<TKey, TValue>(
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer,
        string? autoOffsetReset = NativeConsumer.DefaultOffsetResetStrategy) =>
        KafkaConsumer<TKey, TValue>.CreateOverMock(configs, keyDeserializer, valueDeserializer, autoOffsetReset);

    // The awaited construction completes within the call for deserializers whose Configure does not wait, which
    // every caller's are; a test with one that waits awaits CreateOverMock itself.
    internal static AsyncKafkaConsumer<TKey, TValue> CreateAsync<TKey, TValue>(
        IAsyncDeserializer<TKey>? keyDeserializer,
        IAsyncDeserializer<TValue>? valueDeserializer,
        string? autoOffsetReset = NativeConsumer.DefaultOffsetResetStrategy) =>
        AsyncKafkaConsumer<TKey, TValue>
            .CreateOverMock(s_noConfigs, keyDeserializer, valueDeserializer, autoOffsetReset, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

    internal static void AddRecord<TKey, TValue>(
        this KafkaConsumer<TKey, TValue> consumer, string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        consumer.Native.AddRecord(topic, partition, offset, key, value);

    internal static void AddRecord<TKey, TValue>(
        this AsyncKafkaConsumer<TKey, TValue> consumer, string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        consumer.Native.AddRecord(topic, partition, offset, key, value);

    internal static void UpdateBeginningOffset<TKey, TValue>(
        this KafkaConsumer<TKey, TValue> consumer, string topic, int partition, long offset) =>
        consumer.Native.UpdateBeginningOffset(topic, partition, offset);

    internal static void UpdatePartitions<TKey, TValue>(
        this KafkaConsumer<TKey, TValue> consumer,
        string topic,
        int partitionCount,
        int leaderId,
        string leaderHost,
        int leaderPort) =>
        consumer.Native.UpdatePartitions(topic, partitionCount, leaderId, leaderHost, leaderPort);
}
