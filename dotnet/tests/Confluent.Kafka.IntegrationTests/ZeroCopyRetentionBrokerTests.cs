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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;

using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// The memory forms against a broker, where a polled batch owns its bytes: a key or value a deserializer keeps stays
/// readable after the next poll and after the consumer is closed and disposed, on the sync and the async consumer,
/// and a pin a call-scoped deserializer leaves open keeps the batch from being released until the pin is.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class ZeroCopyRetentionBrokerTests
{
    private const int FirstCount = 5;

    private const int NextCount = 5;

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(0.5);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public ZeroCopyRetentionBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task KafkaConsumer_KeysAndValuesItsDeserializersKeep_StayReadable_AfterTheNextPollAndItsClose()
    {
        string topic = await TopicAsync();
        Produce(topic, 0, FirstCount);
        List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> kept =
            new List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>();
        using (KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>.Create(
                ConsumerConfigs(), Serdes.ReadOnlyMemory().Deserializer(), Serdes.ReadOnlyMemory().Deserializer()))
        {
            consumer.Assign(new[] { new TopicPartition(topic, 0) });
            PollInto(kept, () => consumer.Poll(s_poll), FirstCount);
            AssertKept(kept, 0);

            Produce(topic, FirstCount, NextCount);
            List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> next =
                new List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>();
            PollInto(next, () => consumer.Poll(s_poll), NextCount);
            Collect();
            AssertKept(next, FirstCount);
            AssertKept(kept, 0);

            consumer.Close();
        }

        Collect();
        AssertKept(kept, 0);
    }

    [SkippableFact]
    public Task AsyncKafkaConsumer_KeysAndValuesTheBuiltInDeserializerKeeps_StayReadable_AfterTheNextPollAndItsClose() =>
        AsyncConsumerKeepsAsync(new AsyncReadOnlyMemoryDeserializer(), new AsyncReadOnlyMemoryDeserializer());

    [SkippableFact]
    public Task AsyncKafkaConsumer_KeysAndValuesAnAwaitedDeserializerKeeps_StayReadable_AfterTheNextPollAndItsClose() =>
        AsyncConsumerKeepsAsync(new YieldingRetainingDeserializer(), new YieldingRetainingDeserializer());

    [SkippableFact]
    public async Task AsyncKafkaConsumer_APinACallScopedDeserializerLeavesOpen_KeepsTheBatch_UntilItIsReleased()
    {
        string topic = await TopicAsync();
        Produce(topic, 0, FirstCount);
        PinningDeserializer values = new PinningDeserializer(FirstCount);
        await using (AsyncKafkaConsumer<string, int> consumer =
            await AsyncKafkaConsumer<string, int>.Create(ConsumerConfigs(), new AsyncStringDeserializer(), values))
        {
            await consumer.Assign(new[] { new TopicPartition(topic, 0) });
            Assert.Equal(FirstCount, await CountAsync(() => consumer.Poll(s_poll), FirstCount));
            AssertPinned(values);

            Produce(topic, FirstCount, NextCount);
            Assert.Equal(NextCount, await CountAsync(() => consumer.Poll(s_poll), NextCount));
            Collect();
            AssertPinned(values);

            await consumer.Close();
        }

        Collect();
        AssertPinned(values);

        foreach (IGrouping<SafeHandle, int> batch in Enumerable.Range(0, FirstCount).GroupBy(i => values.Batches[i]))
        {
            int[] pins = batch.ToArray();
            for (int i = 0; i < pins.Length - 1; i++)
            {
                values.Pins[pins[i]].Dispose();
            }

            Assert.False(batch.Key.IsClosed);

            values.Pins[pins[pins.Length - 1]].Dispose();

            Assert.True(batch.Key.IsClosed);
        }
    }

    private async Task AsyncConsumerKeepsAsync(
        IAsyncDeserializer<ReadOnlyMemory<byte>?> keyDeserializer,
        IAsyncDeserializer<ReadOnlyMemory<byte>?> valueDeserializer)
    {
        string topic = await TopicAsync();
        Produce(topic, 0, FirstCount);
        List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> kept =
            new List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>();
        await using (AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            await AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>.Create(
                ConsumerConfigs(), keyDeserializer, valueDeserializer))
        {
            await consumer.Assign(new[] { new TopicPartition(topic, 0) });
            await PollIntoAsync(kept, () => consumer.Poll(s_poll), FirstCount);
            AssertKept(kept, 0);

            Produce(topic, FirstCount, NextCount);
            List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> next =
                new List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>();
            await PollIntoAsync(next, () => consumer.Poll(s_poll), NextCount);
            Collect();
            AssertKept(next, FirstCount);
            AssertKept(kept, 0);

            await consumer.Close();
        }

        Collect();
        AssertKept(kept, 0);
    }

    // Keeps the keys and the values only, so nothing else of a poll's records keeps its bytes.
    private static void PollInto(
        List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> kept,
        Func<ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>> poll,
        int count)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (kept.Count < count && elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record in poll())
            {
                kept.Add(new KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>(
                    record.Key()!.Value, record.Value()!.Value));
            }
        }

        Assert.Equal(count, kept.Count);
    }

    private static async Task PollIntoAsync(
        List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> kept,
        Func<Task<ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>>> poll,
        int count)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (kept.Count < count && elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record in await poll())
            {
                kept.Add(new KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>(
                    record.Key()!.Value, record.Value()!.Value));
            }
        }

        Assert.Equal(count, kept.Count);
    }

    private static async Task<int> CountAsync(Func<Task<ConsumerRecords<string, int>>> poll, int count)
    {
        int polled = 0;
        Stopwatch elapsed = Stopwatch.StartNew();
        while (polled < count && elapsed.Elapsed < s_wait)
        {
            foreach (ConsumerRecord<string, int> record in await poll())
            {
                Assert.Equal(Value(checked((int)record.Offset())).Length, record.Value());
                polled++;
            }
        }

        return polled;
    }

    private static void AssertKept(List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> kept, int first)
    {
        for (int i = 0; i < kept.Count; i++)
        {
            AssertNativeBytes(Key(first + i), kept[i].Key);
            AssertNativeBytes(Value(first + i), kept[i].Value);
        }
    }

    private static void AssertNativeBytes(byte[] expected, ReadOnlyMemory<byte> actual)
    {
        Assert.False(MemoryMarshal.TryGetArray(actual, out _));
        Assert.True(MemoryMarshal.TryGetMemoryManager(actual, out MemoryManager<byte>? manager));
        Assert.NotNull(manager);
        Assert.True(actual.Span.SequenceEqual(expected), "the bytes of '" + Encoding.UTF8.GetString(expected) + "'");
    }

    private static unsafe void AssertPinned(PinningDeserializer values)
    {
        Assert.Equal(FirstCount, values.Pins.Count);
        for (int i = 0; i < FirstCount; i++)
        {
            byte[] expected = Value(i);
            Assert.False(values.Batches[i].IsClosed);
            Assert.Throws<ObjectDisposedException>(() => values.Lent[i].Span.Length);
            Assert.True(
                new ReadOnlySpan<byte>(values.Pins[i].Pointer, expected.Length).SequenceEqual(expected),
                "the bytes of '" + Encoding.UTF8.GetString(expected) + "'");
        }
    }

    // The handle of a memory's batch is internal: reflection reads it, to see when the batch is released.
    private static SafeHandle BatchOf(ReadOnlyMemory<byte> memory)
    {
        Assert.True(MemoryMarshal.TryGetMemoryManager(memory, out MemoryManager<byte>? manager));
        PropertyInfo? batch = manager!.GetType().GetProperty("Batch", BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsAssignableFrom<SafeHandle>(batch?.GetValue(manager));
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static byte[] Key(int i) => Encoding.UTF8.GetBytes("key-" + i);

    private static byte[] Value(int i) =>
        Encoding.UTF8.GetBytes("value-" + i + "-" + new string((char)('a' + (i % 26)), 500 + i));

    private void Produce(string topic, int first, int count)
    {
        using KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            ProducerConfigs(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        List<KafkaFuture<RecordMetadata>> sent = new List<KafkaFuture<RecordMetadata>>();
        for (int i = first; i < first + count; i++)
        {
            sent.Add(producer.Send(new ProducerRecord<byte[], byte[]>(topic, value: Value(i), key: Key(i), partition: 0)));
        }

        foreach (KafkaFuture<RecordMetadata> future in sent)
        {
            _ = future.Get(s_wait);
        }
    }

    private async Task<string> TopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-zero-copy-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = _broker.ExternalBootstrap! };

    private Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = "dotnet-zero-copy-" + Guid.NewGuid().ToString("N").Substring(0, 12),
            ["auto.offset.reset"] = "earliest",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>A deserializer that keeps the bytes as given, after it has yielded, so the consumer awaits it.</summary>
    private sealed class YieldingRetainingDeserializer : IAsyncDeserializerWithRetention<ReadOnlyMemory<byte>?>
    {
        public async ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return data;
        }
    }

    /// <summary>
    /// A call-scoped deserializer that decodes a value to its length and, for the first values it is given, leaves a
    /// pin of the memory open, taken after it has yielded, and keeps the memory and its batch's handle.
    /// </summary>
    private sealed class PinningDeserializer : IAsyncDeserializer<int>
    {
        private readonly int _pinned;

        internal PinningDeserializer(int pinned) => _pinned = pinned;

        internal List<MemoryHandle> Pins { get; } = new List<MemoryHandle>();

        internal List<ReadOnlyMemory<byte>> Lent { get; } = new List<ReadOnlyMemory<byte>>();

        internal List<SafeHandle> Batches { get; } = new List<SafeHandle>();

        public async ValueTask<int> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            await Task.Yield();
            if (Pins.Count < _pinned)
            {
                Pins.Add(data.Pin());
                Lent.Add(data);
                Batches.Add(BatchOf(data));
            }

            return data.Length;
        }
    }
}
