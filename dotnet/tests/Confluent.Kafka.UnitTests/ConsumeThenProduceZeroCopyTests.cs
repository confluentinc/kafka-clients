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
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A fetched key and value sent on without a copy: an asynchronous producer whose serializer is awaited sends the
/// batch's own bytes, which stay valid during the wait although the consumer record and the consumer are gone, and
/// the address the producer pins is the batch's.
/// </summary>
public sealed class ConsumeThenProduceZeroCopyTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string InTopic = "consume-then-produce-in";

    private const string OutTopic = "consume-then-produce-out";

    private static readonly TopicPartition s_tp0 = new TopicPartition(InTopic, 0);

    [Fact]
    public async Task AFetchedValue_SentThroughAnAwaitedSerializer_IsTheBatchsOwnBytes_AfterItsRecordAndConsumerAreGone()
    {
        Holding value = new Holding();
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(new AsyncReadOnlyMemorySerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = PollAndSend(producer);
        Collect();
        Assert.False(firstStage.IsCompleted);
        Assert.Equal(0, producer.HistoryCount());

        value.Complete();
        RecordMetadata metadata = await TestTimeout.Run(() => firstStage.Delivery(), s_deadline);

        Assert.True(value.GivenTheBatchsMemory);
        Assert.Equal("fetched-value", value.ReadAfterTheWait);
        Assert.Equal(OutTopic, metadata.Topic());
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task TheBytesTheProducerPins_AreTheBatchsOwn_AtTheBatchsAddress()
    {
        ReadOnlyMemory<byte> fetched = PollOneValue();
        Collect();
        Assert.True(MemoryMarshal.TryGetMemoryManager(fetched, out BytesMemoryManager? batch));
        ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record =
            new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(OutTopic, value: fetched, key: null, partition: 0);
        Holding value = new Holding();
        value.Complete();

        SerializedProducerRecord serialized = await TestTimeout.Run(
            () => SerializedProducerRecord.SerializeAsync(
                record,
                new AsyncSerializerCall<ReadOnlyMemory<byte>?>(new AsyncReadOnlyMemorySerializer()),
                new AsyncSerializerCall<ReadOnlyMemory<byte>?>(value),
                mock: false,
                CancellationToken.None).AsTask(),
            s_deadline);

        Assert.Null(serialized.Key);
        Assert.True(MemoryMarshal.TryGetMemoryManager(serialized.Value!.Value, out BytesMemoryManager? sent));
        Assert.Same(batch, sent);
        SendPins pins = default;
        pins.Pin(serialized.Key, serialized.Value, out IntPtr keyBytes, out IntPtr valueBytes);
        try
        {
            KafkaBytes pinned = Marshal.PtrToStructure<KafkaBytes>(valueBytes);
            Assert.Equal(IntPtr.Zero, keyBytes);
            Assert.Equal(AddressOf(fetched), pinned.Data);
            Assert.Equal(fetched.Length, pinned.Length);
        }
        finally
        {
            pins.Release();
        }
    }

    // Not inlined, so the consumer record is unreachable once it returns and only the send keeps the bytes.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ValueTask<AsyncKafkaFuture<RecordMetadata>> PollAndSend(
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer)
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Fetching();
        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> fetched = Assert.Single(consumer.Poll(s_pollTimeout));

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(
            new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                OutTopic, value: fetched.Value(), key: fetched.Key(), partition: 0));
        TestTimeout.Dispose(consumer);
        return firstStage;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ReadOnlyMemory<byte> PollOneValue()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Fetching();
        ReadOnlyMemory<byte> value = Assert.Single(consumer.Poll(s_pollTimeout)).Value()!.Value;
        TestTimeout.Dispose(consumer);
        return value;
    }

    private static MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> Fetching()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Serdes.ReadOnlyMemory().Deserializer(), Serdes.ReadOnlyMemory().Deserializer());
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        consumer.AddRecord(InTopic, 0, 0, Encoding.UTF8.GetBytes("fetched-key"), Encoding.UTF8.GetBytes("fetched-value"));
        return consumer;
    }

    private static unsafe IntPtr AddressOf(ReadOnlyMemory<byte> memory)
    {
        using MemoryHandle pin = memory.Pin();
        return (IntPtr)pin.Pointer;
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    /// <summary>
    /// A serializer that returns the memory it is given, after a wait across which it holds it, and records whether
    /// that memory is a fetched batch's own and what it reads there after the wait.
    /// </summary>
    private sealed class Holding : IAsyncSerializer<ReadOnlyMemory<byte>?>
    {
        private readonly TaskCompletionSource<bool> _later =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal bool GivenTheBatchsMemory { get; private set; }

        internal string? ReadAfterTheWait { get; private set; }

        public async ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, ReadOnlyMemory<byte>? data, CancellationToken cancellationToken)
        {
            GivenTheBatchsMemory = MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(data!.Value, out _);
            await _later.Task.ConfigureAwait(false);
            ReadAfterTheWait = Encoding.UTF8.GetString(data.Value.ToArray());
            return data;
        }

        internal void Complete() => _later.TrySetResult(true);
    }
}
