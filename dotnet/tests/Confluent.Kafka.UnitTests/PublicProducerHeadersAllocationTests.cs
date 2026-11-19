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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0+ only.
#if NET8_0_OR_GREATER

/// <summary>
/// The send-path allocation audit for record headers, on both producer flavors: a record with four headers costs the
/// caller's thread what a record with none costs, so the send copies each header into the native record without a
/// managed allocation of its own. Both records are built before the measured window and sent unchanged each time.
/// </summary>
public sealed class PublicProducerHeadersAllocationTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "headers-alloc-topic";

    private const int SendCount = 64;

    private const int HeaderCount = 4;

    // Less than one object per send, while one object per header costs at least 4 * 24 B per send.
    private const long PerSendHeadersBudgetBytes = 16;

    // Each matched pair is repeated and the lowest difference taken: a per-header allocation raises every attempt.
    private const int MeasurementAttempts = 4;

    [Fact]
    public void SyncSend_RecordWithHeaders_AllocatesNothingPerHeader()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<byte[], byte[]> plain = Record(0);
        ProducerRecord<byte[], byte[]> withHeaders = Record(HeaderCount);
        KafkaFuture<RecordMetadata>[] futures = new KafkaFuture<RecordMetadata>[SendCount];

        for (int i = 0; i < 3; i++)
        {
            _ = MeasureSync(producer, plain, futures);
            GetAll(futures);
            _ = MeasureSync(producer, withHeaders, futures);
            GetAll(futures);
        }

        long perSend = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long none = MeasureSync(producer, plain, futures);
            GetAll(futures);
            long four = MeasureSync(producer, withHeaders, futures);
            GetAll(futures);
            perSend = Math.Min(perSend, (four - none) / SendCount);
        }

        AssertWithinBudget(perSend);
    }

    [Fact]
    public async Task AsyncSend_RecordWithHeaders_AllocatesNothingPerHeader()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<byte[], byte[]> plain = Record(0);
        ProducerRecord<byte[], byte[]> withHeaders = Record(HeaderCount);
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[SendCount];

        for (int i = 0; i < 3; i++)
        {
            _ = MeasureAsync(producer, plain, sends);
            await AwaitAll(sends);
            _ = MeasureAsync(producer, withHeaders, sends);
            await AwaitAll(sends);
        }

        long perSend = long.MaxValue;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            long none = MeasureAsync(producer, plain, sends);
            await AwaitAll(sends);
            long four = MeasureAsync(producer, withHeaders, sends);
            await AwaitAll(sends);
            perSend = Math.Min(perSend, (four - none) / SendCount);
        }

        AssertWithinBudget(perSend);
    }

    private static ProducerRecord<byte[], byte[]> Record(int headerCount)
    {
        Header[] headers = new Header[headerCount];
        for (int i = 0; i < headerCount; i++)
        {
            headers[i] = new Header("header-" + i, i == 0 ? null : Encoding.UTF8.GetBytes("value-" + i));
        }

        return new ProducerRecord<byte[], byte[]>(
            Topic, value: Encoding.UTF8.GetBytes("value"), key: Encoding.UTF8.GetBytes("key"), partition: 0, headers: headers);
    }

    private static long MeasureSync(
        MockProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record, KafkaFuture<RecordMetadata>[] futures)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            futures[i] = producer.Send(record);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long MeasureAsync(
        AsyncMockProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record, Task<RecordMetadata>[] sends)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < SendCount; i++)
        {
            sends[i] = producer.Send(record).Delivery();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static void GetAll(KafkaFuture<RecordMetadata>[] futures) =>
        TestTimeout.Run(
            () =>
            {
                foreach (KafkaFuture<RecordMetadata> future in futures)
                {
                    _ = future.Get();
                }
            },
            s_deadline);

    private static async Task AwaitAll(Task<RecordMetadata>[] sends) =>
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

    private static void AssertWithinBudget(long perSend) =>
        Assert.True(
            perSend <= PerSendHeadersBudgetBytes,
            $"A send with {HeaderCount} headers allocated {perSend} B more per send than one without (budget " +
            $"{PerSendHeadersBudgetBytes} B): the send must copy each header into the native record without a " +
            "managed allocation (an enumerator, a ToArray, a key string's bytes).");
}

#endif
