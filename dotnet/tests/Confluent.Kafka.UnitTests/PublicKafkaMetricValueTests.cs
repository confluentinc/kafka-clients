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
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The values of a client's <c>Metrics()</c>, without a broker: each is a <see cref="KafkaMetric"/>, as each value of
/// Java's map is, measured again on each read, readable after its client is disposed and after its map is dropped,
/// freed once by its finalizer, and readable while the producer sends and flushes.
/// </summary>
public sealed class PublicKafkaMetricValueTests
{
    private const string FlushTime = "flush-time-ns-total";

    private const string LastPoll = "last-poll-seconds-ago";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, object?> UnreachableProducer() => new()
    {
        ["bootstrap.servers"] = "localhost:1",
    };

    private static Dictionary<string, object?> UnreachableConsumer(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public void Metrics_SyncRealProducer_EveryValueIsAKafkaMetric()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertEveryValueIsAKafkaMetric(producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealProducer_EveryValueIsAKafkaMetric()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = await AsyncKafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertEveryValueIsAKafkaMetric(producer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealConsumer_EveryValueIsAKafkaMetric()
    {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-sync"), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertEveryValueIsAKafkaMetric(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealConsumer_EveryValueIsAKafkaMetric()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-async"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertEveryValueIsAKafkaMetric(consumer.Metrics());
    }

    [Fact]
    public void Metrics_SyncMockProducerWithAMockMetric_ItsValueIsAKafkaMetricOverTheSharedMetric()
    {
        KafkaProducer<byte[], byte[]> real = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);

        SetMockMetric(mock, real, FlushTime);
        IReadOnlyDictionary<MetricName, IMetric> metrics = mock.Metrics();

        AssertEveryValueIsAKafkaMetric(metrics);
        KafkaMetric kept = Assert.IsType<KafkaMetric>(Assert.Single(metrics).Value);
        Assert.Equal(FlushTime, kept.MetricName().Name());
        AssertMovesAfterFlushes(kept, real.Flush);
    }

    [Fact]
    public async Task Metrics_AsyncMockProducerWithAMockMetric_ItsValueIsAKafkaMetricOverTheSharedMetric()
    {
        AsyncKafkaProducer<byte[], byte[]> real = await AsyncKafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(
            new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);

        SetMockMetric(mock, real, FlushTime);
        IReadOnlyDictionary<MetricName, IMetric> metrics = mock.Metrics();

        AssertEveryValueIsAKafkaMetric(metrics);
        KafkaMetric kept = Assert.IsType<KafkaMetric>(Assert.Single(metrics).Value);
        Assert.Equal(FlushTime, kept.MetricName().Name());
        await AssertMovesAfterFlushesAsync(kept, () => real.Flush());
    }

    [Fact]
    public void KeptMetric_SyncRealProducer_IsMeasuredAgainOnEachRead()
    {
        // flush-time-ns-total moves without a broker: each Flush adds its own duration to it.
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric kept = Find(producer.Metrics(), FlushTime, "producer-metrics");

        AssertMovesAfterFlushes(kept, producer.Flush);
    }

    [Fact]
    public async Task KeptMetric_AsyncRealConsumer_IsMeasuredAgainOnEachRead()
    {
        // last-poll-seconds-ago moves without a broker: it is -1 until the first Poll, which records its start before
        // it refuses a consumer that has no subscription.
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-live-async"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        KafkaMetric kept = Find(consumer.Metrics(), LastPoll, "consumer-metrics");
        Assert.Equal(-1.0, (double)kept.MetricValue());

        IllegalStateException e = await Assert.ThrowsAsync<IllegalStateException>(() => consumer.Poll(TimeSpan.Zero));

        Assert.Equal("Consumer is not subscribed to any topics or assigned any partitions", e.Message);
        Assert.True((double)kept.MetricValue() >= 0.0, "last-poll-seconds-ago did not move after the poll");
    }

    [Fact]
    public void KeptMetric_SyncRealConsumer_IsMeasuredAgainOnEachRead()
    {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-live-sync"), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        KafkaMetric kept = Find(consumer.Metrics(), LastPoll, "consumer-metrics");
        Assert.Equal(-1.0, (double)kept.MetricValue());

        IllegalStateException e = Assert.Throws<IllegalStateException>(() => consumer.Poll(TimeSpan.Zero));

        Assert.Equal("Consumer is not subscribed to any topics or assigned any partitions", e.Message);
        Assert.True((double)kept.MetricValue() >= 0.0, "last-poll-seconds-ago did not move after the poll");
    }

    [Fact]
    public void KeptMetric_ToString_IsTheCoresText()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-text"), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        KafkaMetric stat = Find(producer.Metrics(), FlushTime, "producer-metrics");
        string text = stat.ToString();
        Assert.StartsWith("KafkaMetric [metricName=" + stat.MetricName() + ", metricValueProvider=", text, StringComparison.Ordinal);
        Assert.EndsWith("]", text, StringComparison.Ordinal);

        KafkaMetric gauge = Find(consumer.Metrics(), LastPoll, "consumer-metrics");
        Assert.Equal("KafkaMetric [metricName=" + gauge.MetricName() + "]", gauge.ToString());
    }

    [Fact]
    public void KeptMetric_StaysReadableAfterItsProducerIsDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        KafkaMetric kept;
        double before;
        try
        {
            kept = Find(producer.Metrics(), FlushTime, "producer-metrics");
            producer.Flush();
            before = (double)kept.MetricValue();
        }
        finally
        {
            TestTimeout.Dispose(producer);
        }

        double after = Assert.IsType<double>(kept.MetricValue());
        Assert.True(after >= before, $"flush-time-ns-total went back from {before} to {after}");
        Assert.StartsWith("KafkaMetric [metricName=" + kept.MetricName(), kept.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task KeptMetric_StaysReadableAfterItsConsumerIsDisposed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("kafka-metric-after-dispose"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        KafkaMetric kept;
        try
        {
            kept = Find(consumer.Metrics(), LastPoll, "consumer-metrics");
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }

        Assert.IsType<double>(kept.MetricValue());
        Assert.Equal("KafkaMetric [metricName=" + kept.MetricName() + "]", kept.ToString());
    }

    [Fact]
    public void DroppedMetrics_AreFreedByTheirFinalizers_AndTheClientsDisposeFreesNoKeptOne()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        KafkaMetric kept;
        List<WeakReference> dropped;
        try
        {
            kept = Find(producer.Metrics(), FlushTime, "producer-metrics");
            dropped = TakeAndDrop(producer, 3);
        }
        finally
        {
            TestTimeout.Dispose(producer);
        }

        Collect();

        Assert.NotEmpty(dropped);
        Assert.All(dropped, reference => Assert.False(reference.IsAlive, "a dropped metric or its handle was not freed"));
        Assert.False(HandleOf(kept).IsClosed);
        Assert.IsType<double>(kept.MetricValue());

        Collect();
        Assert.IsType<double>(kept.MetricValue());
        GC.KeepAlive(kept);
    }

    [Fact]
    public async Task KeptMetric_ReadWhileTheProducerSendsAndFlushes_MeasuresEveryTime()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = await AsyncKafkaProducer<byte[], byte[]>.Create(
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = "localhost:1",
                ["max.block.ms"] = 300,
            },
            new AsyncByteArraySerializer(),
            new AsyncByteArraySerializer());
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        KafkaMetric kept = Find(producer.Metrics(), FlushTime, "producer-metrics");

        using CancellationTokenSource stop = new CancellationTokenSource();
        Task<int> reader = Task.Run(() =>
        {
            int reads = 0;
            double last = (double)kept.MetricValue();
            while (!stop.IsCancellationRequested || reads == 0)
            {
                double value = (double)kept.MetricValue();
                Assert.True(value >= last, $"flush-time-ns-total went back from {last} to {value}");
                last = value;
                reads++;
            }

            return reads;
        });

        await TestTimeout.Run(
            async () =>
            {
                List<Task> sends = new List<Task>();
                for (int i = 0; i < 8; i++)
                {
                    sends.Add(SendIgnoringItsOutcome(producer, "kafka-metric-concurrent-" + i));
                }

                for (int i = 0; i < 20; i++)
                {
                    await producer.Flush();
                }

                await Task.WhenAll(sends);
            },
            s_deadline);

        stop.Cancel();
        int reads = await reader;
        Assert.True(reads > 0);
    }

    private static async Task SendIgnoringItsOutcome(AsyncKafkaProducer<byte[], byte[]> producer, string topic)
    {
        try
        {
            AsyncKafkaFuture<RecordMetadata> future = await producer.Send(
                new ProducerRecord<byte[], byte[]>(topic, value: Encoding.UTF8.GetBytes("v")));
            await future.Get();
        }
        catch (Exception)
        {
            // An unreachable broker fails the record after max.block.ms; only the concurrent reads are checked.
        }
    }

    private static void AssertEveryValueIsAKafkaMetric(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        Assert.NotEmpty(metrics);
        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            KafkaMetric metric = Assert.IsType<KafkaMetric>(entry.Value);
            Assert.Same(metric, (KafkaMetric)entry.Value);
            Assert.Equal(entry.Key, metric.MetricName());
            object value = metric.MetricValue();
            Assert.True(
                value is double || value is string || value is long || value is int,
                $"Unexpected metric value type {value.GetType()} for {entry.Key.Name()}");
        }
    }

    private static void AssertMovesAfterFlushes(KafkaMetric kept, Action flush)
    {
        double before = (double)kept.MetricValue();
        double after = before;
        for (int i = 0; i < 100 && after == before; i++)
        {
            flush();
            after = (double)kept.MetricValue();
        }

        Assert.True(after > before, $"flush-time-ns-total stayed at {before} after 100 flushes");
    }

    private static async Task AssertMovesAfterFlushesAsync(KafkaMetric kept, Func<Task> flush)
    {
        double before = (double)kept.MetricValue();
        double after = before;
        for (int i = 0; i < 100 && after == before; i++)
        {
            await flush();
            after = (double)kept.MetricValue();
        }

        Assert.True(after > before, $"flush-time-ns-total stayed at {before} after 100 flushes");
    }

    private static KafkaMetric Find(IReadOnlyDictionary<MetricName, IMetric> metrics, string name, string group)
    {
        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            if (entry.Key.Name() == name && entry.Key.Group() == group)
            {
                return Assert.IsType<KafkaMetric>(entry.Value);
            }
        }

        throw new InvalidOperationException($"no metric {group}/{name}");
    }

    private static SafeHandle HandleOf(KafkaMetric metric) =>
        (SafeHandle)typeof(KafkaMetric).GetField("_handle", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(metric)!;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static List<WeakReference> TakeAndDrop(KafkaProducer<byte[], byte[]> producer, int calls)
    {
        List<WeakReference> references = new List<WeakReference>();
        for (int call = 0; call < calls; call++)
        {
            foreach (KeyValuePair<MetricName, IMetric> entry in producer.Metrics())
            {
                KafkaMetric metric = (KafkaMetric)entry.Value;
                metric.MetricValue();
                references.Add(new WeakReference(metric));
                references.Add(new WeakReference(HandleOf(metric)));
            }
        }

        return references;
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    private static void SetMockMetric(object mock, object real, string name)
    {
        NativeProducer mockNative = NativeOf(mock);
        NativeProducer realNative = NativeOf(real);
        IntPtr outErr = IntPtr.Zero;
        IntPtr view = NativeMethods.ProducerMetrics(realNative.Handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr iterator = IntPtr.Zero;
        try
        {
            iterator = NativeMethods.MetricsViewIterator(view, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            while (NativeMethods.IteratorHasNext(iterator, ref outErr) != 0)
            {
                IntPtr entry = NativeMethods.IteratorNext(iterator, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.MapEntryKey(entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                if (MetricsViewMarshal.CopyOutName(key).Name() != name)
                {
                    continue;
                }

                IntPtr metric = NativeMethods.MetricsViewGet(view, key, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                try
                {
                    SetMockMetrics(mockNative.Handle, key, metric);
                }
                finally
                {
                    IntPtr metricErr = IntPtr.Zero;
                    NativeMethods.KafkaMetricDestroy(metric, ref metricErr);
                    KafkaException.ThrowIfFfiFailed(ref metricErr);
                }

                return;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            throw new InvalidOperationException("the real producer has no metric " + name);
        }
        finally
        {
            IntPtr iteratorErr = IntPtr.Zero;
            NativeMethods.IteratorDestroy(iterator, ref iteratorErr);
            IntPtr viewErr = IntPtr.Zero;
            NativeMethods.MetricsViewDestroy(view, ref viewErr);
            KafkaException.ThrowIfFfiFailed(ref iteratorErr);
            KafkaException.ThrowIfFfiFailed(ref viewErr);
        }
    }

    private static void SetMockMetrics(SafeProducerHandle mock, IntPtr name, IntPtr metric)
    {
        IntPtr outErr = IntPtr.Zero;
        bool added = false;
        try
        {
            Native.MockProducerSetMockMetrics(mock.DangerousAddRefClass(ref added), name, metric, ref outErr);
        }
        finally
        {
            if (added)
            {
                mock.DangerousRelease();
            }
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static NativeProducer NativeOf(object producer) =>
        (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;

    /// <summary>The C ABI calls only these tests make: a mock producer's metric, set from a real producer's.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_set_mock_metrics_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MockProducerSetMockMetrics(IntPtr self, IntPtr name, IntPtr metric, ref IntPtr outErr);
    }
}
