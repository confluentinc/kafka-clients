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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The producer's <c>Metrics()</c>, Java's <c>metrics()</c>, on the four producer types: the mocks' map is empty, as
/// Java's MockProducer's is until <c>setMockMetrics</c>, and a real producer's is populated with well-formed,
/// value-keyed entries; every map is a live view that answers after close; both interfaces declare it. Java's
/// <c>KafkaProducerTest.testMetricConfigRecordingLevel</c> and <c>testFlushMeasureLatency</c> are translated here;
/// the metrics tests of <c>KafkaProducerTest</c> that are not are listed, with their reasons, on
/// <see cref="PublicMetricsLiveMapTests"/>.
/// </summary>
public sealed class PublicProducerMetricsTests
{
    private static Dictionary<string, object?> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        Assert.Empty(producer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealProducer_IsPopulatedAndWellFormed()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            RealConfig(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealProducer_IsPopulatedAndWellFormed()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = await AsyncKafkaProducer<byte[], byte[]>.Create(
            RealConfig(), new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public void Metrics_RealProducer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // Rebuild a real key with the same name, group and tags but another description: the dictionary keys on value
        // identity, which excludes the description.
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            RealConfig(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName original = entry.Key;
            MetricName rebuilt = new MetricName(
                original.Name(),
                original.Group(),
                original.Description() + "-mutated",
                RebuildTags(original.Tags()));

            Assert.True(metrics.ContainsKey(rebuilt));
            IMetric found = metrics[rebuilt];
            Assert.Equal(entry.Key, found.MetricName());
            Assert.Equal(original.Description(), found.MetricName().Description());

            // Each read wraps the native metric in a new KafkaMetric, where Java's map returns one object.
            Assert.NotSame(entry.Value, found);
            return; // one entry is enough to prove the contract.
        }
    }

    [Fact]
    public void Metrics_CalledTwice_BothMapsReadTheSameRegistry()
    {
        // Each call returns a new read-only wrapper over the one view the producer took at construction, as Java's
        // returns a new unmodifiableMap over its registry.
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            RealConfig(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        IReadOnlyDictionary<MetricName, IMetric> first = producer.Metrics();
        IReadOnlyDictionary<MetricName, IMetric> second = producer.Metrics();

        Assert.NotSame(first, second);
        Assert.Equal(first.Count, second.Count);
        Assert.Equal(GroupAndNames(first), GroupAndNames(second));
        foreach (MetricName name in first.Keys)
        {
            Assert.True(second.ContainsKey(name));
        }

        AssertPopulatedAndWellFormed(first);
        AssertPopulatedAndWellFormed(second);
    }

    [Fact]
    public void Metrics_SyncMockAfterDispose_AnswersItsEmptyMap()
    {
        // Java's MockProducer.metrics() makes no closed check.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        IReadOnlyDictionary<MetricName, IMetric> before = producer.Metrics();
        TestTimeout.Dispose(producer);

        Assert.Empty(producer.Metrics());
        Assert.Empty(before);
    }

    [Fact]
    public async Task Metrics_AsyncMockAfterDisposeAsync_AnswersItsEmptyMap()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        IReadOnlyDictionary<MetricName, IMetric> before = producer.Metrics();
        await TestTimeout.DisposeAsync(producer);

        Assert.Empty(producer.Metrics());
        Assert.Empty(before);
    }

    [Fact]
    public void Metrics_SyncRealAfterDispose_AnswersWhatTheCoreLeavesRegistered()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            RealConfig(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        IReadOnlyDictionary<MetricName, IMetric> before = producer.Metrics();
        Assert.Contains("producer-metrics:flush-time-ns-total", GroupAndNames(before));
        TestTimeout.Dispose(producer);

        AssertMetricsRemovedOnClose(before);
        AssertMetricsRemovedOnClose(producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealAfterDisposeAsync_AnswersWhatTheCoreLeavesRegistered()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = await AsyncKafkaProducer<byte[], byte[]>.Create(
            RealConfig(), new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        IReadOnlyDictionary<MetricName, IMetric> before = producer.Metrics();
        await TestTimeout.DisposeAsync(producer);

        AssertMetricsRemovedOnClose(before);
        AssertMetricsRemovedOnClose(producer.Metrics());
    }

    // Java's KafkaProducerTest.testFlushMeasureLatency. Java runs it on a MockTime that advances a millisecond per read
    // and a MockClient; its assertions hold on real time too, as each flush takes some time, so the producer runs on
    // real time against an unreachable broker.
    [Fact]
    public void FlushMeasureLatency()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9000",
        };
        KafkaProducer<string, string> producer = KafkaProducer<string, string>.Create(
            configs, Serdes.String().Serializer(), Serdes.String().Serializer());
        using (TestTimeout.Disposal(producer))
        {
            producer.Flush();
            double first = GetMetricValue(producer, "flush-time-ns-total");
            Assert.True(first > 0);
            producer.Flush();
            Assert.True(GetMetricValue(producer, "flush-time-ns-total") > first);
        }
    }

    // Java's KafkaProducerTest.testMetricConfigRecordingLevel. Java reads the level of the Metrics registry's
    // configuration, which the binding does not hand out; every metric the registry adds without a configuration of its
    // own carries the registry's, so the level is read from the metrics' configurations.
    [Fact]
    public void MetricConfigRecordingLevel()
    {
        Dictionary<string, object?> props = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9000",
        };
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            props, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using (TestTimeout.Disposal(producer))
        {
            AssertRecordLevel(Sensor.RecordingLevel.INFO, producer.Metrics());
        }

        props["metrics.recording.level"] = "DEBUG";
        KafkaProducer<byte[], byte[]> producer2 = KafkaProducer<byte[], byte[]>.Create(
            props, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using (TestTimeout.Disposal(producer2))
        {
            AssertRecordLevel(Sensor.RecordingLevel.DEBUG, producer2.Metrics());
        }
    }

    // Declared on each interface, with no shared base.

    [Fact]
    public void Metrics_ReachableViaIProducer()
    {
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IProducer<byte[], byte[]> producer = mock;
        Assert.Empty(producer.Metrics());
    }

    [Fact]
    public void Metrics_ReachableViaIAsyncProducer()
    {
        AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IAsyncProducer<byte[], byte[]> producer = mock;
        Assert.Empty(producer.Metrics());
    }

    // Java's getMetricValue: the producer-metrics metric of that name.
    private static double GetMetricValue(KafkaProducer<string, string> producer, string name)
    {
        foreach (KeyValuePair<MetricName, IMetric> entry in producer.Metrics())
        {
            if (entry.Key.Name() == name && entry.Key.Group() == "producer-metrics")
            {
                return (double)entry.Value.MetricValue();
            }
        }

        throw new KeyNotFoundException(name);
    }

    private static void AssertRecordLevel(
        Sensor.RecordingLevel expected, IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        Assert.NotEmpty(metrics);
        foreach (IMetric metric in metrics.Values)
        {
            Assert.Equal(expected, Assert.IsType<KafkaMetric>(metric).Config()!.RecordLevel());
        }
    }

    private static void AssertPopulatedAndWellFormed(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        Assert.NotNull(metrics);
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName name = entry.Key;
            IMetric metric = entry.Value;

            Assert.NotNull(name);
            Assert.NotNull(name.Name());
            Assert.NotNull(name.Group());
            Assert.NotNull(name.Description());
            Assert.NotNull(name.Tags());

            Assert.NotNull(metric);
            // IMetric.Name must equal (by value identity) the dictionary key.
            Assert.Equal(name, metric.MetricName());

            // Value is a boxed CLR value whose runtime type IS the kind: one of the four.
            object value = metric.MetricValue();
            Assert.NotNull(value);
            Assert.True(
                value is double || value is string || value is long || value is int,
                $"Unexpected metric value type {value.GetType()} for {name.Name()}");
        }
    }

    // The analogue of Java's KafkaConsumerTest.testMetricsRemovedOnClose for the producer: after close the map still
    // answers and shows the registry as the core leaves it. Java's KafkaProducer.close removes its own timers
    // (KafkaProducerMetrics: flush, metadata wait, the transaction calls) and its selector's metrics, and leaves the
    // sender's and the accumulator's registered; the core removes the same timers and registers no selector metrics,
    // so this list is what both keep of the metrics the core registers.
    private static void AssertMetricsRemovedOnClose(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        string[] expected =
        {
            "kafka-metrics-count:count",
            "producer-metrics:batch-size-avg",
            "producer-metrics:batch-size-max",
            "producer-metrics:batch-split-rate",
            "producer-metrics:batch-split-total",
            "producer-metrics:buffer-available-bytes",
            "producer-metrics:buffer-exhausted-rate",
            "producer-metrics:buffer-exhausted-total",
            "producer-metrics:buffer-total-bytes",
            "producer-metrics:bufferpool-wait-ratio",
            "producer-metrics:bufferpool-wait-time-ns-total",
            "producer-metrics:compression-rate-avg",
            "producer-metrics:metadata-age",
            "producer-metrics:produce-throttle-time-avg",
            "producer-metrics:produce-throttle-time-max",
            "producer-metrics:record-error-rate",
            "producer-metrics:record-error-total",
            "producer-metrics:record-queue-time-avg",
            "producer-metrics:record-queue-time-max",
            "producer-metrics:record-retry-rate",
            "producer-metrics:record-retry-total",
            "producer-metrics:record-send-rate",
            "producer-metrics:record-send-total",
            "producer-metrics:record-size-avg",
            "producer-metrics:record-size-max",
            "producer-metrics:records-per-request-avg",
            "producer-metrics:request-latency-avg",
            "producer-metrics:request-latency-max",
            "producer-metrics:requests-in-flight",
            "producer-metrics:waiting-threads",
        };

        Assert.Equal(expected, GroupAndNames(metrics));
        Assert.Equal(expected.Length, metrics.Count);
        AssertPopulatedAndWellFormed(metrics);
    }

    private static List<string> GroupAndNames(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        List<string> names = new List<string>();
        foreach (MetricName name in metrics.Keys)
        {
            names.Add(name.Group() + ":" + name.Name());
        }

        names.Sort(StringComparer.Ordinal);
        return names;
    }

    private static Dictionary<string, string> RebuildTags(IReadOnlyDictionary<string, string> tags)
    {
        Dictionary<string, string> copy = new Dictionary<string, string>(tags.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        return copy;
    }
}
