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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The map a client's <c>Metrics()</c> returns, Java's <c>Collections.unmodifiableMap(metrics.metrics())</c>, without a
/// broker: a live view, so a map taken earlier sees a metric registered later; lookups, keys and values read the
/// registry; it is not an <see cref="IDictionary{TKey, TValue}"/>; and the view outlives its client and its maps, and
/// is freed by the garbage collector once nothing holds it.
/// </summary>
/// <remarks>
/// Not translated from Java's <c>KafkaConsumerTest</c>: <c>testPollTimeMetrics</c>, <c>testPollIdleRatio</c> and
/// <c>testPollIdleRatioZero</c> advance a <c>MockTime</c> the consumer runs on, which the binding cannot inject into the
/// core; and the <c>testSubscribingCustomMetrics…</c> tests (and the producer's, with
/// <c>testUnSubscribingNonExisingMetricsDoesntCauseError</c>) call <c>registerMetricForSubscription</c> and
/// <c>unregisterMetricFromSubscription</c>, which the binding does not translate. <c>testMetricsRemovedOnClose</c> is
/// translated in <see cref="PublicConsumerMetricsTests"/> and <see cref="PublicProducerMetricsTests"/>.
/// </remarks>
public sealed class PublicMetricsLiveMapTests
{
    private const string FlushTime = "flush-time-ns-total";

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
    public void LiveMap_SyncMockProducer_AMapTakenEarlierSeesAMetricSetLater()
    {
        KafkaProducer<byte[], byte[]> real = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IReadOnlyDictionary<MetricName, IMetric> earlier = mock.Metrics();
        Assert.Empty(earlier);

        KafkaMetric flushTime = MockProducerMetrics.Find(real.Metrics(), FlushTime);
        MockProducerMetrics.Set(mock, flushTime.MetricName(), flushTime);

        AssertHoldsOnly(earlier, flushTime.MetricName());
    }

    [Fact]
    public async Task LiveMap_AsyncMockProducer_AMapTakenEarlierSeesAMetricSetLater()
    {
        AsyncKafkaProducer<byte[], byte[]> real = await AsyncKafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(
            new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IReadOnlyDictionary<MetricName, IMetric> earlier = mock.Metrics();
        Assert.Empty(earlier);

        KafkaMetric flushTime = MockProducerMetrics.Find(real.Metrics(), FlushTime);
        MockProducerMetrics.Set(mock, flushTime.MetricName(), flushTime);

        AssertHoldsOnly(earlier, flushTime.MetricName());
    }

    [Fact]
    public void LiveMap_AMockMetricSetUnderAnotherName_IsKeyedByThatName()
    {
        // Java's MockProducer.setMockMetrics(name, metric) keys the map by the name it is given; the metric keeps its own.
        KafkaProducer<byte[], byte[]> real = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        KafkaMetric flushTime = MockProducerMetrics.Find(real.Metrics(), FlushTime);
        MetricName renamed = new MetricName("renamed", "mock-group", "a mock metric", new Dictionary<string, string> { ["k"] = "v" });

        MockProducerMetrics.Set(mock, renamed, flushTime);

        IReadOnlyDictionary<MetricName, IMetric> metrics = mock.Metrics();
        KeyValuePair<MetricName, IMetric> entry = Assert.Single(metrics);
        Assert.Equal(renamed, entry.Key);
        Assert.Equal("a mock metric", entry.Key.Description());
        Assert.Equal(flushTime.MetricName(), entry.Value.MetricName());
        Assert.Equal(flushTime.MetricName(), metrics[renamed].MetricName());
        Assert.Equal(new[] { renamed }, metrics.Keys);
    }

    [Fact]
    public void LiveMap_KeysValuesAndEntries_ReadTheRegistry()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();

        List<MetricName> keys = new List<MetricName>(metrics.Keys);
        List<IMetric> values = new List<IMetric>(metrics.Values);
        List<MetricName> entryKeys = new List<MetricName>();
        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            entryKeys.Add(entry.Key);
            Assert.Equal(entry.Key, entry.Value.MetricName());
            Assert.IsType<KafkaMetric>(entry.Value);
        }

        Assert.NotEmpty(keys);
        Assert.Equal(metrics.Count, keys.Count);
        Assert.Equal(metrics.Count, values.Count);
        Assert.Equal(new HashSet<MetricName>(keys), new HashSet<MetricName>(entryKeys));
        Assert.All(values, value => Assert.IsType<KafkaMetric>(value));
        Assert.All(keys, key => Assert.True(metrics.ContainsKey(key)));
        Assert.All(keys, key => Assert.Equal(key, metrics[key].MetricName()));
    }

    [Fact]
    public void LiveMap_AMissingKey_IsNotFound_AndTheIndexerThrowsKeyNotFound()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        MetricName missing = new MetricName("no-such-metric", "producer-metrics", "", new Dictionary<string, string>());

        Assert.False(metrics.ContainsKey(missing));
        Assert.False(metrics.TryGetValue(missing, out IMetric? value));
        Assert.Null(value);
        KeyNotFoundException e = Assert.Throws<KeyNotFoundException>(() => metrics[missing]);
        Assert.Equal($"The given key '{missing}' was not present in the dictionary.", e.Message);
    }

    [Fact]
    public void LiveMap_ANullKey_ThrowsArgumentNull()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();

        Assert.Equal("key", Assert.Throws<ArgumentNullException>(() => metrics.ContainsKey(null!)).ParamName);
        Assert.Equal("key", Assert.Throws<ArgumentNullException>(() => metrics.TryGetValue(null!, out _)).ParamName);
        Assert.Equal("key", Assert.Throws<ArgumentNullException>(() => metrics[null!]).ParamName);
    }

    [Fact]
    public void LiveMap_AKeyACStringCannotCarry_IsNotFound()
    {
        // A '\0' would end the C string, so the core would look up a shorter, registered name; a null tag value has no
        // C form. Neither can be a registered name, so the lookup finds nothing, as Java's does. A '\0' in the
        // description, which equality ignores, still finds the metric.
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        MetricName name = MockProducerMetrics.Find(metrics, FlushTime).MetricName();
        Dictionary<string, string> tags = new Dictionary<string, string>(name.Tags().Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in name.Tags())
        {
            tags[tag.Key] = tag.Value;
        }

        string tagKey = Assert.Single(name.Tags()).Key;
        MetricName[] uncarried =
        {
            new MetricName(name.Name() + "\0x", name.Group(), name.Description(), tags),
            new MetricName(name.Name(), name.Group() + "\0x", name.Description(), tags),
            new MetricName(name.Name(), name.Group(), name.Description(), With(tags, tagKey, tags[tagKey] + "\0x")),
            new MetricName(name.Name(), name.Group(), name.Description(), With(tags, tagKey + "\0x", tags[tagKey], tagKey)),
            new MetricName(name.Name(), name.Group(), name.Description(), With(tags, tagKey, null!)),
        };

        foreach (MetricName key in uncarried)
        {
            Assert.False(metrics.ContainsKey(key), key.ToString());
            Assert.False(metrics.TryGetValue(key, out _), key.ToString());
            Assert.Throws<KeyNotFoundException>(() => metrics[key]);
        }

        MetricName described = new MetricName(name.Name(), name.Group(), "a\0b", tags);
        Assert.True(metrics.ContainsKey(described));
        Assert.Equal(name, metrics[described].MetricName());
    }

    [Fact]
    public async Task LiveMap_IsNotAnIDictionary()
    {
        // Java's unmodifiableMap throws UnsupportedOperationException from clear(); this map has no IDictionary form.
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("live-map-read-only"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        foreach (IReadOnlyDictionary<MetricName, IMetric> metrics in new[] { producer.Metrics(), consumer.Metrics() })
        {
            Assert.False(metrics is IDictionary<MetricName, IMetric>);
            Assert.False(metrics is IDictionary);
            Assert.False(metrics is ICollection<KeyValuePair<MetricName, IMetric>>);
            Assert.Throws<InvalidCastException>(() => (IDictionary<MetricName, IMetric>)metrics);
        }
    }

    [Fact]
    public void LiveMap_AMetricOutlivesItsMap()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        (KafkaMetric kept, WeakReference map) = TakeAMetricAndDropItsMap(producer);

        Collect();

        Assert.False(map.IsAlive, "the map was not collected");
        Assert.IsType<double>(kept.MetricValue());
        producer.Flush();
        Assert.IsType<double>(kept.MetricValue());
    }

    [Fact]
    public void LiveMap_TheClientsDisposeFreesNoView_AndTheViewIsCollectedOnceNothingHoldsIt()
    {
        WeakReference view = DisposeAProducerHoldingItsMap();

        Collect();

        Assert.False(view.IsAlive, "the view outlived its client and every map over it");
    }

    [Fact]
    public async Task LiveMap_AConsumersView_IsCollectedOnceTheConsumerAndItsMapsAreGone()
    {
        WeakReference view = await DisposeAConsumerAfterTakingAMap();

        Collect();

        Assert.False(view.IsAlive, "the view outlived its client and every map over it");
    }

    private static void AssertHoldsOnly(IReadOnlyDictionary<MetricName, IMetric> metrics, MetricName name)
    {
        int count = metrics.Count;
        Assert.Equal(1, count);
        Assert.True(metrics.ContainsKey(name));
        Assert.True(metrics.TryGetValue(name, out IMetric? found));
        Assert.Equal(name, Assert.IsType<KafkaMetric>(found).MetricName());
        Assert.Equal(name, metrics[name].MetricName());
        Assert.Equal(new[] { name }, metrics.Keys);
        Assert.Equal(name, Assert.Single(metrics.Values).MetricName());
        Assert.Equal(name, Assert.Single(metrics).Key);
    }

    private static Dictionary<string, string> With(
        Dictionary<string, string> tags, string key, string value, string? removed = null)
    {
        Dictionary<string, string> copy = new Dictionary<string, string>(tags, StringComparer.Ordinal);
        if (removed is not null)
        {
            copy.Remove(removed);
        }

        copy[key] = value;
        return copy;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (KafkaMetric Kept, WeakReference Map) TakeAMetricAndDropItsMap(KafkaProducer<byte[], byte[]> producer)
    {
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        return (MockProducerMetrics.Find(metrics, FlushTime), new WeakReference(metrics));
    }

    // The map a caller holds keeps the view alive and readable after the producer's Dispose, which frees no view.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference DisposeAProducerHoldingItsMap()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            UnreachableProducer(), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        IReadOnlyDictionary<MetricName, IMetric> held = producer.Metrics();
        SafeHandle view = ViewOf(producer);
        TestTimeout.Dispose(producer);

        Collect();

        Assert.False(view.IsClosed, "the producer's Dispose freed the view");
        Assert.NotEmpty(held);
        GC.KeepAlive(held);
        return new WeakReference(view);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<WeakReference> DisposeAConsumerAfterTakingAMap()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            UnreachableConsumer("live-map-collected"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        Assert.NotEmpty(consumer.Metrics());
        WeakReference view = new WeakReference(ViewOf(consumer));
        await TestTimeout.DisposeAsync(consumer);
        return view;
    }

    private static SafeHandle ViewOf(object client)
    {
        object native = client.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(client)!;
        return (SafeHandle)native.GetType()
            .GetField("_metricsView", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(native)!;
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
}
