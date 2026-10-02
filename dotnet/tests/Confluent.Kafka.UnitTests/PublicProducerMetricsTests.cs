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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M11/P8 producer <c>Metrics()</c> surface — Java
/// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c> — through the <b>public</b> producer
/// types (sync + async, real + mock), broker-free. The producer twin of
/// <see cref="PublicConsumerMetricsTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// <c>Metrics()</c> is declared identically on <c>IProducer</c> and <c>IAsyncProducer</c> (no
/// <c>IProducerCommon</c> — decision D-6), so both interface legs are exercised.
/// </para>
/// <para>
/// The mock's <c>metrics()</c> is an <b>empty</b> map (Java <c>MockProducer</c> parity — its
/// <c>mockMetrics</c> is empty unless seeded, and the ABI exposes no seeding entry point), so the
/// mock legs prove the <c>count == 0</c> marshal path. The real producer registers its sensor set
/// at construction, so a broker-free real producer's map is <b>non-empty</b> — the real legs prove
/// the populated copy-out and the <see cref="MetricName"/>-key value identity end to end.
/// </para>
/// <para>
/// <b>No concurrent-access mapping is asserted (decision D-5).</b> Unlike the consumer, the
/// producer ABI documents no concurrent-access null: it takes the core producer <c>Mutex</c> and
/// blocks. So there is no <c>InvalidOperationException("… not safe for multi-threaded access.")</c>
/// contract to pin here — the binding's null guard is unreachable by construction and deliberately
/// carries a producer-accurate message instead.
/// </para>
/// </remarks>
public sealed class PublicProducerMetricsTests
{
    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    // ---- Mock: empty map (Java MockProducer parity), sync + async ----

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Assert.Empty(producer.Metrics());
    }

    // ---- Real producer: populated map, well-formed entries (sync + async) ----

    [Fact]
    public void Metrics_SyncRealProducer_IsPopulatedAndWellFormed()
    {
        using KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public void Metrics_AsyncRealProducer_IsPopulatedAndWellFormed()
    {
        using AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public void Metrics_RealProducer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // Round-trip the MetricName-key value identity end to end: pick a real key, rebuild it
        // with the SAME name/group/tags but a DIFFERENT description, and index the same entry —
        // proving the dictionary keys on value identity and excludes description.
        using KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName original = entry.Key;
            MetricName rebuilt = new MetricName(
                original.Name,
                original.Group,
                original.Description + "-mutated",
                RebuildTags(original.Tags));

            Assert.True(metrics.ContainsKey(rebuilt));
            Assert.Same(entry.Value, metrics[rebuilt]);
            return; // one entry is enough to prove the contract.
        }
    }

    [Fact]
    public void Metrics_CalledTwice_ReturnsIndependentSnapshots()
    {
        // Each call marshals a FRESH owned map and destroys the native root before returning
        // (ffi §A2 Category-3 read-all-then-destroy-once). So the two results are distinct
        // dictionary instances that both stay readable — proving nothing borrows the freed root.
        using KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<MetricName, IMetric> first = producer.Metrics();
        IReadOnlyDictionary<MetricName, IMetric> second = producer.Metrics();

        Assert.NotSame(first, second);
        Assert.Equal(first.Count, second.Count);

        // The FIRST snapshot's strings survive the second call's destroy (they were copied out).
        AssertPopulatedAndWellFormed(first);
        AssertPopulatedAndWellFormed(second);
    }

    // ---- Post-dispose → ObjectDisposedException, all four client types ----

    [Fact]
    public void Metrics_SyncMockAfterDispose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealAfterDispose_ThrowsObjectDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        producer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncMockAfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealAfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    // ---- Reachable through BOTH interfaces (decision D-6: declared on each, no shared base) ----

    [Fact]
    public void Metrics_ReachableViaIProducer()
    {
        using MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IProducer<byte[], byte[]> producer = mock;
        Assert.Empty(producer.Metrics());
    }

    [Fact]
    public void Metrics_ReachableViaIAsyncProducer()
    {
        using AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IAsyncProducer<byte[], byte[]> producer = mock;
        Assert.Empty(producer.Metrics());
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
            Assert.NotNull(name.Name);
            Assert.NotNull(name.Group);
            Assert.NotNull(name.Description);
            Assert.NotNull(name.Tags);

            Assert.NotNull(metric);
            // IMetric.Name must equal (by value identity) the dictionary key.
            Assert.Equal(name, metric.Name);

            // Value is a boxed CLR value whose runtime type IS the kind: one of the four.
            object value = metric.Value;
            Assert.NotNull(value);
            Assert.True(
                value is double || value is string || value is long || value is int,
                $"Unexpected metric value type {value.GetType()} for {name.Name}");
        }
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
