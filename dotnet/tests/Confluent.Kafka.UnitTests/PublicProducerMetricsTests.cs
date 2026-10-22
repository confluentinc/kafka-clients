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
/// The producer's <c>Metrics()</c>, Java's <c>metrics()</c>, on the four producer types: the mocks' map is empty, as
/// Java's MockProducer's is, and a real producer's is populated with well-formed, value-keyed entries; both
/// interfaces declare it.
/// </summary>
public sealed class PublicProducerMetricsTests
{
    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = producer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        Assert.Empty(producer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealProducer_IsPopulatedAndWellFormed()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public void Metrics_AsyncRealProducer_IsPopulatedAndWellFormed()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        AssertPopulatedAndWellFormed(producer.Metrics());
    }

    [Fact]
    public void Metrics_RealProducer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // Rebuild a real key with the same name, group and tags but another description: the dictionary keys on value
        // identity, which excludes the description.
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

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
        // Each call copies out a fresh map and destroys its native root, so both results stay readable.
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        IReadOnlyDictionary<MetricName, IMetric> first = producer.Metrics();
        IReadOnlyDictionary<MetricName, IMetric> second = producer.Metrics();

        Assert.NotSame(first, second);
        Assert.Equal(first.Count, second.Count);

        // The FIRST snapshot's strings survive the second call's destroy (they were copied out).
        AssertPopulatedAndWellFormed(first);
        AssertPopulatedAndWellFormed(second);
    }

    [Fact]
    public void Metrics_SyncMockAfterDispose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealAfterDispose_ThrowsObjectDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncMockAfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(producer);
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealAfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            RealConfig(), Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(producer);
        Assert.Throws<ObjectDisposedException>(() => producer.Metrics());
    }

    // Declared on each interface, with no shared base.

    [Fact]
    public void Metrics_ReachableViaIProducer()
    {
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IProducer<byte[], byte[]> producer = mock;
        Assert.Empty(producer.Metrics());
    }

    [Fact]
    public void Metrics_ReachableViaIAsyncProducer()
    {
        AsyncMockProducer<byte[], byte[]> mock = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
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
