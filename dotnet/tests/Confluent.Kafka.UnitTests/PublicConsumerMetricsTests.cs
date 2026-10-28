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
/// <see cref="IConsumerCommon.Metrics"/> on the four consumer classes: an empty map on the mocks, as Java's
/// MockConsumer; well-formed entries on a real consumer without a broker, keyed by <see cref="MetricName"/> value
/// identity; <see cref="ObjectDisposedException"/> after Dispose; and, while a poll holds the consumer, a state read
/// refused beside an answered Metrics.
/// </summary>
public sealed class PublicConsumerMetricsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, string> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Assert.Empty(consumer.Metrics());
    }

    [Fact]
    public void Metrics_AsyncRealConsumer_IsPopulatedAndWellFormed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-async"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealConsumer_IsPopulatedAndWellFormed()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-sync"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public void Metrics_RealConsumer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // A key rebuilt with another description finds the same entry: MetricName equality ignores the description.
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-key-identity"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName original = entry.Key;
            MetricName rebuilt = new MetricName(
                original.Name,
                original.Group,
                original.Description + "-mutated",
                new Dictionary<string, string>(RebuildTags(original.Tags), StringComparer.Ordinal));

            Assert.True(metrics.ContainsKey(rebuilt));
            Assert.Same(entry.Value, metrics[rebuilt]);
            return;
        }
    }

    [Fact]
    public void Metrics_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
    }

    [Fact]
    public void Metrics_ReachableViaIConsumerCommon()
    {
        MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IConsumerCommon consumer = mock;
        Assert.Empty(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_ConcurrentWithInFlightPoll_AnswersWhileThePollHoldsTheConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("metrics-concurrent"), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        // Subscribe completes without a broker, so the poll that follows waits.
        await TestTimeout.Run(() => consumer.Subscribe(new[] { "metrics-concurrent-probe" }), s_deadline);

        Task<ConsumerRecords<byte[], byte[]>> inFlight = consumer.Poll(TimeSpan.FromSeconds(20));
        try
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => consumer.Assignment());
            Assert.Equal("KafkaConsumer is not safe for multi-threaded access.", ex.Message);

            AssertPopulatedAndWellFormed(consumer.Metrics());
            Assert.False(inFlight.IsCompleted);
        }
        finally
        {
            await DrainInFlightPollAsync(consumer, inFlight);
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
            Assert.NotNull(name.Name);
            Assert.NotNull(name.Group);
            Assert.NotNull(name.Description);
            Assert.NotNull(name.Tags);

            Assert.NotNull(metric);
            Assert.Equal(name, metric.Name);

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

    private static async Task DrainInFlightPollAsync(AsyncKafkaConsumer<byte[], byte[]> consumer, Task<ConsumerRecords<byte[], byte[]>> inFlight)
    {
        consumer.Wakeup();
        try
        {
            await TestTimeout.Run(
                async () =>
                {
                    try
                    {
                        await inFlight;
                    }
                    catch (KafkaException)
                    {
                    }
                },
                s_deadline);
        }
        catch (TimeoutException)
        {
        }
    }
}
