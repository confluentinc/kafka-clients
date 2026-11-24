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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IConsumerCommon.Metrics"/> on the four consumer classes: an empty map on the mocks, as Java's
/// MockConsumer, which throws Java's <see cref="IllegalStateException"/> once closed; well-formed entries on a real
/// consumer without a broker, keyed by <see cref="MetricName"/> value identity, in a live view that answers after
/// close; and, while a poll holds the consumer, a state read refused beside an answered Metrics.
/// </summary>
public sealed class PublicConsumerMetricsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, object?> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public void Metrics_AsyncMock_IsEmpty()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotNull(metrics);
        Assert.Empty(metrics);
    }

    [Fact]
    public void Metrics_SyncMock_IsEmpty()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Assert.Empty(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealConsumer_IsPopulatedAndWellFormed()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-async"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public void Metrics_SyncRealConsumer_IsPopulatedAndWellFormed()
    {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-sync"), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertPopulatedAndWellFormed(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_RealConsumer_KeyHasValueIdentity_DescriptionExcluded()
    {
        // A key rebuilt with another description finds the same entry: MetricName equality ignores the description.
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-key-identity"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        IReadOnlyDictionary<MetricName, IMetric> metrics = consumer.Metrics();
        Assert.NotEmpty(metrics);

        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            MetricName original = entry.Key;
            MetricName rebuilt = new MetricName(
                original.Name(),
                original.Group(),
                original.Description() + "-mutated",
                new Dictionary<string, string>(RebuildTags(original.Tags()), StringComparer.Ordinal));

            Assert.True(metrics.ContainsKey(rebuilt));
            IMetric found = metrics[rebuilt];
            Assert.Equal(entry.Key, found.MetricName());
            Assert.Equal(original.Description(), found.MetricName().Description());
            Assert.NotSame(entry.Value, found);
            return;
        }
    }

    [Fact]
    public void Metrics_SyncMockAfterDispose_ThrowsJavasClosedMessage()
    {
        // Java's MockConsumer.metrics() calls ensureNotClosed().
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        IReadOnlyDictionary<MetricName, IMetric> before = consumer.Metrics();
        TestTimeout.Dispose(consumer);

        IllegalStateException e = Assert.Throws<IllegalStateException>(() => consumer.Metrics());
        Assert.Equal("This consumer has already been closed.", e.Message);
        Assert.Empty(before);
    }

    [Fact]
    public async Task Metrics_AsyncMockAfterDisposeAsync_ThrowsJavasClosedMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>("latest");
        await TestTimeout.DisposeAsync(consumer);

        IllegalStateException e = Assert.Throws<IllegalStateException>(() => consumer.Metrics());
        Assert.Equal("This consumer has already been closed.", e.Message);
    }

    // Java's KafkaConsumerTest.testMetricsRemovedOnClose (the CONSUMER protocol): a consumer without group.id, closed
    // with a zero timeout, keeps only the kafka-metrics-count:count meta-metric, and its map still answers.
    [Fact]
    public void Metrics_SyncRealWithoutGroupId_AfterClose_KeepsOnlyTheCountMetaMetric()
    {
        KafkaConsumer<string, string> consumer = KafkaConsumer<string, string>.Create(
            new Dictionary<string, object?>
            {
                ["group.protocol"] = "consumer",
                ["bootstrap.servers"] = "localhost:9999",
            },
            Serdes.String().Deserializer(),
            Serdes.String().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertMetricsMap(consumer, metricsShouldBePresent: true);
        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        AssertMetricsMap(consumer, metricsShouldBePresent: false);
    }

    [Fact]
    public void Metrics_SyncRealAfterDispose_AnswersWhatTheCoreLeavesRegistered()
    {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-after-close-sync"), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        IReadOnlyDictionary<MetricName, IMetric> before = consumer.Metrics();
        Assert.Contains("consumer-metrics:last-poll-seconds-ago", GroupAndNames(before));
        TestTimeout.Dispose(consumer);

        AssertMetricsRemovedOnClose(before);
        AssertMetricsRemovedOnClose(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_AsyncRealAfterDisposeAsync_AnswersWhatTheCoreLeavesRegistered()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-after-close-async"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        IReadOnlyDictionary<MetricName, IMetric> before = consumer.Metrics();
        await TestTimeout.DisposeAsync(consumer);

        AssertMetricsRemovedOnClose(before);
        AssertMetricsRemovedOnClose(consumer.Metrics());
    }

    [Fact]
    public void Metrics_ReachableViaIConsumerCommon()
    {
        MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>("latest");
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        IConsumerCommon consumer = mock;
        Assert.Empty(consumer.Metrics());
    }

    [Fact]
    public async Task Metrics_ConcurrentWithInFlightPoll_AnswersWhileThePollHoldsTheConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await AsyncKafkaConsumer<byte[], byte[]>.Create(
            RealConfig("metrics-concurrent"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        // Subscribe completes without a broker, so the poll that follows waits.
        await TestTimeout.Run(() => consumer.Subscribe(new[] { "metrics-concurrent-probe" }), s_deadline);

        Task<ConsumerRecords<byte[], byte[]>> inFlight = consumer.Poll(TimeSpan.FromSeconds(20));
        try
        {
            ConcurrentModificationException ex = Assert.Throws<ConcurrentModificationException>(() => consumer.Assignment());
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
            Assert.NotNull(name.Name());
            Assert.NotNull(name.Group());
            Assert.NotNull(name.Description());
            Assert.NotNull(name.Tags());

            Assert.NotNull(metric);
            Assert.Equal(name, metric.MetricName());

            object value = metric.MetricValue();
            Assert.NotNull(value);
            Assert.True(
                value is double || value is string || value is long || value is int,
                $"Unexpected metric value type {value.GetType()} for {name.Name()}");
        }
    }

    // Java's KafkaConsumerTest.assertMetricsMap: the count meta-metric is present, and the other metrics are present
    // or, after close, gone.
    private static void AssertMetricsMap(KafkaConsumer<string, string> consumer, bool metricsShouldBePresent)
    {
        // Copy the map because we're going to modify it.
        Dictionary<MetricName, IMetric> metrics = new Dictionary<MetricName, IMetric>();
        foreach (KeyValuePair<MetricName, IMetric> entry in consumer.Metrics())
        {
            metrics.Add(entry.Key, entry.Value);
        }

        // There's a meta-metric named "count" that is automatically added to the metrics map.
        MetricName? countMetricName = null;
        foreach (MetricName name in metrics.Keys)
        {
            if (name.Name() == "count" && name.Group() == "kafka-metrics-count")
            {
                countMetricName = name;
            }
        }

        Assert.True(countMetricName is not null, "The \"count\" meta-metric was unexpectedly missing from the Consumer metrics");
        Assert.True(
            metrics.Remove(countMetricName!),
            "The \"count\" meta-metric key was removed from the Consumer metrics map, but it unexpectedly had no entry");
        if (metricsShouldBePresent)
        {
            Assert.True(metrics.Count > 0, "The consumer should have created metrics, but they are unexpectedly empty");
        }
        else
        {
            Assert.True(
                metrics.Count == 0,
                "The consumer should have removed its metrics on close(), but there are metrics remaining: "
                    + string.Join(", ", GroupAndNames(metrics)));
        }
    }

    // A consumer with a group.id: after close the map still answers and shows what the close leaves registered. Java's
    // close leaves the coordinator's metrics too, as it never closes the commit, heartbeat and rebalance metrics managers
    // that register them; the core does the same.
    private static void AssertMetricsRemovedOnClose(IReadOnlyDictionary<MetricName, IMetric> metrics)
    {
        string[] expected =
        {
            "consumer-coordinator-metrics:assigned-partitions",
            "consumer-coordinator-metrics:commit-latency-avg",
            "consumer-coordinator-metrics:commit-latency-max",
            "consumer-coordinator-metrics:commit-rate",
            "consumer-coordinator-metrics:commit-total",
            "consumer-coordinator-metrics:failed-rebalance-rate-per-hour",
            "consumer-coordinator-metrics:failed-rebalance-total",
            "consumer-coordinator-metrics:heartbeat-rate",
            "consumer-coordinator-metrics:heartbeat-response-time-max",
            "consumer-coordinator-metrics:heartbeat-total",
            "consumer-coordinator-metrics:last-heartbeat-seconds-ago",
            "consumer-coordinator-metrics:last-rebalance-seconds-ago",
            "consumer-coordinator-metrics:rebalance-latency-avg",
            "consumer-coordinator-metrics:rebalance-latency-max",
            "consumer-coordinator-metrics:rebalance-latency-total",
            "consumer-coordinator-metrics:rebalance-rate-per-hour",
            "consumer-coordinator-metrics:rebalance-total",
            "kafka-metrics-count:count",
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
        catch (System.TimeoutException)
        {
        }
    }
}
