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
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="KafkaMetric"/>'s <c>Config()</c>, <c>Config(MetricConfig)</c>, <c>IsMeasurable()</c> and
/// <c>Measurable()</c>, over a client's metrics, without a broker, and over a quota error's metric.
/// </summary>
/// <remarks>
/// Java's <c>KafkaMetricTest</c> builds each metric with the public constructor, which takes a <c>Time</c> that is not
/// public API and which the binding does not have. <c>testIsMeasurableWithGaugeProvider</c>,
/// <c>testKafkaMetricAcceptsNonMeasurableNonGaugeProvider</c>, <c>testToStringWithLambdaProvider</c> and
/// <c>testToStringWithAnonymousClassProvider</c> run over a metric with a provider of the test's own, registered
/// through the C ABI (<see cref="NativeMetricRegistry"/>), and <c>testIsMeasurable</c> over the producer's
/// <c>flush-time-ns-total</c> stat. Not translated: <c>testMeasurableValueReturnsZeroWhenNotMeasurable</c>
/// (<c>measurableValue</c> is package-private in Java), <c>testConstructorWithNullProvider</c> (it tests the
/// constructor), and <c>testToStringWithStatProvider</c> (the core names a stat by its Rust type path where Java names
/// <c>org.apache.kafka.common.metrics.stats.Avg</c>, a core follow-up).
/// </remarks>
public sealed class PublicKafkaMetricMembersTests
{
    private const string FlushTime = "flush-time-ns-total";

    private const string LastPoll = "last-poll-seconds-ago";

    private const string NotMeasurable =
        "Not a measurable: the metric of a quota violation holds only its name and its value";

    // Java's METRIC_NAME_1 and METRIC_NAME_2.
    private static readonly MetricName s_metricName1 =
        new MetricName("name", "group", "description", new Dictionary<string, string>());

    private static readonly MetricName s_metricName2 = new MetricName(
        "request-latency-avg",
        "consumer-fetch-manager-metrics",
        "The average request latency in ms",
        new Dictionary<string, string> { ["client-id"] = "consumer-1" });

    [Fact]
    public void IsMeasurable_AStatMetric_IsMeasurable_AndMeasurableMeasuresIt()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");

        Assert.True(metric.IsMeasurable());
        IMeasurable measurable = metric.Measurable();
        Assert.NotNull(measurable);

        producer.Flush();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        MetricConfig config = metric.Config()!;
        double measured = measurable.Measure(config, now);

        Assert.Equal((double)metric.MetricValue(), measured);
        Assert.True(measured > 0.0, "flush-time-ns-total is still 0 after a flush");
        Assert.Equal(measured, measurable.Value(config, now));
        Assert.Equal(measured, MeasurableExtensions.Value(measurable, config, now));
    }

    [Fact]
    public void IsMeasurableWithGaugeProvider()
    {
        KafkaMetric metric = NativeMetricRegistry.Gauge(s_metricName1);

        Assert.False(metric.IsMeasurable());
        IllegalStateException e = Assert.Throws<IllegalStateException>(() => metric.Measurable());

        // Java's message names the provider's class, the core's names its type or says it has none.
        Assert.StartsWith("Not a measurable: ", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void KafkaMetricAcceptsNonMeasurableNonGaugeProvider()
    {
        KafkaMetric metric = NativeMetricRegistry.StringProvider(s_metricName1);

        object value = metric.MetricValue();

        Assert.Equal(NativeMetricRegistry.ProviderValue, value);
    }

    [Fact]
    public void ToStringWithLambdaProvider()
    {
        ToStringOnLambdaOrAnonymousClass(NativeMetricRegistry.Measurable(s_metricName2));
    }

    [Fact]
    public void ToStringWithAnonymousClassProvider()
    {
        // Through the C ABI a lambda and an anonymous class are both a C implementation with no type name; this one
        // goes through the other constructor, MetricValueProvider_new, as the core's translation of this test does.
        ToStringOnLambdaOrAnonymousClass(NativeMetricRegistry.Gauge(s_metricName2));
    }

    [Fact]
    public void Measurable_IsANewObjectOnEachCall_OverTheSameMetric()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        producer.Flush();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        IMeasurable first = metric.Measurable();
        IMeasurable second = metric.Measurable();

        Assert.NotSame(first, second);
        Assert.Equal(first.Measure(new MetricConfig(), now), second.Measure(new MetricConfig(), now));
    }

    [Fact]
    public void Measurable_KeepsItsMetricAlive_AndMeasuresAfterTheProducerIsDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        IMeasurable measurable;
        double before;
        try
        {
            producer.Flush();
            measurable = MeasurableOfADroppedMetric(producer);
            before = measurable.Measure(new MetricConfig(), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
        finally
        {
            TestTimeout.Dispose(producer);
        }

        Collect();

        double after = measurable.Measure(new MetricConfig(), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        Assert.True(after >= before, $"flush-time-ns-total went back from {before} to {after}");
    }

    [Fact]
    public void Measure_ANullConfig_ThrowsArgumentNullException()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IMeasurable measurable = Find(producer.Metrics(), FlushTime, "producer-metrics").Measurable();

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(() => measurable.Measure(null!, 0));

        Assert.Equal("config", e.ParamName);
    }

    [Fact]
    public void Measure_AConfigurationTheCoreCannotHold_ThrowsBeforeMeasuring()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        IMeasurable measurable = Find(producer.Metrics(), FlushTime, "producer-metrics").Measurable();
        MetricConfig nullValue = new MetricConfig().Tags(new Dictionary<string, string> { ["k"] = null! });
        MetricConfig unknownLevel = new MetricConfig().RecordLevel((Sensor.RecordingLevel)5);

        AssertNullArgument("config must not contain a null tag value", "config", () => measurable.Measure(nullValue, 0));
        IllegalArgumentException e =
            Assert.Throws<IllegalArgumentException>(() => measurable.Measure(unknownLevel, 0));
        Assert.Equal("Unexpected RecordLevel id `5`, it should be between `0` and `2` (inclusive)", e.Message);
    }

    [Fact]
    public void MeasurableExtensionsValue_ANullMeasurable_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => MeasurableExtensions.Value(null!, new MetricConfig(), 0));

        Assert.Equal("measurable", e.ParamName);
    }

    [Fact]
    public void Config_AClientsMetric_IsTheClientsMetricsConfiguration()
    {
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = "localhost:1",
                ["client.id"] = "metric-config-é",
                ["metrics.num.samples"] = 3,
                ["metrics.sample.window.ms"] = 12345L,
                ["metrics.recording.level"] = "DEBUG",
            },
            Serdes.ByteArray().Serializer(),
            Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        MetricConfig config = Find(producer.Metrics(), FlushTime, "producer-metrics").Config()!;

        Assert.Null(config.Quota());
        Assert.Equal(3, config.Samples());
        Assert.Equal(long.MaxValue, config.EventWindow());
        Assert.Equal(12345L, config.TimeWindowMs());
        Assert.Equal(Sensor.RecordingLevel.DEBUG, config.RecordLevel());
        AssertTags(new Dictionary<string, string> { ["client-id"] = "metric-config-é" }, config.Tags());
    }

    [Theory]
    [InlineData(LastPoll, "consumer-metrics")]
    [InlineData("count", "kafka-metrics-count")]
    public void Config_AConsumersMetric_HasTheConsumersDefaultMetricsConfiguration(string name, string group)
    {
        KafkaConsumer<byte[], byte[]> consumer = Consumer("kafka-metric-consumer-config");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        MetricConfig config = Find(consumer.Metrics(), name, group).Config()!;

        Assert.Equal(MetricConfig.DEFAULT_NUM_SAMPLES, config.Samples());
        Assert.Equal(30000L, config.TimeWindowMs());
        Assert.Equal(Sensor.RecordingLevel.INFO, config.RecordLevel());
    }

    [Fact]
    public void Config_SetThenRead_ReturnsTheNewConfiguration()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");

        metric.Config(new MetricConfig().Samples(5));

        MetricConfig config = metric.Config()!;
        Assert.Equal(5, config.Samples());
        Assert.Equal(30000L, config.TimeWindowMs());
        Assert.Empty(config.Tags()!);
    }

    [Fact]
    public void Config_EveryField_RoundTrips()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        Dictionary<string, string> tags = new Dictionary<string, string>
        {
            ["client-id"] = "c-1",
            ["ключ"] = "значение-日本",
            ["empty"] = string.Empty,
        };

        metric.Config(new MetricConfig()
            .Quota(Quota.UpperBound(10.5))
            .Samples(4)
            .EventWindow(77)
            .TimeWindow(TimeSpan.FromSeconds(7))
            .Tags(tags)
            .RecordLevel(Sensor.RecordingLevel.TRACE));

        MetricConfig config = metric.Config()!;
        Assert.Equal(Quota.UpperBound(10.5), config.Quota());
        Assert.Equal(4, config.Samples());
        Assert.Equal(77L, config.EventWindow());
        Assert.Equal(7000L, config.TimeWindowMs());
        AssertTags(tags, config.Tags());
        Assert.Equal(Sensor.RecordingLevel.TRACE, config.RecordLevel());

        metric.Config(new MetricConfig().Quota(Quota.LowerBound(-1.5)).RecordLevel(Sensor.RecordingLevel.DEBUG));

        config = metric.Config()!;
        Assert.Equal(Quota.LowerBound(-1.5), config.Quota());
        Assert.False(config.Quota()!.IsUpperBound());
        Assert.Equal(Sensor.RecordingLevel.DEBUG, config.RecordLevel());
    }

    [Fact]
    public void Config_ReturnsACopy_SoChangingItDoesNotChangeTheMetric()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        MetricConfig first = metric.Config()!;
        int samples = first.Samples();

        first.Samples(samples + 7).Quota(Quota.UpperBound(1.0));

        Assert.NotSame(first, metric.Config());
        Assert.Equal(samples, metric.Config()!.Samples());
        Assert.Null(metric.Config()!.Quota());
    }

    [Fact]
    public void Config_CopiesTheConfigurationItIsGiven()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        Dictionary<string, string> tags = new Dictionary<string, string> { ["a"] = "b" };
        MetricConfig given = new MetricConfig().Samples(6).Tags(tags);

        metric.Config(given);
        given.Samples(9);
        tags["c"] = "d";

        Assert.Equal(6, metric.Config()!.Samples());
        AssertTags(new Dictionary<string, string> { ["a"] = "b" }, metric.Config()!.Tags());
    }

    [Fact]
    public void Config_ANullTagsMap_SetsNoTags()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");

        metric.Config(new MetricConfig().Tags(null));

        Assert.Empty(metric.Config()!.Tags()!);
    }

    [Fact]
    public void Config_IsReadAfterTheProducerIsDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        KafkaMetric metric;
        try
        {
            metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        }
        finally
        {
            TestTimeout.Dispose(producer);
        }

        metric.Config(new MetricConfig().Samples(8));

        Assert.Equal(8, metric.Config()!.Samples());
        Assert.True(metric.IsMeasurable());
    }

    [Fact]
    public void ConfigOfNull_ThrowsArgumentNullException()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(() => metric.Config(null!));

        Assert.Equal("config", e.ParamName);
    }

    [Fact]
    public void Config_ANullTagValue_ThrowsArgumentNullException_AndKeepsTheConfiguration()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        int samples = metric.Config()!.Samples();
        MetricConfig config = new MetricConfig()
            .Samples(samples + 1)
            .Tags(new Dictionary<string, string> { ["k"] = null! });

        AssertNullArgument("config must not contain a null tag value", "config", () => metric.Config(config));

        Assert.Equal(samples, metric.Config()!.Samples());
    }

    [Fact]
    public void Config_ANullTagKey_ThrowsArgumentNullException()
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        MetricConfig config = new MetricConfig().Tags(new NullKeyTags());

        AssertNullArgument("config must not contain a null tag key", "config", () => metric.Config(config));
    }

    [Theory]
    [InlineData("k\0ey", "v")]
    [InlineData("k", "v\0")]
    public void Config_ATagWithANul_ThrowsArgumentException(string key, string value)
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        MetricConfig config = new MetricConfig().Tags(new Dictionary<string, string> { [key] = value });

        ArgumentException e = Assert.Throws<ArgumentException>(() => metric.Config(config));

        Assert.Equal("config", e.ParamName);
        Assert.Equal(new ArgumentException(MetricConfigMarshal.InvalidTagMessage, "config").Message, e.Message);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(-1)]
    public void Config_ARecordingLevelNoMemberHas_ThrowsForIdsIllegalArgumentException(short level)
    {
        KafkaProducer<byte[], byte[]> producer = Producer();
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        KafkaMetric metric = Find(producer.Metrics(), FlushTime, "producer-metrics");
        MetricConfig config = new MetricConfig().RecordLevel((Sensor.RecordingLevel)level);

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => metric.Config(config));

        Assert.Equal(
            "Unexpected RecordLevel id `" + level.ToString(CultureInfo.InvariantCulture)
                + "`, it should be between `0` and `2` (inclusive)",
            e.Message);
        Assert.Equal(Sensor.RecordingLevel.INFO, metric.Config()!.RecordLevel());
    }

    [Fact]
    public void AQuotaErrorsMetric_IsNotMeasurable_AndHasNoConfigurationUntilOneIsSet()
    {
        KafkaMetric metric = QuotaErrorsMetric();

        Assert.False(metric.IsMeasurable());
        Assert.Null(metric.Config());
        IllegalStateException e = Assert.Throws<IllegalStateException>(() => metric.Measurable());
        Assert.Equal(NotMeasurable, e.Message);
        Assert.Equal(12.5, metric.MetricValue());
    }

    [Fact]
    public void AQuotaErrorsMetric_KeepsACopyOfTheConfigurationItIsGiven()
    {
        KafkaMetric metric = QuotaErrorsMetric();
        Dictionary<string, string> tags = new Dictionary<string, string> { ["a"] = "b" };
        MetricConfig given = new MetricConfig()
            .Quota(Quota.UpperBound(2.0))
            .Samples(4)
            .TimeWindow(TimeSpan.FromSeconds(3))
            .Tags(tags)
            .RecordLevel(Sensor.RecordingLevel.TRACE);

        metric.Config(given);
        given.Samples(9);
        tags["c"] = "d";

        MetricConfig config = metric.Config()!;
        Assert.NotSame(given, config);
        Assert.NotSame(config, metric.Config());
        Assert.Equal(Quota.UpperBound(2.0), config.Quota());
        Assert.Equal(4, config.Samples());
        Assert.Equal(3000L, config.TimeWindowMs());
        AssertTags(new Dictionary<string, string> { ["a"] = "b" }, config.Tags());
        Assert.Equal(Sensor.RecordingLevel.TRACE, config.RecordLevel());

        config.Samples(2);
        Assert.Equal(4, metric.Config()!.Samples());
        Assert.False(metric.IsMeasurable());
        Assert.Equal(12.5, metric.MetricValue());
    }

    [Fact]
    public void AQuotaErrorsMetric_RefusesWhatAClientsMetricRefuses()
    {
        KafkaMetric metric = QuotaErrorsMetric();

        Assert.Equal("config", Assert.Throws<ArgumentNullException>(() => metric.Config(null!)).ParamName);
        Assert.Throws<IllegalArgumentException>(
            () => metric.Config(new MetricConfig().RecordLevel((Sensor.RecordingLevel)7)));
        Assert.Null(metric.Config());
    }

    private static KafkaProducer<byte[], byte[]> Producer() => KafkaProducer<byte[], byte[]>.Create(
        new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:1" },
        Serdes.ByteArray().Serializer(),
        Serdes.ByteArray().Serializer());

    private static KafkaConsumer<byte[], byte[]> Consumer(string groupId) => KafkaConsumer<byte[], byte[]>.Create(
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = groupId,
        },
        Serdes.ByteArray().Deserializer(),
        Serdes.ByteArray().Deserializer());

    private static KafkaMetric QuotaErrorsMetric() => new KafkaMetric(
        new MetricName("produce-throttle", "producer-metrics", "d", new Dictionary<string, string>()), 12.5);

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

    // Java's testToStringOnLambdaOrAnonymousClass: the text names no provider.
    private static void ToStringOnLambdaOrAnonymousClass(KafkaMetric metric)
    {
        Assert.Equal(
            "KafkaMetric [metricName=MetricName [name=request-latency-avg, "
                + "group=consumer-fetch-manager-metrics, "
                + "description=The average request latency in ms, "
                + "tags={client-id=consumer-1}]]",
            metric.ToString());
    }

    // The metric is reachable only through the measurable this returns.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static IMeasurable MeasurableOfADroppedMetric(KafkaProducer<byte[], byte[]> producer) =>
        Find(producer.Metrics(), FlushTime, "producer-metrics").Measurable();

    // The core keeps tags in its own key order, so they are compared as a set of entries.
    private static void AssertTags(
        IReadOnlyDictionary<string, string> expected, IReadOnlyDictionary<string, string>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(
            expected.OrderBy(tag => tag.Key, StringComparer.Ordinal),
            actual!.OrderBy(tag => tag.Key, StringComparer.Ordinal));
    }

    private static void AssertNullArgument(string message, string parameterName, Action call)
    {
        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(call);
        Assert.Equal(parameterName, failure.ParamName);
        Assert.Equal(new ArgumentNullException(parameterName, message).Message, failure.Message);
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

    // A map with a null key, which a Dictionary cannot hold and Java's HashMap can.
    private sealed class NullKeyTags : IReadOnlyDictionary<string, string>
    {
        private readonly KeyValuePair<string, string>[] _entries = { new KeyValuePair<string, string>(null!, "v") };

        public int Count => _entries.Length;

        public IEnumerable<string> Keys => _entries.Select(entry => entry.Key);

        public IEnumerable<string> Values => _entries.Select(entry => entry.Value);

        public string this[string key] => throw new KeyNotFoundException();

        public bool ContainsKey(string key) => false;

        public bool TryGetValue(string key, out string value)
        {
            value = null!;
            return false;
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, string>>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
