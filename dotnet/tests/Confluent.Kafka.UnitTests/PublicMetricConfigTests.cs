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

using Confluent.Kafka.Common.Metrics;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="MetricConfig"/> is Java's value class: Java's defaults, setters that return the same instance, the one
/// precondition of <c>samples(int)</c> with Java's message, and Java's <c>timeWindow(long, TimeUnit)</c> as a
/// <see cref="TimeSpan"/> truncated to whole milliseconds.
/// </summary>
public sealed class PublicMetricConfigTests
{
    [Fact]
    public void ANewConfig_HasJavasDefaults()
    {
        MetricConfig config = new MetricConfig();

        Assert.Null(config.Quota());
        Assert.Equal(2, MetricConfig.DEFAULT_NUM_SAMPLES);
        Assert.Equal(MetricConfig.DEFAULT_NUM_SAMPLES, config.Samples());
        Assert.Equal(long.MaxValue, config.EventWindow());
        Assert.Equal(30000L, config.TimeWindowMs());
        Assert.NotNull(config.Tags());
        Assert.Empty(config.Tags()!);
        Assert.Equal(Sensor.RecordingLevel.INFO, config.RecordLevel());
    }

    [Fact]
    public void EachSetter_ReturnsTheSameConfig_AndItsGetterReadsTheValue()
    {
        MetricConfig config = new MetricConfig();
        Quota quota = Quota.UpperBound(10.5);
        Dictionary<string, string> tags = new Dictionary<string, string> { ["client-id"] = "c-1" };

        Assert.Same(config, config.Quota(quota));
        Assert.Same(config, config.Samples(5));
        Assert.Same(config, config.EventWindow(77));
        Assert.Same(config, config.TimeWindow(TimeSpan.FromSeconds(7)));
        Assert.Same(config, config.Tags(tags));
        Assert.Same(config, config.RecordLevel(Sensor.RecordingLevel.TRACE));

        Assert.Same(quota, config.Quota());
        Assert.Equal(5, config.Samples());
        Assert.Equal(77L, config.EventWindow());
        Assert.Equal(7000L, config.TimeWindowMs());
        Assert.Same(tags, config.Tags());
        Assert.Equal(Sensor.RecordingLevel.TRACE, config.RecordLevel());
    }

    [Fact]
    public void Setters_Chain_AsJavas()
    {
        MetricConfig config = new MetricConfig()
            .Samples(3)
            .TimeWindow(TimeSpan.FromMilliseconds(1500))
            .RecordLevel(Sensor.RecordingLevel.DEBUG)
            .Quota(Quota.LowerBound(1.0));

        Assert.Equal(3, config.Samples());
        Assert.Equal(1500L, config.TimeWindowMs());
        Assert.Equal(Sensor.RecordingLevel.DEBUG, config.RecordLevel());
        Assert.Equal(Quota.LowerBound(1.0), config.Quota());
    }

    [Fact]
    public void QuotaAndTags_CanBeSetToNull_AsJavas()
    {
        MetricConfig config = new MetricConfig().Quota(Quota.UpperBound(1.0)).Tags(null).Quota(null);

        Assert.Null(config.Quota());
        Assert.Null(config.Tags());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void Samples_BelowOne_ThrowsJavasIllegalArgumentException_AndKeepsTheValue(int samples)
    {
        MetricConfig config = new MetricConfig().Samples(4);

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => config.Samples(samples));

        Assert.Equal("The number of samples must be at least 1.", e.Message);
        Assert.Equal(4, config.Samples());
    }

    [Fact]
    public void Samples_One_IsAccepted()
    {
        Assert.Equal(1, new MetricConfig().Samples(1).Samples());
    }

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(9_999L, 0L)]
    [InlineData(10_000L, 1L)]
    [InlineData(15_999L, 1L)]
    [InlineData(-9_999L, 0L)]
    [InlineData(-15_999L, -1L)]
    [InlineData(300_000_000L, 30_000L)]
    public void TimeWindow_IsWholeMilliseconds_TruncatedTowardZero_AsJavasTimeUnitConvert(long ticks, long expected)
    {
        Assert.Equal(expected, new MetricConfig().TimeWindow(TimeSpan.FromTicks(ticks)).TimeWindowMs());
    }

    [Fact]
    public void TimeWindow_TheLargestTimeSpan_FitsInMilliseconds()
    {
        Assert.Equal(
            TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerMillisecond,
            new MetricConfig().TimeWindow(TimeSpan.MaxValue).TimeWindowMs());
        Assert.Equal(
            TimeSpan.MinValue.Ticks / TimeSpan.TicksPerMillisecond,
            new MetricConfig().TimeWindow(TimeSpan.MinValue).TimeWindowMs());
    }

    [Fact]
    public void Tags_AreHeldByReference_AsJavasMap()
    {
        Dictionary<string, string> tags = new Dictionary<string, string>();
        MetricConfig config = new MetricConfig().Tags(tags);

        tags["added"] = "later";

        Assert.Equal("later", config.Tags()!["added"]);
    }

    [Fact]
    public void ANewConfig_HasTagsOfItsOwn()
    {
        Assert.NotSame(new MetricConfig().Tags(), new MetricConfig().Tags());
    }

    [Fact]
    public void MetricConfig_IsNotSealed_AsJavasIsNotFinal()
    {
        Assert.False(typeof(MetricConfig).IsSealed);
    }
}
