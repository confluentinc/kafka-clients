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

using Confluent.Kafka.Common.Metrics;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Quota"/> is Java's class: Java's <c>MetricsTest.testQuotasEquality</c>, translated, and Java's
/// <c>acceptable</c>, <c>equals</c>, <c>hashCode</c> and <c>toString</c>.
/// </summary>
/// <remarks>
/// <para>
/// Not translated: Java's other <c>MetricsTest</c> tests, <c>testMetricName</c>, <c>testSimpleStats</c>,
/// <c>testHierarchicalSensors</c>, <c>testBadSensorHierarchy</c>, <c>testRemoveChildSensor</c>,
/// <c>testRemoveSensor</c>, <c>testRemoveInactiveMetrics</c>, <c>testRemoveMetric</c>, <c>testTimeWindowing</c>,
/// <c>testOldDataHasNoEffect</c>, <c>testSampledStatReturnsNaNWhenNoValuesExist</c>,
/// <c>testSampledStatReturnsInitialValueWhenNoValuesExist</c>, <c>testDuplicateMetricName</c>, <c>testQuotas</c>,
/// <c>testPercentiles</c>, <c>shouldPinSmallerValuesToMin</c>, <c>shouldPinLargerValuesToMax</c>,
/// <c>testPercentilesWithRandomNumbersAndLinearBucketing</c>, <c>testRateWindowing</c>, <c>testSimpleRate</c>,
/// <c>testMetricInstances</c>, <c>testConcurrentReadUpdate</c> and <c>testConcurrentReadUpdateReport</c>: they build a
/// <c>Metrics</c> registry, its sensors and its stats, and record and report through them; the binding hands out none
/// of these (the registry is the native client's) and does not translate the stats or the reporters.
/// </para>
/// <para>
/// For the same reason the other test classes of Java's <c>org.apache.kafka.common.metrics</c> package and its
/// <c>internals</c> and <c>stats</c> packages are not translated: <c>JmxReporterTest</c>, <c>KafkaMbeanTest</c>,
/// <c>KafkaMetricsContextTest</c>, <c>TokenBucketTest</c>, <c>IntGaugeSuiteTest</c>, <c>MetricsUtilsTest</c>,
/// <c>PluginMetricsImplTest</c>, <c>FrequenciesTest</c>, <c>HistogramTest</c>, <c>MeterTest</c>, <c>RateTest</c> and
/// <c>SampledStatTest</c>. <c>KafkaMetricTest</c> is in <see cref="PublicKafkaMetricMembersTests"/>.
/// </para>
/// </remarks>
public sealed class PublicQuotaTests
{
    [Fact]
    public void QuotasEquality()
    {
        Quota quota1 = Quota.UpperBound(10.5);
        Quota quota2 = Quota.LowerBound(10.5);

        Assert.NotEqual(quota1, quota2);

        Quota quota3 = Quota.LowerBound(10.5);

        Assert.Equal(quota2, quota3);
    }

    [Fact]
    public void TheFactories_SetTheBoundAndTheKind()
    {
        Assert.True(Quota.UpperBound(5.0).IsUpperBound());
        Assert.Equal(5.0, Quota.UpperBound(5.0).Bound());
        Assert.False(Quota.LowerBound(-2.5).IsUpperBound());
        Assert.Equal(-2.5, Quota.LowerBound(-2.5).Bound());
        Assert.Equal(Quota.UpperBound(5.0), new Quota(5.0, true));
        Assert.Equal(Quota.LowerBound(5.0), new Quota(5.0, false));
    }

    [Theory]
    [InlineData(true, 10.0, 9.9, true)]
    [InlineData(true, 10.0, 10.0, true)]
    [InlineData(true, 10.0, 10.1, false)]
    [InlineData(false, 10.0, 10.1, true)]
    [InlineData(false, 10.0, 10.0, true)]
    [InlineData(false, 10.0, 9.9, false)]
    [InlineData(true, 10.0, double.NaN, false)]
    [InlineData(false, 10.0, double.NaN, false)]
    public void Acceptable_IsJavas(bool upper, double bound, double value, bool expected)
    {
        Assert.Equal(expected, new Quota(bound, upper).Acceptable(value));
    }

    [Fact]
    public void Equals_ComparesTheBoundsAsDoubles_AsJavasDoubleEquality()
    {
        Assert.Equal(Quota.UpperBound(0.0), Quota.UpperBound(-0.0));
        Assert.True(Quota.UpperBound(0.0) == Quota.UpperBound(-0.0));

        Quota nan = Quota.UpperBound(double.NaN);
        Assert.True(nan.Equals(nan));
        Assert.False(nan.Equals(Quota.UpperBound(double.NaN)));
        Assert.True(nan != Quota.UpperBound(double.NaN));
    }

    [Fact]
    public void Equals_OtherObjects_AreNotEqual()
    {
        Quota quota = Quota.UpperBound(1.0);

        Assert.False(quota.Equals(null));
        Assert.False(quota.Equals((object?)null));
        Assert.False(quota.Equals((object)1.0));
        Assert.False(quota == null);
        Assert.True((Quota?)null == null);
    }

    [Theory]
    [InlineData(10.5, true, 1272)]
    [InlineData(10.5, false, 1271)]
    [InlineData(-10.9, true, 652)]
    [InlineData(0.0, false, 961)]
    [InlineData(double.NaN, true, 962)]
    public void GetHashCode_IsJavas(double bound, bool upper, int expected)
    {
        Assert.Equal(expected, new Quota(bound, upper).GetHashCode());
    }

    [Fact]
    public void GetHashCode_ABoundOutsideAnInt_IsSaturatedAsJavasCast()
    {
        int high = unchecked((31 * (31 + int.MaxValue)) + 1);
        int low = unchecked((31 * (31 + int.MinValue)) + 0);

        Assert.Equal(high, Quota.UpperBound(1e20).GetHashCode());
        Assert.Equal(high, Quota.UpperBound(double.PositiveInfinity).GetHashCode());
        Assert.Equal(low, Quota.LowerBound(-1e20).GetHashCode());
    }

    [Theory]
    [InlineData(10.5, true, "upper=10.5")]
    [InlineData(5.0, false, "lower=5.0")]
    [InlineData(double.PositiveInfinity, true, "upper=Infinity")]
    [InlineData(double.NaN, false, "lower=NaN")]
    public void ToString_IsJavas(double bound, bool upper, string expected)
    {
        Assert.Equal(expected, new Quota(bound, upper).ToString());
    }

    [Fact]
    public void Quota_IsSealed_AsJavasIsFinal()
    {
        Assert.True(typeof(Quota).IsSealed);
    }
}
