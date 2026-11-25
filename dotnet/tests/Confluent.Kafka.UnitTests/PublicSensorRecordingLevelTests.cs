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
using System.Globalization;
using System.Reflection;

using Confluent.Kafka.Common.Metrics;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Sensor.RecordingLevel"/> and Java's methods on it in <see cref="RecordingLevelMethods"/>: Java's
/// <c>SensorTest.testRecordLevelEnum</c>, translated, and the messages of <c>forId</c>, <c>forName</c> and
/// <c>shouldRecord</c>.
/// </summary>
/// <remarks>
/// Not translated: Java's other <c>SensorTest</c> tests, <c>testShouldRecordForInfoLevelSensor</c>,
/// <c>testShouldRecordForDebugLevelSensor</c>, <c>testShouldRecordForTraceLevelSensor</c>,
/// <c>testExpiredSensor</c>, <c>testIdempotentAdd</c>, <c>testCheckQuotasInMultiThreads</c>,
/// <c>shouldReturnPresenceOfMetrics</c>, <c>testStrictQuotaEnforcementWithRate</c>,
/// <c>testStrictQuotaEnforcementWithTokenBucket</c>, <c>testRecordAndCheckQuotaUseMetricConfigOfEachStat</c> and
/// <c>testUpdatingMetricConfigIsReflectedInTheSensor</c>: they build a <c>Metrics</c> registry and a sensor and record
/// through them, and the binding hands out neither (the registry is the native client's). A configuration set on a
/// client's metric is read back in <see cref="PublicKafkaMetricMembersTests"/>, and the recording level of a client's
/// metrics in <see cref="PublicConsumerMetricsTests"/> and <see cref="PublicProducerMetricsTests"/>.
/// </remarks>
public sealed class PublicSensorRecordingLevelTests
{
    [Fact]
    public void RecordLevelEnum()
    {
        Sensor.RecordingLevel configLevel = Sensor.RecordingLevel.INFO;
        Assert.True(Sensor.RecordingLevel.INFO.ShouldRecord((short)configLevel));
        Assert.False(Sensor.RecordingLevel.DEBUG.ShouldRecord((short)configLevel));
        Assert.False(Sensor.RecordingLevel.TRACE.ShouldRecord((short)configLevel));

        configLevel = Sensor.RecordingLevel.DEBUG;
        Assert.True(Sensor.RecordingLevel.INFO.ShouldRecord((short)configLevel));
        Assert.True(Sensor.RecordingLevel.DEBUG.ShouldRecord((short)configLevel));
        Assert.False(Sensor.RecordingLevel.TRACE.ShouldRecord((short)configLevel));

        configLevel = Sensor.RecordingLevel.TRACE;
        Assert.True(Sensor.RecordingLevel.INFO.ShouldRecord((short)configLevel));
        Assert.True(Sensor.RecordingLevel.DEBUG.ShouldRecord((short)configLevel));
        Assert.True(Sensor.RecordingLevel.TRACE.ShouldRecord((short)configLevel));

        Assert.Equal(Sensor.RecordingLevel.DEBUG, ValueOf(Sensor.RecordingLevel.DEBUG.ToString()));
        Assert.Equal(Sensor.RecordingLevel.INFO, ValueOf(Sensor.RecordingLevel.INFO.ToString()));
        Assert.Equal(Sensor.RecordingLevel.TRACE, ValueOf(Sensor.RecordingLevel.TRACE.ToString()));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(int.MaxValue)]
    public void ShouldRecord_AnUnknownConfigId_ThrowsJavasIllegalStateException(int configId)
    {
        IllegalStateException e = Assert.Throws<IllegalStateException>(
            () => Sensor.RecordingLevel.INFO.ShouldRecord(configId));

        Assert.Equal(
            "Did not recognize recording level " + configId.ToString(CultureInfo.InvariantCulture), e.Message);
    }

    [Theory]
    [InlineData(0, Sensor.RecordingLevel.INFO)]
    [InlineData(1, Sensor.RecordingLevel.DEBUG)]
    [InlineData(2, Sensor.RecordingLevel.TRACE)]
    public void ForId_IsCalledOnTheEnumOrOnItsMethodsClass(int id, Sensor.RecordingLevel expected)
    {
        Assert.Equal(expected, Sensor.RecordingLevel.ForId(id));
        Assert.Equal(expected, RecordingLevelMethods.ForId(id));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(int.MinValue)]
    public void ForId_OutsideTheIds_ThrowsJavasIllegalArgumentException(int id)
    {
        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => Sensor.RecordingLevel.ForId(id));

        Assert.Equal(
            "Unexpected RecordLevel id `" + id.ToString(CultureInfo.InvariantCulture)
                + "`, it should be between `0` and `2` (inclusive)",
            e.Message);
    }

    [Theory]
    [InlineData("INFO", Sensor.RecordingLevel.INFO)]
    [InlineData("debug", Sensor.RecordingLevel.DEBUG)]
    [InlineData("Trace", Sensor.RecordingLevel.TRACE)]
    public void ForName_IgnoresCase(string name, Sensor.RecordingLevel expected)
    {
        Assert.Equal(expected, Sensor.RecordingLevel.ForName(name));
        Assert.Equal(expected, RecordingLevelMethods.ForName(name));
    }

    [Theory]
    [InlineData("warn", "WARN")]
    [InlineData("", "")]
    [InlineData("0", "0")]
    public void ForName_AnUnknownName_ThrowsJavasValueOfMessage(string name, string upper)
    {
        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => Sensor.RecordingLevel.ForName(name));

        Assert.Equal("No enum constant org.apache.kafka.common.metrics.Sensor.RecordingLevel." + upper, e.Message);
    }

    [Fact]
    public void ForName_Null_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(() => Sensor.RecordingLevel.ForName(null!));

        Assert.Equal("name", e.ParamName);
    }

    [Fact]
    public void MaxRecordingLevelKey_IsJavas()
    {
        Assert.Equal(2, RecordingLevelMethods.MAX_RECORDING_LEVEL_KEY);
    }

    [Fact]
    public void ToString_IsJavasName()
    {
        Assert.Equal("INFO", Sensor.RecordingLevel.INFO.ToString());
        Assert.Equal("DEBUG", Sensor.RecordingLevel.DEBUG.ToString());
        Assert.Equal("TRACE", Sensor.RecordingLevel.TRACE.ToString());
    }

    [Fact]
    public void TheUnderlyingType_IsJavasShortId()
    {
        Assert.Equal(typeof(short), Enum.GetUnderlyingType(typeof(Sensor.RecordingLevel)));
    }

    [Fact]
    public void Sensor_HasNoPublicConstructor_AndIsSealed_AsJavasFinalClass()
    {
        Assert.Empty(typeof(Sensor).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.True(typeof(Sensor).IsSealed);
    }

    private static Sensor.RecordingLevel ValueOf(string name) =>
        (Sensor.RecordingLevel)Enum.Parse(typeof(Sensor.RecordingLevel), name);
}
