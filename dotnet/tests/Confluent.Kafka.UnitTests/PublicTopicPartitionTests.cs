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
using System.Globalization;
using System.Reflection;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="TopicPartition"/> value type (M7/P2a; relaxed in M15/P13.2 G3-4) — a
/// pure value-type check with <b>no consumer or admin interaction</b>. Its constructor, like
/// Java's (<c>TopicPartition.java:32-35</c>), stores whatever it is given: a negative partition
/// and a <see langword="null"/> topic are both representable, and nothing here throws.
/// </summary>
/// <remarks>
/// <para>
/// Until G3-4 this class held the single authoritative test for the constructor's
/// negative-partition rejection. That guard is gone, so the checks that <em>reject</em> such a
/// value now live where the value is used, each with its own test: the consumer's own
/// stricter-than-Java "Partition must not be negative." preconditions (M15/P13.2 D11 —
/// <c>ConsumerNegativePartitionGuardTests</c>), the admin RPCs' null-topic guards (D1), and the
/// core's own per-key / whole-request answer for the reassignment RPCs
/// (<c>PublicAdminNegativePartitionTests</c>).
/// </para>
/// <para>
/// A null topic (from the constructor, or from <c>default(TopicPartition)</c>) must behave as
/// Java's <c>Objects.equals</c> / <c>Objects.hashCode</c> / string concatenation make it: equal
/// to the other null-topic value with the same partition, hashable without a throw, and printed
/// as <c>"null-{partition}"</c> (D13) — yet <em>not</em> equal to a topic literally named
/// <c>"null"</c>, which prints the same.
/// </para>
/// </remarks>
public sealed class PublicTopicPartitionTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void NegativePartition_IsStoredAsGiven(int partition)
    {
        TopicPartition tp = new TopicPartition("t", partition);

        Assert.Equal("t", tp.Topic);
        Assert.Equal(partition, tp.Partition);

        // Java: topic + "-" + partition — the sign is kept, so -1 prints "t--1".
        Assert.Equal("t-" + partition.ToString(CultureInfo.InvariantCulture), tp.ToString());
    }

    [Fact]
    public void NegativePartition_Minus1_PrintsAsJavaDoes() =>
        Assert.Equal("t--1", new TopicPartition("t", -1).ToString());

    [Fact]
    public void NullTopic_IsStoredAsGiven_AndEqualsDefault()
    {
        TopicPartition constructed = new TopicPartition(null!, 0);

        Assert.Null(constructed.Topic);
        Assert.Equal(0, constructed.Partition);

        Assert.Equal(default(TopicPartition), constructed);
        Assert.True(constructed == default(TopicPartition));
        Assert.False(constructed != default(TopicPartition));
        Assert.True(constructed.Equals((object)default(TopicPartition)));
        Assert.Equal(default(TopicPartition).GetHashCode(), constructed.GetHashCode());

        // Usable as a set key without a throw — the dictionary/set use the type promises.
        HashSet<TopicPartition> set = new HashSet<TopicPartition> { constructed };
        Assert.Contains(default(TopicPartition), set);
    }

    [Fact]
    public void NullTopic_DiffersFromANonNullTopic_AndFromADifferentPartition()
    {
        TopicPartition nullTopic = new TopicPartition(null!, 5);

        // Objects.equals(null, "null") is false in Java: the same printed form, not equal.
        Assert.NotEqual(new TopicPartition("null", 5), nullTopic);
        Assert.NotEqual(nullTopic, new TopicPartition("null", 5));
        Assert.NotEqual(new TopicPartition(null!, 6), nullTopic);
        Assert.Equal(new TopicPartition(null!, 5), nullTopic);
    }

    [Fact]
    public void ToString_OfANullTopic_IsJavasConcatenation()
    {
        Assert.Equal("null-5", new TopicPartition(null!, 5).ToString());
        Assert.Equal("null-0", default(TopicPartition).ToString());
        Assert.Equal("null--1", new TopicPartition(null!, -1).ToString());
        Assert.Equal("orders-3", new TopicPartition("orders", 3).ToString());
    }

    /// <summary>
    /// D10: the annotation stays <c>string</c> on both the constructor's <c>topic</c> parameter
    /// and the <see cref="TopicPartition.Topic"/> property, although the constructor now accepts
    /// <see langword="null"/> at run time — every value the binding hands back has a non-null
    /// topic, because a null one cannot cross the C ABI (D1).
    /// </summary>
    [Fact]
    public void TheTopicAnnotation_StaysNonNullable()
    {
        ConstructorInfo ctor = Assert.Single(typeof(TopicPartition).GetConstructors());
        ParameterInfo[] parameters = ctor.GetParameters();
        Assert.Equal(2, parameters.Length);
        Assert.Equal("topic", parameters[0].Name);
        Assert.Equal(typeof(string), parameters[0].ParameterType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[0]));
        Assert.Equal("partition", parameters[1].Name);
        Assert.Equal(typeof(int), parameters[1].ParameterType);

        PropertyInfo topic = typeof(TopicPartition).GetProperty(nameof(TopicPartition.Topic))!;
        Assert.Equal(typeof(string), topic.PropertyType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(topic));
    }
}
