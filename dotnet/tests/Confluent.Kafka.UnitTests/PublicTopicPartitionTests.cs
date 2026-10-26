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
/// Tests the public <see cref="TopicPartition"/> value type, whose constructor stores whatever it is given, as Java's
/// does: a negative partition and a <see langword="null"/> topic in equality, hash and string form.
/// </summary>
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

        HashSet<TopicPartition> set = new HashSet<TopicPartition> { constructed };
        Assert.Contains(default(TopicPartition), set);
    }

    [Fact]
    public void NullTopic_DiffersFromANonNullTopic_AndFromADifferentPartition()
    {
        TopicPartition nullTopic = new TopicPartition(null!, 5);

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
