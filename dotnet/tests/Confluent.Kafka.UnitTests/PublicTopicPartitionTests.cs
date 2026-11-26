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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="TopicPartition"/> class, whose constructor stores whatever it is given, as Java's
/// does: a negative partition and a <see langword="null"/> topic in equality, hash and string form. Java's
/// <c>TopicPartitionTest</c> is not translated: both of its tests check Java serialization, which this class does
/// not support. <c>testSerializationRoundtrip</c> round-trips through Java's <c>ObjectOutputStream</c>: the class is
/// not <c>[Serializable]</c>, and .NET 9 removed <c>BinaryFormatter</c>; Python's <c>pickle</c> round trip has no
/// .NET peer. <c>testTopiPartitionSerializationCompatibility</c> reads a checked-in Java-serialized file, which
/// Python does not translate either.
/// </summary>
public sealed class PublicTopicPartitionTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void NegativePartition_IsStoredAsGiven(int partition)
    {
        TopicPartition tp = new TopicPartition("t", partition);

        Assert.Equal("t", tp.Topic());
        Assert.Equal(partition, tp.Partition());

        Assert.Equal("t-" + partition.ToString(CultureInfo.InvariantCulture), tp.ToString());
    }

    [Fact]
    public void NegativePartition_Minus1_PrintsAsJavaDoes() =>
        Assert.Equal("t--1", new TopicPartition("t", -1).ToString());

    [Fact]
    public void NullTopic_IsStoredAsGiven_AndEqualsAnotherNullTopic()
    {
        TopicPartition constructed = new TopicPartition(null!, 0);
        TopicPartition other = new TopicPartition(null!, 0);

        Assert.Null(constructed.Topic());
        Assert.Equal(0, constructed.Partition());

        Assert.NotSame(other, constructed);
        Assert.Equal(other, constructed);
        Assert.True(constructed.Equals(other));
        Assert.True(constructed.Equals((object)other));
        Assert.Equal(other.GetHashCode(), constructed.GetHashCode());

        HashSet<TopicPartition> set = new HashSet<TopicPartition> { constructed };
        Assert.Contains(other, set);
    }

    [Fact]
    public void Default_IsNull() => Assert.Null(default(TopicPartition));

    [Fact]
    public void EqualityWithNull_OnEitherSide_IsFalse_AndTwoNullsAreEqual()
    {
        TopicPartition tp = new TopicPartition("t", 0);
        TopicPartition? none = null;

        Assert.False(tp.Equals(none));
        Assert.False(tp.Equals((object?)null));
        Assert.False(Equals(tp, none));
        Assert.False(Equals(none, tp));
        Assert.False(EqualityComparer<TopicPartition?>.Default.Equals(none, tp));
        Assert.False(EqualityComparer<TopicPartition?>.Default.Equals(tp, none));
        Assert.True(EqualityComparer<TopicPartition?>.Default.Equals(none, none));
    }

    [Fact]
    public void Equals_ComparesValues_NotReferences()
    {
        TopicPartition tp = new TopicPartition("t", 1);

        Assert.True(tp.Equals(tp));
        Assert.True(tp.Equals(new TopicPartition("t", 1)));
        Assert.True(tp.Equals((object)new TopicPartition("t", 1)));
        Assert.False(tp.Equals(new TopicPartition("t", 2)));
        Assert.False(tp.Equals(new TopicPartition("u", 1)));
        Assert.False(tp.Equals((object)"t-1"));
    }

    [Fact]
    public void TheEqualityOperators_CompareReferences_AsJavas()
    {
        TopicPartition tp = new TopicPartition("t", 1);
        TopicPartition equal = new TopicPartition("t", 1);
        TopicPartition alias = tp;

        Assert.True(tp.Equals(equal));
        Assert.False(tp == equal);
        Assert.True(tp != equal);
        Assert.True(tp == alias);
        Assert.False(tp != alias);
    }

    [Theory]
    [InlineData("t", 0)]
    [InlineData("orders", 7)]
    [InlineData(null, 0)]
    [InlineData(null, 5)]
    public void HashCode_IsStableAcrossCalls_AndEqualForEqualValues(string? topic, int partition)
    {
        TopicPartition tp = new TopicPartition(topic!, partition);
        int first = tp.GetHashCode();

        Assert.Equal(first, tp.GetHashCode());
        Assert.Equal(first, tp.GetHashCode());
        Assert.Equal(first, new TopicPartition(topic!, partition).GetHashCode());
    }

    [Fact]
    public void Shape_IsASealedClassWithOneConstructor()
    {
        Type type = typeof(TopicPartition);

        Assert.True(type.IsClass);
        Assert.True(type.IsSealed);
        Assert.Equal(typeof(object), type.BaseType);
        Assert.Contains(typeof(IEquatable<TopicPartition>), type.GetInterfaces());
        ConstructorInfo ctor = Assert.Single(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.Equal(new[] { typeof(string), typeof(int) }, Array.ConvertAll(ctor.GetParameters(), p => p.ParameterType));
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
        Assert.Equal("null-0", new TopicPartition(null!, 0).ToString());
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

        MethodInfo topic = typeof(TopicPartition).GetMethod(nameof(TopicPartition.Topic), Type.EmptyTypes)!;
        Assert.Equal(typeof(string), topic.ReturnType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(topic));
    }
}
