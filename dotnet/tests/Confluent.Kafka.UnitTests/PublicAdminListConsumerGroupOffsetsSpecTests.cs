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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ListConsumerGroupOffsetsSpec"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec</c>: the unset selection (every partition the group
/// committed) stays apart from an empty one in the property, equality, the hash and the rendering.
/// </summary>
public sealed class PublicAdminListConsumerGroupOffsetsSpecTests
{
    private static readonly TopicPartition s_ordersZero = new TopicPartition("orders", 0);
    private static readonly TopicPartition s_ordersOne = new TopicPartition("orders", 1);
    private static readonly TopicPartition s_payments = new TopicPartition("payments", 0);

    [Fact]
    public void Default_IsTheUnsetSelection_NotAnEmptyOne()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();

        Assert.Null(spec.TopicPartitions);
    }

    [Fact]
    public void PublicShape_IsJavasOneAccessorAsASettableNullableProperty()
    {
        Assert.Equal(
            new[] { "TopicPartitions" },
            typeof(ListConsumerGroupOffsetsSpec)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Empty(
            typeof(ListConsumerGroupOffsetsSpec)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        ConstructorInfo constructor = Assert.Single(typeof(ListConsumerGroupOffsetsSpec).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        // The property stays nullable: null is the request for every partition.
        PropertyInfo topicPartitions = typeof(ListConsumerGroupOffsetsSpec).GetProperty(
            nameof(ListConsumerGroupOffsetsSpec.TopicPartitions))!;
        Assert.Equal(typeof(IReadOnlyCollection<TopicPartition>), topicPartitions.PropertyType);
        Assert.NotNull(topicPartitions.SetMethod);
        Assert.NotNull(topicPartitions.GetMethod);
    }

    [Fact]
    public void TopicPartitions_RoundTripsIncludingBackToNull()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec
        {
            TopicPartitions = new[] { s_ordersZero, s_payments },
        };

        Assert.Equal(new[] { s_ordersZero, s_payments }, spec.TopicPartitions);

        spec.TopicPartitions = Array.Empty<TopicPartition>();
        Assert.NotNull(spec.TopicPartitions);
        Assert.Empty(spec.TopicPartitions!);

        spec.TopicPartitions = null;
        Assert.Null(spec.TopicPartitions);
    }

    [Fact]
    public void TopicPartitions_KeepsDuplicatesAndOrder_BecauseJavaHoldsACollectionNotASet()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec
        {
            TopicPartitions = new[] { s_ordersOne, s_ordersZero, s_ordersOne },
        };

        Assert.Equal(new[] { s_ordersOne, s_ordersZero, s_ordersOne }, spec.TopicPartitions);
    }

    [Fact]
    public void TopicPartitions_IsCopiedOnAssignment_NotAliased()
    {
        List<TopicPartition> source = new List<TopicPartition> { s_ordersZero };
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec { TopicPartitions = source };

        Assert.NotSame(source, spec.TopicPartitions);

        int hashBefore = spec.GetHashCode();
        source.Add(s_payments);

        Assert.Equal(new[] { s_ordersZero }, spec.TopicPartitions);
        Assert.Equal(hashBefore, spec.GetHashCode());
        Assert.Equal("ListConsumerGroupOffsetsSpec(topicPartitions=[orders-0])", spec.ToString());
    }

    [Fact]
    public void Equals_ComparesElementsInOrder()
    {
        ListConsumerGroupOffsetsSpec spec = Spec(s_ordersZero, s_ordersOne);

        Assert.True(spec.Equals(spec));
        Assert.True(spec.Equals(Spec(s_ordersZero, s_ordersOne)));
        Assert.Equal(spec.GetHashCode(), Spec(s_ordersZero, s_ordersOne).GetHashCode());

        Assert.False(spec.Equals(Spec(s_ordersOne, s_ordersZero)));
        Assert.False(spec.Equals(Spec(s_ordersZero)));
        Assert.False(spec.Equals(Spec(s_ordersZero, s_ordersOne, s_payments)));
        Assert.False(spec.Equals(Spec(s_ordersZero, s_payments)));
    }

    [Fact]
    public void Equals_RejectsNullAndForeignTypes()
    {
        ListConsumerGroupOffsetsSpec spec = Spec(s_ordersZero);

        Assert.False(spec.Equals(null));
        Assert.False(spec.Equals("ListConsumerGroupOffsetsSpec(topicPartitions=[orders-0])"));
        Assert.False(spec.Equals(new DescribeClassicGroupsOptions()));
    }

    [Fact]
    public void UnsetSelection_IsNeverEqualToAnEmptyOne()
    {
        ListConsumerGroupOffsetsSpec unset = new ListConsumerGroupOffsetsSpec();
        ListConsumerGroupOffsetsSpec empty = Spec();

        Assert.True(unset.Equals(new ListConsumerGroupOffsetsSpec()));
        Assert.Equal(unset.GetHashCode(), new ListConsumerGroupOffsetsSpec().GetHashCode());

        Assert.False(unset.Equals(empty));
        Assert.False(empty.Equals(unset));
        Assert.NotEqual(unset.GetHashCode(), empty.GetHashCode());
    }

    [Fact]
    public void GetHashCode_IsOrderSensitive_LikeEquals()
    {
        Assert.NotEqual(Spec(s_ordersZero, s_ordersOne).GetHashCode(), Spec(s_ordersOne, s_ordersZero).GetHashCode());
        Assert.Equal(Spec(s_ordersZero).GetHashCode(), Spec(s_ordersZero).GetHashCode());
    }

    [Fact]
    public void ToString_RendersLikeJava_AndShowsUnsetApartFromEmpty()
    {
        Assert.Equal(
            "ListConsumerGroupOffsetsSpec(topicPartitions=null)",
            new ListConsumerGroupOffsetsSpec().ToString());

        Assert.Equal("ListConsumerGroupOffsetsSpec(topicPartitions=[])", Spec().ToString());

        Assert.Equal(
            "ListConsumerGroupOffsetsSpec(topicPartitions=[orders-0, payments-0])",
            Spec(s_ordersZero, s_payments).ToString());
    }

    [Fact]
    public void NothingIsDeprecated_BecauseJavaDeprecatesNothingHere()
    {
        Assert.Null(typeof(ListConsumerGroupOffsetsSpec).GetCustomAttribute<ObsoleteAttribute>());

        foreach (PropertyInfo property in typeof(ListConsumerGroupOffsetsSpec).GetProperties())
        {
            Assert.Null(property.GetCustomAttribute<ObsoleteAttribute>());
        }
    }

    private static ListConsumerGroupOffsetsSpec Spec(params TopicPartition[] topicPartitions) =>
        new ListConsumerGroupOffsetsSpec { TopicPartitions = topicPartitions };
}
