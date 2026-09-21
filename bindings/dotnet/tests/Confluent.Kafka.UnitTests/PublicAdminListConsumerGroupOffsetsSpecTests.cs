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
/// Pins <see cref="ListConsumerGroupOffsetsSpec"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The unset selection is the point of this type.</b> Java's uninitialized field
/// (<c>:30</c>) means "every partition the group has committed offsets for" (<c>:34</c>,
/// <c>:46</c>), which an explicitly empty collection does <em>not</em>. The C ABI carries
/// the distinction as a per-group <c>all_partitions</c> boolean, so the tests below assert
/// the two states stay apart in the property, in equality, in the hash and in the
/// rendering — a later submit slice that collapsed them would otherwise go green.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring, the result type and the client method
/// arrive in later slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupOffsetsSpecTests
{
    private static readonly TopicPartition s_ordersZero = new TopicPartition("orders", 0);
    private static readonly TopicPartition s_ordersOne = new TopicPartition("orders", 1);
    private static readonly TopicPartition s_payments = new TopicPartition("payments", 0);

    /// <summary>
    /// A freshly constructed spec matches Java's uninitialized field (<c>:30</c>): the
    /// selection is unset, meaning every partition the group has committed offsets for.
    /// </summary>
    [Fact]
    public void Default_IsTheUnsetSelection_NotAnEmptyOne()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();

        Assert.Null(spec.TopicPartitions);
    }

    /// <summary>
    /// The public property set is exactly Java's one accessor pair collapsed into one
    /// property — no member dropped, none invented, and no factory or constructor Java does
    /// not have.
    /// </summary>
    [Fact]
    public void PublicShape_IsJavasOneAccessorAsASettableNullableProperty()
    {
        Assert.Equal(
            new[] { "TopicPartitions" },
            typeof(ListConsumerGroupOffsetsSpec)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java declares no static members on this class.
        Assert.Empty(
            typeof(ListConsumerGroupOffsetsSpec)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        // Java declares no constructor either, so the only one is the implicit default.
        ConstructorInfo constructor = Assert.Single(typeof(ListConsumerGroupOffsetsSpec).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        // Java's fluent setter is a setter, so the property is settable; its type must stay
        // nullable, because null is the "all partitions" request.
        PropertyInfo topicPartitions = typeof(ListConsumerGroupOffsetsSpec).GetProperty(
            nameof(ListConsumerGroupOffsetsSpec.TopicPartitions))!;
        Assert.Equal(typeof(IReadOnlyCollection<TopicPartition>), topicPartitions.PropertyType);
        Assert.NotNull(topicPartitions.SetMethod);
        Assert.NotNull(topicPartitions.GetMethod);
    }

    /// <summary>
    /// Java's setter stores what it is handed and its getter reports it back — including
    /// back to <see langword="null"/>, which a write-once field would fail and which is the
    /// only way to ask for every partition again after narrowing the selection.
    /// </summary>
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

    /// <summary>
    /// The selection keeps duplicates and keeps the order it was given, because Java's
    /// field is a <c>Collection</c> (<c>:30</c>) and not a <c>Set</c> — a spec that
    /// deduplicated would be answering a different question than the caller asked.
    /// </summary>
    [Fact]
    public void TopicPartitions_KeepsDuplicatesAndOrder_BecauseJavaHoldsACollectionNotASet()
    {
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec
        {
            TopicPartitions = new[] { s_ordersOne, s_ordersZero, s_ordersOne },
        };

        Assert.Equal(new[] { s_ordersOne, s_ordersZero, s_ordersOne }, spec.TopicPartitions);
    }

    /// <summary>
    /// Assigning copies, so a caller that keeps mutating the collection it passed in cannot
    /// change a spec that has already been built — including its equality and its
    /// rendering, which a later submit slice and a caller's own map lookups both depend on.
    /// </summary>
    /// <remarks>
    /// This is the one deliberate deviation from Java, which stores the reference
    /// (<c>:40</c>); see the type's remarks for why a read-only view is not enough here.
    /// </remarks>
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

    /// <summary>
    /// Equality is element-by-element and order-sensitive, matching the <c>List</c>
    /// behaviour Java's <c>Objects.equals</c> (<c>:61</c>) delegates to for the collection
    /// type the setter's javadoc names (<c>:36</c>).
    /// </summary>
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

    /// <summary>
    /// Equality rejects null and unrelated types, as Java's <c>instanceof</c> guard
    /// (<c>:57</c>) does.
    /// </summary>
    [Fact]
    public void Equals_RejectsNullAndForeignTypes()
    {
        ListConsumerGroupOffsetsSpec spec = Spec(s_ordersZero);

        Assert.False(spec.Equals(null));
        Assert.False(spec.Equals("ListConsumerGroupOffsetsSpec(topicPartitions=[orders-0])"));
        Assert.False(spec.Equals(new DescribeClassicGroupsOptions()));
    }

    /// <summary>
    /// The unset selection equals only another unset selection — never an empty one. This
    /// is the assertion that keeps Java's "all partitions" (<c>:34</c>) from collapsing into
    /// "no partitions" once the submit slice starts reading the property.
    /// </summary>
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

    /// <summary>
    /// The hash is an order-sensitive fold, so it agrees with
    /// <see cref="ListConsumerGroupOffsetsSpec.Equals(object)"/>: reordering the selection
    /// is a different spec and hashes differently.
    /// </summary>
    [Fact]
    public void GetHashCode_IsOrderSensitive_LikeEquals()
    {
        Assert.NotEqual(Spec(s_ordersZero, s_ordersOne).GetHashCode(), Spec(s_ordersOne, s_ordersZero).GetHashCode());
        Assert.Equal(Spec(s_ordersZero).GetHashCode(), Spec(s_ordersZero).GetHashCode());
    }

    /// <summary>
    /// The rendering matches Java's <c>toString()</c> (<c>:70</c>), whose bare
    /// concatenation prints an unset collection as <c>null</c> and a set one in
    /// <c>AbstractCollection</c> form.
    /// </summary>
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

    /// <summary>
    /// Java carries no <c>@Deprecated</c> on this class or its accessors, so nothing here
    /// carries <see cref="ObsoleteAttribute"/> — asserted rather than left implicit, because
    /// an attribute nothing asserts is one a later slice can add or drop unnoticed.
    /// </summary>
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
