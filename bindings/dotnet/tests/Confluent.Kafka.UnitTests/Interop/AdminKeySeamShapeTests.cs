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
using System.Runtime.CompilerServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins the <b>shape</b> of the admin bridge's key seam (M15/P2a's G1) and value seam
/// (M15/P2b). These are reflection assertions because every property they protect is
/// invisible to a behavioural test: a key seam narrowed back to
/// <c>Func&lt;string, TKey&gt;</c> passes every RPC keyed by a single string; a value
/// channel widened back to nullable passes every RPC that has a value; and a
/// comparer-less constructor overload behaves identically for <c>string</c> keys —
/// <see cref="EqualityComparer{T}.Default"/> <em>is</em> ordinal for strings. Each would
/// be found only by the phase that cannot express its RPC any more, which is the churn
/// the seams were widened to prevent.
/// </summary>
public sealed class AdminKeySeamShapeTests
{
    /// <summary>
    /// The walker exposes <b>exactly two</b> <c>Complete</c> overloads, and <b>both</b>
    /// read the key from the <b>result handle and the index</b>, not from a string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The narrower <c>Func&lt;string, TKey&gt;</c> is the obvious seam and fits
    /// <c>createTopics</c>, <c>deleteTopics</c>, <c>describeTopics</c>,
    /// <c>createPartitions</c> and <c>listTopics</c> — every RPC keyed by a single
    /// <c>get_key(i)</c> string. It cannot fit <c>deleteRecords</c>:
    /// <c>kafka_admin_DeleteRecordsResult_t</c> declares <b>no</b> <c>get_key</c> at all
    /// (its accessors are <c>count</c> / <c>get_topic</c> / <c>get_partition</c> /
    /// <c>get_low_watermark</c> / <c>get_error</c> / <c>destroy</c>), so its key is
    /// composed from two accessors and there is no string to parse.
    /// </para>
    /// <para>
    /// ⚠ <b>The count is asserted, not sidestepped.</b> M15/P2b added the second overload
    /// and this assertion was <c>.Single(…)</c> before — the correct reaction is to pin
    /// <em>both</em>, never to relax the predicate to <c>.First(…)</c> or to filter down
    /// to the old signature, either of which would quietly stop protecting the seam that
    /// P2a paid a generality cost for.
    /// </para>
    /// </remarks>
    [Fact]
    public void EveryCompleteOverload_ReadsTheKeyFromTheResultHandleAndIndex_NotAString()
    {
        MethodInfo[] overloads = CompleteOverloads();

        Assert.Equal(2, overloads.Length);

        foreach (MethodInfo overload in overloads)
        {
            ParameterInfo reader = Assert.Single(
                overload.GetParameters(), parameter => parameter.Name == "readKey");

            Type[] typeArguments = reader.ParameterType.GetGenericArguments();

            Assert.Equal(typeof(Func<,,>), reader.ParameterType.GetGenericTypeDefinition());
            Assert.Equal(typeof(IntPtr), typeArguments[0]);
            Assert.Equal(typeof(int), typeArguments[1]);

            // The third argument is the method's own TKey, so it is an open generic
            // parameter rather than a closed type — which is what makes the seam key-type
            // agnostic.
            Assert.True(typeArguments[2].IsGenericParameter);
            Assert.Equal("TKey", typeArguments[2].Name);
        }
    }

    /// <summary>
    /// The <b>value</b> axis is symmetric with the key axis — a reader over
    /// <c>(result, index)</c> — and it is <b>not nullable</b>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This is the M15/P2b trap, pinned.</b> Before P2b the value channel was
    /// <c>Func&lt;IntPtr, TValue&gt;?</c> paired with a nullable <c>Accessors.GetValue</c>,
    /// and a <see langword="null"/> meant "result shape 2". An RPC whose per-key value is
    /// an <b>inline scalar</b> — <c>deleteRecords</c>' <c>int64_t get_low_watermark(i)</c>
    /// — cannot be named by a pointer-returning accessor, so the obvious way to describe
    /// it was to null the channel out; the walker would then have taken the shape-2 branch
    /// and <em>silently discarded the watermark</em>. No exception, no failing test, a
    /// wrong answer returned to the caller. Two properties make that unrepresentable and
    /// both are asserted here: the reader takes <c>(result, index)</c> so an inline scalar
    /// <em>is</em> expressible, and it is non-nullable so "no value" cannot be spelled on
    /// this overload at all.
    /// </remarks>
    [Fact]
    public void TheValueCarryingOverload_ReadsTheValueFromTheResultHandleAndIndex_AndIsNotNullable()
    {
        MethodInfo valueCarrying = ValueCarryingComplete();

        ParameterInfo reader = Assert.Single(
            valueCarrying.GetParameters(), parameter => parameter.Name == "readValue");

        Type[] typeArguments = reader.ParameterType.GetGenericArguments();

        Assert.Equal(typeof(Func<,,>), reader.ParameterType.GetGenericTypeDefinition());
        Assert.Equal(typeof(IntPtr), typeArguments[0]);
        Assert.Equal(typeof(int), typeArguments[1]);
        Assert.True(typeArguments[2].IsGenericParameter);
        Assert.Equal("TValue", typeArguments[2].Name);

        Assert.False(
            IsNullableAnnotated(reader),
            "a nullable value reader re-opens the silent-discard misuse M15/P2b removed");
    }

    /// <summary>
    /// The <b>shape-2</b> overload has no value channel <em>at all</em>, and accepts only
    /// a <see cref="VoidKeyedAdminOperation{TKey}"/>.
    /// </summary>
    /// <remarks>
    /// This is the other half of making the misuse unrepresentable. "This result has no
    /// per-key value" is stated by <em>which overload you call</em> rather than by nulling
    /// a parameter, and the operation parameter is narrowed so a value-carrying
    /// <see cref="KeyedAdminOperation{TKey, TValue}"/> cannot be routed down the
    /// value-dropping path in the first place — the compiler rejects it.
    /// </remarks>
    [Fact]
    public void TheVoidOverload_HasNoValueChannel_AndAcceptsOnlyTheVoidOperation()
    {
        MethodInfo voidShape = Assert.Single(
            CompleteOverloads(), method => method.GetGenericArguments().Length == 1);

        Assert.DoesNotContain(
            voidShape.GetParameters(),
            parameter => parameter.Name!.IndexOf("value", StringComparison.OrdinalIgnoreCase) >= 0);

        ParameterInfo operation = Assert.Single(
            voidShape.GetParameters(), parameter => parameter.Name == "operation");

        Assert.Equal(
            typeof(VoidKeyedAdminOperation<>),
            operation.ParameterType.GetGenericTypeDefinition());
    }

    /// <summary>
    /// The <b>shape-3</b> walker has no per-key <b>error</b> channel at all, because the
    /// ABI has no per-key error function to point it at.
    /// </summary>
    /// <remarks>
    /// ⚠ M15/P2b's plan proposed expressing shape 3 by making
    /// <c>Accessors.GetError</c> <see langword="null"/>-able, so that "GetError == null"
    /// meant "aggregate". That would have re-created, on the error axis, exactly the
    /// null-as-shape-discriminator hazard the phase existed to remove from the value axis
    /// — with a worse failure mode, since a missed null check on an error accessor is a
    /// null dereference. Stating the absence <em>structurally</em> (this method simply has
    /// no error parameter, and takes no <see cref="KeyedResultMarshal.Accessors"/> at all)
    /// delivers the same intent with nothing to get wrong.
    /// </remarks>
    [Fact]
    public void TheAggregateWalker_HasNoPerKeyErrorChannel()
    {
        MethodInfo aggregate = Assert.Single(
            typeof(KeyedResultMarshal).GetMethods(BindingFlags.NonPublic | BindingFlags.Static),
            method => method.Name == nameof(KeyedResultMarshal.CompleteAggregate));

        Assert.DoesNotContain(
            aggregate.GetParameters(),
            parameter => parameter.Name!.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0);

        // It does not even take the keyed accessor set, so there is no GetError reachable
        // through it either.
        Assert.DoesNotContain(
            aggregate.GetParameters(),
            parameter => parameter.ParameterType == typeof(KeyedResultMarshal.Accessors));

        ParameterInfo operation = Assert.Single(
            aggregate.GetParameters(), parameter => parameter.Name == "operation");

        Assert.Equal(
            typeof(SingleAdminOperation<>),
            operation.ParameterType.GetGenericTypeDefinition());
    }

    /// <summary>
    /// The walker's non-public static methods are exactly these five. Adding one is a
    /// deliberate act that must be recorded here.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This assertion exists to go RED when a method is added to the walker, and the
    /// correct reaction is to extend it.</b> M15/P2b learned the shape of that lesson when
    /// its second <c>Complete</c> overload broke
    /// <see cref="EveryCompleteOverload_ReadsTheKeyFromTheResultHandleAndIndex_NotAString"/>'s
    /// <c>Single(…)</c>; the fix there was to pin <em>both</em> overloads rather than relax
    /// the predicate. M15/P3 added <see cref="KeyedResultMarshal.CompleteList"/>, which the
    /// earlier assertions' name filters (<c>== nameof(Complete)</c>,
    /// <c>== nameof(CompleteAggregate)</c>) did not match, so it landed without turning any
    /// of them red — this set-equality is the reaction to that.
    /// <para>
    /// ⚠ <b>The set is selected by <see cref="BindingFlags"/> alone — never by a name
    /// predicate — and that is the whole point (M15/P3 round 2, finding 69.4).</b> An
    /// earlier version of this test filtered on <c>StartsWith("Complete")</c>, which
    /// reproduced the very defect the paragraph above describes, one width wider: a
    /// callable named anything else (measured with <c>WalkSomething</c>) landed green,
    /// while the <c>CompleteSomething</c> control went red. A name filter can only pin the
    /// names it already knows, so the next addition escapes it exactly when it is least
    /// expected to.
    /// </para>
    /// <para>
    /// <see cref="CompilerGeneratedAttribute"/> is excluded so a future compiler-emitted
    /// static cannot turn this red for no reason. Measured today: none of the five is
    /// compiler-generated, and the lambda display class is a <em>nested type</em>, which
    /// <see cref="Type.GetMethods(BindingFlags)"/> on the containing type never returns.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheWalker_ExposesExactlyTheKnownCallables()
    {
        string[] callables = typeof(KeyedResultMarshal)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                nameof(KeyedResultMarshal.Complete),
                nameof(KeyedResultMarshal.Complete),
                nameof(KeyedResultMarshal.CompleteAggregate),
                nameof(KeyedResultMarshal.CompleteList),
                nameof(KeyedResultMarshal.ReadStringKey),
            },
            callables);
    }

    /// <summary>
    /// The <b>sub-shape-3b</b> walker takes <b>no accessor set, no key reader and no error
    /// channel</b>, and produces a <em>collection</em> rather than a map.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This is M15/P3's D14, pinned structurally.</b> The tempting alternative was to
    /// describe "no key" and "no error" by nulling fields on
    /// <see cref="KeyedResultMarshal.Accessors"/> — which is precisely the
    /// null-as-shape-discriminator hazard P2b removed from the value axis and
    /// <see cref="TheAggregateWalker_HasNoPerKeyErrorChannel"/> already forbids on the
    /// error axis. Giving the shape its own callable states the same intent with nothing to
    /// get wrong, and leaves <see cref="KeyedResultMarshal.Accessors"/> untouched —
    /// which <see cref="Accessors_CarryNeitherAKeyNorAValueAccessor"/> re-asserts.
    /// </para>
    /// <para>
    /// The collection-vs-map distinction is asserted because it is the thing Java's
    /// signature decides: <c>KafkaFuture&lt;Collection&lt;ConfigResource&gt;&gt;</c>
    /// (<c>ListConfigResourcesResult.java:42</c>), not a map. Routing these RPCs through
    /// <see cref="KeyedResultMarshal.CompleteAggregate{TKey, TValue}"/> would have forced an
    /// invented key onto the public surface.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheListWalker_HasNoAccessorSet_NoKeyReader_AndNoErrorChannel()
    {
        MethodInfo list = Assert.Single(
            typeof(KeyedResultMarshal).GetMethods(BindingFlags.NonPublic | BindingFlags.Static),
            method => method.Name == nameof(KeyedResultMarshal.CompleteList));

        ParameterInfo[] parameters = list.GetParameters();

        // No accessor set at all — so there is no GetError reachable through it either.
        Assert.DoesNotContain(parameters, parameter => parameter.ParameterType == typeof(KeyedResultMarshal.Accessors));
        Assert.DoesNotContain(
            parameters,
            parameter => parameter.Name!.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0);

        // And no key reader: these results have no key, composite or otherwise.
        Assert.DoesNotContain(
            parameters,
            parameter => parameter.Name!.IndexOf("key", StringComparison.OrdinalIgnoreCase) >= 0);

        // It takes the count accessor directly, in place of the set.
        Assert.Single(parameters, parameter => parameter.ParameterType == typeof(KeyedResultMarshal.CountAccessor));

        // The value reader is the same (result, index) shape as the keyed overloads', and
        // is NOT nullable — "no value" is not spellable on this callable either.
        ParameterInfo reader = Assert.Single(parameters, parameter => parameter.Name == "readValue");
        Type[] typeArguments = reader.ParameterType.GetGenericArguments();
        Assert.Equal(typeof(Func<,,>), reader.ParameterType.GetGenericTypeDefinition());
        Assert.Equal(typeof(IntPtr), typeArguments[0]);
        Assert.Equal(typeof(int), typeArguments[1]);
        Assert.True(typeArguments[2].IsGenericParameter);
        Assert.Equal("TValue", typeArguments[2].Name);
        Assert.False(
            IsNullableAnnotated(reader),
            "a nullable value reader re-opens the silent-discard misuse M15/P2b removed");

        // The awaiter is the single-completion bridge, carrying a COLLECTION — not the
        // dictionary CompleteAggregate builds.
        ParameterInfo operation = Assert.Single(parameters, parameter => parameter.Name == "operation");
        Assert.Equal(typeof(SingleAdminOperation<>), operation.ParameterType.GetGenericTypeDefinition());
        Assert.Equal(
            typeof(IReadOnlyCollection<>),
            operation.ParameterType.GetGenericArguments()[0].GetGenericTypeDefinition());
    }

    /// <summary>
    /// Neither <c>get_key</c> nor <c>get_value</c> is part of the shared accessor set,
    /// because neither is universal — see
    /// <see cref="EveryCompleteOverload_ReadsTheKeyFromTheResultHandleAndIndex_NotAString"/>
    /// and
    /// <see cref="TheValueCarryingOverload_ReadsTheValueFromTheResultHandleAndIndex_AndIsNotNullable"/>.
    /// Keeping either there would give a result two places that fact could come from, and
    /// only one of them could be right.
    /// </summary>
    [Fact]
    public void Accessors_CarryNeitherAKeyNorAValueAccessor()
    {
        PropertyInfo[] properties =
            typeof(KeyedResultMarshal.Accessors).GetProperties(BindingFlags.NonPublic | BindingFlags.Instance);

        Assert.DoesNotContain(properties, property => property.Name.Contains("Key", StringComparison.Ordinal));
        Assert.DoesNotContain(properties, property => property.Name.Contains("Value", StringComparison.Ordinal));

        // ⚠ M15/P3's D14, restated as a count: the set is exactly `count` + `getError` and
        // gained NOTHING when sub-shape 3b landed. A phase describing a new shape by adding
        // a nullable field here — a null key reader, a null error channel — is the defect
        // this whole file exists to make impossible, so the surface is pinned as a set
        // rather than only checked for two forbidden names.
        Assert.Equal(
            new[] { "Count", "GetError" },
            properties.Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal));

        ConstructorInfo only = Assert.Single(
            typeof(KeyedResultMarshal.Accessors)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));

        Assert.Equal(
            new[] { "count", "getError" },
            only.GetParameters().Select(parameter => parameter.Name));

        // And the one accessor that remains is required, not a nullable discriminator.
        Assert.False(
            IsNullableAnnotated(only.GetParameters()[1]),
            "a nullable getError would re-introduce null-as-shape-discriminator on the error axis");
    }

    /// <summary>
    /// The key comparer is a <b>required</b> constructor argument on both keyed
    /// operations, so neither can silently fall back to
    /// <see cref="EqualityComparer{T}.Default"/>. P1 chose
    /// <see cref="StringComparer.Ordinal"/> deliberately so a result's public views and
    /// its per-key sources could not disagree about key identity; threading it explicitly
    /// is what keeps that choice attached to the key type rather than to the language's
    /// default.
    /// </summary>
    [Theory]
    [InlineData(typeof(KeyedAdminOperation<,>))]
    [InlineData(typeof(VoidKeyedAdminOperation<>))]
    public void KeyedOperations_RequireAnExplicitComparer(Type operationType)
    {
        ConstructorInfo only = Assert.Single(
            operationType.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));

        ParameterInfo comparer = Assert.Single(
            only.GetParameters(), parameter => parameter.Name == "keyComparer");

        Assert.Equal(typeof(IEqualityComparer<>), comparer.ParameterType.GetGenericTypeDefinition());
        Assert.False(comparer.IsOptional, "a defaulted comparer is the fallback this test exists to forbid");
    }

    /// <summary>
    /// Every RPC's key and value reader is a <c>static readonly</c> field, so walking a
    /// result allocates no delegates — and, more importantly here, so a test driving the
    /// walker uses <b>production's own</b> readers (<c>definition-of-done.md</c> §12)
    /// rather than a look-alike that could keep passing after production changed.
    /// </summary>
    [Theory]
    [InlineData(nameof(AdminCallbacks.CreateTopicsKey))]
    [InlineData(nameof(AdminCallbacks.TopicMetadataAndConfigValue))]
    [InlineData(nameof(AdminCallbacks.DeleteTopicsNameKey))]
    [InlineData(nameof(AdminCallbacks.DeleteTopicsIdKey))]
    [InlineData(nameof(AdminCallbacks.DescribeTopicsNameKey))]
    [InlineData(nameof(AdminCallbacks.DescribeTopicsIdKey))]
    [InlineData(nameof(AdminCallbacks.TopicDescriptionValue))]
    [InlineData(nameof(AdminCallbacks.ListTopicsKey))]
    [InlineData(nameof(AdminCallbacks.TopicListingValue))]
    [InlineData(nameof(AdminCallbacks.CreatePartitionsKey))]
    [InlineData(nameof(AdminCallbacks.DeleteRecordsKey))]
    [InlineData(nameof(AdminCallbacks.DeletedRecordsValue))]
    [InlineData(nameof(AdminCallbacks.ConfigResourceValue))]
    [InlineData(nameof(AdminCallbacks.ClientMetricsResourceListingValue))]
    public void EveryReader_IsAHoistedStaticReadonlyField(string fieldName)
    {
        FieldInfo field = Assert.IsAssignableFrom<FieldInfo>(
            typeof(AdminCallbacks).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static));

        Assert.True(field.IsInitOnly, $"{fieldName} must be readonly");
        Assert.Equal(typeof(Func<,,>), field.FieldType.GetGenericTypeDefinition());
        Assert.Equal(typeof(IntPtr), field.FieldType.GetGenericArguments()[0]);
        Assert.Equal(typeof(int), field.FieldType.GetGenericArguments()[1]);
        Assert.NotNull(field.GetValue(null));
    }

    private static MethodInfo[] CompleteOverloads() =>
        typeof(KeyedResultMarshal)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Name == nameof(KeyedResultMarshal.Complete))
            .ToArray();

    private static MethodInfo ValueCarryingComplete() =>
        Assert.Single(CompleteOverloads(), method => method.GetGenericArguments().Length == 2);

    /// <summary>
    /// Whether a parameter's own type is annotated <c>?</c> under <c>#nullable enable</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b><see cref="ParameterInfo.ParameterType"/> cannot see this.</b> Nullability of a
    /// reference type is erased from the CLR type and survives only as the compiler-emitted
    /// <c>NullableAttribute</c> / <c>NullableContextAttribute</c> pair, so a test that
    /// compares <see cref="Type"/>s alone would pass equally against
    /// <c>Func&lt;IntPtr, int, TValue&gt;</c> and <c>Func&lt;IntPtr, int, TValue&gt;?</c>.
    /// <see cref="System.Reflection.NullabilityInfoContext"/> would do this for us but
    /// post-dates the net462 leg of the TFM matrix, so the flags are decoded by hand — by
    /// <see cref="NullableAnnotation"/>, the test project's single decoder, where byte
    /// <c>2</c> means "annotated" (nullable) and a parameter with no attribute of its own
    /// resolves against the nearest enclosing <c>NullableContextAttribute</c>.
    /// </remarks>
    private static bool IsNullableAnnotated(ParameterInfo parameter) =>
        NullableAnnotation.Flag(parameter) == NullableAnnotation.Annotated;
}
