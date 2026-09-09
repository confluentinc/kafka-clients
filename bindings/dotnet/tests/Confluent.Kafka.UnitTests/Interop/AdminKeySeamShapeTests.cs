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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins the <b>shape</b> of the per-key admin bridge's key seam (M15/P2a's G1). These are
/// reflection assertions because both properties they protect are invisible to every
/// behavioural test: a key seam narrowed back to <c>Func&lt;string, TKey&gt;</c> passes
/// every RPC bound so far, and a comparer-less constructor overload behaves identically
/// for <c>string</c> keys — <see cref="EqualityComparer{T}.Default"/> <em>is</em> ordinal
/// for strings. Both would be found only by the phase that cannot express its RPC any
/// more, which is the churn the seam was widened to prevent.
/// </summary>
public sealed class AdminKeySeamShapeTests
{
    /// <summary>
    /// The key reader takes the <b>result handle and the index</b>, not a string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The narrower <c>Func&lt;string, TKey&gt;</c> is the obvious seam and fits
    /// <c>createTopics</c>, <c>deleteTopics</c> and <c>describeTopics</c> — every RPC
    /// keyed by a single <c>get_key(i)</c> string. It cannot fit <c>deleteRecords</c>:
    /// <c>kafka_admin_DeleteRecordsResult_t</c> declares <b>no</b> <c>get_key</c> at all
    /// (its accessors are <c>count</c> / <c>get_topic</c> / <c>get_partition</c> /
    /// <c>get_low_watermark</c> / <c>get_error</c> / <c>destroy</c>), so its key is
    /// composed from two accessors and there is no string to parse.
    /// </para>
    /// </remarks>
    [Fact]
    public void KeyReader_TakesTheResultHandleAndIndex_NotAString()
    {
        MethodInfo complete = typeof(KeyedResultMarshal)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(method => method.Name == nameof(KeyedResultMarshal.Complete));

        ParameterInfo reader = complete.GetParameters()
            .Single(parameter => parameter.Name == "readKey");

        Type[] typeArguments = reader.ParameterType.GetGenericArguments();

        Assert.Equal(typeof(Func<,,>), reader.ParameterType.GetGenericTypeDefinition());
        Assert.Equal(typeof(IntPtr), typeArguments[0]);
        Assert.Equal(typeof(int), typeArguments[1]);

        // The third argument is the method's own TKey, so it is an open generic parameter
        // rather than a closed type — which is what makes the seam key-type agnostic.
        Assert.True(typeArguments[2].IsGenericParameter);
        Assert.Equal("TKey", typeArguments[2].Name);
    }

    /// <summary>
    /// <c>get_key</c> is <b>not</b> part of the shared accessor set, because it is not
    /// universal — see <see cref="KeyReader_TakesTheResultHandleAndIndex_NotAString"/>.
    /// Keeping it there beside the key reader would give a result two places a key could
    /// come from, and only one of them could be right.
    /// </summary>
    [Fact]
    public void Accessors_CarryNoKeyAccessor()
    {
        Assert.DoesNotContain(
            typeof(KeyedResultMarshal.Accessors).GetProperties(BindingFlags.NonPublic | BindingFlags.Instance),
            property => property.Name.Contains("Key", StringComparison.Ordinal));

        ConstructorInfo only = Assert.Single(
            typeof(KeyedResultMarshal.Accessors)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));

        Assert.Equal(
            new[] { "count", "getError", "getValue" },
            only.GetParameters().Select(parameter => parameter.Name));
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
}
