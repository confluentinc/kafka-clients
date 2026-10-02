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
using System.Reflection;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins <see cref="NullableAnnotation"/>'s own two silent-misbinding guards, because
/// every other assertion in the suite reaches it through
/// <see cref="PropertyInfo"/> / <see cref="ParameterInfo"/> and so exercises neither.
/// </summary>
/// <remarks>
/// This file exists for the same reason the decoder's context walk does: an assertion
/// nothing can falsify is worth less than no assertion, because it reads as coverage.
/// A <see cref="MethodInfo"/> and a <see cref="Type"/> both bind to the
/// <c>Flag(MemberInfo)</c> overload with no compiler diagnostic, so if that overload
/// mishandles either, the mistake is invisible at the call site — which is precisely how
/// the defect these tests guard reached review.
/// </remarks>
public class NullableAnnotationTests
{
    /// <summary>
    /// A <see cref="MethodInfo"/> must resolve to its <b>return's</b> annotation, matching
    /// the <see cref="ParameterInfo"/> sibling that call sites use today.
    /// </summary>
    /// <remarks>
    /// The nullable leg is the load-bearing one. Reading the method's own attributes and
    /// falling back to <see cref="MemberInfo.DeclaringType"/> — skipping the method's own
    /// <c>NullableContextAttribute</c> scope — yields <see cref="NullableAnnotation.NotAnnotated"/>
    /// for <c>AllTopicNames</c>, so this assertion fails on the unrouted decoder with no
    /// source change needed anywhere else. The non-nullable leg holds it honest in the
    /// other direction: a decoder hardwired to <see cref="NullableAnnotation.Annotated"/>
    /// would pass the first assertion and fail this one.
    /// </remarks>
    [Fact]
    public void Flag_OnAMethodInfo_ReadsTheReturnsAnnotation_NotTheDeclaringTypesContext()
    {
        MethodInfo nullableReturn = typeof(DescribeTopicsResult).GetMethod(
            nameof(DescribeTopicsResult.AllTopicNames), Type.EmptyTypes)!;
        MethodInfo nonNullableReturn = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.NamesToListings), Type.EmptyTypes)!;

        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag((MemberInfo)nullableReturn));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag((MemberInfo)nonNullableReturn));

        // The two overloads must not disagree: the ParameterInfo form is what every
        // shipped call site uses, so it is the reference the MemberInfo form answers to.
        Assert.Equal(
            NullableAnnotation.Flag(nullableReturn.ReturnParameter),
            NullableAnnotation.Flag((MemberInfo)nullableReturn));
        Assert.Equal(
            NullableAnnotation.Flag(nonNullableReturn.ReturnParameter),
            NullableAnnotation.Flag((MemberInfo)nonNullableReturn));
    }

    /// <summary>
    /// A <see cref="Type"/> has no member annotation to report — its own
    /// <c>NullableAttribute</c> describes its <b>base type</b> — so asking must throw
    /// rather than return a plausible byte.
    /// </summary>
    [Fact]
    public void Flag_OnAType_Throws_RatherThanSilentlyReadingTheBaseTypesAnnotation()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => NullableAnnotation.Flag((MemberInfo)typeof(TopicListing)));

        Assert.Equal("member", thrown.ParamName);
        Assert.Contains("Pass a member, not a type", thrown.Message, StringComparison.Ordinal);
    }
}
