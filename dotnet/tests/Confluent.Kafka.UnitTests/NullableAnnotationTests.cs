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
/// Tests <see cref="NullableAnnotation"/>'s handling of a <see cref="MethodInfo"/> and a <see cref="Type"/>, which both
/// bind to its <c>Flag(MemberInfo)</c> overload with no compiler diagnostic.
/// </summary>
public class NullableAnnotationTests
{
    [Fact]
    public void Flag_OnAMethodInfo_ReadsTheReturnsAnnotation_NotTheDeclaringTypesContext()
    {
        MethodInfo nullableReturn = typeof(Config).GetMethod(nameof(Config.Get), new[] { typeof(string) })!;
        MethodInfo nonNullableReturn = typeof(Config).GetMethod(nameof(Config.ToString), Type.EmptyTypes)!;

        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag((MemberInfo)nullableReturn));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag((MemberInfo)nonNullableReturn));

        Assert.Equal(
            NullableAnnotation.Flag(nullableReturn.ReturnParameter),
            NullableAnnotation.Flag((MemberInfo)nullableReturn));
        Assert.Equal(
            NullableAnnotation.Flag(nonNullableReturn.ReturnParameter),
            NullableAnnotation.Flag((MemberInfo)nonNullableReturn));
    }

    [Fact]
    public void Flag_OnAType_Throws_RatherThanSilentlyReadingTheBaseTypesAnnotation()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => NullableAnnotation.Flag((MemberInfo)typeof(TopicListing)));

        Assert.Equal("member", thrown.ParamName);
        Assert.Contains("Pass a member, not a type", thrown.Message, StringComparison.Ordinal);
    }
}
