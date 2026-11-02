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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>Java's <c>CloseOptionsTest</c>, plus the shape of <see cref="CloseOptions"/> and <see cref="GroupMembershipOperation"/>.</summary>
public sealed class PublicCloseOptionsTests
{
    private const string UndefinedOperationMessage = "operation must be a defined GroupMembershipOperation member.";

    [Fact]
    public void OperationShouldNotBeUndefined()
    {
        GroupMembershipOperation undefined = (GroupMembershipOperation)42;

        AssertUndefinedOperation(Assert.Throws<ArgumentOutOfRangeException>(
            () => CloseOptions.GroupMembershipOperation(undefined)), undefined);
        AssertUndefinedOperation(Assert.Throws<ArgumentOutOfRangeException>(
            () => CloseOptions.Timeout(TimeSpan.Zero).WithGroupMembershipOperation(undefined)), undefined);
    }

    [Fact]
    public void OperationShouldHaveDefaultValue()
    {
        Assert.Equal(GroupMembershipOperation.Default, CloseOptions.Timeout(TimeSpan.Zero).GroupMembershipOperation());
    }

    [Fact]
    public void TimeoutCouldBeNull()
    {
        CloseOptions closeOptions = CloseOptions.Timeout(null);

        Assert.Null(closeOptions.Timeout());
    }

    [Fact]
    public void TimeoutShouldBeDefaultEmpty()
    {
        Assert.Null(CloseOptions.GroupMembershipOperation(GroupMembershipOperation.Default).Timeout());
    }

    [Fact]
    public void Factories_SetTheirOwnFieldOnly()
    {
        CloseOptions timed = CloseOptions.Timeout(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(3), timed.Timeout());
        Assert.Equal(GroupMembershipOperation.Default, timed.GroupMembershipOperation());

        CloseOptions leaving = CloseOptions.GroupMembershipOperation(GroupMembershipOperation.LeaveGroup);
        Assert.Null(leaving.Timeout());
        Assert.Equal(GroupMembershipOperation.LeaveGroup, leaving.GroupMembershipOperation());
    }

    [Fact]
    public void FluentSetters_ReturnTheSameInstance_AndOverwrite()
    {
        CloseOptions options = CloseOptions.Timeout(TimeSpan.FromSeconds(1));

        Assert.Same(options, options.WithTimeout(TimeSpan.FromSeconds(2)));
        Assert.Same(options, options.WithGroupMembershipOperation(GroupMembershipOperation.RemainInGroup));
        Assert.Equal(TimeSpan.FromSeconds(2), options.Timeout());
        Assert.Equal(GroupMembershipOperation.RemainInGroup, options.GroupMembershipOperation());

        options.WithTimeout(null);
        Assert.Null(options.Timeout());
    }

    [Fact]
    public void Timeout_KeepsANegativeValue_ForCloseToReject()
    {
        Assert.Equal(TimeSpan.FromMilliseconds(-1), CloseOptions.Timeout(TimeSpan.FromMilliseconds(-1)).Timeout());
    }

    [Fact]
    public void CloseOptions_HasNoPublicConstructor_AndIsSealed()
    {
        Assert.Empty(typeof(CloseOptions).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.True(typeof(CloseOptions).IsSealed);
    }

    [Fact]
    public void GroupMembershipOperation_IsANamespaceLevelEnum_InJavaOrder()
    {
        Type type = typeof(GroupMembershipOperation);

        Assert.True(type.IsEnum);
        Assert.Null(type.DeclaringType);
        Assert.Equal(
            new[] { "LeaveGroup", "RemainInGroup", "Default" },
            type.GetFields(BindingFlags.Public | BindingFlags.Static).OrderBy(f => (int)f.GetValue(null)!).Select(f => f.Name));
    }

    private static void AssertUndefinedOperation(ArgumentOutOfRangeException thrown, GroupMembershipOperation undefined)
    {
        Assert.Equal("operation", thrown.ParamName);
        Assert.StartsWith(UndefinedOperationMessage, thrown.Message, StringComparison.Ordinal);
        Assert.Equal(undefined, thrown.ActualValue);
    }
}
