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
using System.Linq;
using System.Reflection;
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Shape and behaviour of <see cref="MemberDescription"/> against Java's
/// <c>org.apache.kafka.clients.admin.MemberDescription</c>.
/// </summary>
public sealed class PublicAdminMemberDescriptionTests
{
    private static readonly MemberAssignment s_current =
        new MemberAssignment(new[] { new TopicPartition("orders", 0) });

    private static readonly MemberAssignment s_target =
        new MemberAssignment(new[] { new TopicPartition("orders", 1) });

    [Fact]
    public void Constructor_IsJavasCurrentOne_TheFourDeprecatedOnesAreNotTranslated()
    {
        var parameters = typeof(MemberDescription)
            .GetConstructors()
            .Select(static constructor => constructor.GetParameters().Select(static p => p.ParameterType).ToArray())
            .ToArray();

        var only = Assert.Single(parameters);
        Assert.Equal(
            new[]
            {
                typeof(string),
                typeof(string),
                typeof(string),
                typeof(string),
                typeof(string),
                typeof(MemberAssignment),
                typeof(MemberAssignment),
                typeof(int?),
                typeof(bool?),
            },
            only);
    }

    [Fact]
    public void Accessors_AreJavasNine_AsReadOnlyProperties()
    {
        var properties = typeof(MemberDescription).GetProperties();

        Assert.Equal(9, properties.Length);
        Assert.All(properties, static property => Assert.Null(property.SetMethod));

        Assert.Equal(typeof(string), PropertyType(nameof(MemberDescription.ConsumerId)));
        Assert.Equal(typeof(string), PropertyType(nameof(MemberDescription.GroupInstanceId)));
        Assert.Equal(typeof(string), PropertyType(nameof(MemberDescription.RackId)));
        Assert.Equal(typeof(string), PropertyType(nameof(MemberDescription.ClientId)));
        Assert.Equal(typeof(string), PropertyType(nameof(MemberDescription.Host)));
        Assert.Equal(typeof(MemberAssignment), PropertyType(nameof(MemberDescription.Assignment)));
        Assert.Equal(typeof(MemberAssignment), PropertyType(nameof(MemberDescription.TargetAssignment)));

        // The two ABI presence-flag members are nullable, never sentinel-valued.
        Assert.Equal(typeof(int?), PropertyType(nameof(MemberDescription.MemberEpoch)));
        Assert.Equal(typeof(bool?), PropertyType(nameof(MemberDescription.Upgraded)));
    }

    [Fact]
    public void NothingIsDeprecated_BecauseNothingInJavasClassIs()
    {
        // Java's class carries no @Deprecated, and none of its nine accessors does either;
        // only its four forwarding constructors do, and those are not translated.
        Assert.Null(typeof(MemberDescription).GetCustomAttribute<ObsoleteAttribute>());

        var deprecated = typeof(MemberDescription)
            .GetMembers()
            .Where(static member => member.GetCustomAttribute<ObsoleteAttribute>() is not null)
            .ToArray();

        Assert.Empty(deprecated);
    }

    [Fact]
    public void Constructor_ReadsBackEveryAccessor()
    {
        var description = Canonical();

        Assert.Equal("m-1", description.ConsumerId);
        Assert.Equal("gi-1", description.GroupInstanceId);
        Assert.Equal("rack-1", description.RackId);
        Assert.Equal("c-1", description.ClientId);
        Assert.Equal("h-1", description.Host);
        Assert.Equal(s_current, description.Assignment);
        Assert.Equal(s_target, description.TargetAssignment);
        Assert.Equal(5, description.MemberEpoch);
        Assert.True(description.Upgraded);
    }

    [Fact]
    public void NullArguments_CoalesceLikeJavas()
    {
        // Java :50-56 — the three plain strings become "" and the assignment becomes an
        // empty MemberAssignment; the five Optionals stay absent.
        var description = new MemberDescription(null, null, null, null, null, null, null, null, null);

        Assert.Equal(string.Empty, description.ConsumerId);
        Assert.Null(description.GroupInstanceId);
        Assert.Null(description.RackId);
        Assert.Equal(string.Empty, description.ClientId);
        Assert.Equal(string.Empty, description.Host);
        Assert.Equal(new MemberAssignment(null), description.Assignment);
        Assert.Empty(description.Assignment.TopicPartitions);
        Assert.Null(description.TargetAssignment);
        Assert.Null(description.MemberEpoch);
        Assert.Null(description.Upgraded);
    }

    [Fact]
    public void Equality_CoversAllNineMembers()
    {
        var description = Canonical();

        Assert.True(description.Equals(Canonical()));
        Assert.True(description.Equals(description));
        Assert.Equal(Canonical().GetHashCode(), description.GetHashCode());

        // Each member discriminates on its own.
        Assert.False(description.Equals(
            new MemberDescription("other", "gi-1", "rack-1", "c-1", "h-1", s_current, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", null, "rack-1", "c-1", "h-1", s_current, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", null, "c-1", "h-1", s_current, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "other", "h-1", s_current, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "other", s_current, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "h-1", s_target, s_target, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "h-1", s_current, null, 5, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "h-1", s_current, s_target, 6, true)));
        Assert.False(description.Equals(
            new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "h-1", s_current, s_target, 5, false)));

        Assert.False(description.Equals(null));
        Assert.False(description.Equals("m-1"));
    }

    [Fact]
    public void AnAbsentValueDoesNotEqualItsFalsyPresentOne()
    {
        // The ABI reports memberEpoch and upgraded as presence flags; absent must not
        // collapse into 0 or false.
        var absent = new MemberDescription("m-1", null, null, "c-1", "h-1", s_current, null, null, null);
        var zero = new MemberDescription("m-1", null, null, "c-1", "h-1", s_current, null, 0, false);

        Assert.False(absent.Equals(zero));
    }

    [Fact]
    public void ToString_MirrorsJavasRendering()
    {
        // Java :239-250. groupInstanceId/rackId render through orElse("null"),
        // targetAssignment inherits Optional.toString(), and memberEpoch/upgraded render
        // through orElse(null) — three spellings for five Optionals, all mirrored.
        Assert.Equal(
            "(memberId=m-1, groupInstanceId=gi-1, rackId=rack-1, clientId=c-1, host=h-1, "
            + "assignment=(topicPartitions=orders-0), "
            + "targetAssignment=Optional[(topicPartitions=orders-1)], "
            + "memberEpoch=5, upgraded=true)",
            Canonical().ToString());

        Assert.Equal(
            "(memberId=, groupInstanceId=null, rackId=null, clientId=, host=, "
            + "assignment=(topicPartitions=), "
            + "targetAssignment=Optional.empty, "
            + "memberEpoch=null, upgraded=null)",
            new MemberDescription(null, null, null, null, null, null, null, null, null).ToString());
    }

    [Fact]
    public void ToString_SpellsAPresentFalseUpgradedTheJavaWay()
    {
        // Java's Boolean.toString() is lowercase; .NET's would render "False".
        var description = new MemberDescription("m-1", null, null, "c-1", "h-1", s_current, null, 0, false);

        Assert.Contains("memberEpoch=0, upgraded=false)", description.ToString(), StringComparison.Ordinal);
    }

    private static MemberDescription Canonical() =>
        new MemberDescription("m-1", "gi-1", "rack-1", "c-1", "h-1", s_current, s_target, 5, true);

    private static Type PropertyType(string name) =>
        typeof(MemberDescription).GetProperty(name)!.PropertyType;
}
