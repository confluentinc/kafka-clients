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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins <see cref="DescribeClassicGroupsResult"/> and <see cref="AsyncDescribeClassicGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeClassicGroupsResult</c>: the public constructor over one future per group, the per-group
/// map, and <c>All()</c>, which waits for every future before it fails.
/// </summary>
public sealed class PublicAdminDescribeClassicGroupsResultTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Constructors_ArePublic_OverOneFuturePerGroup_AndRejectNull()
    {
        ConstructorInfo sync = Assert.Single(typeof(DescribeClassicGroupsResult).GetConstructors());
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>>),
            Assert.Single(sync.GetParameters()).ParameterType);
        ConstructorInfo async = Assert.Single(typeof(AsyncDescribeClassicGroupsResult).GetConstructors());
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>),
            Assert.Single(async.GetParameters()).ParameterType);

        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => new DescribeClassicGroupsResult(null!)).ParamName);
        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => new AsyncDescribeClassicGroupsResult(null!)).ParamName);
    }

    [Theory]
    [InlineData(typeof(DescribeClassicGroupsResult))]
    [InlineData(typeof(AsyncDescribeClassicGroupsResult))]
    public void PublicShape_IsJavasTwoAccessors(Type result)
    {
        Assert.Equal(new[] { "DescribedGroups" }, result.GetProperties().Select(property => property.Name));
        Assert.Equal(
            new[] { "All" },
            result
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => !method.IsSpecialName)
                .Select(method => method.Name));
        Assert.Empty(result.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Null(result.GetCustomAttribute<ObsoleteAttribute>());
    }

    [Fact]
    public void ValueType_IsTheClassicDescription()
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>>),
            typeof(DescribeClassicGroupsResult).GetProperty(nameof(DescribeClassicGroupsResult.DescribedGroups))!.PropertyType);
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>>),
            typeof(DescribeClassicGroupsResult).GetMethod(nameof(DescribeClassicGroupsResult.All))!.ReturnType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>),
            typeof(AsyncDescribeClassicGroupsResult).GetProperty(nameof(AsyncDescribeClassicGroupsResult.DescribedGroups))!.PropertyType);
        Assert.Equal(
            typeof(AsyncKafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>>),
            typeof(AsyncDescribeClassicGroupsResult).GetMethod(nameof(AsyncDescribeClassicGroupsResult.All))!.ReturnType);
    }
    [Fact]
    public void DescribedGroups_IsACopy_ThatALaterChangeToTheGivenMapDoesNotReach_Sync()
    {
        KafkaFuture<ClassicGroupDescription> first = KafkaFuture.CompletedFuture(Description("g-1"));
        Dictionary<string, KafkaFuture<ClassicGroupDescription>> futures =
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>>(StringComparer.Ordinal) { ["g-1"] = first };
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(futures);

        futures["g-2"] = KafkaFuture.CompletedFuture(Description("g-2"));
        futures.Remove("g-1");

        Assert.NotSame(futures, result.DescribedGroups);
        Assert.Equal(new[] { "g-1" }, result.DescribedGroups.Keys);
        Assert.Equal(first, result.DescribedGroups["g-1"]);
        Assert.Equal(new[] { "g-1" }, result.All().Get(s_deadline).Keys);
    }

    [Fact]
    public async Task DescribedGroups_IsACopy_ThatALaterChangeToTheGivenMapDoesNotReach_Async()
    {
        AsyncKafkaFuture<ClassicGroupDescription> first = AsyncKafkaFuture.CompletedFuture(Description("g-1"));
        Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> futures =
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>(StringComparer.Ordinal) { ["g-1"] = first };
        AsyncDescribeClassicGroupsResult result = new AsyncDescribeClassicGroupsResult(futures);

        futures["g-2"] = AsyncKafkaFuture.CompletedFuture(Description("g-2"));
        futures.Remove("g-1");

        Assert.NotSame(futures, result.DescribedGroups);
        Assert.Equal(new[] { "g-1" }, result.DescribedGroups.Keys);
        Assert.Equal(first, result.DescribedGroups["g-1"]);
        Assert.Equal(new[] { "g-1" }, (await result.All().Get()).Keys);
    }

    [Fact]
    public async Task All_YieldsEveryDescription_KeyedOrdinally()
    {
        ClassicGroupDescription lower = Description("group");
        ClassicGroupDescription upper = Description("GROUP");
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["group"] = KafkaFuture.CompletedFuture(lower),
                ["GROUP"] = KafkaFuture.CompletedFuture(upper),
            });
        AsyncDescribeClassicGroupsResult asyncResult = new AsyncDescribeClassicGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["group"] = AsyncKafkaFuture.CompletedFuture(lower),
                ["GROUP"] = AsyncKafkaFuture.CompletedFuture(upper),
            });

        foreach (IReadOnlyDictionary<string, ClassicGroupDescription> all in new[] { result.All().Get(s_deadline), await asyncResult.All().Get() })
        {
            Assert.Equal(2, all.Count);
            Assert.Same(lower, all["group"]);
            Assert.Same(upper, all["GROUP"]);
        }
    }

    [Fact]
    public async Task All_OverAnEmptyMap_YieldsAnEmptyMap()
    {
        Assert.Empty(new DescribeClassicGroupsResult(new Dictionary<string, KafkaFuture<ClassicGroupDescription>>()).All().Get(s_deadline));
        Assert.Empty(await new AsyncDescribeClassicGroupsResult(new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>()).All().Get());
    }

    [Fact]
    public void All_HandsBackANewFutureEachCall()
    {
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>> { ["g-1"] = KafkaFuture.CompletedFuture(Description("g-1")) });
        AsyncDescribeClassicGroupsResult asyncResult = new AsyncDescribeClassicGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> { ["g-1"] = AsyncKafkaFuture.CompletedFuture(Description("g-1")) });

        Assert.NotEqual(result.All(), result.All());
        Assert.NotEqual(asyncResult.All(), asyncResult.All());
    }

    [Fact]
    public async Task AGroupFailure_FailsOnlyItsOwnFuture_AndTheAggregate()
    {
        KafkaFutureImpl<ClassicGroupDescription> broken = new KafkaFutureImpl<ClassicGroupDescription>();
        InvalidOperationException failure = new InvalidOperationException("group is broken");
        broken.CompleteWithError(failure);
        ClassicGroupDescription healthy = Description("healthy");

        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>>
            {
                ["healthy"] = KafkaFuture.CompletedFuture(healthy),
                ["broken"] = new KafkaFuture<ClassicGroupDescription>(broken),
            });
        Assert.Same(healthy, result.DescribedGroups["healthy"].Get(s_deadline));
        Assert.Same(failure, Assert.Throws<InvalidOperationException>(() => result.All().Get(s_deadline)));

        AsyncDescribeClassicGroupsResult asyncResult = new AsyncDescribeClassicGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>
            {
                ["healthy"] = AsyncKafkaFuture.CompletedFuture(healthy),
                ["broken"] = new AsyncKafkaFuture<ClassicGroupDescription>(broken),
            });
        Assert.Same(healthy, await asyncResult.DescribedGroups["healthy"].Get());
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => asyncResult.All().Get()));
    }

    [Fact]
    public async Task All_WaitsForEveryFutureBeforeItFails()
    {
        InvalidOperationException failure = new InvalidOperationException("group is broken");
        KafkaFutureImpl<ClassicGroupDescription> broken = new KafkaFutureImpl<ClassicGroupDescription>();
        broken.CompleteWithError(failure);
        KafkaFutureImpl<ClassicGroupDescription> stillRunning = new KafkaFutureImpl<ClassicGroupDescription>();

        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>>
            {
                ["broken"] = new KafkaFuture<ClassicGroupDescription>(broken),
                ["still-running"] = new KafkaFuture<ClassicGroupDescription>(stillRunning),
            });
        AsyncDescribeClassicGroupsResult asyncResult = new AsyncDescribeClassicGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>
            {
                ["broken"] = new AsyncKafkaFuture<ClassicGroupDescription>(broken),
                ["still-running"] = new AsyncKafkaFuture<ClassicGroupDescription>(stillRunning),
            });
        KafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>> all = result.All();
        AsyncKafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>> asyncAll = asyncResult.All();
        Assert.False(all.IsDone);
        Assert.False(asyncAll.IsDone);

        stillRunning.Complete(Description("still-running"));

        Assert.Same(failure, Assert.Throws<InvalidOperationException>(() => all.Get(s_deadline)));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => asyncAll.Get()));
    }

    private static ClassicGroupDescription Description(string groupId) => new ClassicGroupDescription(groupId, "consumer", "range", null, ClassicGroupState.Stable, null);
}
