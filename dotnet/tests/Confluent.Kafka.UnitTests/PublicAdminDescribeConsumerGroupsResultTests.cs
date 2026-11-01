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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins <see cref="DescribeConsumerGroupsResult"/> and <see cref="AsyncDescribeConsumerGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsResult</c>: the public constructor over one future per group, the per-group
/// map, and <c>All()</c>, which waits for every future before it fails.
/// </summary>
public sealed class PublicAdminDescribeConsumerGroupsResultTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Constructors_ArePublic_OverOneFuturePerGroup_AndRejectNull()
    {
        ConstructorInfo sync = Assert.Single(typeof(DescribeConsumerGroupsResult).GetConstructors());
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<ConsumerGroupDescription>>),
            Assert.Single(sync.GetParameters()).ParameterType);
        ConstructorInfo async = Assert.Single(typeof(AsyncDescribeConsumerGroupsResult).GetConstructors());
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>),
            Assert.Single(async.GetParameters()).ParameterType);

        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => new DescribeConsumerGroupsResult(null!)).ParamName);
        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => new AsyncDescribeConsumerGroupsResult(null!)).ParamName);
    }

    [Theory]
    [InlineData(typeof(DescribeConsumerGroupsResult))]
    [InlineData(typeof(AsyncDescribeConsumerGroupsResult))]
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
    public void DescribedGroups_IsACopy_ThatALaterChangeToTheGivenMapDoesNotReach_Sync()
    {
        KafkaFuture<ConsumerGroupDescription> first = KafkaFuture.CompletedFuture(Description("g-1"));
        Dictionary<string, KafkaFuture<ConsumerGroupDescription>> futures =
            new Dictionary<string, KafkaFuture<ConsumerGroupDescription>>(StringComparer.Ordinal) { ["g-1"] = first };
        DescribeConsumerGroupsResult result = new DescribeConsumerGroupsResult(futures);

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
        AsyncKafkaFuture<ConsumerGroupDescription> first = AsyncKafkaFuture.CompletedFuture(Description("g-1"));
        Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> futures =
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>(StringComparer.Ordinal) { ["g-1"] = first };
        AsyncDescribeConsumerGroupsResult result = new AsyncDescribeConsumerGroupsResult(futures);

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
        ConsumerGroupDescription lower = Description("group");
        ConsumerGroupDescription upper = Description("GROUP");
        DescribeConsumerGroupsResult result = new DescribeConsumerGroupsResult(
            new Dictionary<string, KafkaFuture<ConsumerGroupDescription>>(StringComparer.Ordinal)
            {
                ["group"] = KafkaFuture.CompletedFuture(lower),
                ["GROUP"] = KafkaFuture.CompletedFuture(upper),
            });
        AsyncDescribeConsumerGroupsResult asyncResult = new AsyncDescribeConsumerGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>(StringComparer.Ordinal)
            {
                ["group"] = AsyncKafkaFuture.CompletedFuture(lower),
                ["GROUP"] = AsyncKafkaFuture.CompletedFuture(upper),
            });

        foreach (IReadOnlyDictionary<string, ConsumerGroupDescription> all in new[] { result.All().Get(s_deadline), await asyncResult.All().Get() })
        {
            Assert.Equal(2, all.Count);
            Assert.Same(lower, all["group"]);
            Assert.Same(upper, all["GROUP"]);
        }
    }

    [Fact]
    public async Task All_OverAnEmptyMap_YieldsAnEmptyMap()
    {
        Assert.Empty(new DescribeConsumerGroupsResult(new Dictionary<string, KafkaFuture<ConsumerGroupDescription>>()).All().Get(s_deadline));
        Assert.Empty(await new AsyncDescribeConsumerGroupsResult(new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>()).All().Get());
    }

    [Fact]
    public void All_HandsBackANewFutureEachCall()
    {
        DescribeConsumerGroupsResult result = new DescribeConsumerGroupsResult(
            new Dictionary<string, KafkaFuture<ConsumerGroupDescription>> { ["g-1"] = KafkaFuture.CompletedFuture(Description("g-1")) });
        AsyncDescribeConsumerGroupsResult asyncResult = new AsyncDescribeConsumerGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> { ["g-1"] = AsyncKafkaFuture.CompletedFuture(Description("g-1")) });

        Assert.NotEqual(result.All(), result.All());
        Assert.NotEqual(asyncResult.All(), asyncResult.All());
    }

    [Fact]
    public async Task AGroupFailure_FailsOnlyItsOwnFuture_AndTheAggregate()
    {
        KafkaFutureImpl<ConsumerGroupDescription> broken = new KafkaFutureImpl<ConsumerGroupDescription>();
        InvalidOperationException failure = new InvalidOperationException("group is broken");
        broken.CompleteWithError(failure);
        ConsumerGroupDescription healthy = Description("healthy");

        DescribeConsumerGroupsResult result = new DescribeConsumerGroupsResult(
            new Dictionary<string, KafkaFuture<ConsumerGroupDescription>>
            {
                ["healthy"] = KafkaFuture.CompletedFuture(healthy),
                ["broken"] = new KafkaFuture<ConsumerGroupDescription>(broken),
            });
        Assert.Same(healthy, result.DescribedGroups["healthy"].Get(s_deadline));
        Assert.Same(failure, Assert.Throws<InvalidOperationException>(() => result.All().Get(s_deadline)));

        AsyncDescribeConsumerGroupsResult asyncResult = new AsyncDescribeConsumerGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>
            {
                ["healthy"] = AsyncKafkaFuture.CompletedFuture(healthy),
                ["broken"] = new AsyncKafkaFuture<ConsumerGroupDescription>(broken),
            });
        Assert.Same(healthy, await asyncResult.DescribedGroups["healthy"].Get());
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => asyncResult.All().Get()));
    }

    [Fact]
    public async Task All_WaitsForEveryFutureBeforeItFails()
    {
        InvalidOperationException failure = new InvalidOperationException("group is broken");
        KafkaFutureImpl<ConsumerGroupDescription> broken = new KafkaFutureImpl<ConsumerGroupDescription>();
        broken.CompleteWithError(failure);
        KafkaFutureImpl<ConsumerGroupDescription> stillRunning = new KafkaFutureImpl<ConsumerGroupDescription>();

        DescribeConsumerGroupsResult result = new DescribeConsumerGroupsResult(
            new Dictionary<string, KafkaFuture<ConsumerGroupDescription>>
            {
                ["broken"] = new KafkaFuture<ConsumerGroupDescription>(broken),
                ["still-running"] = new KafkaFuture<ConsumerGroupDescription>(stillRunning),
            });
        AsyncDescribeConsumerGroupsResult asyncResult = new AsyncDescribeConsumerGroupsResult(
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>
            {
                ["broken"] = new AsyncKafkaFuture<ConsumerGroupDescription>(broken),
                ["still-running"] = new AsyncKafkaFuture<ConsumerGroupDescription>(stillRunning),
            });
        KafkaFuture<IReadOnlyDictionary<string, ConsumerGroupDescription>> all = result.All();
        AsyncKafkaFuture<IReadOnlyDictionary<string, ConsumerGroupDescription>> asyncAll = asyncResult.All();
        Assert.False(all.IsDone);
        Assert.False(asyncAll.IsDone);

        stillRunning.Complete(Description("still-running"));

        Assert.Same(failure, Assert.Throws<InvalidOperationException>(() => all.Get(s_deadline)));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => asyncAll.Get()));
    }

    private static ConsumerGroupDescription Description(string groupId) => new ConsumerGroupDescription(groupId, false, null, "range", GroupType.Consumer, GroupState.Stable, null, null, null, null);
}
