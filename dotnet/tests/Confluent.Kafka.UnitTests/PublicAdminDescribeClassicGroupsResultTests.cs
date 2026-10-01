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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins <see cref="DescribeClassicGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeClassicGroupsResult</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>A per-group failure is not a call failure.</b> One group's task can fault while
/// another's yields a description, which is the whole reason Java publishes the per-key map
/// alongside <see cref="DescribeClassicGroupsResult.All"/>. Both directions are asserted:
/// the healthy group still resolves, and the aggregate still faults.
/// </para>
/// <para>
/// ⚠ <b><see cref="DescribeClassicGroupsResult.All"/> waits for <em>every</em> task before
/// it faults</b>, matching Java's <c>allOf(...).thenApply(...)</c> (<c>:49-62</c>). A gather
/// written without the <c>WhenAll</c> would fault the instant it touched the broken key, so
/// the still-pending case is asserted explicitly — it is the only case that tells the two
/// implementations apart.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring and the client method arrive in later
/// slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminDescribeClassicGroupsResultTests
{
    private static ClassicGroupDescription Description(string groupId) =>
        new ClassicGroupDescription(
            groupId, "consumer", "range", null, ClassicGroupState.Stable, null);

    private static Task<ClassicGroupDescription> Completed(string groupId) =>
        Task.FromResult(Description(groupId));

    private static TaskCompletionSource<ClassicGroupDescription> Pending() =>
        new TaskCompletionSource<ClassicGroupDescription>(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Java's constructor is <b>public</b> (<c>:34</c>) and is mirrored as public rather
    /// than narrowed; it rejects a null map before anything can dereference it.
    /// </summary>
    [Fact]
    public void Constructor_IsPublicAndRejectsNull()
    {
        ConstructorInfo constructor = Assert.Single(typeof(DescribeClassicGroupsResult).GetConstructors());
        Assert.True(constructor.IsPublic);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task<ClassicGroupDescription>>),
            Assert.Single(constructor.GetParameters()).ParameterType);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => new DescribeClassicGroupsResult(null!));
        Assert.Equal("futures", error.ParamName);
    }

    /// <summary>
    /// The public surface is exactly Java's two accessors — the per-key map and the
    /// aggregate — with the aggregate a <b>method</b>, because it starts work and hands back
    /// a fresh <see cref="Task"/>, which is not what a property promises.
    /// </summary>
    [Fact]
    public void PublicShape_IsJavasTwoAccessors()
    {
        Assert.Equal(
            new[] { "DescribedGroups" },
            typeof(DescribeClassicGroupsResult)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Equal(
            new[] { "All" },
            typeof(DescribeClassicGroupsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => !method.IsSpecialName)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java declares no statics on this class.
        Assert.Empty(
            typeof(DescribeClassicGroupsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        // Java carries no @Deprecated here, so neither does this.
        Assert.Null(typeof(DescribeClassicGroupsResult).GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// The value type is the classic description, not the consumer-group one — Java's two
    /// results differ in exactly this, and a slice that swapped them would compile at the
    /// call site while handing back the wrong generation's description.
    /// </summary>
    [Fact]
    public void ValueType_IsTheClassicDescription()
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task<ClassicGroupDescription>>),
            typeof(DescribeClassicGroupsResult)
                .GetProperty(nameof(DescribeClassicGroupsResult.DescribedGroups))!
                .PropertyType);

        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<string, ClassicGroupDescription>>),
            typeof(DescribeClassicGroupsResult)
                .GetMethod(nameof(DescribeClassicGroupsResult.All))!
                .ReturnType);
    }

    /// <summary>
    /// <see cref="DescribeClassicGroupsResult.DescribedGroups"/> hands back the stored map
    /// — the recorded deviation from Java's <c>new HashMap&lt;&gt;(futures)</c> copy
    /// (<c>:42</c>), which exists only to deny a mutation the declared read-only type
    /// already denies.
    /// </summary>
    [Fact]
    public void DescribedGroups_IsTheStoredMapRatherThanJavasDefensiveCopy()
    {
        Dictionary<string, Task<ClassicGroupDescription>> futures =
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["g-1"] = Completed("g-1"),
            };

        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(futures);

        Assert.Same(futures, result.DescribedGroups);
        Assert.Same(result.DescribedGroups, result.DescribedGroups);
    }

    /// <summary>
    /// The aggregate carries every group, keyed the way it was requested.
    /// </summary>
    [Fact]
    public async Task All_YieldsEveryDescriptionKeyedByGroupId()
    {
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["g-1"] = Completed("g-1"),
                ["g-2"] = Completed("g-2"),
            });

        IReadOnlyDictionary<string, ClassicGroupDescription> all = await result.All();

        Assert.Equal(2, all.Count);
        Assert.Equal("g-1", all["g-1"].GroupId);
        Assert.Equal("g-2", all["g-2"].GroupId);
    }

    /// <summary>
    /// An empty request yields an empty map rather than a task that never completes.
    /// </summary>
    [Fact]
    public async Task All_OverAnEmptyMap_YieldsAnEmptyMap()
    {
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal));

        Assert.Empty(await result.All());
    }

    /// <summary>
    /// Each call starts a fresh aggregate, as Java's <c>all()</c> builds a new
    /// <c>KafkaFuture</c> per call — the behaviour that makes this a method rather than a
    /// property.
    /// </summary>
    [Fact]
    public void All_HandsBackAFreshTaskEachCall()
    {
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["g-1"] = Completed("g-1"),
            });

        Assert.NotSame(result.All(), result.All());
    }

    /// <summary>
    /// A group that fails faults only its own task; the healthy group still resolves, while
    /// the aggregate fails.
    /// </summary>
    [Fact]
    public async Task AGroupFailure_FaultsOnlyItsOwnTask_AndTheAggregate()
    {
        TaskCompletionSource<ClassicGroupDescription> broken = Pending();
        broken.SetException(new InvalidOperationException("group is broken"));

        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["healthy"] = Completed("healthy"),
                ["broken"] = broken.Task,
            });

        ClassicGroupDescription healthy = await result.DescribedGroups["healthy"];
        Assert.Equal("healthy", healthy.GroupId);

        InvalidOperationException error =
            await Assert.ThrowsAsync<InvalidOperationException>(() => result.All());
        Assert.Equal("group is broken", error.Message);
    }

    /// <summary>
    /// ⚠ The aggregate waits for <b>every</b> task before faulting — Java's
    /// <c>allOf(...)</c> semantics. A gather that read each result as it walked the map
    /// would fault the moment it reached the broken key, while a group was still in flight.
    /// </summary>
    [Fact]
    public async Task All_WaitsForEveryTaskBeforeItFaults()
    {
        TaskCompletionSource<ClassicGroupDescription> broken = Pending();
        broken.SetException(new InvalidOperationException("group is broken"));
        TaskCompletionSource<ClassicGroupDescription> stillRunning = Pending();

        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["broken"] = broken.Task,
                ["still-running"] = stillRunning.Task,
            });

        Task<IReadOnlyDictionary<string, ClassicGroupDescription>> all = result.All();
        Assert.False(all.IsCompleted);

        stillRunning.SetResult(Description("still-running"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => all);
    }

    /// <summary>
    /// The aggregate is keyed ordinally — what the bridge keys group ids by, and how Kafka
    /// compares them — so two ids differing only in case stay two entries.
    /// </summary>
    [Fact]
    public async Task All_KeysOrdinally()
    {
        DescribeClassicGroupsResult result = new DescribeClassicGroupsResult(
            new Dictionary<string, Task<ClassicGroupDescription>>(StringComparer.Ordinal)
            {
                ["group"] = Completed("group"),
                ["GROUP"] = Completed("GROUP"),
            });

        IReadOnlyDictionary<string, ClassicGroupDescription> all = await result.All();

        Assert.Equal(2, all.Count);
        Assert.Equal("group", all["group"].GroupId);
        Assert.Equal("GROUP", all["GROUP"].GroupId);
    }
}
