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
/// Pins <see cref="ListConsumerGroupOffsetsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This result publishes no per-key map</b>, unlike its two describe siblings — Java's
/// <c>futures</c> field has no getter (<c>:38</c>). The shape test asserts that absence
/// directly, because "add the map property the other results have" is the natural and wrong
/// thing for a later slice to do.
/// </para>
/// <para>
/// ⚠ <b>The two <c>PartitionsToOffsetAndMetadata</c> forms are overloads</b>, and the
/// no-argument one has its own precondition — exactly one group — that an optional parameter
/// could not express. Both the one-group success and the multi-group and empty rejections are
/// asserted.
/// </para>
/// <para>
/// ⚠ <b>A partition with a <see langword="null"/> offset is present, not absent.</b> That is
/// Java's documented contract for "the group has no committed offset here" (<c>:47</c>), and
/// it is the distinction the interop slice has to carry across the ABI's per-partition
/// <c>has_offset</c> flag.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring and the client method arrive in later
/// slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupOffsetsResultTests
{
    private static readonly TopicPartition s_committed = new TopicPartition("orders", 0);
    private static readonly TopicPartition s_uncommitted = new TopicPartition("orders", 1);

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata?>
        {
            [s_committed] = new OffsetAndMetadata(offset, "m"),

            // Present-but-null: the group committed nothing for this partition (Java :47).
            [s_uncommitted] = null,
        };

    private static Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Completed(long offset) =>
        Task.FromResult(Offsets(offset));

    private static TaskCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Pending() =>
        new TaskCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

    private static ListConsumerGroupOffsetsResult Result(
        params (string GroupId, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Future)[] entries)
    {
        Dictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures =
            new Dictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(
                StringComparer.Ordinal);
        foreach ((string groupId, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future) in entries)
        {
            futures.Add(groupId, future);
        }

        return new ListConsumerGroupOffsetsResult(futures);
    }

    /// <summary>
    /// Java's constructor is <b>package-private</b> (<c>:40</c>), so it is mirrored as
    /// internal rather than widened to public the way
    /// <see cref="DescribeClassicGroupsResult"/>'s genuinely-public one is.
    /// </summary>
    [Fact]
    public void Constructor_IsInternalMirroringJavasPackagePrivateOne()
    {
        Assert.Empty(typeof(ListConsumerGroupOffsetsResult).GetConstructors());

        ConstructorInfo constructor = Assert.Single(
            typeof(ListConsumerGroupOffsetsResult).GetConstructors(
                BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>),
            Assert.Single(constructor.GetParameters()).ParameterType);
    }

    /// <summary>
    /// The public surface is exactly Java's three methods, and — unlike the describe results
    /// — <b>no per-key map property</b>, because Java's <c>futures</c> field has no getter.
    /// </summary>
    [Fact]
    public void PublicShape_IsJavasThreeMethodsAndNoMapProperty()
    {
        Assert.Empty(typeof(ListConsumerGroupOffsetsResult).GetProperties());

        Assert.Equal(
            new[] { "All", "PartitionsToOffsetAndMetadata", "PartitionsToOffsetAndMetadata" },
            typeof(ListConsumerGroupOffsetsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => !method.IsSpecialName)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java declares no statics on this class.
        Assert.Empty(
            typeof(ListConsumerGroupOffsetsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        // Java carries no @Deprecated here, so neither does this.
        Assert.Null(typeof(ListConsumerGroupOffsetsResult).GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// The two <c>PartitionsToOffsetAndMetadata</c> forms are genuine overloads — one taking
    /// nothing, one taking a required group id — not a single method with an optional
    /// parameter that would let <c>null</c> through as "no group".
    /// </summary>
    [Fact]
    public void PartitionsToOffsetAndMetadata_AreTwoOverloadsWithNoOptionalParameter()
    {
        MethodInfo[] overloads = typeof(ListConsumerGroupOffsetsResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => method.Name == nameof(ListConsumerGroupOffsetsResult.PartitionsToOffsetAndMetadata))
            .OrderBy(method => method.GetParameters().Length)
            .ToArray();

        Assert.Equal(2, overloads.Length);
        Assert.Empty(overloads[0].GetParameters());

        ParameterInfo groupId = Assert.Single(overloads[1].GetParameters());
        Assert.Equal(typeof(string), groupId.ParameterType);
        Assert.False(groupId.IsOptional);

        foreach (MethodInfo overload in overloads)
        {
            Assert.Equal(
                typeof(Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>),
                overload.ReturnType);
        }
    }

    /// <summary>
    /// The aggregate's value type is the per-group map of per-partition offsets — Java's
    /// <c>Map&lt;String, Map&lt;TopicPartition, OffsetAndMetadata&gt;&gt;</c> (<c>:72</c>).
    /// </summary>
    [Fact]
    public void All_YieldsAMapOfGroupIdToPartitionOffsets()
    {
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>),
            typeof(ListConsumerGroupOffsetsResult)
                .GetMethod(nameof(ListConsumerGroupOffsetsResult.All))!
                .ReturnType);
    }

    /// <summary>
    /// With exactly one group, the no-argument form yields that group's offsets — Java's
    /// <c>futures.values().iterator().next()</c> (<c>:54</c>).
    /// </summary>
    [Fact]
    public async Task PartitionsToOffsetAndMetadata_NoArgument_YieldsTheOnlyGroup()
    {
        ListConsumerGroupOffsetsResult result = Result(("g", Completed(7)));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await result.PartitionsToOffsetAndMetadata();

        Assert.Equal(7L, offsets[s_committed]!.Offset);
        Assert.Same(result.PartitionsToOffsetAndMetadata("g"), result.PartitionsToOffsetAndMetadata());
    }

    /// <summary>
    /// Java's guard is <c>futures.size() != 1</c> (<c>:50</c>), so it rejects <b>both</b>
    /// several groups and none — the empty case is the one a <c>&gt; 1</c> translation would
    /// silently let through into an empty-sequence crash.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    [InlineData(3)]
    public void PartitionsToOffsetAndMetadata_NoArgument_RejectsAnyCountButOne(int groupCount)
    {
        ListConsumerGroupOffsetsResult result = Result(
            Enumerable
                .Range(0, groupCount)
                .Select(index => (GroupId: "g" + index, Future: Completed(index)))
                .ToArray());

        // Block-bodied so this binds xUnit's Action overload: the point of the assertion is
        // that the call throws *synchronously*, which Assert.ThrowsAsync would not distinguish
        // from a returned task that merely faults.
        InvalidOperationException error =
            Assert.Throws<InvalidOperationException>(() => { _ = result.PartitionsToOffsetAndMetadata(); });
        Assert.Equal(
            "Offsets from multiple consumer groups were requested. " +
            "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.",
            error.Message);
    }

    /// <summary>
    /// The group-id form hands back that group's own awaitable, and each group keeps its own
    /// outcome — Java's <c>futures.get(groupId)</c> (<c>:65</c>).
    /// </summary>
    [Fact]
    public async Task PartitionsToOffsetAndMetadata_ByGroupId_IsPerGroup()
    {
        ListConsumerGroupOffsetsResult result = Result(("a", Completed(1)), ("b", Completed(2)));

        Assert.Equal(1L, (await result.PartitionsToOffsetAndMetadata("a"))[s_committed]!.Offset);
        Assert.Equal(2L, (await result.PartitionsToOffsetAndMetadata("b"))[s_committed]!.Offset);
    }

    /// <summary>
    /// A group that was never requested is a caller mistake, so it throws synchronously rather
    /// than faulting a returned task — Java throws <c>IllegalArgumentException</c> out of the
    /// method (<c>:63-64</c>). Lookup is ordinal, so case does not match.
    /// </summary>
    [Fact]
    public void PartitionsToOffsetAndMetadata_ByGroupId_RejectsAnUnrequestedGroupSynchronously()
    {
        ListConsumerGroupOffsetsResult result = Result(("a", Completed(1)));

        // Block-bodied lambdas bind xUnit's Action overload, so what is asserted is a
        // synchronous throw rather than a faulted task (see the no-argument test above).
        ArgumentException error =
            Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("missing"); });
        Assert.Equal("groupId", error.ParamName);
        Assert.Contains("Offsets for consumer group 'missing' were not requested.", error.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("A"); });

        ArgumentNullException nullError =
            Assert.Throws<ArgumentNullException>(() => { _ = result.PartitionsToOffsetAndMetadata(null!); });
        Assert.Equal("groupId", nullError.ParamName);
    }

    /// <summary>
    /// A partition the group has no committed offset for is <b>present with a null value</b>,
    /// not dropped — Java's documented contract (<c>:47</c>, <c>:59-60</c>) and the
    /// distinction the ABI's per-partition <c>has_offset</c> flag carries.
    /// </summary>
    [Fact]
    public async Task UncommittedPartition_IsPresentWithANullValueRatherThanAbsent()
    {
        ListConsumerGroupOffsetsResult result = Result(("g", Completed(5)));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await result.PartitionsToOffsetAndMetadata();

        Assert.True(offsets.ContainsKey(s_uncommitted));
        Assert.Null(offsets[s_uncommitted]);

        // An unlisted partition is the other thing entirely: not in the map at all.
        Assert.False(offsets.ContainsKey(new TopicPartition("orders", 99)));
    }

    /// <summary>
    /// <see cref="ListConsumerGroupOffsetsResult.All"/> gathers every group, keyed ordinally,
    /// and starts a fresh gather per call — which is why it is a method rather than a
    /// property.
    /// </summary>
    [Fact]
    public async Task All_GathersEveryGroupOrdinallyAndIsFreshPerCall()
    {
        ListConsumerGroupOffsetsResult result = Result(("a", Completed(1)), ("A", Completed(2)));

        Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> first =
            result.All();
        Assert.NotSame(first, result.All());

        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> all = await first;

        Assert.Equal(2, all.Count);
        Assert.Equal(1L, all["a"][s_committed]!.Offset);
        Assert.Equal(2L, all["A"][s_committed]!.Offset);
        Assert.Null(all["a"][s_uncommitted]);
    }

    /// <summary>
    /// A per-group failure is not a call failure: the healthy group still resolves while the
    /// broken one faults its own task, and only the aggregate carries the failure up.
    /// </summary>
    [Fact]
    public async Task PerGroupFailure_FaultsOnlyThatGroupAndTheAggregate()
    {
        TaskCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> broken = Pending();
        ListConsumerGroupOffsetsResult result = Result(("ok", Completed(1)), ("bad", broken.Task));

        KafkaException failure = new KafkaException("nope");
        broken.SetException(failure);

        Assert.Equal(1L, (await result.PartitionsToOffsetAndMetadata("ok"))[s_committed]!.Offset);

        Assert.Same(
            failure,
            await Assert.ThrowsAsync<KafkaException>(() => result.PartitionsToOffsetAndMetadata("bad")));
        Assert.Same(failure, await Assert.ThrowsAsync<KafkaException>(() => result.All()));
    }

    /// <summary>
    /// <see cref="ListConsumerGroupOffsetsResult.All"/> waits for <em>every</em> group before
    /// it faults, matching Java's <c>allOf(...).thenApply(...)</c> (<c>:73</c>). A gather
    /// written without the <c>WhenAll</c> would fault the instant it touched the broken key,
    /// so the still-pending case is the only one that tells the two apart.
    /// </summary>
    [Fact]
    public async Task All_WaitsForEveryGroupBeforeItFaults()
    {
        TaskCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> broken = Pending();
        TaskCompletionSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> slow = Pending();
        ListConsumerGroupOffsetsResult result = Result(("bad", broken.Task), ("slow", slow.Task));

        Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> all = result.All();

        broken.SetException(new KafkaException("gone"));
        Assert.False(all.IsCompleted);

        slow.SetResult(Offsets(3));
        await Assert.ThrowsAsync<KafkaException>(() => all);
    }

    /// <summary>
    /// The empty result is representable and every accessor behaves: the aggregate yields an
    /// empty map, and the no-argument form rejects it because Java's <c>size() != 1</c> guard
    /// covers zero as well.
    /// </summary>
    [Fact]
    public async Task EmptyResult_AggregatesToAnEmptyMap()
    {
        ListConsumerGroupOffsetsResult result = Result();

        Assert.Empty(await result.All());
        Assert.Throws<InvalidOperationException>(() => { _ = result.PartitionsToOffsetAndMetadata(); });
        Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("g"); });
    }
}
