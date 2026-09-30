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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The three topic results are user-constructible (M15/P13.2 G1-6) — Java's
/// <c>protected</c> constructors (<c>CreateTopicsResult.java:35</c>,
/// <c>DeleteTopicsResult.java:34</c>, <c>DescribeTopicsResult.java:37</c>), published
/// <c>public</c> on the still-<see langword="sealed"/> types (D5), so a test or a mock can
/// fabricate a result. Pure managed; nothing native.
/// </summary>
public sealed class PublicAdminTopicResultConstructionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string BothSpecified = "topicIdFutures and nameFutures cannot both be specified.";

    private const string BothNull = "topicIdFutures and nameFutures cannot both be null.";

    /// <summary>
    /// Each type stays sealed and has exactly one public constructor, whose parameters are
    /// Java's names with the types the public accessors read (the accessor is the
    /// contract): <see cref="CreateTopicsResult"/>'s map is non-nullable, the other two
    /// take two nullable maps of which exactly one must be non-null.
    /// </summary>
    [Fact]
    public void Shape_OnePublicConstructorPerType_MirroringJava()
    {
        AssertOnlyConstructor(
            typeof(CreateTopicsResult),
            (typeof(IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>>), "futures", NullableAnnotation.NotAnnotated));

        AssertOnlyConstructor(
            typeof(DeleteTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, Task>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, Task>), "nameFutures", NullableAnnotation.Annotated));

        AssertOnlyConstructor(
            typeof(DescribeTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, Task<TopicDescription>>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, Task<TopicDescription>>), "nameFutures", NullableAnnotation.Annotated));

        // The parameter types are exactly what the typed accessors publish.
        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, Task>),
            typeof(DeleteTopicsResult).GetProperty(nameof(DeleteTopicsResult.TopicIdValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task>),
            typeof(DeleteTopicsResult).GetProperty(nameof(DeleteTopicsResult.TopicNameValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, Task<TopicDescription>>),
            typeof(DescribeTopicsResult).GetProperty(nameof(DescribeTopicsResult.TopicIdValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task<TopicDescription>>),
            typeof(DescribeTopicsResult).GetProperty(nameof(DescribeTopicsResult.TopicNameValues))!.PropertyType);
    }

    /// <summary>
    /// <see cref="CreateTopicsResult"/> rejects a null map up front — where Java stores it
    /// and throws <c>NullPointerException</c> on first use.
    /// </summary>
    [Fact]
    public void CreateTopicsResult_RejectsANullMap()
    {
        ArgumentNullException failure =
            Assert.Throws<ArgumentNullException>(() => new CreateTopicsResult(null!));
        Assert.Equal("futures", failure.ParamName);
    }

    /// <summary>
    /// Java's exactly-one check, with Java's two messages verbatim
    /// (<c>DeleteTopicsResult.java:35-38</c>, <c>DescribeTopicsResult.java:38-41</c>). An
    /// <see cref="ArgumentException"/> with no parameter name — neither argument is wrong
    /// on its own — so the message is the text alone, with no <c>(Parameter …)</c> suffix.
    /// </summary>
    [Fact]
    public void DeleteAndDescribe_RequireExactlyOneMap_WithJavasMessages()
    {
        Dictionary<Uuid, Task> deleteIds = new Dictionary<Uuid, Task>();
        Dictionary<string, Task> deleteNames = new Dictionary<string, Task>();
        Dictionary<Uuid, Task<TopicDescription>> describeIds = new Dictionary<Uuid, Task<TopicDescription>>();
        Dictionary<string, Task<TopicDescription>> describeNames = new Dictionary<string, Task<TopicDescription>>();

        foreach ((Action construct, string message) in new (Action, string)[]
        {
            (() => _ = new DeleteTopicsResult(deleteIds, deleteNames), BothSpecified),
            (() => _ = new DeleteTopicsResult(null, null), BothNull),
            (() => _ = new DescribeTopicsResult(describeIds, describeNames), BothSpecified),
            (() => _ = new DescribeTopicsResult(null, null), BothNull),
        })
        {
            ArgumentException failure = Assert.Throws<ArgumentException>(construct);
            Assert.Null(failure.ParamName);
            Assert.Equal(message, failure.Message);
        }

        // Either one alone is accepted — empty maps included.
        Assert.NotNull(new DeleteTopicsResult(deleteIds, null));
        Assert.NotNull(new DeleteTopicsResult(null, deleteNames));
        Assert.NotNull(new DescribeTopicsResult(describeIds, null));
        Assert.NotNull(new DescribeTopicsResult(null, describeNames));
    }

    /// <summary>
    /// A fabricated <see cref="CreateTopicsResult"/> behaves like a client-built one: the
    /// typed accessors project the supplied metadata, <see cref="CreateTopicsResult.Values"/>
    /// hands back the <b>same</b> task instances (erased to <see cref="Task"/>), and
    /// <see cref="CreateTopicsResult.All"/> completes when every one does and faults with a
    /// supplied failure.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_Fabricated_RoundTrips()
    {
        Uuid topicId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
        Task<TopicMetadataAndConfig> created = Task.FromResult(
            new TopicMetadataAndConfig(topicId, 3, 2, new Config(Array.Empty<ConfigEntry>())));

        CreateTopicsResult result = new CreateTopicsResult(
            new Dictionary<string, Task<TopicMetadataAndConfig>> { ["t"] = created });

        int numPartitions = 0;
        int replicationFactor = 0;
        Uuid reportedId = Uuid.Zero;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("t"), s_deadline);
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor("t"), s_deadline);
        await TestTimeout.Run(async () => reportedId = await result.TopicId("t"), s_deadline);
        Assert.Equal(3, numPartitions);
        Assert.Equal(2, replicationFactor);
        Assert.Equal(topicId, reportedId);

        Assert.Same(created, Assert.Single(result.Values).Value);
        await TestTimeout.Run(() => result.Values["t"], s_deadline);
        await TestTimeout.Run(() => result.All(), s_deadline);

        // A supplied failure faults its own task and All(), with the supplied instance.
        KafkaException fault = new KafkaException("fabricated failure");
        TaskCompletionSource<TopicMetadataAndConfig> failed =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        failed.SetException(fault);

        CreateTopicsResult mixed = new CreateTopicsResult(
            new Dictionary<string, Task<TopicMetadataAndConfig>> { ["ok"] = created, ["bad"] = failed.Task });

        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.Values["bad"], s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.All(), s_deadline)));
        await TestTimeout.Run(() => mixed.Values["ok"], s_deadline);
    }

    /// <summary>
    /// The caller's map is read live by <b>every</b> view (85.4). Java's <c>values()</c>,
    /// <c>all()</c> and the four accessors all read the stored <c>futures</c> map when they
    /// are called (<c>CreateTopicsResult.java:43-46</c>, <c>:51-53</c>, <c>:66</c>), so an
    /// entry the caller adds or replaces after construction shows up in all of them at
    /// once. <see cref="CreateTopicsResult.Values"/>, <see cref="CreateTopicsResult.All"/>
    /// and <see cref="CreateTopicsResult.NumPartitions"/> must agree after each mutation.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_CallerMapMutatedAfterConstruction_AllViewsAgree()
    {
        Dictionary<string, Task<TopicMetadataAndConfig>> map = new Dictionary<string, Task<TopicMetadataAndConfig>>();
        CreateTopicsResult result = new CreateTopicsResult(map);
        Assert.Empty(result.Values);

        // Added after construction, still pending: every view sees the same pending entry.
        TaskCompletionSource<TopicMetadataAndConfig> pending =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        map["t"] = pending.Task;

        KeyValuePair<string, Task> added = Assert.Single(result.Values);
        Assert.Equal("t", added.Key);
        Assert.Same(pending.Task, added.Value);
        Task all = result.All();
        Task<int> partitions = result.NumPartitions("t");
        Assert.False(all.IsCompleted);
        Assert.False(partitions.IsCompleted);

        pending.SetResult(Metadata(3));
        await TestTimeout.Run(() => all, s_deadline);
        Assert.Equal(3, await TestTimeout.Run(() => partitions, s_deadline));

        // Replaced after construction: every view moves to the replacement together.
        Task<TopicMetadataAndConfig> replacement = Task.FromResult(Metadata(5));
        map["t"] = replacement;

        Assert.Same(replacement, Assert.Single(result.Values).Value);
        Assert.Equal(5, await TestTimeout.Run(() => result.NumPartitions("t"), s_deadline));
        await TestTimeout.Run(() => result.All(), s_deadline);

        // A failing entry added later faults Values' entry, All() and the accessor alike.
        KafkaException fault = new KafkaException("fabricated late failure");
        TaskCompletionSource<TopicMetadataAndConfig> failed =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        failed.SetException(fault);
        map["late"] = failed.Task;

        Assert.Equal(new[] { "late", "t" }, result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["late"], s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.NumPartitions("late"), s_deadline)));
    }

    /// <summary>
    /// Each access of <see cref="CreateTopicsResult.Values"/> is a fresh snapshot of the
    /// caller's map as it is at that moment — Java's <c>values()</c> collects a new map on
    /// every call (<c>CreateTopicsResult.java:43-46</c>). So an earlier snapshot keeps the
    /// membership it was taken with, and a later one picks up both additions and removals.
    /// </summary>
    [Fact]
    public void CreateTopicsResult_Values_EachAccessReflectsTheMapAtThatMoment()
    {
        Task<TopicMetadataAndConfig> a = Task.FromResult(Metadata(1));
        Task<TopicMetadataAndConfig> b = Task.FromResult(Metadata(2));
        Dictionary<string, Task<TopicMetadataAndConfig>> map =
            new Dictionary<string, Task<TopicMetadataAndConfig>> { ["a"] = a };
        CreateTopicsResult result = new CreateTopicsResult(map);

        IReadOnlyDictionary<string, Task> first = result.Values;
        map["b"] = b;
        IReadOnlyDictionary<string, Task> second = result.Values;
        map.Remove("a");
        IReadOnlyDictionary<string, Task> third = result.Values;

        Assert.Same(a, Assert.Single(first).Value);
        Assert.False(first.ContainsKey("b"));

        Assert.Equal(2, second.Count);
        Assert.Same(a, second["a"]);
        Assert.Same(b, second["b"]);

        Assert.Same(b, Assert.Single(third).Value);
        Assert.False(third.ContainsKey("a"));
    }

    /// <summary>
    /// A caller map with a non-ordinal comparer gets the split Java has. Java's
    /// <c>values()</c> collects into a new map with <c>Collectors.toMap</c> (a
    /// <c>HashMap</c>, so lookup is by <c>String.equals</c>), while each typed accessor
    /// calls <c>futures.get(topic)</c> on the caller's map (<c>CreateTopicsResult.java:92</c>).
    /// So with a case-insensitive map, <see cref="CreateTopicsResult.Values"/> misses
    /// <c>"T"</c> for a topic stored as <c>"t"</c>, while
    /// <see cref="CreateTopicsResult.NumPartitions"/> finds it.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_CaseInsensitiveCallerMap_SplitsAsJavaDoes()
    {
        Task<TopicMetadataAndConfig> created = Task.FromResult(Metadata(3));
        Dictionary<string, Task<TopicMetadataAndConfig>> map =
            new Dictionary<string, Task<TopicMetadataAndConfig>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = created,
            };
        Assert.True(map.ContainsKey("T"));

        CreateTopicsResult result = new CreateTopicsResult(map);

        // Values: ordinal, like Java's HashMap — the stored spelling hits, the other misses.
        KeyValuePair<string, Task> entry = Assert.Single(result.Values);
        Assert.Equal("t", entry.Key);
        Assert.Same(created, entry.Value);
        Assert.False(result.Values.ContainsKey("T"));

        // The typed accessors: the caller's map, like Java's futures.get(topic).
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("T"), s_deadline));
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("t"), s_deadline));
    }

    /// <summary>
    /// A fabricated by-id <see cref="DeleteTopicsResult"/>: the map is held by reference,
    /// the by-name view is null, and <see cref="DeleteTopicsResult.All"/> faults with the
    /// supplied failure. A by-name one completes.
    /// </summary>
    [Fact]
    public async Task DeleteTopicsResult_Fabricated_RoundTrips()
    {
        KafkaException fault = new KafkaException("fabricated delete failure");
        TaskCompletionSource<bool> failed =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        failed.SetException(fault);

        Dictionary<Uuid, Task> byId = new Dictionary<Uuid, Task>
        {
            [new Uuid(1L, 1L)] = Task.CompletedTask,
            [new Uuid(2L, 2L)] = failed.Task,
        };

        DeleteTopicsResult result = new DeleteTopicsResult(byId, null);

        Assert.Same(byId, result.TopicIdValues);
        Assert.Null(result.TopicNameValues);
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All(), s_deadline)));

        DeleteTopicsResult byName = new DeleteTopicsResult(
            null, new Dictionary<string, Task> { ["t"] = Task.CompletedTask });
        Assert.Null(byName.TopicIdValues);
        await TestTimeout.Run(() => byName.All(), s_deadline);
    }

    /// <summary>
    /// A fabricated <see cref="DescribeTopicsResult"/>: the aggregate for the side that was
    /// supplied is returned, the other side's is null (Java's <c>all(null)</c>
    /// short-circuit), and a user-built result's aggregate is keyed with Java's own
    /// semantics — <b>ordinal</b> for names, even when the supplied dictionary is
    /// case-insensitive, as a Java <c>HashMap&lt;String, …&gt;</c> is.
    /// </summary>
    [Fact]
    public async Task DescribeTopicsResult_Fabricated_RoundTrips()
    {
        TopicDescription description = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());

        Dictionary<string, Task<TopicDescription>> byName =
            new Dictionary<string, Task<TopicDescription>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = Task.FromResult(description),
            };
        Assert.True(byName.ContainsKey("T"));

        DescribeTopicsResult result = new DescribeTopicsResult(null, byName);

        Assert.Same(byName, result.TopicNameValues);
        Assert.Null(result.TopicIdValues);
        Assert.Null(result.AllTopicIds());

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!, s_deadline);
        Assert.Same(description, Assert.Single(all).Value);
        Assert.True(all.ContainsKey("t"));
        Assert.False(all.ContainsKey("T"));

        Uuid topicId = new Uuid(7L, 9L);
        DescribeTopicsResult byIdResult = new DescribeTopicsResult(
            new Dictionary<Uuid, Task<TopicDescription>> { [topicId] = Task.FromResult(description) }, null);

        Assert.Null(byIdResult.AllTopicNames());
        IReadOnlyDictionary<Uuid, TopicDescription> allById = default!;
        await TestTimeout.Run(async () => allById = await byIdResult.AllTopicIds()!, s_deadline);
        Assert.Same(description, allById[topicId]);
        Assert.True(allById.ContainsKey(new Uuid(7L, 9L)));
    }

    /// <summary>
    /// A user-built <see cref="DescribeTopicsResult"/> whose map's comparer is finer than
    /// ordinal — reference equality, holding two distinct <c>"t"</c> instances — still
    /// yields a successful <see cref="DescribeTopicsResult.AllTopicNames"/>, with one entry
    /// carrying the last-enumerated value. That is Java's <c>all(Map)</c>, which fills a
    /// <c>HashMap</c> with <c>put</c> (<c>DescribeTopicsResult.java:105</c>): an equal key
    /// is replaced, never rejected. (M15/P13.2 finding 85.5.)
    /// </summary>
    [Fact]
    public async Task DescribeTopicsResult_TwoOrdinallyEqualNames_AggregateKeepsTheLastValue()
    {
        TopicDescription first = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());
        TopicDescription second = new TopicDescription("t", true, Array.Empty<TopicPartitionInfo>());

        string nameA = new string('t', 1);
        string nameB = new string('t', 1);
        Assert.NotSame(nameA, nameB);

        Dictionary<string, Task<TopicDescription>> byName =
            new Dictionary<string, Task<TopicDescription>>(ReferenceComparer.Instance)
            {
                [nameA] = Task.FromResult(first),
                [nameB] = Task.FromResult(second),
            };
        Assert.Equal(2, byName.Count);

        DescribeTopicsResult result = new DescribeTopicsResult(null, byName);

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!, s_deadline);

        TopicDescription lastEnumerated = await byName.Last().Value;
        KeyValuePair<string, TopicDescription> only = Assert.Single(all);
        Assert.Equal("t", only.Key);
        Assert.Same(lastEnumerated, only.Value);
    }

    /// <summary>
    /// Reference equality over strings, built by hand so it compiles on the net462 leg
    /// (<c>ReferenceEqualityComparer</c> is net5+).
    /// </summary>
    private sealed class ReferenceComparer : IEqualityComparer<string>
    {
        public static readonly ReferenceComparer Instance = new ReferenceComparer();

        public bool Equals(string? x, string? y) => ReferenceEquals(x, y);

        public int GetHashCode(string obj) => RuntimeHelpers.GetHashCode(obj);
    }

    private static TopicMetadataAndConfig Metadata(int numPartitions) =>
        new TopicMetadataAndConfig(Uuid.Zero, numPartitions, 1, new Config(Array.Empty<ConfigEntry>()));

    private static void AssertOnlyConstructor(Type type, params (Type Type, string Name, byte Flag)[] expected)
    {
        Assert.True(type.IsSealed, type.Name);

        ConstructorInfo constructor = Assert.Single(type.GetConstructors());
        ParameterInfo[] parameters = constructor.GetParameters();

        Assert.Equal(expected.Select(parameter => parameter.Type), parameters.Select(parameter => parameter.ParameterType));
        Assert.Equal(expected.Select(parameter => parameter.Name), parameters.Select(parameter => parameter.Name));
        Assert.Equal(
            expected.Select(parameter => parameter.Flag),
            parameters.Select(parameter => NullableAnnotation.Flag(parameter, 0)));
    }
}
