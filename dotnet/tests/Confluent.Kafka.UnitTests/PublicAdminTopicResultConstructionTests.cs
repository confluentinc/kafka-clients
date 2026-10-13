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
/// The construction of the three topic results — Java's <c>protected</c> constructors
/// (<c>CreateTopicsResult.java:35</c>, <c>DeleteTopicsResult.java:34</c>,
/// <c>DescribeTopicsResult.java:37</c>). All three became <c>internal</c> when they moved
/// to the sync flavour (M15/P14 S4, Q3 (b)): a recorded Java-parity deviation, follow-up
/// FU-5. The fabrication round-trips below therefore build the async twins, whose
/// constructors are internal too. Pure managed; nothing native.
/// </summary>
public sealed class PublicAdminTopicResultConstructionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string BothSpecified = "topicIdFutures and nameFutures cannot both be specified.";

    private const string BothNull = "topicIdFutures and nameFutures cannot both be null.";

    /// <summary>
    /// Each type stays sealed, has no public constructor, and has exactly one
    /// <c>internal</c> one (M15/P14 S4, Q3 (b), follow-up FU-5), whose parameters are Java's
    /// names with the types the accessors read (the accessor is the contract):
    /// <see cref="CreateTopicsResult"/>'s map is non-nullable, the other two take two
    /// nullable maps of which exactly one must be non-null. Asserted as internal, so a
    /// regression that re-publishes one turns this red.
    /// </summary>
    [Fact]
    public void Shape_OneConstructorPerType_MirroringJava()
    {
        AssertOnlyInternalConstructor(
            typeof(CreateTopicsResult),
            (typeof(IReadOnlyDictionary<string, KafkaFuture<TopicMetadataAndConfig>>), "futures", NullableAnnotation.NotAnnotated));

        AssertOnlyInternalConstructor(
            typeof(DeleteTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, KafkaFuture>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, KafkaFuture>), "nameFutures", NullableAnnotation.Annotated));

        AssertOnlyInternalConstructor(
            typeof(DescribeTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>), "nameFutures", NullableAnnotation.Annotated));

        // The parameter types are exactly what the typed accessors publish.
        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, KafkaFuture>),
            typeof(DeleteTopicsResult).GetProperty(nameof(DeleteTopicsResult.TopicIdValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture>),
            typeof(DeleteTopicsResult).GetProperty(nameof(DeleteTopicsResult.TopicNameValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>),
            typeof(DescribeTopicsResult).GetProperty(nameof(DescribeTopicsResult.TopicIdValues))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>),
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
        Dictionary<Uuid, KafkaFuture> deleteIds = new Dictionary<Uuid, KafkaFuture>();
        Dictionary<string, KafkaFuture> deleteNames = new Dictionary<string, KafkaFuture>();
        Dictionary<Uuid, KafkaFuture<TopicDescription>> describeIds = new Dictionary<Uuid, KafkaFuture<TopicDescription>>();
        Dictionary<string, KafkaFuture<TopicDescription>> describeNames =
            new Dictionary<string, KafkaFuture<TopicDescription>>();

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
    /// A fabricated <see cref="AsyncCreateTopicsResult"/> behaves like a client-built one: the
    /// typed accessors project the supplied metadata, <see cref="AsyncCreateTopicsResult.Values"/>
    /// hands back the <b>same</b> task instances (erased to <see cref="Task"/>), and
    /// <see cref="AsyncCreateTopicsResult.All"/> completes when every one does and faults with a
    /// supplied failure.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_Fabricated_RoundTrips()
    {
        Uuid topicId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
        Task<TopicMetadataAndConfig> created = Task.FromResult(
            new TopicMetadataAndConfig(topicId, 3, 2, new Config(Array.Empty<ConfigEntry>())));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>>
            {
                ["t"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(created),
            });

        int numPartitions = 0;
        int replicationFactor = 0;
        Uuid reportedId = Uuid.Zero;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("t").Get(), s_deadline);
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor("t").Get(), s_deadline);
        await TestTimeout.Run(async () => reportedId = await result.TopicId("t").Get(), s_deadline);
        Assert.Equal(3, numPartitions);
        Assert.Equal(2, replicationFactor);
        Assert.Equal(topicId, reportedId);

        Assert.Same(created, Assert.Single(result.Values).Value.Get());
        await TestTimeout.Run(() => result.Values["t"].Get(), s_deadline);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        // A supplied failure faults its own task and All(), with the supplied instance.
        KafkaException fault = new KafkaException("fabricated failure");
        TaskCompletionSource<TopicMetadataAndConfig> failed =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        failed.SetException(fault);

        AsyncCreateTopicsResult mixed = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>>
            {
                ["ok"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(created),
                ["bad"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(failed.Task),
            });

        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.Values["bad"].Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
        await TestTimeout.Run(() => mixed.Values["ok"].Get(), s_deadline);
    }

    /// <summary>
    /// The caller's map is read live by <b>every</b> view (85.4). Java's <c>values()</c>,
    /// <c>all()</c> and the four accessors all read the stored <c>futures</c> map when they
    /// are called (<c>CreateTopicsResult.java:43-46</c>, <c>:51-53</c>, <c>:66</c>), so an
    /// entry the caller adds or replaces after construction shows up in all of them at
    /// once. <see cref="AsyncCreateTopicsResult.Values"/>, <see cref="AsyncCreateTopicsResult.All"/>
    /// and <see cref="AsyncCreateTopicsResult.NumPartitions"/> must agree after each mutation.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_CallerMapMutatedAfterConstruction_AllViewsAgree()
    {
        Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>> map = new Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>>();
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);
        Assert.Empty(result.Values);

        // Added after construction, still pending: every view sees the same pending entry.
        TaskCompletionSource<TopicMetadataAndConfig> pending =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        map["t"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(pending.Task);

        KeyValuePair<string, AsyncKafkaFuture> added = Assert.Single(result.Values);
        Assert.Equal("t", added.Key);
        Assert.Same(pending.Task, added.Value.Get());
        Task all = result.All().Get();
        Task<int> partitions = result.NumPartitions("t").Get();
        Assert.False(all.IsCompleted);
        Assert.False(partitions.IsCompleted);

        pending.SetResult(Metadata(3));
        await TestTimeout.Run(() => all, s_deadline);
        Assert.Equal(3, await TestTimeout.Run(() => partitions, s_deadline));

        // Replaced after construction: every view moves to the replacement together.
        Task<TopicMetadataAndConfig> replacement = Task.FromResult(Metadata(5));
        map["t"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(replacement);

        Assert.Same(replacement, Assert.Single(result.Values).Value.Get());
        Assert.Equal(5, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        // A failing entry added later faults Values' entry, All() and the accessor alike.
        KafkaException fault = new KafkaException("fabricated late failure");
        TaskCompletionSource<TopicMetadataAndConfig> failed =
            new TaskCompletionSource<TopicMetadataAndConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        failed.SetException(fault);
        map["late"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(failed.Task);

        Assert.Equal(new[] { "late", "t" }, result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["late"].Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.NumPartitions("late").Get(), s_deadline)));
    }

    /// <summary>
    /// Each access of <see cref="AsyncCreateTopicsResult.Values"/> is a fresh snapshot of the
    /// caller's map as it is at that moment — Java's <c>values()</c> collects a new map on
    /// every call (<c>CreateTopicsResult.java:43-46</c>). So an earlier snapshot keeps the
    /// membership it was taken with, and a later one picks up both additions and removals.
    /// </summary>
    [Fact]
    public void CreateTopicsResult_Values_EachAccessReflectsTheMapAtThatMoment()
    {
        Task<TopicMetadataAndConfig> a = Task.FromResult(Metadata(1));
        Task<TopicMetadataAndConfig> b = Task.FromResult(Metadata(2));
        Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>> { ["a"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(a) };
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        IReadOnlyDictionary<string, AsyncKafkaFuture> first = result.Values;
        map["b"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(b);
        IReadOnlyDictionary<string, AsyncKafkaFuture> second = result.Values;
        map.Remove("a");
        IReadOnlyDictionary<string, AsyncKafkaFuture> third = result.Values;

        Assert.Same(a, Assert.Single(first).Value.Get());
        Assert.False(first.ContainsKey("b"));

        Assert.Equal(2, second.Count);
        Assert.Same(a, second["a"].Get());
        Assert.Same(b, second["b"].Get());

        Assert.Same(b, Assert.Single(third).Value.Get());
        Assert.False(third.ContainsKey("a"));
    }

    /// <summary>
    /// A caller map with a non-ordinal comparer gets the split Java has. Java's
    /// <c>values()</c> collects into a new map with <c>Collectors.toMap</c> (a
    /// <c>HashMap</c>, so lookup is by <c>String.equals</c>), while each typed accessor
    /// calls <c>futures.get(topic)</c> on the caller's map (<c>CreateTopicsResult.java:92</c>).
    /// So with a case-insensitive map, <see cref="AsyncCreateTopicsResult.Values"/> misses
    /// <c>"T"</c> for a topic stored as <c>"t"</c>, while
    /// <see cref="AsyncCreateTopicsResult.NumPartitions"/> finds it.
    /// </summary>
    [Fact]
    public async Task CreateTopicsResult_CaseInsensitiveCallerMap_SplitsAsJavaDoes()
    {
        Task<TopicMetadataAndConfig> created = Task.FromResult(Metadata(3));
        Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<TopicMetadataAndConfig>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = new AsyncKafkaFuture<TopicMetadataAndConfig>(created),
            };
        Assert.True(map.ContainsKey("T"));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        // Values: ordinal, like Java's HashMap — the stored spelling hits, the other misses.
        KeyValuePair<string, AsyncKafkaFuture> entry = Assert.Single(result.Values);
        Assert.Equal("t", entry.Key);
        Assert.Same(created, entry.Value.Get());
        Assert.False(result.Values.ContainsKey("T"));

        // The typed accessors: the caller's map, like Java's futures.get(topic).
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("T").Get(), s_deadline));
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
    }

    /// <summary>
    /// A fabricated by-id <see cref="AsyncDeleteTopicsResult"/>: the map is held by reference,
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

        Dictionary<Uuid, AsyncKafkaFuture> byId = new Dictionary<Uuid, AsyncKafkaFuture>
        {
            [new Uuid(1L, 1L)] = new AsyncKafkaFuture(Task.CompletedTask),
            [new Uuid(2L, 2L)] = new AsyncKafkaFuture(failed.Task),
        };

        AsyncDeleteTopicsResult result = new AsyncDeleteTopicsResult(byId, null);

        Assert.Same(byId, result.TopicIdValues);
        Assert.Null(result.TopicNameValues);
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));

        AsyncDeleteTopicsResult byName = new AsyncDeleteTopicsResult(
            null, new Dictionary<string, AsyncKafkaFuture> { ["t"] = new AsyncKafkaFuture(Task.CompletedTask) });
        Assert.Null(byName.TopicIdValues);
        await TestTimeout.Run(() => byName.All().Get(), s_deadline);
    }

    /// <summary>
    /// A fabricated <see cref="AsyncDescribeTopicsResult"/>: the aggregate for the side that was
    /// supplied is returned, the other side's is null (Java's <c>all(null)</c>
    /// short-circuit), and a user-built result's aggregate is keyed with Java's own
    /// semantics — <b>ordinal</b> for names, even when the supplied dictionary is
    /// case-insensitive, as a Java <c>HashMap&lt;String, …&gt;</c> is.
    /// </summary>
    [Fact]
    public async Task DescribeTopicsResult_Fabricated_RoundTrips()
    {
        TopicDescription description = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());

        Dictionary<string, AsyncKafkaFuture<TopicDescription>> byName =
            new Dictionary<string, AsyncKafkaFuture<TopicDescription>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = new AsyncKafkaFuture<TopicDescription>(Task.FromResult(description)),
            };
        Assert.True(byName.ContainsKey("T"));

        AsyncDescribeTopicsResult result = new AsyncDescribeTopicsResult(null, byName);

        Assert.Same(byName, result.TopicNameValues);
        Assert.Null(result.TopicIdValues);
        Assert.Null(result.AllTopicIds());

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!.Value.Get(), s_deadline);
        Assert.Same(description, Assert.Single(all).Value);
        Assert.True(all.ContainsKey("t"));
        Assert.False(all.ContainsKey("T"));

        Uuid topicId = new Uuid(7L, 9L);
        AsyncDescribeTopicsResult byIdResult = new AsyncDescribeTopicsResult(
            new Dictionary<Uuid, AsyncKafkaFuture<TopicDescription>>
            {
                [topicId] = new AsyncKafkaFuture<TopicDescription>(Task.FromResult(description)),
            },
            null);

        Assert.Null(byIdResult.AllTopicNames());
        IReadOnlyDictionary<Uuid, TopicDescription> allById = default!;
        await TestTimeout.Run(async () => allById = await byIdResult.AllTopicIds()!.Value.Get(), s_deadline);
        Assert.Same(description, allById[topicId]);
        Assert.True(allById.ContainsKey(new Uuid(7L, 9L)));
    }

    /// <summary>
    /// A user-built <see cref="AsyncDescribeTopicsResult"/> whose map's comparer is finer than
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

        Dictionary<string, AsyncKafkaFuture<TopicDescription>> byName =
            new Dictionary<string, AsyncKafkaFuture<TopicDescription>>(ReferenceComparer.Instance)
            {
                [nameA] = new AsyncKafkaFuture<TopicDescription>(Task.FromResult(first)),
                [nameB] = new AsyncKafkaFuture<TopicDescription>(Task.FromResult(second)),
            };
        Assert.Equal(2, byName.Count);

        AsyncDescribeTopicsResult result = new AsyncDescribeTopicsResult(null, byName);

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!.Value.Get(), s_deadline);

        TopicDescription lastEnumerated = await byName.Last().Value.Get();
        KeyValuePair<string, TopicDescription> only = Assert.Single(all);
        Assert.Equal("t", only.Key);
        Assert.Same(lastEnumerated, only.Value);
    }

    /// <summary>
    /// A <see cref="TopicMetadataAndConfig"/> built from an error throws, from every
    /// accessor, a <see cref="KafkaException"/> that keeps the stored error's identity
    /// fields — Java rethrows the stored exception itself (<c>CreateTopicsResult.java:151-154</c>),
    /// so a caller branching on the code or the retriable flag must see the stored values.
    /// The stored error has a <b>non-zero</b> code and is retriable, so neither assertion
    /// can pass against a default <see cref="KafkaException"/> (code 0, not retriable).
    /// </summary>
    [Fact]
    public void TopicMetadataAndConfig_FromAnError_EveryAccessorKeepsTheStoredIdentity()
    {
        const int Code = 7;
        const string Message = "Topic metadata unavailable: request timed out.";
        KafkaException stored = new KafkaException(Code, Message, isRetriable: true);
        TopicMetadataAndConfig metadata = new TopicMetadataAndConfig(stored);

        Assert.False(metadata.HasMetadata);

        (string Name, Action Call)[] accessors =
        {
            (nameof(TopicMetadataAndConfig.Config), () => metadata.Config()),
            (nameof(TopicMetadataAndConfig.TopicId), () => metadata.TopicId()),
            (nameof(TopicMetadataAndConfig.NumPartitions), () => metadata.NumPartitions()),
            (nameof(TopicMetadataAndConfig.ReplicationFactor), () => metadata.ReplicationFactor()),
        };

        foreach ((string name, Action call) in accessors)
        {
            KafkaException thrown = Assert.Throws<KafkaException>(call);
            Assert.True(Code == thrown.Code, name);
            Assert.True(thrown.IsRetriable, name);
            Assert.Equal(Message, thrown.Message);
            Assert.Same(stored, thrown.InnerException);
            Assert.NotSame(stored, thrown);
        }
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

    /// <summary>
    /// No public constructor at all, and exactly one <c>internal</c> one
    /// (<see cref="MethodBase.IsAssembly"/>) with the expected parameters. A <c>private</c>
    /// constructor body behind it (<see cref="DescribeTopicsResult"/> has one, taking the
    /// aggregate's comparers) is not part of the shape.
    /// </summary>
    private static void AssertOnlyInternalConstructor(Type type, params (Type Type, string Name, byte Flag)[] expected)
    {
        Assert.True(type.IsSealed, type.Name);
        Assert.Empty(type.GetConstructors());

        ConstructorInfo constructor = Assert.Single(
            type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);

        AssertParameters(constructor, expected);
    }

    private static void AssertParameters(ConstructorInfo constructor, (Type Type, string Name, byte Flag)[] expected)
    {
        ParameterInfo[] parameters = constructor.GetParameters();

        Assert.Equal(expected.Select(parameter => parameter.Type), parameters.Select(parameter => parameter.ParameterType));
        Assert.Equal(expected.Select(parameter => parameter.Name), parameters.Select(parameter => parameter.Name));
        Assert.Equal(
            expected.Select(parameter => parameter.Flag),
            parameters.Select(parameter => NullableAnnotation.Flag(parameter, 0)));
    }
}
