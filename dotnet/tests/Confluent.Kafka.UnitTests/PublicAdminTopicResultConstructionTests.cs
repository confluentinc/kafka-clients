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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The construction of the three topic results, whose constructors are internal where Java's are protected: a
/// recorded deviation. The round trips build the async twins, whose constructors are internal too. Nothing native.
/// </summary>
public sealed class PublicAdminTopicResultConstructionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string BothSpecified = "topicIdFutures and nameFutures cannot both be specified.";

    private const string BothNull = "topicIdFutures and nameFutures cannot both be null.";

    [Fact]
    public void Shape_OneConstructorPerType_MirroringJava()
    {
        AssertOnlyInternalConstructor(
            typeof(CreateTopicsResult),
            (typeof(IReadOnlyDictionary<string, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>), "futures", NullableAnnotation.NotAnnotated));

        AssertOnlyInternalConstructor(
            typeof(DeleteTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, KafkaFuture>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, KafkaFuture>), "nameFutures", NullableAnnotation.Annotated));

        AssertOnlyInternalConstructor(
            typeof(DescribeTopicsResult),
            (typeof(IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>), "topicIdFutures", NullableAnnotation.Annotated),
            (typeof(IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>), "nameFutures", NullableAnnotation.Annotated));

        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, KafkaFuture>),
            Getters.Get(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicIdValues)).ReturnType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture>),
            Getters.Get(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicNameValues)).ReturnType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>),
            Getters.Get(typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicIdValues)).ReturnType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>),
            Getters.Get(typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicNameValues)).ReturnType);
    }

    [Fact]
    public void CreateTopicsResult_RejectsANullMap()
    {
        ArgumentNullException failure =
            Assert.Throws<ArgumentNullException>(() => new CreateTopicsResult(null!));
        Assert.Equal("futures", failure.ParamName);
    }

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
            IllegalArgumentException failure = Assert.Throws<IllegalArgumentException>(construct);
            Assert.Null(failure.ParamName);
            Assert.Equal(message, failure.Message);
        }

        // Either one alone is accepted — empty maps included.
        Assert.NotNull(new DeleteTopicsResult(deleteIds, null));
        Assert.NotNull(new DeleteTopicsResult(null, deleteNames));
        Assert.NotNull(new DescribeTopicsResult(describeIds, null));
        Assert.NotNull(new DescribeTopicsResult(null, describeNames));
    }

    [Fact]
    public async Task CreateTopicsResult_Fabricated_RoundTrips()
    {
        Uuid topicId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> created = FutureState.CompletedFuture(
            new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>(),
            new CreateTopicsResult.TopicMetadataAndConfig(topicId, 3, 2, new Config(Array.Empty<ConfigEntry>())));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>
            {
                ["t"] = created,
            });

        int numPartitions = 0;
        int replicationFactor = 0;
        Uuid reportedId = Uuid.ZERO_UUID;
        await TestTimeout.Run(async () => numPartitions = await result.NumPartitions("t").Get(), s_deadline);
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor("t").Get(), s_deadline);
        await TestTimeout.Run(async () => reportedId = await result.TopicId("t").Get(), s_deadline);
        Assert.Equal(3, numPartitions);
        Assert.Equal(2, replicationFactor);
        Assert.Equal(topicId, reportedId);

        Assert.Same(created.Get(), Assert.Single(result.Values()).Value.Get());
        await TestTimeout.Run(() => result.Values()["t"].Get(), s_deadline);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        KafkaException fault = new KafkaException("fabricated failure");
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> failed = TestFutures.AsyncFailed<CreateTopicsResult.TopicMetadataAndConfig>(fault);

        AsyncCreateTopicsResult mixed = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>
            {
                ["ok"] = created,
                ["bad"] = failed,
            });

        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.Values()["bad"].Get(), s_deadline)));
        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
        await TestTimeout.Run(() => mixed.Values()["ok"].Get(), s_deadline);
    }

    [Fact]
    public async Task CreateTopicsResult_CallerMapMutatedAfterConstruction_AllViewsAgree()
    {
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map = new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>();
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);
        Assert.Empty(result.Values());

        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> pending = new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>();
        map["t"] = pending;

        KeyValuePair<string, AsyncKafkaFuture> added = Assert.Single(result.Values());
        Assert.Equal("t", added.Key);
        Assert.Same(pending.Get(), added.Value.Get());
        Task all = result.All().Get();
        Task<int> partitions = result.NumPartitions("t").Get();
        Assert.False(all.IsCompleted);
        Assert.False(partitions.IsCompleted);

        Assert.True(pending.Complete(Metadata(3)));
        await TestTimeout.Run(() => all, s_deadline);
        Assert.Equal(3, await TestTimeout.Run(() => partitions, s_deadline));

        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> replacement = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>(), Metadata(5));
        map["t"] = replacement;

        Assert.Same(replacement.Get(), Assert.Single(result.Values()).Value.Get());
        Assert.Equal(5, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        KafkaException fault = new KafkaException("fabricated late failure");
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> failed = TestFutures.AsyncFailed<CreateTopicsResult.TopicMetadataAndConfig>(fault);
        map["late"] = failed;

        Assert.Equal(new[] { "late", "t" }, result.Values().Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values()["late"].Get(), s_deadline)));
        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));
        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.NumPartitions("late").Get(), s_deadline)));
    }

    [Fact]
    public void CreateTopicsResult_Values_EachAccessReflectsTheMapAtThatMoment()
    {
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> a = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>(), Metadata(1));
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> b = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>(), Metadata(2));
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> { ["a"] = a };
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        IReadOnlyDictionary<string, AsyncKafkaFuture> first = result.Values();
        map["b"] = b;
        IReadOnlyDictionary<string, AsyncKafkaFuture> second = result.Values();
        map.Remove("a");
        IReadOnlyDictionary<string, AsyncKafkaFuture> third = result.Values();

        Assert.Same(a.Get(), Assert.Single(first).Value.Get());
        Assert.False(first.ContainsKey("b"));

        Assert.Equal(2, second.Count);
        Assert.Same(a.Get(), second["a"].Get());
        Assert.Same(b.Get(), second["b"].Get());

        Assert.Same(b.Get(), Assert.Single(third).Value.Get());
        Assert.False(third.ContainsKey("a"));
    }

    [Fact]
    public async Task CreateTopicsResult_CaseInsensitiveCallerMap_SplitsAsJavaDoes()
    {
        AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> created = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>(), Metadata(3));
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = created,
            };
        Assert.True(map.ContainsKey("T"));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        // Values: ordinal, like Java's HashMap — the stored spelling hits, the other misses.
        KeyValuePair<string, AsyncKafkaFuture> entry = Assert.Single(result.Values());
        Assert.Equal("t", entry.Key);
        Assert.Same(created.Get(), entry.Value.Get());
        Assert.False(result.Values().ContainsKey("T"));

        // The typed accessors: the caller's map, like Java's futures.get(topic).
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("T").Get(), s_deadline));
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
    }

    [Fact]
    public async Task DeleteTopicsResult_Fabricated_RoundTrips()
    {
        KafkaException fault = new KafkaException("fabricated delete failure");
        AsyncKafkaFutureImpl<object?> failed = TestFutures.AsyncFailed(fault);

        Dictionary<Uuid, AsyncKafkaFuture> byId = new Dictionary<Uuid, AsyncKafkaFuture>
        {
            [new Uuid(1L, 1L)] = TestFutures.AsyncDone(),
            [new Uuid(2L, 2L)] = failed,
        };

        AsyncDeleteTopicsResult result = new AsyncDeleteTopicsResult(byId, null);

        Assert.Same(byId, result.TopicIdValues());
        Assert.Null(result.TopicNameValues());
        Assert.Same(fault, await FutureAssert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));

        AsyncDeleteTopicsResult byName = new AsyncDeleteTopicsResult(
            null, new Dictionary<string, AsyncKafkaFuture> { ["t"] = TestFutures.AsyncDone() });
        Assert.Null(byName.TopicIdValues());
        await TestTimeout.Run(() => byName.All().Get(), s_deadline);
    }

    [Fact]
    public async Task DescribeTopicsResult_Fabricated_RoundTrips()
    {
        TopicDescription description = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());

        Dictionary<string, AsyncKafkaFuture<TopicDescription>> byName =
            new Dictionary<string, AsyncKafkaFuture<TopicDescription>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<TopicDescription>(), description),
            };
        Assert.True(byName.ContainsKey("T"));

        AsyncDescribeTopicsResult result = new AsyncDescribeTopicsResult(null, byName);

        Assert.Same(byName, result.TopicNameValues());
        Assert.Null(result.TopicIdValues());
        Assert.Null(result.AllTopicIds());

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!.Get(), s_deadline);
        Assert.Same(description, Assert.Single(all).Value);
        Assert.True(all.ContainsKey("t"));
        Assert.False(all.ContainsKey("T"));

        Uuid topicId = new Uuid(7L, 9L);
        AsyncDescribeTopicsResult byIdResult = new AsyncDescribeTopicsResult(
            new Dictionary<Uuid, AsyncKafkaFuture<TopicDescription>>
            {
                [topicId] = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<TopicDescription>(), description),
            },
            null);

        Assert.Null(byIdResult.AllTopicNames());
        IReadOnlyDictionary<Uuid, TopicDescription> allById = default!;
        await TestTimeout.Run(async () => allById = await byIdResult.AllTopicIds()!.Get(), s_deadline);
        Assert.Same(description, allById[topicId]);
        Assert.True(allById.ContainsKey(new Uuid(7L, 9L)));
    }

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
                [nameA] = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<TopicDescription>(), first),
                [nameB] = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<TopicDescription>(), second),
            };
        Assert.Equal(2, byName.Count);

        AsyncDescribeTopicsResult result = new AsyncDescribeTopicsResult(null, byName);

        IReadOnlyDictionary<string, TopicDescription> all = default!;
        await TestTimeout.Run(async () => all = await result.AllTopicNames()!.Get(), s_deadline);

        TopicDescription lastEnumerated = await byName.Last().Value.Get();
        KeyValuePair<string, TopicDescription> only = Assert.Single(all);
        Assert.Equal("t", only.Key);
        Assert.Same(lastEnumerated, only.Value);
    }

    [Fact]
    public void TopicMetadataAndConfig_FromAnError_EveryAccessorThrowsTheStoredException()
    {
        const string Message = "Topic metadata unavailable: request timed out.";
        Common.Errors.TimeoutException stored = new Common.Errors.TimeoutException(Message);
        CreateTopicsResult.TopicMetadataAndConfig metadata = new CreateTopicsResult.TopicMetadataAndConfig(stored);

        (string Name, Action Call)[] accessors =
        {
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.Config), () => metadata.Config()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.TopicId), () => metadata.TopicId()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.NumPartitions), () => metadata.NumPartitions()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.ReplicationFactor), () => metadata.ReplicationFactor()),
        };

        foreach ((string name, Action call) in accessors)
        {
            Common.Errors.TimeoutException thrown = Assert.Throws<Common.Errors.TimeoutException>(call);
            Assert.True(ReferenceEquals(stored, thrown), name);
            Assert.Equal(Message, thrown.Message);
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

    private static CreateTopicsResult.TopicMetadataAndConfig Metadata(int numPartitions) =>
        new CreateTopicsResult.TopicMetadataAndConfig(Uuid.ZERO_UUID, numPartitions, 1, new Config(Array.Empty<ConfigEntry>()));

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
