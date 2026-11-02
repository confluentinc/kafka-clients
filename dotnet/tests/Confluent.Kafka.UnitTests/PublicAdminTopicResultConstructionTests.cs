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

    [Fact]
    public async Task CreateTopicsResult_Fabricated_RoundTrips()
    {
        Uuid topicId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> created = KafkaFutureImpl.CompletedFuture(
            new CreateTopicsResult.TopicMetadataAndConfig(topicId, 3, 2, new Config(Array.Empty<ConfigEntry>())));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>
            {
                ["t"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(created),
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

        Assert.Same(created.GetAsync(), Assert.Single(result.Values).Value.Get());
        await TestTimeout.Run(() => result.Values["t"].Get(), s_deadline);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        KafkaException fault = new KafkaException("fabricated failure");
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> failed = TestFutures.Failed<CreateTopicsResult.TopicMetadataAndConfig>(fault);

        AsyncCreateTopicsResult mixed = new AsyncCreateTopicsResult(
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>
            {
                ["ok"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(created),
                ["bad"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(failed),
            });

        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.Values["bad"].Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
        await TestTimeout.Run(() => mixed.Values["ok"].Get(), s_deadline);
    }

    [Fact]
    public async Task CreateTopicsResult_CallerMapMutatedAfterConstruction_AllViewsAgree()
    {
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map = new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>();
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);
        Assert.Empty(result.Values);

        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> pending = new KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>();
        map["t"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(pending);

        KeyValuePair<string, AsyncKafkaFuture> added = Assert.Single(result.Values);
        Assert.Equal("t", added.Key);
        Assert.Same(pending.GetAsync(), added.Value.Get());
        Task all = result.All().Get();
        Task<int> partitions = result.NumPartitions("t").Get();
        Assert.False(all.IsCompleted);
        Assert.False(partitions.IsCompleted);

        Assert.True(pending.Complete(Metadata(3)));
        await TestTimeout.Run(() => all, s_deadline);
        Assert.Equal(3, await TestTimeout.Run(() => partitions, s_deadline));

        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> replacement = KafkaFutureImpl.CompletedFuture(Metadata(5));
        map["t"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(replacement);

        Assert.Same(replacement.GetAsync(), Assert.Single(result.Values).Value.Get());
        Assert.Equal(5, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        KafkaException fault = new KafkaException("fabricated late failure");
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> failed = TestFutures.Failed<CreateTopicsResult.TopicMetadataAndConfig>(fault);
        map["late"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(failed);

        Assert.Equal(new[] { "late", "t" }, result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["late"].Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.NumPartitions("late").Get(), s_deadline)));
    }

    [Fact]
    public void CreateTopicsResult_Values_EachAccessReflectsTheMapAtThatMoment()
    {
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> a = KafkaFutureImpl.CompletedFuture(Metadata(1));
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> b = KafkaFutureImpl.CompletedFuture(Metadata(2));
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> { ["a"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(a) };
        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        IReadOnlyDictionary<string, AsyncKafkaFuture> first = result.Values;
        map["b"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(b);
        IReadOnlyDictionary<string, AsyncKafkaFuture> second = result.Values;
        map.Remove("a");
        IReadOnlyDictionary<string, AsyncKafkaFuture> third = result.Values;

        Assert.Same(a.GetAsync(), Assert.Single(first).Value.Get());
        Assert.False(first.ContainsKey("b"));

        Assert.Equal(2, second.Count);
        Assert.Same(a.GetAsync(), second["a"].Get());
        Assert.Same(b.GetAsync(), second["b"].Get());

        Assert.Same(b.GetAsync(), Assert.Single(third).Value.Get());
        Assert.False(third.ContainsKey("a"));
    }

    [Fact]
    public async Task CreateTopicsResult_CaseInsensitiveCallerMap_SplitsAsJavaDoes()
    {
        KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> created = KafkaFutureImpl.CompletedFuture(Metadata(3));
        Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> map =
            new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(created),
            };
        Assert.True(map.ContainsKey("T"));

        AsyncCreateTopicsResult result = new AsyncCreateTopicsResult(map);

        // Values: ordinal, like Java's HashMap — the stored spelling hits, the other misses.
        KeyValuePair<string, AsyncKafkaFuture> entry = Assert.Single(result.Values);
        Assert.Equal("t", entry.Key);
        Assert.Same(created.GetAsync(), entry.Value.Get());
        Assert.False(result.Values.ContainsKey("T"));

        // The typed accessors: the caller's map, like Java's futures.get(topic).
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("T").Get(), s_deadline));
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("t").Get(), s_deadline));
    }

    [Fact]
    public async Task DeleteTopicsResult_Fabricated_RoundTrips()
    {
        KafkaException fault = new KafkaException("fabricated delete failure");
        KafkaFutureImpl<object?> failed = TestFutures.Failed(fault);

        Dictionary<Uuid, AsyncKafkaFuture> byId = new Dictionary<Uuid, AsyncKafkaFuture>
        {
            [new Uuid(1L, 1L)] = new AsyncKafkaFuture(TestFutures.Done()),
            [new Uuid(2L, 2L)] = new AsyncKafkaFuture(failed),
        };

        AsyncDeleteTopicsResult result = new AsyncDeleteTopicsResult(byId, null);

        Assert.Same(byId, result.TopicIdValues);
        Assert.Null(result.TopicNameValues);
        Assert.Same(fault, await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline)));

        AsyncDeleteTopicsResult byName = new AsyncDeleteTopicsResult(
            null, new Dictionary<string, AsyncKafkaFuture> { ["t"] = new AsyncKafkaFuture(TestFutures.Done()) });
        Assert.Null(byName.TopicIdValues);
        await TestTimeout.Run(() => byName.All().Get(), s_deadline);
    }

    [Fact]
    public async Task DescribeTopicsResult_Fabricated_RoundTrips()
    {
        TopicDescription description = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());

        Dictionary<string, AsyncKafkaFuture<TopicDescription>> byName =
            new Dictionary<string, AsyncKafkaFuture<TopicDescription>>(StringComparer.OrdinalIgnoreCase)
            {
                ["t"] = new AsyncKafkaFuture<TopicDescription>(KafkaFutureImpl.CompletedFuture(description)),
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
                [topicId] = new AsyncKafkaFuture<TopicDescription>(KafkaFutureImpl.CompletedFuture(description)),
            },
            null);

        Assert.Null(byIdResult.AllTopicNames());
        IReadOnlyDictionary<Uuid, TopicDescription> allById = default!;
        await TestTimeout.Run(async () => allById = await byIdResult.AllTopicIds()!.Value.Get(), s_deadline);
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
                [nameA] = new AsyncKafkaFuture<TopicDescription>(KafkaFutureImpl.CompletedFuture(first)),
                [nameB] = new AsyncKafkaFuture<TopicDescription>(KafkaFutureImpl.CompletedFuture(second)),
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

    [Fact]
    public void TopicMetadataAndConfig_FromAnError_EveryAccessorKeepsTheStoredIdentity()
    {
        const int Code = 7;
        const string Message = "Topic metadata unavailable: request timed out.";
        KafkaException stored = new KafkaException(Code, Message, isRetriable: true);
        CreateTopicsResult.TopicMetadataAndConfig metadata = new CreateTopicsResult.TopicMetadataAndConfig(stored);

        Assert.False(metadata.HasMetadata);

        (string Name, Action Call)[] accessors =
        {
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.Config), () => metadata.Config()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.TopicId), () => metadata.TopicId()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.NumPartitions), () => metadata.NumPartitions()),
            (nameof(CreateTopicsResult.TopicMetadataAndConfig.ReplicationFactor), () => metadata.ReplicationFactor()),
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

    private static CreateTopicsResult.TopicMetadataAndConfig Metadata(int numPartitions) =>
        new CreateTopicsResult.TopicMetadataAndConfig(Uuid.Zero, numPartitions, 1, new Config(Array.Empty<ConfigEntry>()));

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
