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
/// <see cref="ListConsumerGroupOffsetsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult</c>: no per-key map property, the two
/// <c>PartitionsToOffsetAndMetadata</c> overloads with the no-argument one's exactly-one-group precondition, and a
/// partition with no committed offset present with a null value.
/// </summary>
public sealed class PublicAdminListConsumerGroupOffsetsResultTests
{
    private static readonly TopicPartition s_committed = new TopicPartition("orders", 0);
    private static readonly TopicPartition s_uncommitted = new TopicPartition("orders", 1);

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata?>
        {
            [s_committed] = new OffsetAndMetadata(offset, "m"),

            // Present with a null value: the group committed nothing for this partition.
            [s_uncommitted] = null,
        };

    private static KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Completed(long offset)
    {
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future = Pending();
        future.Complete(Offsets(offset));
        return future;
    }

    private static KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Pending() =>
        new KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>();

    private static AsyncListConsumerGroupOffsetsResult Result(
        params (string GroupId, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Future)[] entries)
    {
        Dictionary<string, AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures =
            new Dictionary<string, AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(
                StringComparer.Ordinal);
        foreach ((string groupId, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future) in entries)
        {
            futures.Add(groupId, new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(future));
        }

        return new AsyncListConsumerGroupOffsetsResult(futures);
    }

    [Fact]
    public void Constructor_IsInternalMirroringJavasPackagePrivateOne()
    {
        Assert.Empty(typeof(ListConsumerGroupOffsetsResult).GetConstructors());

        ConstructorInfo constructor = Assert.Single(
            typeof(ListConsumerGroupOffsetsResult).GetConstructors(
                BindingFlags.NonPublic | BindingFlags.Instance),
            c => !c.IsPrivate);
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>),
            Assert.Single(constructor.GetParameters()).ParameterType);
    }

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

        Assert.Empty(
            typeof(ListConsumerGroupOffsetsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        Assert.Null(typeof(ListConsumerGroupOffsetsResult).GetCustomAttribute<ObsoleteAttribute>());
    }

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
                typeof(KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>),
                overload.ReturnType);
        }
    }

    [Fact]
    public void All_YieldsAMapOfGroupIdToPartitionOffsets()
    {
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>),
            typeof(ListConsumerGroupOffsetsResult)
                .GetMethod(nameof(ListConsumerGroupOffsetsResult.All))!
                .ReturnType);
    }

    [Fact]
    public async Task PartitionsToOffsetAndMetadata_NoArgument_YieldsTheOnlyGroup()
    {
        AsyncListConsumerGroupOffsetsResult result = Result(("g", Completed(7)));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await result.PartitionsToOffsetAndMetadata().Get();

        Assert.Equal(7L, offsets[s_committed]!.Offset);
        Assert.Same(result.PartitionsToOffsetAndMetadata("g").Get(), result.PartitionsToOffsetAndMetadata().Get());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    [InlineData(3)]
    public void PartitionsToOffsetAndMetadata_NoArgument_RejectsAnyCountButOne(int groupCount)
    {
        AsyncListConsumerGroupOffsetsResult result = Result(
            Enumerable
                .Range(0, groupCount)
                .Select(index => (GroupId: "g" + index, Future: Completed(index)))
                .ToArray());

        // Block-bodied, so this binds xUnit's Action overload and asserts a synchronous throw, not a faulted task.
        InvalidOperationException error =
            Assert.Throws<InvalidOperationException>(() => { _ = result.PartitionsToOffsetAndMetadata(); });
        Assert.Equal(
            "Offsets from multiple consumer groups were requested. " +
            "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.",
            error.Message);
    }

    [Fact]
    public async Task PartitionsToOffsetAndMetadata_ByGroupId_IsPerGroup()
    {
        AsyncListConsumerGroupOffsetsResult result = Result(("a", Completed(1)), ("b", Completed(2)));

        Assert.Equal(1L, (await result.PartitionsToOffsetAndMetadata("a").Get())[s_committed]!.Offset);
        Assert.Equal(2L, (await result.PartitionsToOffsetAndMetadata("b").Get())[s_committed]!.Offset);
    }

    [Fact]
    public void PartitionsToOffsetAndMetadata_ByGroupId_RejectsAnUnrequestedGroupSynchronously()
    {
        AsyncListConsumerGroupOffsetsResult result = Result(("a", Completed(1)));

        // Block-bodied, so this asserts a synchronous throw, not a faulted task.
        ArgumentException error =
            Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("missing"); });
        Assert.Equal("groupId", error.ParamName);
        Assert.Contains("Offsets for consumer group 'missing' were not requested.", error.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("A"); });

        ArgumentNullException nullError =
            Assert.Throws<ArgumentNullException>(() => { _ = result.PartitionsToOffsetAndMetadata(null!); });
        Assert.Equal("groupId", nullError.ParamName);
    }

    [Fact]
    public async Task UncommittedPartition_IsPresentWithANullValueRatherThanAbsent()
    {
        AsyncListConsumerGroupOffsetsResult result = Result(("g", Completed(5)));

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets =
            await result.PartitionsToOffsetAndMetadata().Get();

        Assert.True(offsets.ContainsKey(s_uncommitted));
        Assert.Null(offsets[s_uncommitted]);

        Assert.False(offsets.ContainsKey(new TopicPartition("orders", 99)));
    }

    [Fact]
    public async Task All_GathersEveryGroupOrdinallyAndIsFreshPerCall()
    {
        AsyncListConsumerGroupOffsetsResult result = Result(("a", Completed(1)), ("A", Completed(2)));

        Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> first =
            result.All().Get();
        Assert.NotSame(first, result.All().Get());

        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> all = await first;

        Assert.Equal(2, all.Count);
        Assert.Equal(1L, all["a"][s_committed]!.Offset);
        Assert.Equal(2L, all["A"][s_committed]!.Offset);
        Assert.Null(all["a"][s_uncommitted]);
    }

    [Fact]
    public async Task PerGroupFailure_FaultsOnlyThatGroupAndTheAggregate()
    {
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> broken = Pending();
        AsyncListConsumerGroupOffsetsResult result = Result(("ok", Completed(1)), ("bad", broken));

        KafkaException failure = new KafkaException("nope");
        broken.CompleteWithError(failure);

        Assert.Equal(1L, (await result.PartitionsToOffsetAndMetadata("ok").Get())[s_committed]!.Offset);

        Assert.Same(
            failure,
            await Assert.ThrowsAsync<KafkaException>(() => result.PartitionsToOffsetAndMetadata("bad").Get()));
        Assert.Same(failure, await Assert.ThrowsAsync<KafkaException>(() => result.All().Get()));
    }

    [Fact]
    public async Task All_WaitsForEveryGroupBeforeItFaults()
    {
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> broken = Pending();
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> slow = Pending();
        AsyncListConsumerGroupOffsetsResult result = Result(("bad", broken), ("slow", slow));

        Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> all = result.All().Get();

        broken.CompleteWithError(new KafkaException("gone"));
        Assert.False(all.IsCompleted);

        slow.Complete(Offsets(3));
        await Assert.ThrowsAsync<KafkaException>(() => all);
    }

    [Fact]
    public async Task EmptyResult_AggregatesToAnEmptyMap()
    {
        AsyncListConsumerGroupOffsetsResult result = Result();

        Assert.Empty(await result.All().Get());
        Assert.Throws<InvalidOperationException>(() => { _ = result.PartitionsToOffsetAndMetadata(); });
        Assert.Throws<ArgumentException>(() => { _ = result.PartitionsToOffsetAndMetadata("g"); });
    }
}
