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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Quota;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public constructors of the five admin results Java declares public, and of their async peers: a result built
/// from the user's futures answers from them, a later change to the user's map does not reach it, and a null argument
/// is rejected. Nothing native.
/// </summary>
public sealed class PublicAdminResultConstructorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_p0 = new TopicPartition("t", 0);

    private static readonly TopicPartition s_p1 = new TopicPartition("t", 1);

    private static readonly ClientQuotaEntity s_alice = Entity("alice");

    private static readonly ClientQuotaEntity s_bob = Entity("bob");

    public static TheoryData<Type, Type, string> Constructors => new TheoryData<Type, Type, string>
    {
        { typeof(AlterClientQuotasResult), typeof(IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture>), "futures" },
        { typeof(AsyncAlterClientQuotasResult), typeof(IReadOnlyDictionary<ClientQuotaEntity, AsyncKafkaFuture>), "futures" },
        { typeof(AlterUserScramCredentialsResult), typeof(IReadOnlyDictionary<string, KafkaFuture>), "futures" },
        { typeof(AsyncAlterUserScramCredentialsResult), typeof(IReadOnlyDictionary<string, AsyncKafkaFuture>), "futures" },
        { typeof(DeleteRecordsResult), typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>>), "futures" },
        { typeof(AsyncDeleteRecordsResult), typeof(IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>>), "futures" },
        {
            typeof(DescribeClientQuotasResult),
            typeof(KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>),
            "entities"
        },
        {
            typeof(AsyncDescribeClientQuotasResult),
            typeof(AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>),
            "entities"
        },
        {
            typeof(ListOffsetsResult),
            typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>),
            "futures"
        },
        {
            typeof(AsyncListOffsetsResult),
            typeof(IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>),
            "futures"
        },
    };

    [Theory]
    [MemberData(nameof(Constructors))]
    public void Constructor_IsJavasOnePublicConstructor(Type result, Type parameterType, string parameterName)
    {
        ConstructorInfo constructor = Assert.Single(result.GetConstructors(BindingFlags.Instance | BindingFlags.Public));
        ParameterInfo parameter = Assert.Single(constructor.GetParameters());
        Assert.Equal(parameterType, parameter.ParameterType);
        Assert.Equal(parameterName, parameter.Name);
    }

    [Fact]
    public void Constructor_RejectsANullArgument_NamingIt()
    {
        foreach ((Action construct, string parameterName) in new (Action, string)[]
        {
            (() => _ = new AlterClientQuotasResult(null!), "futures"),
            (() => _ = new AsyncAlterClientQuotasResult(null!), "futures"),
            (() => _ = new AlterUserScramCredentialsResult(null!), "futures"),
            (() => _ = new AsyncAlterUserScramCredentialsResult(null!), "futures"),
            (() => _ = new DeleteRecordsResult(null!), "futures"),
            (() => _ = new AsyncDeleteRecordsResult(null!), "futures"),
            (() => _ = new DescribeClientQuotasResult(default), "entities"),
            (() => _ = new AsyncDescribeClientQuotasResult(default), "entities"),
            (() => _ = new ListOffsetsResult(null!), "futures"),
            (() => _ = new AsyncListOffsetsResult(null!), "futures"),
        })
        {
            ArgumentNullException failure = Assert.Throws<ArgumentNullException>(construct);
            Assert.Equal(parameterName, failure.ParamName);
            Assert.Equal(new ArgumentNullException(parameterName).Message, failure.Message);
        }
    }

    [Fact]
    public void AlterClientQuotasResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("bob failed");
        KafkaFuture done = KafkaFuture.CompletedFuture<object?>(null);
        KafkaFuture failed = new KafkaFuture(TestFutures.Failed(failure));
        Dictionary<ClientQuotaEntity, KafkaFuture> source = new Dictionary<ClientQuotaEntity, KafkaFuture> { [s_alice] = done };

        AlterClientQuotasResult result = new AlterClientQuotasResult(source);
        source[s_bob] = failed;
        source.Remove(s_alice);

        Assert.NotSame(source, result.Values());
        AssertOnly(result.Values(), s_alice, done);
        result.All().Get(s_deadline);

        AlterClientQuotasResult mixed = new AlterClientQuotasResult(
            new Dictionary<ClientQuotaEntity, KafkaFuture> { [s_alice] = done, [s_bob] = failed });
        mixed.Values()[s_alice].Get(s_deadline);
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.Values()[s_bob].Get(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.All().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncAlterClientQuotasResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("bob failed");
        AsyncKafkaFuture done = AsyncKafkaFuture.CompletedFuture<object?>(null);
        AsyncKafkaFuture failed = new AsyncKafkaFuture(TestFutures.Failed(failure));
        Dictionary<ClientQuotaEntity, AsyncKafkaFuture> source = new Dictionary<ClientQuotaEntity, AsyncKafkaFuture> { [s_alice] = done };

        AsyncAlterClientQuotasResult result = new AsyncAlterClientQuotasResult(source);
        source[s_bob] = failed;
        source.Remove(s_alice);

        Assert.NotSame(source, result.Values());
        AssertOnly(result.Values(), s_alice, done);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        AsyncAlterClientQuotasResult mixed = new AsyncAlterClientQuotasResult(
            new Dictionary<ClientQuotaEntity, AsyncKafkaFuture> { [s_alice] = done, [s_bob] = failed });
        await TestTimeout.Run(() => mixed.Values()[s_alice].Get(), s_deadline);
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.Values()[s_bob].Get(), s_deadline)));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
    }

    [Fact]
    public void AlterUserScramCredentialsResult_AnswersFromTheGivenFutures_AndCopiesTheMapByOrdinalKey()
    {
        KafkaException failure = new KafkaException("bob failed");
        KafkaFuture done = KafkaFuture.CompletedFuture<object?>(null);
        KafkaFuture failed = new KafkaFuture(TestFutures.Failed(failure));
        Dictionary<string, KafkaFuture> source =
            new Dictionary<string, KafkaFuture>(StringComparer.OrdinalIgnoreCase) { ["Alice"] = done };

        AlterUserScramCredentialsResult result = new AlterUserScramCredentialsResult(source);
        source["bob"] = failed;
        source.Remove("Alice");

        Assert.NotSame(source, result.Values());
        AssertOnly(result.Values(), "Alice", done);
        Assert.False(result.Values().ContainsKey("alice"));
        result.All().Get(s_deadline);

        AlterUserScramCredentialsResult mixed = new AlterUserScramCredentialsResult(
            new Dictionary<string, KafkaFuture>(StringComparer.Ordinal) { ["alice"] = done, ["bob"] = failed });
        mixed.Values()["alice"].Get(s_deadline);
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.Values()["bob"].Get(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.All().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncAlterUserScramCredentialsResult_AnswersFromTheGivenFutures_AndCopiesTheMapByOrdinalKey()
    {
        KafkaException failure = new KafkaException("bob failed");
        AsyncKafkaFuture done = AsyncKafkaFuture.CompletedFuture<object?>(null);
        AsyncKafkaFuture failed = new AsyncKafkaFuture(TestFutures.Failed(failure));
        Dictionary<string, AsyncKafkaFuture> source =
            new Dictionary<string, AsyncKafkaFuture>(StringComparer.OrdinalIgnoreCase) { ["Alice"] = done };

        AsyncAlterUserScramCredentialsResult result = new AsyncAlterUserScramCredentialsResult(source);
        source["bob"] = failed;
        source.Remove("Alice");

        Assert.NotSame(source, result.Values());
        AssertOnly(result.Values(), "Alice", done);
        Assert.False(result.Values().ContainsKey("alice"));
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        AsyncAlterUserScramCredentialsResult mixed = new AsyncAlterUserScramCredentialsResult(
            new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal) { ["alice"] = done, ["bob"] = failed });
        await TestTimeout.Run(() => mixed.Values()["alice"].Get(), s_deadline);
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.Values()["bob"].Get(), s_deadline)));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
    }

    [Fact]
    public void DeleteRecordsResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("t-1 failed");
        DeletedRecords deleted = new DeletedRecords(42);
        KafkaFuture<DeletedRecords> done = KafkaFuture.CompletedFuture(deleted);
        KafkaFuture<DeletedRecords> failed = new KafkaFuture<DeletedRecords>(TestFutures.Failed<DeletedRecords>(failure));
        Dictionary<TopicPartition, KafkaFuture<DeletedRecords>> source =
            new Dictionary<TopicPartition, KafkaFuture<DeletedRecords>> { [s_p0] = done };

        DeleteRecordsResult result = new DeleteRecordsResult(source);
        source[s_p1] = failed;
        source.Remove(s_p0);

        Assert.NotSame(source, result.LowWatermarks());
        AssertOnly(result.LowWatermarks(), s_p0, done);
        Assert.Same(deleted, result.LowWatermarks()[s_p0].Get(s_deadline));
        result.All().Get(s_deadline);

        DeleteRecordsResult mixed = new DeleteRecordsResult(
            new Dictionary<TopicPartition, KafkaFuture<DeletedRecords>> { [s_p0] = done, [s_p1] = failed });
        Assert.Equal(42, mixed.LowWatermarks()[s_p0].Get(s_deadline).LowWatermark());
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.LowWatermarks()[s_p1].Get(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.All().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncDeleteRecordsResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("t-1 failed");
        DeletedRecords deleted = new DeletedRecords(42);
        AsyncKafkaFuture<DeletedRecords> done = AsyncKafkaFuture.CompletedFuture(deleted);
        AsyncKafkaFuture<DeletedRecords> failed = new AsyncKafkaFuture<DeletedRecords>(TestFutures.Failed<DeletedRecords>(failure));
        Dictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> source =
            new Dictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> { [s_p0] = done };

        AsyncDeleteRecordsResult result = new AsyncDeleteRecordsResult(source);
        source[s_p1] = failed;
        source.Remove(s_p0);

        Assert.NotSame(source, result.LowWatermarks());
        AssertOnly(result.LowWatermarks(), s_p0, done);
        Assert.Same(deleted, await TestTimeout.Run(() => result.LowWatermarks()[s_p0].Get(), s_deadline));
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        AsyncDeleteRecordsResult mixed = new AsyncDeleteRecordsResult(
            new Dictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> { [s_p0] = done, [s_p1] = failed });
        Assert.Equal(42, (await TestTimeout.Run(() => mixed.LowWatermarks()[s_p0].Get(), s_deadline)).LowWatermark());
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.LowWatermarks()[s_p1].Get(), s_deadline)));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
    }

    [Fact]
    public void DescribeClientQuotasResult_AnswersFromTheGivenFuture()
    {
        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> quotas = Quotas();
        KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> done =
            KafkaFuture.CompletedFuture(quotas);

        DescribeClientQuotasResult result = new DescribeClientQuotasResult(done);

        Assert.Equal(done, result.Entities());
        Assert.Same(quotas, result.Entities().Get(s_deadline));

        KafkaException failure = new KafkaException("describe failed");
        DescribeClientQuotasResult failedResult = new DescribeClientQuotasResult(
            new KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                TestFutures.Failed<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(failure)));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => failedResult.Entities().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncDescribeClientQuotasResult_AnswersFromTheGivenFuture()
    {
        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> quotas = Quotas();
        AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> done =
            AsyncKafkaFuture.CompletedFuture(quotas);

        AsyncDescribeClientQuotasResult result = new AsyncDescribeClientQuotasResult(done);

        Assert.Equal(done, result.Entities());
        Assert.Same(quotas, await TestTimeout.Run(() => result.Entities().Get(), s_deadline));

        KafkaException failure = new KafkaException("describe failed");
        AsyncDescribeClientQuotasResult failedResult = new AsyncDescribeClientQuotasResult(
            new AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                TestFutures.Failed<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(failure)));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => failedResult.Entities().Get(), s_deadline)));
    }

    [Fact]
    public void ListOffsetsResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("t-1 failed");
        ListOffsetsResult.ListOffsetsResultInfo info = new ListOffsetsResult.ListOffsetsResultInfo(5, 100, 3);
        KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> done = KafkaFuture.CompletedFuture(info);
        KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> failed = new KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>(
            TestFutures.Failed<ListOffsetsResult.ListOffsetsResultInfo>(failure));
        Dictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> source =
            new Dictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> { [s_p0] = done };

        ListOffsetsResult result = new ListOffsetsResult(source);
        source[s_p1] = failed;
        source.Remove(s_p0);

        Assert.Equal(done, result.PartitionResult(s_p0));
        Assert.Same(info, result.PartitionResult(s_p0).Get(s_deadline));
        IllegalArgumentException notAttempted = Assert.Throws<IllegalArgumentException>(() => result.PartitionResult(s_p1));
        Assert.Equal("List Offsets for partition \"t-1\" was not attempted", notAttempted.Message);
        KeyValuePair<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> only = Assert.Single(result.All().Get(s_deadline));
        Assert.Equal(s_p0, only.Key);
        Assert.Same(info, only.Value);

        ListOffsetsResult mixed = new ListOffsetsResult(
            new Dictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> { [s_p0] = done, [s_p1] = failed });
        Assert.Same(info, mixed.PartitionResult(s_p0).Get(s_deadline));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.PartitionResult(s_p1).Get(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<KafkaException>(() => mixed.All().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncListOffsetsResult_AnswersFromTheGivenFutures_AndCopiesTheMap()
    {
        KafkaException failure = new KafkaException("t-1 failed");
        ListOffsetsResult.ListOffsetsResultInfo info = new ListOffsetsResult.ListOffsetsResultInfo(5, 100, 3);
        AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> done = AsyncKafkaFuture.CompletedFuture(info);
        AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> failed = new AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>(
            TestFutures.Failed<ListOffsetsResult.ListOffsetsResultInfo>(failure));
        Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> source =
            new Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> { [s_p0] = done };

        AsyncListOffsetsResult result = new AsyncListOffsetsResult(source);
        source[s_p1] = failed;
        source.Remove(s_p0);

        Assert.Equal(done, result.PartitionResult(s_p0));
        Assert.Same(info, await TestTimeout.Run(() => result.PartitionResult(s_p0).Get(), s_deadline));
        IllegalArgumentException notAttempted = Assert.Throws<IllegalArgumentException>(() => result.PartitionResult(s_p1));
        Assert.Equal("List Offsets for partition \"t-1\" was not attempted", notAttempted.Message);
        KeyValuePair<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> only =
            Assert.Single(await TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Equal(s_p0, only.Key);
        Assert.Same(info, only.Value);

        AsyncListOffsetsResult mixed = new AsyncListOffsetsResult(
            new Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> { [s_p0] = done, [s_p1] = failed });
        Assert.Same(info, await TestTimeout.Run(() => mixed.PartitionResult(s_p0).Get(), s_deadline));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.PartitionResult(s_p1).Get(), s_deadline)));
        Assert.Same(
            failure,
            await FutureAssert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => mixed.All().Get(), s_deadline)));
    }

    private static void AssertOnly<TKey, TValue>(IReadOnlyDictionary<TKey, TValue> map, TKey key, TValue value)
    {
        KeyValuePair<TKey, TValue> entry = Assert.Single(map);
        Assert.Equal(key, entry.Key);
        Assert.Equal(value, entry.Value);
    }

    private static ClientQuotaEntity Entity(string user) =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.USER] = user });

    private static IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> Quotas() =>
        new Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>
        {
            [s_alice] = new Dictionary<string, double>(StringComparer.Ordinal) { ["producer_byte_rate"] = 1024 },
        };
}
