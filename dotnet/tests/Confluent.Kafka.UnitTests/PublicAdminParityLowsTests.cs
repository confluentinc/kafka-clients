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
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Three small Java-parity points: the three <c>*Result</c> constructors Java declares public, kept internal here on
/// both flavours as a recorded deviation; the result collections Java returns as unmodifiable views; and the closed
/// SCRAM alteration hierarchy.
/// </summary>
public sealed class PublicAdminParityLowsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task G4_10_DescribeClientQuotasResult_ConstructorIsInternal_ARecordedDeviation()
    {
        AssertJavaTwinConstructorIsInternal(
            typeof(DescribeClientQuotasResult),
            typeof(KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>));
        AssertJavaTwinConstructorIsInternal(
            typeof(AsyncDescribeClientQuotasResult),
            typeof(AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>));

        ClientQuotaEntity entity = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "alice" });
        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> quotas =
            new Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>
            {
                [entity] = new Dictionary<string, double>(StringComparer.Ordinal) { ["producer_byte_rate"] = 1024 },
            };
        KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> entities =
            KafkaFutureImpl.CompletedFuture(quotas);

        AsyncDescribeClientQuotasResult result = new AsyncDescribeClientQuotasResult(
            new AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(entities));

        Assert.Same(entities.GetAsync(), result.Entities().Get());
        Assert.Same(quotas, await TestTimeout.Run(() => result.Entities().Get(), s_deadline));

        ArgumentNullException thrown =
            Assert.Throws<ArgumentNullException>(() => new AsyncDescribeClientQuotasResult(default));
        Assert.Equal("entities", thrown.ParamName);
    }

    [Fact]
    public async Task G4_10_AlterClientQuotasResult_ConstructorIsInternal_ARecordedDeviation()
    {
        AssertJavaTwinConstructorIsInternal(
            typeof(AlterClientQuotasResult), typeof(IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture>));
        AssertJavaTwinConstructorIsInternal(
            typeof(AsyncAlterClientQuotasResult), typeof(IReadOnlyDictionary<ClientQuotaEntity, AsyncKafkaFuture>));

        ClientQuotaEntity alice = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "alice" });
        ClientQuotaEntity bob = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "bob" });
        KafkaException failure = new KafkaException("bob failed");
        Dictionary<ClientQuotaEntity, AsyncKafkaFuture> futures = new Dictionary<ClientQuotaEntity, AsyncKafkaFuture>
        {
            [alice] = new AsyncKafkaFuture(TestFutures.Done()),
        };

        AsyncAlterClientQuotasResult result = new AsyncAlterClientQuotasResult(futures);

        Assert.Same(futures, result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        futures[bob] = new AsyncKafkaFuture(TestFutures.Failed(failure));
        KafkaException thrown =
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Same(failure, thrown);

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => new AsyncAlterClientQuotasResult(null!)).ParamName);
    }

    [Fact]
    public async Task G4_10_AlterUserScramCredentialsResult_ConstructorIsInternal_ARecordedDeviation()
    {
        AssertJavaTwinConstructorIsInternal(
            typeof(AlterUserScramCredentialsResult), typeof(IReadOnlyDictionary<string, KafkaFuture>));
        AssertJavaTwinConstructorIsInternal(
            typeof(AsyncAlterUserScramCredentialsResult), typeof(IReadOnlyDictionary<string, AsyncKafkaFuture>));

        KafkaException failure = new KafkaException("bob failed");
        Dictionary<string, AsyncKafkaFuture> futures = new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal)
        {
            ["alice"] = new AsyncKafkaFuture(TestFutures.Done()),
            ["bob"] = new AsyncKafkaFuture(TestFutures.Failed(failure)),
        };

        AsyncAlterUserScramCredentialsResult result = new AsyncAlterUserScramCredentialsResult(futures);

        Assert.Same(futures, result.Values);
        KafkaException thrown =
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Same(failure, thrown);

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => new AsyncAlterUserScramCredentialsResult(null!)).ParamName);
    }

    [Fact]
    public void G3_11_PartitionReassignment_ListsAreReadOnly()
    {
        PartitionReassignment reassignment =
            new PartitionReassignment(new[] { 1, 2, 3 }, new[] { 3 }, new[] { 1 });

        foreach (IReadOnlyList<int> list in new[]
        {
            reassignment.Replicas,
            reassignment.AddingReplicas,
            reassignment.RemovingReplicas,
        })
        {
            IList<int> mutable = Assert.IsAssignableFrom<IList<int>>(list);
            Assert.True(mutable.IsReadOnly);
            Assert.Throws<NotSupportedException>(() => mutable.Add(9));
            Assert.Throws<NotSupportedException>(() => mutable.Clear());
        }

        Assert.Equal(new[] { 1, 2, 3 }, reassignment.Replicas);
        Assert.Equal(new[] { 3 }, reassignment.AddingReplicas);
        Assert.Equal(new[] { 1 }, reassignment.RemovingReplicas);
    }

    [Fact]
    public void G3_11_LogDirDescription_ReplicaInfosIsAReadOnlyView()
    {
        TopicPartition first = new TopicPartition("t", 0);
        TopicPartition second = new TopicPartition("t", 1);
        Dictionary<TopicPartition, ReplicaInfo> replicas = new Dictionary<TopicPartition, ReplicaInfo>
        {
            [first] = new ReplicaInfo(1, 2, false),
        };

        LogDirDescription description = new LogDirDescription(null, replicas);

        IDictionary<TopicPartition, ReplicaInfo> mutable =
            Assert.IsAssignableFrom<IDictionary<TopicPartition, ReplicaInfo>>(description.ReplicaInfos);
        Assert.True(mutable.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => mutable.Clear());
        Assert.Throws<NotSupportedException>(() => mutable.Add(second, new ReplicaInfo(3, 4, true)));
        Assert.Single(description.ReplicaInfos);

        replicas[second] = new ReplicaInfo(3, 4, true);
        Assert.Equal(2, description.ReplicaInfos.Count);
    }

    [Fact]
    public void G4_13_UserScramCredentialAlteration_IsClosedToOtherAssemblies()
    {
        ConstructorInfo constructor = Assert.Single(
            typeof(UserScramCredentialAlteration).GetConstructors(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));

        Assert.True(constructor.IsFamilyAndAssembly, "expected a private protected constructor");
        Assert.True(typeof(UserScramCredentialUpsertion).IsSealed);
        Assert.True(typeof(UserScramCredentialDeletion).IsSealed);
    }

    private static void AssertJavaTwinConstructorIsInternal(Type result, Type parameterType)
    {
        Assert.Empty(result.GetConstructors(BindingFlags.Instance | BindingFlags.Public));

        ConstructorInfo? twin = result.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic, null, new[] { parameterType }, null);
        Assert.True(twin is not null, $"{result.Name} has no ({parameterType.Name}) constructor");
        Assert.True(twin!.IsAssembly, $"{result.Name}({parameterType.Name}) is not internal");
    }
}
