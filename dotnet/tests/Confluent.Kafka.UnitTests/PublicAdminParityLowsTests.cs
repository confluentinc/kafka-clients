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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the small Java-parity fixes from the <c>f2f397e5</c> AdminClient audit: the three
/// <c>*Result</c> constructors Java declares public (G4-10), and the result collections Java
/// returns as unmodifiable views (G3-11).
/// </summary>
public sealed class PublicAdminParityLowsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// G4-10: Java's <c>DescribeClientQuotasResult(KafkaFuture)</c> is public
    /// (<c>DescribeClientQuotasResult.java:39</c>), so a caller can fabricate one.
    /// </summary>
    [Fact]
    public async Task G4_10_DescribeClientQuotasResult_IsPubliclyConstructible()
    {
        ClientQuotaEntity entity = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "alice" });
        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> quotas =
            new Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>
            {
                [entity] = new Dictionary<string, double>(StringComparer.Ordinal) { ["producer_byte_rate"] = 1024 },
            };
        Task<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> entities =
            Task.FromResult(quotas);

        DescribeClientQuotasResult result = new DescribeClientQuotasResult(entities);

        Assert.Same(entities, result.Entities());
        Assert.Same(quotas, await TestTimeout.Run(() => result.Entities(), s_deadline));

        ArgumentNullException thrown =
            Assert.Throws<ArgumentNullException>(() => new DescribeClientQuotasResult(null!));
        Assert.Equal("entities", thrown.ParamName);
    }

    /// <summary>
    /// G4-10: Java's <c>AlterClientQuotasResult(Map)</c> is public
    /// (<c>AlterClientQuotasResult.java:38</c>). The map is held by reference, as Java holds
    /// it, and <c>All()</c> faults with an entity's failure.
    /// </summary>
    [Fact]
    public async Task G4_10_AlterClientQuotasResult_IsPubliclyConstructible()
    {
        ClientQuotaEntity alice = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "alice" });
        ClientQuotaEntity bob = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [ClientQuotaEntity.User] = "bob" });
        KafkaException failure = new KafkaException("bob failed");
        Dictionary<ClientQuotaEntity, Task> futures = new Dictionary<ClientQuotaEntity, Task>
        {
            [alice] = Task.CompletedTask,
        };

        AlterClientQuotasResult result = new AlterClientQuotasResult(futures);

        Assert.Same(futures, result.Values);
        await TestTimeout.Run(() => result.All(), s_deadline);

        futures[bob] = Task.FromException(failure);
        KafkaException thrown =
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All(), s_deadline));
        Assert.Same(failure, thrown);

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => new AlterClientQuotasResult(null!)).ParamName);
    }

    /// <summary>
    /// G4-10: Java's <c>AlterUserScramCredentialsResult(Map)</c> is public
    /// (<c>AlterUserScramCredentialsResult.java:38</c>).
    /// </summary>
    [Fact]
    public async Task G4_10_AlterUserScramCredentialsResult_IsPubliclyConstructible()
    {
        KafkaException failure = new KafkaException("bob failed");
        Dictionary<string, Task> futures = new Dictionary<string, Task>(StringComparer.Ordinal)
        {
            ["alice"] = Task.CompletedTask,
            ["bob"] = Task.FromException(failure),
        };

        AlterUserScramCredentialsResult result = new AlterUserScramCredentialsResult(futures);

        Assert.Same(futures, result.Values);
        KafkaException thrown =
            await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All(), s_deadline));
        Assert.Same(failure, thrown);

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => new AlterUserScramCredentialsResult(null!)).ParamName);
    }

    /// <summary>
    /// G3-11: Java wraps each list in <c>Collections.unmodifiableList</c>
    /// (<c>PartitionReassignment.java:33-35</c>), so casting a returned list back to a mutable
    /// one and changing it must fail, as Java's <c>UnsupportedOperationException</c> does.
    /// </summary>
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

    /// <summary>
    /// G3-11: Java returns <c>Collections.unmodifiableMap</c> over the stored map
    /// (<c>LogDirDescription.java:69-70</c>) — a read-only <b>view</b>: it cannot be changed
    /// through the description, but a change the caller makes to its own map shows through.
    /// </summary>
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
}
