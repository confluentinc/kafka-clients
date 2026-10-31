// Copyright 2026 Confluent Inc.
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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The per-key result envelope: each key's future is resolved on its own, whatever exception it
/// fails with, and the top-level error stays unset (<c>dotnet/CLAUDE.md</c> §7.6, Per-key results).
/// </summary>
public sealed class AdminPerKeyResultsTests
{
    private const int BrokerLoggerType = 8;
    private const int TopicType = 2;

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task DescribeConfigs_ABrokerLoggerKeyFailsWithNotSupported_TheTopicKeyKeepsItsValue(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();
        Proto.CreateTopicsRequest create = new Proto.CreateTopicsRequest { AdminId = adminId };
        Proto.NewTopic topic = new Proto.NewTopic { Name = "t", NumPartitions = 1, ReplicationFactor = 1 };
        topic.Configs["cleanup.policy"] = "compact";
        create.Topics.Add(topic);
        Proto.CreateTopicsResponse created = await server.Service.CreateTopics(create, server.Context);
        Assert.Null(created.Error);

        Proto.DescribeConfigsRequest request = new Proto.DescribeConfigsRequest { AdminId = adminId };
        request.Resources.Add(new Proto.ConfigResource { ResourceType = BrokerLoggerType, Name = "0" });
        request.Resources.Add(new Proto.ConfigResource { ResourceType = TopicType, Name = "t" });
        Proto.DescribeConfigsResponse response = await server.Service.DescribeConfigs(request, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(2, response.Entries.Count);
        Proto.DescribeConfigsEntry logger =
            Assert.Single(response.Entries, e => e.Key.ConfigResource.ResourceType == BrokerLoggerType);
        Assert.Equal(Proto.DescribeConfigsEntry.OutcomeOneofCase.Error, logger.OutcomeCase);
        Assert.Equal(-31, logger.Error.Code);
        Assert.Equal("Not implemented yet", logger.Error.Message);
        Proto.DescribeConfigsEntry described =
            Assert.Single(response.Entries, e => e.Key.ConfigResource.ResourceType == TopicType);
        Assert.Equal(Proto.DescribeConfigsEntry.OutcomeOneofCase.Value, described.OutcomeCase);
        Assert.Equal("t", described.Key.ConfigResource.Name);
        Proto.ConfigEntry policy = Assert.Single(described.Value.Entries, e => e.Name == "cleanup.policy");
        Assert.Equal("compact", policy.Value);
    }

    // Python's mock throws at the call here, so its server sends one top-level -31 instead.
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CreatePartitions_WhichJavasMockDoesNotImplement_FailsEachKeyWithNotImplementedYet(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();
        Proto.CreatePartitionsRequest request = new Proto.CreatePartitionsRequest { AdminId = adminId };
        request.Partitions.Add(new Proto.NewPartitions { Topic = "a", TotalCount = 2 });
        request.Partitions.Add(new Proto.NewPartitions { Topic = "b", TotalCount = 3 });

        Proto.VoidKeyedResponse response = await server.Service.CreatePartitions(request, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(new[] { "a", "b" }, response.Entries.Select(e => e.Key.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(response.Entries, entry =>
        {
            Assert.Equal(-31, entry.Error.Code);
            Assert.Equal("Not implemented yet", entry.Error.Message);
        });
    }
}
