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
/// The TFM-matrix smoke test for the admin surface: the async mock's <c>CreateTopics</c>, <c>DescribeTopics</c> and
/// <c>DeleteTopics</c> by name and by id, the sync mock's blocking dispose, and a real client's disposal. Only
/// netstandard2.0 APIs are used, so the net462 leg builds it, though it runs only on Windows.
/// </summary>
public sealed class PublicAdminTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-admin-topic";

    [Fact]
    public async Task MockAdminClient_CreateTopicsClose_RoundTrips()
    {
        IAsyncAdmin admin = AsyncMockAdminClient.Create().Build();
        try
        {
            AsyncCreateTopicsResult result = admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
                },
            });

            await TestTimeout.Run(() => result.All().Get(), s_deadline);

            int numPartitions = 0;
            await TestTimeout.Run(async () => numPartitions = await result.NumPartitions(Topic).Get(), s_deadline);
            Assert.Equal(1, numPartitions);
        }
        finally
        {
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), s_deadline);
        }
    }

    [Fact]
    public void MockAdminClient_ViaSyncDispose_RoundTrips()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();

        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task RealAdminClient_CreateDisposeAsync_RoundTrips()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task MockAdminClient_DeleteAndDescribeTopics_RoundTripOnEveryTfm()
    {
        const string Named = "tfm-p2a-by-name";
        const string Identified = "tfm-p2a-by-id";

        IAsyncAdmin admin = AsyncMockAdminClient.Create().Build();
        try
        {
            await TestTimeout.Run(
                () => admin.CreateTopics(new[] { new NewTopic(Named, 1, 1), new NewTopic(Identified, 1, 1) }).All().Get(),
                s_deadline);

            AsyncDescribeTopicsResult described =
                admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Named, Identified }));
            IReadOnlyDictionary<string, TopicDescription> all =
                await TestTimeout.Run(() => described.AllTopicNames()!.Value.Get(), s_deadline);
            Assert.Equal(2, all.Count);

            Uuid topicId = all[Identified].TopicId;

            await TestTimeout.Run(
                () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Named })).All().Get(), s_deadline);
            await TestTimeout.Run(
                () => admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { topicId })).All().Get(), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), s_deadline);
        }
    }
}
