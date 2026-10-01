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
/// The TFM-matrix smoke test for the M15/P1 admin surface: construct a
/// <see cref="MockAdminClient"/>, run <c>CreateTopics</c> end to end through the per-key
/// bridge, read a derived accessor, then close. It uses only APIs available on the
/// netstandard2.0 floor so it <b>compiles on net462</b> (via ns2.0) as well as net8.0 /
/// net10.0 — the net462 leg's <em>build</em> must pass locally even though its
/// <em>run</em> is Windows/CI-only.
/// </summary>
public sealed class PublicAdminTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-admin-topic";

    [Fact]
    public async Task MockAdminClient_CreateTopicsClose_RoundTrips()
    {
        IAdmin admin = new MockAdminClient(1);
        try
        {
            CreateTopicsResult result = admin.CreateTopics(new[]
            {
                new NewTopic(Topic, 1, 1)
                {
                    Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
                },
            });

            await TestTimeout.Run(result.All, s_deadline);

            int numPartitions = 0;
            await TestTimeout.Run(async () => numPartitions = await result.NumPartitions(Topic), s_deadline);
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
        // The blocking teardown (sync AdminClient_close → destroy) on the TFM matrix.
        MockAdminClient admin = new MockAdminClient(1);

        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task RealAdminClient_CreateDisposeAsync_RoundTrips()
    {
        KafkaAdminClient admin = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);
    }

    /// <summary>
    /// M15/P2a's two RPCs on every TFM in the matrix (net462 via netstandard2.0, net8.0,
    /// net10.0). Both key forms, because the by-id path marshals a base64 string the
    /// by-name path does not.
    /// </summary>
    [Fact]
    public async Task MockAdminClient_DeleteAndDescribeTopics_RoundTripOnEveryTfm()
    {
        const string Named = "tfm-p2a-by-name";
        const string Identified = "tfm-p2a-by-id";

        IAdmin admin = new MockAdminClient(1);
        try
        {
            await TestTimeout.Run(
                () => admin.CreateTopics(new[] { new NewTopic(Named, 1, 1), new NewTopic(Identified, 1, 1) }).All(),
                s_deadline);

            DescribeTopicsResult described =
                admin.DescribeTopics(TopicCollection.OfTopicNames(new[] { Named, Identified }));
            IReadOnlyDictionary<string, TopicDescription> all =
                await TestTimeout.Run(() => described.AllTopicNames()!, s_deadline);
            Assert.Equal(2, all.Count);

            Uuid topicId = all[Identified].TopicId;

            await TestTimeout.Run(
                () => admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { Named })).All(), s_deadline);
            await TestTimeout.Run(
                () => admin.DeleteTopics(TopicCollection.OfTopicIds(new[] { topicId })).All(), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), s_deadline);
        }
    }
}
