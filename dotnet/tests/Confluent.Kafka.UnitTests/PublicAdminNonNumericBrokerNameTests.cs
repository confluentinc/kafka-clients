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
/// M15/P13.3 §4.8 (decision D10, tests only) — a <c>BROKER</c> / <c>BROKER_LOGGER</c> config
/// resource whose name is not a decimal <c>int</c>. Java's <c>nodeFor</c> is
/// <c>Integer.valueOf(name)</c>, so it throws <c>NumberFormatException("For input string:
/// \"x\"")</c>. The binding adds no parse of its own (that would be logic in the binding); it
/// forwards what the core answers, and these tests pin that answer.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The real client and the mock answer differently, and both are the core's own
/// choice.</b> The real client cannot throw from the call, so it fails <b>every</b> requested
/// resource of that call with the parse error and sends nothing (<c>kafka_admin_client.rs</c>,
/// <c>describe_configs_with_options</c> / <c>incremental_alter_configs_with_options</c> →
/// <c>failed_config_futures</c>) — which is why no broker is needed to observe it. The mock
/// mirrors Java's <c>MockAdminClient</c>, which parses per resource: only the <c>BROKER</c>
/// resource carries the parse error, the <c>TOPIC</c> beside it gets its own answer, and a
/// <c>BROKER_LOGGER</c> resource is one Java's mock does not implement at all
/// (<c>mock_admin_client.rs</c>, <c>get_resource_description</c> /
/// <c>handle_incremental_resource_alteration</c>).
/// </para>
/// </remarks>
public sealed class PublicAdminNonNumericBrokerNameTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary><c>kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT</c> — Java's <c>NumberFormatException</c> family.</summary>
    private const int LocalIllegalArgumentCode = -3;

    /// <summary>The code the mock reports for an RPC arm Java's mock does not implement.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>The core's message, copied from <c>parse_java_int</c> (Java's own text).</summary>
    private const string ParseMessage = "For input string: \"x\"";

    private const string NotImplemented = "Not implemented yet";

    private const string Topic = "d10-topic";

    /// <summary>
    /// The real client fails every resource of a <c>describeConfigs</c> call — the valid
    /// <c>TOPIC</c> included — with the parse error, for a <c>BROKER</c> or a
    /// <c>BROKER_LOGGER</c> name.
    /// </summary>
    [Theory]
    [InlineData(ConfigResourceType.Broker)]
    [InlineData(ConfigResourceType.BrokerLogger)]
    public async Task RealClient_DescribeConfigs_FailsEveryResource(ConfigResourceType brokerType)
    {
        await using KafkaAdminClient admin = NewUnconnectedClient();
        ConfigResource broker = new ConfigResource(brokerType, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        DescribeConfigsResult result = admin.DescribeConfigs(new[] { broker, topic });

        Assert.Equal(2, result.Values.Count);
        await AssertParseFailure(result.Values[broker]);
        await AssertParseFailure(result.Values[topic]);
    }

    /// <summary>The <c>incrementalAlterConfigs</c> twin of the test above.</summary>
    [Theory]
    [InlineData(ConfigResourceType.Broker)]
    [InlineData(ConfigResourceType.BrokerLogger)]
    public async Task RealClient_IncrementalAlterConfigs_FailsEveryResource(ConfigResourceType brokerType)
    {
        await using KafkaAdminClient admin = NewUnconnectedClient();
        ConfigResource broker = new ConfigResource(brokerType, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AlterConfigsResult result = admin.IncrementalAlterConfigs(Alterations(broker, topic));

        Assert.Equal(2, result.Values.Count);
        await AssertParseFailure(result.Values[broker]);
        await AssertParseFailure(result.Values[topic]);
    }

    /// <summary>
    /// The mock's <c>describeConfigs</c>: the <c>BROKER</c> resource carries the parse error,
    /// the <c>TOPIC</c> beside it is described.
    /// </summary>
    [Fact]
    public async Task Mock_DescribeConfigs_Broker_FailsOnlyThatResource()
    {
        using MockAdminClient admin = await NewMockWithTopic();
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        DescribeConfigsResult result = admin.DescribeConfigs(new[] { broker, topic });

        await AssertParseFailure(result.Values[broker]);
        await TestTimeout.Run(() => result.Values[topic], s_deadline);
    }

    /// <summary>
    /// The mock's <c>describeConfigs</c> for <c>BROKER_LOGGER</c>: Java's mock does not
    /// implement the type, so that resource is refused — no parse happens — and the
    /// <c>TOPIC</c> beside it is described.
    /// </summary>
    [Fact]
    public async Task Mock_DescribeConfigs_BrokerLogger_IsRefused_AndTheTopicIsDescribed()
    {
        using MockAdminClient admin = await NewMockWithTopic();
        ConfigResource brokerLogger = new ConfigResource(ConfigResourceType.BrokerLogger, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        DescribeConfigsResult result = admin.DescribeConfigs(new[] { brokerLogger, topic });

        await AssertNotImplemented(result.Values[brokerLogger]);
        await TestTimeout.Run(() => result.Values[topic], s_deadline);
    }

    /// <summary>The mock's <c>incrementalAlterConfigs</c> for <c>BROKER</c>.</summary>
    [Fact]
    public async Task Mock_IncrementalAlterConfigs_Broker_FailsOnlyThatResource()
    {
        using MockAdminClient admin = await NewMockWithTopic();
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AlterConfigsResult result = admin.IncrementalAlterConfigs(Alterations(broker, topic));

        await AssertParseFailure(result.Values[broker]);
        await TestTimeout.Run(() => result.Values[topic], s_deadline);
    }

    /// <summary>The mock's <c>incrementalAlterConfigs</c> for <c>BROKER_LOGGER</c>.</summary>
    [Fact]
    public async Task Mock_IncrementalAlterConfigs_BrokerLogger_IsRefused_AndTheTopicIsAltered()
    {
        using MockAdminClient admin = await NewMockWithTopic();
        ConfigResource brokerLogger = new ConfigResource(ConfigResourceType.BrokerLogger, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AlterConfigsResult result = admin.IncrementalAlterConfigs(Alterations(brokerLogger, topic));

        await AssertNotImplemented(result.Values[brokerLogger]);
        await TestTimeout.Run(() => result.Values[topic], s_deadline);
    }

    private static async Task AssertParseFailure(Task outcome)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => outcome, s_deadline));
        Assert.Equal(LocalIllegalArgumentCode, failure.Code);
        Assert.Equal(ParseMessage, failure.Message);
    }

    private static async Task AssertNotImplemented(Task outcome)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => outcome, s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);
    }

    private static Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> Alterations(
        params ConfigResource[] resources)
    {
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> alterations =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>();
        foreach (ConfigResource resource in resources)
        {
            alterations[resource] =
                new[] { new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOpType.Set) };
        }

        return alterations;
    }

    private static async Task<MockAdminClient> NewMockWithTopic()
    {
        MockAdminClient admin = new MockAdminClient(1);
        CreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) });
        await TestTimeout.Run(created.All, s_deadline);
        return admin;
    }

    private static KafkaAdminClient NewUnconnectedClient() =>
        new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
}
