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
/// A <c>BROKER</c> or <c>BROKER_LOGGER</c> config resource whose name is not a decimal
/// <c>int</c>: the binding makes no parse of its own and forwards what the core answers. The
/// real client sends the call like any other, and the mock fails only that resource.
/// </summary>
/// <remarks>
/// Java's <c>KafkaAdminClient.nodeFor</c> and <c>MockAdminClient</c> parse the name with
/// <c>Integer.parseInt</c> and fail with <c>NumberFormatException</c>; the core does not, and
/// these tests pin the core's answer.
/// </remarks>
public sealed class PublicAdminNonNumericBrokerNameTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int InvalidRequestCode = 42;

    private const int RequestTimedOutCode = 7;

    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    private const string Topic = "d10-topic";

    private const int CallTimeoutMs = 200;

    [Theory]
    [InlineData(ConfigResourceType.Broker)]
    [InlineData(ConfigResourceType.BrokerLogger)]
    public async Task RealClient_DescribeConfigs_SendsTheCall(ConfigResourceType brokerType)
    {
        AsyncKafkaAdminClient admin = NewUnconnectedClient();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        ConfigResource broker = new ConfigResource(brokerType, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncDescribeConfigsResult result = admin.DescribeConfigs(
            new[] { broker, topic }, new DescribeConfigsOptions { TimeoutMs = CallTimeoutMs });

        Assert.Equal(2, result.Values.Count);
        await AssertTimedOut(result.Values[broker].Get(), "describeConfigs");
        await AssertTimedOut(result.Values[topic].Get(), "describeConfigs");
    }

    [Theory]
    [InlineData(ConfigResourceType.Broker)]
    [InlineData(ConfigResourceType.BrokerLogger)]
    public async Task RealClient_IncrementalAlterConfigs_SendsTheCall(ConfigResourceType brokerType)
    {
        AsyncKafkaAdminClient admin = NewUnconnectedClient();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        ConfigResource broker = new ConfigResource(brokerType, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(
            Alterations(broker, topic), new AlterConfigsOptions { TimeoutMs = CallTimeoutMs });

        Assert.Equal(2, result.Values.Count);
        await AssertTimedOut(result.Values[broker].Get(), "incrementalAlterConfigs");
        await AssertTimedOut(result.Values[topic].Get(), "incrementalAlterConfigs");
    }

    [Fact]
    public async Task Mock_DescribeConfigs_Broker_FailsOnlyThatResource()
    {
        AsyncMockAdminClient admin = await NewMockWithTopic();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncDescribeConfigsResult result = admin.DescribeConfigs(new[] { broker, topic });

        await AssertInvalidRequest(result.Values[broker].Get(), "Broker x not found.");
        await TestTimeout.Run(() => result.Values[topic].Get(), s_deadline);
    }

    [Fact]
    public async Task Mock_DescribeConfigs_BrokerLogger_IsRefused_AndTheTopicIsDescribed()
    {
        AsyncMockAdminClient admin = await NewMockWithTopic();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        ConfigResource brokerLogger = new ConfigResource(ConfigResourceType.BrokerLogger, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncDescribeConfigsResult result = admin.DescribeConfigs(new[] { brokerLogger, topic });

        await AssertNotImplemented(result.Values[brokerLogger].Get());
        await TestTimeout.Run(() => result.Values[topic].Get(), s_deadline);
    }

    [Fact]
    public async Task Mock_IncrementalAlterConfigs_Broker_FailsOnlyThatResource()
    {
        AsyncMockAdminClient admin = await NewMockWithTopic();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        ConfigResource broker = new ConfigResource(ConfigResourceType.Broker, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(Alterations(broker, topic));

        await AssertInvalidRequest(result.Values[broker].Get(), "no such broker as x");
        await TestTimeout.Run(() => result.Values[topic].Get(), s_deadline);
    }

    [Fact]
    public async Task Mock_IncrementalAlterConfigs_BrokerLogger_IsRefused_AndTheTopicIsAltered()
    {
        AsyncMockAdminClient admin = await NewMockWithTopic();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        ConfigResource brokerLogger = new ConfigResource(ConfigResourceType.BrokerLogger, "x");
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, Topic);

        AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(Alterations(brokerLogger, topic));

        await AssertNotImplemented(result.Values[brokerLogger].Get());
        await TestTimeout.Run(() => result.Values[topic].Get(), s_deadline);
    }

    private static async Task AssertTimedOut(Task outcome, string call)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => outcome, s_deadline));
        Assert.Equal(RequestTimedOutCode, failure.Code);
        Assert.True(failure.IsRetriable);
        Assert.Equal("Timed out waiting for a node assignment. Call: " + call, failure.Message);
    }

    private static async Task AssertInvalidRequest(Task outcome, string message)
    {
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => outcome, s_deadline));
        Assert.Equal(InvalidRequestCode, failure.Code);
        Assert.Equal(message, failure.Message);
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

    private static async Task<AsyncMockAdminClient> NewMockWithTopic()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        AsyncCreateTopicsResult created = admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) });
        await TestTimeout.Run(() => created.All().Get(), s_deadline);
        return admin;
    }

    private static AsyncKafkaAdminClient NewUnconnectedClient() =>
        new AsyncKafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
}
