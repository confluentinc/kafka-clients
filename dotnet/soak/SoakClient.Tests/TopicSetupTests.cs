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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// Topic setup over the admin client: the partition count and the replication factor passed
/// through (a negative one as the broker default), failures classified by code, and the admin
/// closed on every path, over a recording proxy of the native <see cref="AsyncMockAdminClient"/>.
/// </summary>
[Collection(EnvironmentCollection.Name)]
public sealed class TopicSetupTests
{
    private static readonly IReadOnlyDictionary<string, string> s_adminConfig =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "x" };

    private static SoakOptions Options(int partitions, int replicationFactor, bool recreate) =>
        SoakTestSupport.Options(
            ("SOAK_TESTID", "soak1"),
            ("SOAK_TOPIC", "t"),
            ("SOAK_PARTITIONS", partitions.ToString(CultureInfo.InvariantCulture)),
            ("SOAK_REPLICATION_FACTOR", replicationFactor.ToString(CultureInfo.InvariantCulture)),
            ("SOAK_RECREATE_TOPIC", recreate ? "True" : "False"));

    private static Task CreateTopic(
        RecordingAdmin recorder,
        SoakOptions options,
        List<TimeSpan>? delays = null,
        TextWriter? output = null) =>
        SoakClient.CreateTopicAsync(
            s_adminConfig,
            options,
            new SoakLogger(SoakLogLevel.Fatal),
            config =>
            {
                Assert.Same(s_adminConfig, config);
                return recorder.Admin;
            },
            delay =>
            {
                delays?.Add(delay);
                return Task.CompletedTask;
            },
            output ?? TextWriter.Null);

    private static string[] Lines(StringWriter output) =>
        output.ToString().Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

    private static IEnumerable<(string Name, int? Partitions, short? ReplicationFactor)> Created(object? newTopics) =>
        Assert.IsAssignableFrom<IEnumerable<NewTopic>>(newTopics)
            .Select(topic => (topic.Name, topic.NumPartitions, topic.ReplicationFactor));

    private static async Task<int> PartitionCount(IAsyncAdmin admin, string topic)
    {
        IReadOnlyDictionary<string, TopicDescription> described =
            await admin.DescribeTopics(new[] { topic }).AllTopicNames()!.Value.Get();
        return described[topic].Partitions.Count;
    }

    [Fact]
    public async Task CreateTopicCreatesAndClosesTheAdmin()
    {
        RecordingAdmin recorder = RecordingAdmin.Over(AsyncMockAdminClient.Create().Build());

        await CreateTopic(recorder, Options(3, 1, recreate: false));

        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
        Assert.Equal(new[] { ("t", (int?)3, (short?)1) }, Created(recorder.Arguments[0][0]));
    }

    [Theory]
    [InlineData(3, 3, 3)]
    [InlineData(0, 0, 0)]
    [InlineData(-1, null, 5)]
    [InlineData(-3, null, 5)]
    public async Task CreateTopicPassesThePartitionCountThrough(
        int partitions, int? sentPartitions, int createdPartitions)
    {
        using var mock = AsyncMockAdminClient.Create().DefaultPartitions(5).Build();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);
        recorder.ForwardsDispose = false;

        await CreateTopic(recorder, Options(partitions, 1, recreate: false));

        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
        Assert.Equal(new[] { ("t", sentPartitions, (short?)1) }, Created(recorder.Arguments[0][0]));
        Assert.Equal(createdPartitions, await PartitionCount(mock, "t"));
    }

    [Theory]
    [InlineData(2, (short)2, 2)]
    [InlineData(0, (short)0, 0)]
    [InlineData(-1, null, 3)]
    [InlineData(-3, null, 3)]
    public async Task CreateTopicPassesTheReplicationFactorThrough(
        int replicationFactor, short? sentReplicationFactor, int createdReplicationFactor)
    {
        using var mock = AsyncMockAdminClient.Create().NumBrokers(3).DefaultReplicationFactor(3).Build();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);
        recorder.ForwardsDispose = false;

        await CreateTopic(recorder, Options(1, replicationFactor, recreate: false));

        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
        Assert.Equal(new[] { ("t", (int?)1, sentReplicationFactor) }, Created(recorder.Arguments[0][0]));
        IReadOnlyDictionary<string, TopicDescription> described =
            await mock.DescribeTopics(new[] { "t" }).AllTopicNames()!.Value.Get();
        Assert.Equal(createdReplicationFactor, Assert.Single(described["t"].Partitions).Replicas.Count);
    }

    [Fact]
    public async Task CreateTopicTreatsTopicExistsAsGood()
    {
        var mock = AsyncMockAdminClient.Create().Build();
        await mock.CreateTopics(new[] { new NewTopic("t", 3, 1) }).All().Get();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);

        await CreateTopic(recorder, Options(3, 1, recreate: false));

        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
    }

    [Fact]
    public async Task CreateTopicAuthFailureIsFatal()
    {
        RecordingAdmin recorder = RecordingAdmin.Over(AsyncMockAdminClient.Create().Build());
        KafkaException authentication = SoakTestSupport.KafkaError(58, "bad creds");
        recorder.CreateTopicsError = authentication;

        SoakFatalStartupException ex = await Assert.ThrowsAsync<SoakFatalStartupException>(
            () => CreateTopic(recorder, Options(3, 1, recreate: false)));

        Assert.Equal(
            "authentication/authorization failed creating topic 't': bad creds. Check sasl.jaas.config "
            + "(username/password) and the API key's ACLs. Restarting will not fix this.",
            ex.Message);
        Assert.Same(authentication, ex.InnerException);
        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
    }

    [Fact]
    public async Task CreateTopicOtherFailureIsTransient()
    {
        var mock = AsyncMockAdminClient.Create().Build();
        mock.TimeoutNextRequest(1);
        RecordingAdmin recorder = RecordingAdmin.Over(mock);

        SoakTransientStartupException ex = await Assert.ThrowsAsync<SoakTransientStartupException>(
            () => CreateTopic(recorder, Options(3, 1, recreate: false)));

        KafkaException timeout = Assert.IsType<KafkaException>(ex.InnerException);
        Assert.Equal(7, timeout.Code);
        Assert.Equal("The mock timed out the request.", timeout.Message);
        Assert.Equal(
            "could not create or verify topic 't': The mock timed out the request.. "
            + "If the cluster is reachable this may clear on retry.",
            ex.Message);
        Assert.Equal(new[] { "CreateTopics", "Dispose" }, recorder.Calls);
    }

    [Theory]
    [InlineData(4, -1, 4, 4, "4")]
    [InlineData(4, 3, 4, 4, "4")]
    [InlineData(0, 3, 0, 0, "0")]
    [InlineData(-1, 3, null, 5, "broker-default")]
    [InlineData(-3, 3, null, 5, "broker-default")]
    public async Task RecreateTopicDeletesThenCreates(
        int partitions, int replicationFactor, int? sentPartitions, int createdPartitions, string partitionsText)
    {
        using var mock = AsyncMockAdminClient.Create().DefaultPartitions(5).Build();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);
        recorder.ForwardsDispose = false;
        var delays = new List<TimeSpan>();
        using var output = new StringWriter();

        await CreateTopic(recorder, Options(partitions, replicationFactor, recreate: true), delays, output);

        Assert.Equal(new[] { "DeleteTopics", "CreateTopics", "Dispose" }, recorder.Calls);
        TopicCollection.TopicNameCollection deleted =
            Assert.IsType<TopicCollection.TopicNameCollection>(recorder.Arguments[0][0]);
        Assert.Equal(new[] { "t" }, deleted.TopicNames());
        Assert.Equal(30000, Assert.IsType<DeleteTopicsOptions>(recorder.Arguments[0][1]).TimeoutMs);
        Assert.Equal(new[] { ("t", sentPartitions, (short?)null) }, Created(recorder.Arguments[1][0]));
        Assert.Equal(createdPartitions, await PartitionCount(mock, "t"));
        Assert.Equal(new[] { TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10) }, delays);
        Assert.Equal(
            new[]
            {
                ">>> CREATE_TOPIC: deleting topic 't' (ignored if absent) ...",
                ">>> 't' did not exist (ok)",
                ">>> waiting 10s after delete ...",
                ">>> CREATE_TOPIC: creating topic 't' (partitions=" + partitionsText + ", rf=broker-default) ...",
                ">>> created 't'",
                ">>> waiting 10s after create ...",
            },
            Lines(output));
    }

    [Fact]
    public async Task RecreateTopicDeletesAnExistingTopic()
    {
        var mock = AsyncMockAdminClient.Create().Build();
        await mock.CreateTopics(new[] { new NewTopic("t", 3, 1) }).All().Get();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);
        using var output = new StringWriter();

        await CreateTopic(recorder, Options(4, 1, recreate: true), output: output);

        Assert.Equal(new[] { "DeleteTopics", "CreateTopics", "Dispose" }, recorder.Calls);
        Assert.Equal(
            new[]
            {
                ">>> CREATE_TOPIC: deleting topic 't' (ignored if absent) ...",
                ">>> deleted 't'",
                ">>> waiting 10s after delete ...",
                ">>> CREATE_TOPIC: creating topic 't' (partitions=4, rf=broker-default) ...",
                ">>> created 't'",
                ">>> waiting 10s after create ...",
            },
            Lines(output));
    }

    [Fact]
    public async Task RecreateTopicTreatsAbsentOnDeleteAndExistsOnCreateAsGood()
    {
        var mock = AsyncMockAdminClient.Create().Build();
        await mock.CreateTopics(new[] { new NewTopic("t", 3, 1) }).All().Get();
        RecordingAdmin recorder = RecordingAdmin.Over(mock);
        recorder.DeleteTopicsError = SoakTestSupport.KafkaError(3, "gone");
        using var output = new StringWriter();

        await CreateTopic(recorder, Options(4, 1, recreate: true), output: output);

        Assert.Equal(new[] { "DeleteTopics", "CreateTopics", "Dispose" }, recorder.Calls);
        Assert.Equal(
            new[]
            {
                ">>> CREATE_TOPIC: deleting topic 't' (ignored if absent) ...",
                ">>> 't' did not exist (ok)",
                ">>> waiting 10s after delete ...",
                ">>> CREATE_TOPIC: creating topic 't' (partitions=4, rf=broker-default) ...",
                ">>> 't' already exists (ok)",
                ">>> waiting 10s after create ...",
            },
            Lines(output));
    }

    [Fact]
    public async Task RecreateTopicAuthFailureOnDeleteIsFatal()
    {
        RecordingAdmin recorder = RecordingAdmin.Over(AsyncMockAdminClient.Create().Build());
        KafkaException authentication = SoakTestSupport.KafkaError(58, "bad creds");
        recorder.DeleteTopicsError = authentication;

        SoakFatalStartupException ex = await Assert.ThrowsAsync<SoakFatalStartupException>(
            () => CreateTopic(recorder, Options(4, 1, recreate: true)));

        Assert.Equal(
            "authentication/authorization failed on delete of topic 't': bad creds. Check sasl.jaas.config "
            + "(username/password) and the API key's ACLs. Restarting will not fix this.",
            ex.Message);
        Assert.Same(authentication, ex.InnerException);
        Assert.Equal(new[] { "DeleteTopics", "Dispose" }, recorder.Calls);
    }

    [Fact]
    public async Task RecreateTopicAuthFailureOnCreateIsFatal()
    {
        RecordingAdmin recorder = RecordingAdmin.Over(AsyncMockAdminClient.Create().Build());
        KafkaException authorization = SoakTestSupport.KafkaError(29, "denied");
        recorder.CreateTopicsError = authorization;

        SoakFatalStartupException ex = await Assert.ThrowsAsync<SoakFatalStartupException>(
            () => CreateTopic(recorder, Options(4, 1, recreate: true)));

        Assert.Equal(
            "authentication/authorization failed on create of topic 't': denied. Check sasl.jaas.config "
            + "(username/password) and the API key's ACLs. Restarting will not fix this.",
            ex.Message);
        Assert.Same(authorization, ex.InnerException);
        Assert.Equal(new[] { "DeleteTopics", "CreateTopics", "Dispose" }, recorder.Calls);
    }

    [Fact]
    public async Task RecreateTopicOtherFailureOnDeleteIsTransient()
    {
        var mock = AsyncMockAdminClient.Create().Build();
        mock.TimeoutNextRequest(1);
        RecordingAdmin recorder = RecordingAdmin.Over(mock);

        SoakTransientStartupException ex = await Assert.ThrowsAsync<SoakTransientStartupException>(
            () => CreateTopic(recorder, Options(4, 1, recreate: true)));

        KafkaException timeout = Assert.IsType<KafkaException>(ex.InnerException);
        Assert.Equal(7, timeout.Code);
        Assert.Equal(
            "could not delete topic 't': The mock timed out the request.. "
            + "If the cluster is reachable this may clear on retry.",
            ex.Message);
        Assert.Equal(new[] { "DeleteTopics", "Dispose" }, recorder.Calls);
    }

    [Fact]
    public async Task RecreateTopicOtherFailureOnCreateIsTransient()
    {
        RecordingAdmin recorder = RecordingAdmin.Over(AsyncMockAdminClient.Create().Build());
        KafkaException timeout = SoakTestSupport.KafkaError(7, "slow");
        recorder.CreateTopicsError = timeout;

        SoakTransientStartupException ex = await Assert.ThrowsAsync<SoakTransientStartupException>(
            () => CreateTopic(recorder, Options(4, 1, recreate: true)));

        Assert.Equal(
            "could not create topic 't': slow. If the cluster is reachable this may clear on retry.",
            ex.Message);
        Assert.Same(timeout, ex.InnerException);
        Assert.Equal(new[] { "DeleteTopics", "CreateTopics", "Dispose" }, recorder.Calls);
    }
}

/// <summary>
/// An <see cref="IAsyncAdmin"/> that records each call and forwards it to a real admin, or throws
/// <see cref="CreateTopicsError"/> / <see cref="DeleteTopicsError"/> for a failure the mock cannot
/// produce; with <see cref="ForwardsDispose"/> off, <c>Dispose</c> leaves the real admin open.
/// </summary>
public class RecordingAdmin : DispatchProxy
{
    private IAsyncAdmin? _target;

    internal List<string> Calls { get; } = new List<string>();

    internal List<object?[]> Arguments { get; } = new List<object?[]>();

    internal Exception? CreateTopicsError { get; set; }

    internal Exception? DeleteTopicsError { get; set; }

    internal bool ForwardsDispose { get; set; } = true;

    internal IAsyncAdmin Admin => (IAsyncAdmin)(object)this;

    internal static RecordingAdmin Over(IAsyncAdmin target)
    {
        var recorder = (RecordingAdmin)(object)Create<IAsyncAdmin, RecordingAdmin>();
        recorder._target = target;
        return recorder;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);
        Calls.Add(targetMethod.Name);
        Arguments.Add(args ?? Array.Empty<object?>());
        if (targetMethod.Name == nameof(IAsyncAdmin.CreateTopics) && CreateTopicsError is not null)
        {
            throw CreateTopicsError;
        }

        if (targetMethod.Name == nameof(IAsyncAdmin.DeleteTopics) && DeleteTopicsError is not null)
        {
            throw DeleteTopicsError;
        }

        if (targetMethod.Name == nameof(IDisposable.Dispose) && !ForwardsDispose)
        {
            return null;
        }

        try
        {
            return targetMethod.Invoke(_target, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }
}
