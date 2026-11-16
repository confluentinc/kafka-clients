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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// Python's <c>test_verifiable_consumer.py</c>: the Rust tool's tests over <c>MockConsumer</c>, and the tool run end
/// to end against an unreachable broker; plus the Rust tool's consumed-count overflow test. The two tests that read
/// the tool's error log are in <see cref="VerifiableConsumerLogTests"/>.
/// </summary>
public sealed class VerifiableConsumerTests : IDisposable
{
    private const string Tool = "VerifiableConsumer";

    private static readonly TopicPartition s_tp0 = new TopicPartition("t", 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition("t", 1);

    private static readonly string[] s_minimal = { "--bootstrap-server", "b", "--topic", "t", "--group-id", "g" };

    private static readonly Regex s_timestamp = new Regex("^\\{\"timestamp\":[0-9]+,");

    private readonly string _tmpPath = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), "verifiable-consumer-tests-" + Guid.NewGuid().ToString("N"))).FullName;

    private readonly StringWriter _stdout = new StringWriter();

    private readonly List<IDisposable> _consumers = new List<IDisposable>();

    private readonly JsonOutput _output;

    public VerifiableConsumerTests()
    {
        _output = new JsonOutput(_stdout);
    }

    public void Dispose()
    {
        foreach (IDisposable consumer in _consumers)
        {
            consumer.Dispose();
        }

        Directory.Delete(_tmpPath, recursive: true);
    }

    [Fact]
    public void HasMessageLimitFalseForNegativeMax()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: -1);
        Assert.False(consumer.HasMessageLimit());
        Assert.False(consumer.IsFinished());
    }

    [Fact]
    public void HasMessageLimitTrueForZeroOrPositiveMax()
    {
        Assert.True(Verifiable(maxMessages: 0).HasMessageLimit());
        Assert.True(Verifiable(maxMessages: 5).HasMessageLimit());
        Assert.True(Verifiable(maxMessages: 0).IsFinished());
        Assert.False(Verifiable(maxMessages: 5).IsFinished());
    }

    [Fact]
    public void OnRecordsReceivedCommitsMaxOffsetPlusOne()
    {
        VerifiableConsumer consumer = Verifiable();
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = consumer.OnRecordsReceived(Records((s_tp0, new long[] { 10, 11, 12 })));
        Assert.Equal(new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(13) }, offsets);
        Assert.Equal(3, consumer.ConsumedMessages);
        Assert.Equal(
            new[]
            {
                """{"name":"records_consumed","count":3,"partitions":[{"topic":"t","partition":0,"count":3,"minOffset":10,"maxOffset":12}]}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void OnRecordsReceivedConsumedCountWrapsOnOverflow()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: -1);
        consumer.ConsumedMessages = int.MaxValue;
        consumer.OnRecordsReceived(Records((s_tp0, new long[] { 10, 11 })));
        Assert.Equal(int.MinValue + 1, consumer.ConsumedMessages);
    }

    [Fact]
    public void OnRecordsReceivedTruncatesAtMaxMessages()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: 2);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            consumer.OnRecordsReceived(Records((s_tp0, new long[] { 100, 101, 102, 103, 104 })));
        Assert.Equal(new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(102) }, offsets);
        Assert.Equal(2, consumer.ConsumedMessages);
        Assert.True(consumer.IsFinished());
        Assert.Equal(
            new[]
            {
                """{"name":"records_consumed","count":5,"partitions":[{"topic":"t","partition":0,"count":2,"minOffset":100,"maxOffset":101}]}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void OnRecordsReceivedStopsAtThePartitionThatFinishes()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: 1);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            consumer.OnRecordsReceived(Records((s_tp0, new long[] { 5 }), (s_tp1, new long[] { 7 })));
        Assert.Single(offsets);
        Assert.Equal(1, consumer.ConsumedMessages);
        Assert.True(consumer.IsFinished());
        JsonElement line = Assert.Single(Lines());
        Assert.Equal(
            (2, 1), (line.GetProperty("count").GetInt32(), line.GetProperty("partitions").GetArrayLength()));
    }

    [Fact]
    public void OnRecordsReceivedAfterTheLimitKeepsNothing()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: 1);
        consumer.ConsumedMessages = 1;
        Assert.Empty(consumer.OnRecordsReceived(Records((s_tp0, new long[] { 1, 2 }))));
        Assert.Equal(new[] { """{"name":"records_consumed","count":2,"partitions":[]}""" }, LinesWithoutTimestamp());
    }

    [Fact]
    public void VerbosePrintsEachRecordKept()
    {
        VerifiableConsumer consumer = Verifiable(maxMessages: 1, verbose: true);
        consumer.OnRecordsReceived(Records(s_tp0, Record(s_tp0, 3, key: "k", value: "3"), Record(s_tp0, 4)));
        List<string> lines = LinesWithoutTimestamp();
        Assert.Equal("""{"name":"record_data","key":"k","value":"3","topic":"t","partition":0,"offset":3}""", lines[0]);
        Assert.Equal(new[] { "record_data", "records_consumed" }, Lines().Select(Name));
    }

    [Fact]
    public void LifecycleEventsPutTimestampThenNameFirst()
    {
        VerifiableConsumer.StartupComplete(_output);
        VerifiableConsumer.ShutdownRequested(_output);
        VerifiableConsumer.ShutdownComplete(_output);
        Assert.Equal(
            new[]
            {
                """{"name":"startup_complete"}""", """{"name":"shutdown_requested"}""", """{"name":"shutdown_complete"}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void PartitionEventsWriteTopicPartitionsAsObjects()
    {
        VerifiableConsumer.PartitionsAssigned(_output, new[] { s_tp1 });
        VerifiableConsumer.PartitionsRevoked(_output, new[] { s_tp0 });
        Assert.Equal(
            new[]
            {
                """{"name":"partitions_assigned","partitions":[{"topic":"t","partition":1}]}""",
                """{"name":"partitions_revoked","partitions":[{"topic":"t","partition":0}]}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void RecordsConsumedFieldsInJavaOrder()
    {
        VerifiableConsumer.RecordsConsumed(_output, 3, Array.Empty<(TopicPartition, int, long, long)>());
        Assert.Equal(new[] { """{"name":"records_consumed","count":3,"partitions":[]}""" }, LinesWithoutTimestamp());
    }

    [Fact]
    public void RecordDataFieldsInJavaOrder()
    {
        VerifiableConsumer.RecordData(_output, Record(s_tp1, 9, key: null, value: "x"));
        Assert.Equal(
            new[] { """{"name":"record_data","key":null,"value":"x","topic":"t","partition":1,"offset":9}""" },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void OffsetsCommittedSuccessOmitsError()
    {
        VerifiableConsumer.OffsetsCommitted(_output, Offsets(13), null, true);
        Assert.Equal(
            new[] { """{"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":13}],"success":true}""" },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void OffsetsCommittedFailureIncludesError()
    {
        VerifiableConsumer.OffsetsCommitted(_output, Offsets(13), "boom", false);
        Assert.Equal(
            new[]
            {
                """{"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":13}],"error":"boom","success":false}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void OnCompleteReportsTheErrorMessage()
    {
        VerifiableConsumer consumer = Verifiable();
        consumer.OnComplete(Offsets(1), new KafkaException("boom"));
        consumer.OnComplete(Offsets(1), new KafkaException());
        consumer.OnComplete(null, new KafkaException("async"));
        Assert.Equal(
            new[]
            {
                """{"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":1}],"error":"boom","success":false}""",
                """{"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":1}],"success":false}""",
                """{"name":"offsets_committed","offsets":[],"error":"async","success":false}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void ListenerMethodsPrintTheRebalance()
    {
        VerifiableConsumer consumer = Verifiable();
        consumer.OnPartitionsAssigned(new[] { s_tp0 });
        consumer.OnPartitionsRevoked(new[] { s_tp0 });
        consumer.OnPartitionsLost(new[] { s_tp1 });
        Assert.Equal(
            new[]
            {
                """{"name":"partitions_assigned","partitions":[{"topic":"t","partition":0}]}""",
                """{"name":"partitions_revoked","partitions":[{"topic":"t","partition":0}]}""",
                """{"name":"partitions_revoked","partitions":[{"topic":"t","partition":1}]}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void CommitSyncReportsSuccess()
    {
        ScriptedConsumer script = Scripted();
        Verifiable(script).CommitSync(Offsets(13));
        Assert.Equal(new[] { Offsets(13) }, script.Commits);
        Assert.Equal(new[] { true }, Lines().Select(line => line.GetProperty("success").GetBoolean()));
    }

    [Fact]
    public void CommitSyncRetriesAfterWakeupThenRaisesIt()
    {
        var wakeup = new WakeupException();
        ScriptedConsumer script = Scripted(wakeup);
        VerifiableConsumer consumer = Verifiable(script);
        Assert.Same(wakeup, Assert.Throws<WakeupException>(() => consumer.CommitSync(Offsets(13))));
        Assert.Equal(new[] { Offsets(13), Offsets(13) }, script.Commits);
        Assert.Equal(new[] { true }, Lines().Select(line => line.GetProperty("success").GetBoolean()));
    }

    [Fact]
    public void CommitSyncRaisesFencedInstanceId()
    {
        ScriptedConsumer script = Scripted(new FencedInstanceIdException("fenced"));
        VerifiableConsumer consumer = Verifiable(script);
        Assert.Throws<FencedInstanceIdException>(() => consumer.CommitSync(Offsets(13)));
        Assert.Empty(Lines());
    }

    [Fact]
    public void CommitSyncReportsAnyOtherError()
    {
        ScriptedConsumer script = Scripted(new KafkaException("commit failed"));
        Verifiable(script).CommitSync(Offsets(13));
        JsonElement line = Assert.Single(Lines());
        Assert.Equal(
            (false, "commit failed"), (line.GetProperty("success").GetBoolean(), line.GetProperty("error").GetString()));
    }

    [Fact]
    public void RunConsumesCommitsAndShutsDown()
    {
        ScriptedConsumer mock = Scripted();
        mock.SchedulePollTask(() => AssignAndAdd(mock.Mock, 0, 1, 2));
        Verifiable(mock, maxMessages: 3).Run();
        List<string> lines = LinesWithoutTimestamp();
        Assert.Equal(
            new[] { "startup_complete", "partitions_assigned", "records_consumed", "offsets_committed", "shutdown_complete" },
            Lines().Select(Name));
        Assert.Equal(
            """{"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":3}],"success":true}""", lines[3]);
        Assert.Equal(
            new Dictionary<TopicPartition, OffsetAndMetadata?> { [s_tp0] = new OffsetAndMetadata(3) }, mock.CommittedAtClose);
        Assert.Equal(new[] { "t" }, mock.SubscriptionAtClose);
        Assert.True(mock.Mock.Closed());
    }

    [Fact]
    public void RunWithAutoCommitDoesNotCommit()
    {
        ScriptedConsumer mock = Scripted();
        mock.SchedulePollTask(() => AssignAndAdd(mock.Mock, 0));
        Verifiable(mock, maxMessages: 1, useAutoCommit: true).Run();
        Assert.Equal(
            new[] { "startup_complete", "partitions_assigned", "records_consumed", "shutdown_complete" },
            Lines().Select(Name));
    }

    [Fact]
    public void CloseWakesTheConsumerEvenIfPrintingFails()
    {
        ScriptedConsumer mock = Scripted();
        var consumer = new VerifiableConsumer(
            mock, new JsonOutput(new FailingWriter()), "t", -1, false, false, false, 30000, TimeSpan.Zero, () => TimeSpan.Zero, Thread.Sleep);
        Assert.Throws<InvalidOperationException>(consumer.Close);
        Assert.Equal(1, mock.Wakeups);
    }

    [Fact]
    public void RunKeepsStartupVisibleForTheMinimum()
    {
        var clock = new FakeClock(100.0, 100.5);
        ScriptedConsumer mock = Scripted();
        mock.SchedulePollTask(mock.Wakeup);
        Verifiable(mock, minStartupVisible: TimeSpan.FromSeconds(3), clock: clock).Run();
        Assert.Equal(new[] { TimeSpan.FromSeconds(2.5) }, clock.Sleeps);
        Assert.Equal(new[] { "startup_complete", "shutdown_complete" }, Lines().Select(Name));
    }

    [Fact]
    public void RunDoesNotWaitOnceTheMinimumHasPassed()
    {
        var clock = new FakeClock(100.0, 103.5);
        ScriptedConsumer mock = Scripted();
        mock.SchedulePollTask(mock.Wakeup);
        Verifiable(mock, minStartupVisible: TimeSpan.FromSeconds(3), clock: clock).Run();
        Assert.Empty(clock.Sleeps);
    }

    [Fact]
    public void RunClosesWithTheCloseTimeout()
    {
        ScriptedConsumer mock = Scripted();
        mock.SchedulePollTask(mock.Wakeup);
        Verifiable(mock, closeTimeoutMs: 15000).Run();
        CloseOptions option = Assert.Single(mock.CloseOptionsGiven);
        Assert.Equal(TimeSpan.FromSeconds(15), option.Timeout());
        Assert.Equal(GroupMembershipOperation.DEFAULT, option.GroupMembershipOperation());
        Assert.True(mock.Mock.Closed());
    }

    [Fact]
    public void CloseAnnouncesTheShutdownAndWakesTheConsumer()
    {
        MockConsumer<string, string> mock = Mock();
        mock.Subscribe(new[] { "t" });
        Verifiable(mock).Close();
        Assert.Equal(new[] { "shutdown_requested" }, Lines().Select(Name));
        Assert.Throws<WakeupException>(() => mock.Poll(TimeSpan.Zero));
    }

    [Fact]
    public void GroupProtocolOfIgnoresCase()
    {
        Assert.Equal(GroupProtocol.CONSUMER, GroupProtocolMethods.Of("consumer"));
        Assert.Equal(GroupProtocol.CLASSIC, GroupProtocolMethods.Of("Classic"));
    }

    [Fact]
    public void GroupProtocolOfRejectsAnUnknownProtocol()
    {
        Assert.Equal(
            "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.BOGUS",
            Assert.Throws<IllegalArgumentException>(() => GroupProtocolMethods.Of("bogus")).Message);
    }

    [Fact]
    public void ParseArgsMinimalIsClassicByDefault()
    {
        (Namespace res, Dictionary<string, object?> props) = Parse(s_minimal);
        Assert.Equal(
            ("classic", (string?)null, (string?)null, (int?)-1, (int?)null, false, false, "earliest"),
            (res.GetString("groupProtocol"), res.GetString("groupRemoteAssignor"), res.GetString("groupInstanceId"),
                res.GetInt("maxMessages"), res.GetInt("sessionTimeout"), res.GetBoolean("verbose"),
                res.GetBoolean("useAutoCommit"), res.GetString("resetPolicy")));
        Assert.Equal(
            new Dictionary<string, object?>
            {
                ["group.protocol"] = "CLASSIC",
                ["partition.assignment.strategy"] = "org.apache.kafka.clients.consumer.RangeAssignor",
                ["group.id"] = "g",
                ["bootstrap.servers"] = "b",
                ["enable.auto.commit"] = false,
                ["auto.offset.reset"] = "earliest",
            },
            props);
    }

    [Fact]
    public void ParseArgsConsumerProtocol()
    {
        (Namespace res, Dictionary<string, object?> props) = Parse(
            s_minimal.Concat(new[]
            {
                "--group-protocol", "consumer", "--group-remote-assignor", "uniform", "--group-instance-id", "inst-1",
                "--max-messages", "100", "--session-timeout", "30000", "--verbose", "--enable-autocommit",
                "--reset-policy", "latest",
            }).ToArray());
        Assert.Equal(
            ((int?)100, true, true), (res.GetInt("maxMessages"), res.GetBoolean("verbose"), res.GetBoolean("useAutoCommit")));
        Assert.Equal(
            new Dictionary<string, object?>
            {
                ["group.protocol"] = "CONSUMER",
                ["group.remote.assignor"] = "uniform",
                ["session.timeout.ms"] = "30000",
                ["group.id"] = "g",
                ["group.instance.id"] = "inst-1",
                ["bootstrap.servers"] = "b",
                ["enable.auto.commit"] = true,
                ["auto.offset.reset"] = "latest",
            },
            props);
    }

    [Fact]
    public void ParseArgsCloseTimeout()
    {
        (Namespace res, Dictionary<string, object?> props) = Parse(s_minimal);
        Assert.Equal(30000, res.GetInt("closeTimeout"));
        (Namespace resWithTimeout, Dictionary<string, object?> propsWithTimeout) =
            Parse(s_minimal.Concat(new[] { "--close-timeout", "15000" }).ToArray());
        Assert.Equal(15000, resWithTimeout.GetInt("closeTimeout"));
        Assert.Equal(props, propsWithTimeout);
    }

    [Fact]
    public void ParseArgsCloseTimeoutMustBeAnInteger()
    {
        Assert.Equal(
            "argument --close-timeout: invalid int value: '15s'",
            ParseError(s_minimal.Concat(new[] { "--close-timeout", "15s" }).ToArray()));
    }

    [Fact]
    public void ParseArgsConsumerProtocolIgnoresTheAssignmentStrategy()
    {
        (_, Dictionary<string, object?> props) =
            Parse(s_minimal.Concat(new[] { "--group-protocol", "CONSUMER", "--assignment-strategy", "x" }).ToArray());
        Assert.DoesNotContain("partition.assignment.strategy", props.Keys);
        Assert.DoesNotContain("group.remote.assignor", props.Keys);
    }

    [Fact]
    public void ParseArgsLastRepeatedOptionWins()
    {
        IEnumerable<string> repeated = Enumerable.Repeat(new[] { "--session-timeout", "10" }, 3).SelectMany(pair => pair);
        (_, Dictionary<string, object?> props) =
            Parse(s_minimal.Concat(repeated).Concat(new[] { "--session-timeout", "20" }).ToArray());
        Assert.Equal("20", props["session.timeout.ms"]);
    }

    [Fact]
    public void ParseArgsCommandLineOverridesTheConfigFile()
    {
        string config = WriteConfig("group.id=from-file\nclient.id=tool\nauto.offset.reset=none\n");
        (_, Dictionary<string, object?> props) = Parse(s_minimal.Concat(new[] { "--command-config", config }).ToArray());
        Assert.Equal(
            ((object?)"g", (object?)"tool", (object?)"earliest"),
            (props["group.id"], props["client.id"], props["auto.offset.reset"]));
    }

    [Fact]
    public void ParseArgsDeprecatedConsumerConfigStillLoads()
    {
        string config = WriteConfig("client.id=tool\n");
        (_, Dictionary<string, object?> props) = Parse(s_minimal.Concat(new[] { "--consumer.config", config }).ToArray());
        Assert.Equal("tool", props["client.id"]);
        Assert.Equal(
            "Option --consumer.config has been deprecated and will be removed in a future version. "
            + "Use --command-config instead.\n",
            _stdout.ToString());
    }

    [Fact]
    public void ParseArgsConfigFilesAreMutuallyExclusive()
    {
        string config = WriteConfig(string.Empty);
        Assert.Equal(
            "Options --consumer.config and --command-config are mutually exclusive.",
            ParseError(s_minimal.Concat(new[] { "--consumer.config", config, "--command-config", config }).ToArray()));
    }

    [Fact]
    public void ParseArgsMissingBootstrapServer()
    {
        Assert.Equal(
            "one of the arguments --bootstrap-server is required", ParseError("--topic", "t", "--group-id", "g"));
    }

    [Fact]
    public void ParseArgsMissingTopicAndGroupId()
    {
        Assert.Equal(
            "the following arguments are required: --topic, --group-id", ParseError("--bootstrap-server", "b"));
    }

    [Fact]
    public void ParseArgsMissingValue()
    {
        Assert.Equal(
            "argument --topic: expected one argument", ParseError(s_minimal.Concat(new[] { "--topic" }).ToArray()));
    }

    [Fact]
    public void ParseArgsUnrecognizedArgument()
    {
        Assert.Equal(
            "unrecognized arguments: --bogus", ParseError(s_minimal.Concat(new[] { "--bogus" }).ToArray()));
    }

    [Fact]
    public void ParseArgsUnknownGroupProtocol()
    {
        Assert.Throws<IllegalArgumentException>(
            () => Parse(s_minimal.Concat(new[] { "--group-protocol", "bogus" }).ToArray()));
    }

    [Fact]
    public void CreateFromArgsBuildsAKafkaConsumer()
    {
        VerifiableConsumer consumer = VerifiableConsumer.CreateFromArgs(
            VerifiableConsumer.ArgParser(),
            new[]
            {
                "--bootstrap-server", "localhost:1", "--topic", "t", "--group-id", "g", "--group-protocol", "consumer",
                "--max-messages", "4", "--verbose", "--close-timeout", "15000",
            },
            _output);
        try
        {
            Assert.Equal(
                ("t", 4, true, false, false, 15000),
                (consumer.Topic, consumer.MaxMessages, consumer.Verbose, consumer.UseAutoCommit, consumer.UseAsyncCommit,
                    consumer.CloseTimeoutMs));
        }
        finally
        {
            consumer.Consumer.Close();
        }
    }

    [Fact]
    public void CreateFromArgsClassicProtocolIsUnsupported()
    {
        Assert.Equal(
            "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).",
            Assert.Throws<UnsupportedOperationException>(
                () => VerifiableConsumer.CreateFromArgs(
                    VerifiableConsumer.ArgParser(),
                    new[] { "--bootstrap-server", "localhost:1", "--topic", "t", "--group-id", "g" },
                    _output)).Message);
    }

    [Fact]
    public async Task ScriptWithoutArgumentsPrintsHelp()
    {
        (int exitCode, string stdout, _) = await ToolProcess.RunAsync(Tool);
        Assert.Equal(0, exitCode);
        Assert.StartsWith("usage: verifiable-consumer", stdout, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScriptWithBadArgumentsExits1()
    {
        (int exitCode, _, string stderr) = await ToolProcess.RunAsync(Tool, "--bootstrap-server", "b");
        Assert.Equal(1, exitCode);
        Assert.EndsWith(
            "verifiable-consumer: error: the following arguments are required: --topic, --group-id\n",
            stderr,
            StringComparison.Ordinal);
    }

    [UnixTheory]
    [InlineData("TERM")]
    [InlineData("INT")]
    public async Task ScriptShutsDownCleanlyOnASignal(string signal)
    {
        using ToolProcess process = ToolProcess.Start(
            Tool, "--bootstrap-server", "localhost:1", "--topic", "t", "--group-id", "g", "--group-protocol", "consumer");
        JsonElement startup = JsonDocument.Parse(await process.ReadLineAsync()).RootElement;
        Assert.Equal("startup_complete", Name(startup));
        var started = Stopwatch.StartNew();
        process.Signal(signal);
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();
        TimeSpan shutdown = started.Elapsed;

        Assert.True(exitCode == 0, stderr);
        List<JsonElement> lines = Lines(stdout);
        Assert.Equal(new[] { "shutdown_requested", "shutdown_complete" }, lines.Select(Name));
        Assert.True(
            lines[1].GetProperty("timestamp").GetInt64() - startup.GetProperty("timestamp").GetInt64() >= 2900, stdout);
        Assert.True(shutdown < TimeSpan.FromSeconds(30), stderr);
        Assert.DoesNotContain("Error during processing", stderr, StringComparison.Ordinal);
    }

    internal static string Name(JsonElement line) => line.GetProperty("name").GetString()!;

    internal static List<JsonElement> Lines(string stdout) =>
        stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .ToList();

    private static ConsumerRecord<string, string> Record(TopicPartition tp, long offset, string? key = null, string? value = "v") =>
        new ConsumerRecord<string, string>(tp.Topic(), tp.Partition(), offset, key, value);

    private static ConsumerRecords<string, string> Records(params (TopicPartition Partition, long[] Offsets)[] byPartition)
    {
        var records = new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<string, string>>>();
        foreach ((TopicPartition partition, long[] offsets) in byPartition)
        {
            records[partition] = offsets.Select(offset => Record(partition, offset)).ToList();
        }

        return new ConsumerRecords<string, string>(records, new Dictionary<TopicPartition, OffsetAndMetadata>());
    }

    private static ConsumerRecords<string, string> Records(TopicPartition partition, params ConsumerRecord<string, string>[] records) =>
        new ConsumerRecords<string, string>(
            new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<string, string>>> { [partition] = records },
            new Dictionary<TopicPartition, OffsetAndMetadata>());

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static void AssignAndAdd(MockConsumer<string, string> mock, params long[] offsets)
    {
        mock.Rebalance(new[] { s_tp0 });
        mock.UpdateBeginningOffset("t", 0, 0);
        foreach (long offset in offsets)
        {
            mock.AddRecord("t", 0, offset, null, Encoding.UTF8.GetBytes(offset.ToString(CultureInfo.InvariantCulture)));
        }
    }

    private static string WithoutTimestamp(string line)
    {
        Assert.Matches(s_timestamp, line);
        return "{" + s_timestamp.Replace(line, string.Empty);
    }

    private (Namespace Res, Dictionary<string, object?> Props) Parse(params string[] args) =>
        VerifiableConsumer.ParseArgs(VerifiableConsumer.ArgParser(), args, _output);

    private string ParseError(params string[] args) =>
        Assert.Throws<ArgumentParserException>(() => Parse(args)).Message;

    private ScriptedConsumer Scripted(params Exception[] commitErrors)
    {
        var consumer = new ScriptedConsumer(commitErrors);
        _consumers.Add(consumer);
        return consumer;
    }

    private MockConsumer<string, string> Mock()
    {
        var consumer = new MockConsumer<string, string>(new StringDeserializer(), new StringDeserializer(), "earliest");
        _consumers.Add(consumer);
        return consumer;
    }

    private VerifiableConsumer Verifiable(
        IConsumer<string, string>? consumer = null,
        int maxMessages = -1,
        bool verbose = false,
        bool useAutoCommit = false,
        int closeTimeoutMs = 30000,
        TimeSpan? minStartupVisible = null,
        FakeClock? clock = null) =>
        new VerifiableConsumer(
            consumer ?? Scripted(),
            _output,
            "t",
            maxMessages,
            useAutoCommit,
            false,
            verbose,
            closeTimeoutMs,
            minStartupVisible ?? TimeSpan.Zero,
            clock is null ? () => TimeSpan.Zero : clock.Monotonic,
            clock is null ? Thread.Sleep : clock.Sleep);

    private List<JsonElement> Lines() => Lines(_stdout.ToString());

    private List<string> LinesWithoutTimestamp() =>
        _stdout.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(WithoutTimestamp).ToList();

    private string WriteConfig(string contents)
    {
        string path = Path.Combine(_tmpPath, "c.properties");
        File.WriteAllText(path, contents);
        return path;
    }

    /// <summary>A scripted monotonic clock, and a sleep that records what it is asked to wait.</summary>
    private sealed class FakeClock
    {
        private readonly Queue<TimeSpan> _readings;

        internal FakeClock(params double[] readingsSeconds)
        {
            _readings = new Queue<TimeSpan>(readingsSeconds.Select(seconds => TimeSpan.FromSeconds(seconds)));
        }

        internal List<TimeSpan> Sleeps { get; } = new List<TimeSpan>();

        internal TimeSpan Monotonic() => _readings.Dequeue();

        internal void Sleep(TimeSpan duration) => Sleeps.Add(duration);
    }

    /// <summary>An output whose every write fails with an error other than a closed pipe.</summary>
    private sealed class FailingWriter : StringWriter
    {
        public override void Write(string? value) => throw new InvalidOperationException("stdout is gone");
    }
}

/// <summary>Serializes the tests that swap the process-wide <see cref="Console.Error"/>.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ConsoleErrorCollection
{
    public const string Name = "Console.Error capture";
}

/// <summary>
/// The two tests of Python's <c>test_verifiable_consumer.py</c> that read the tool's error log, which goes to
/// <see cref="Console.Error"/>, captured with no other test running.
/// </summary>
[Collection(ConsoleErrorCollection.Name)]
public sealed class VerifiableConsumerLogTests
{
    [Fact]
    public void RunEndsQuietlyOnWakeup()
    {
        using var mock = new ScriptedConsumer();
        mock.SchedulePollTask(mock.Wakeup);
        var stdout = new StringWriter();
        string stderr = CapturingStderr(() => Verifiable(mock, stdout).Run());
        Assert.Equal(new[] { "startup_complete", "shutdown_complete" }, Names(stdout));
        Assert.DoesNotContain("] ERROR ", stderr, StringComparison.Ordinal);
        Assert.True(mock.Mock.Closed());
    }

    [Fact]
    public void RunLogsAnyOtherErrorAndStillShutsDown()
    {
        using var mock = new MockConsumer<string, string>(new StringDeserializer(), new StringDeserializer(), "earliest");
        mock.SetPollException(new KafkaException("poll failed"));
        var stdout = new StringWriter();
        string stderr = CapturingStderr(() => Verifiable(mock, stdout).Run());
        Assert.Equal(new[] { "startup_complete", "shutdown_complete" }, Names(stdout));
        Assert.Single(Regex.Matches(stderr, "\\] ERROR "));
        Assert.Matches(
            new Regex(
                "^\\[[^\\]]+\\] ERROR Error during processing, terminating consumer process:  \\(verifiable_consumer\\)\n"
                + "Confluent\\.Kafka\\.Common\\.KafkaException: poll failed\r?\n"),
            stderr);
        Assert.True(mock.Closed());
    }

    private static VerifiableConsumer Verifiable(IConsumer<string, string> consumer, StringWriter stdout) =>
        new VerifiableConsumer(
            consumer, new JsonOutput(stdout), "t", -1, false, false, false, 30000, TimeSpan.Zero, () => TimeSpan.Zero, Thread.Sleep);

    private static IEnumerable<string> Names(StringWriter stdout) =>
        VerifiableConsumerTests.Lines(stdout.ToString()).Select(VerifiableConsumerTests.Name);

    private static string CapturingStderr(Action action)
    {
        TextWriter original = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            action();
        }
        finally
        {
            Console.SetError(original);
        }

        return captured.ToString();
    }
}
