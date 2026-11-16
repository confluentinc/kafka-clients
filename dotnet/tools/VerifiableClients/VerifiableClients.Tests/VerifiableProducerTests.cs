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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// Python's <c>test_verifiable_producer.py</c>: the Rust tool's tests over <c>MockProducer</c>, and the tool run
/// end to end against an unreachable broker; plus the Rust tool's key-counter overflow test. The mock runs a
/// delivery callback on its completion thread, so a test flushes it before reading what the callback did.
/// </summary>
public sealed class VerifiableProducerTests : IDisposable
{
    private const string Tool = "VerifiableProducer";

    private static readonly Regex s_timestamp = new Regex("^\\{\"timestamp\":[0-9]+,");

    private readonly string _tmpPath = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), "verifiable-producer-tests-" + Guid.NewGuid().ToString("N"))).FullName;

    private readonly StringWriter _stdout = new StringWriter();

    private readonly List<RecordingProducer> _producers = new List<RecordingProducer>();

    private readonly JsonOutput _output;

    public VerifiableProducerTests()
    {
        _output = new JsonOutput(_stdout);
    }

    public void Dispose()
    {
        foreach (RecordingProducer producer in _producers)
        {
            producer.Dispose();
        }

        Directory.Delete(_tmpPath, recursive: true);
    }

    [Fact]
    public void GetValueWithoutPrefixIsJustTheNumber()
    {
        VerifiableProducer producer = Verifiable();
        Assert.Equal("0", producer.GetValue(0));
        Assert.Equal("42", producer.GetValue(42));
    }

    [Fact]
    public void GetValueWithPrefixUsesDotSeparator()
    {
        VerifiableProducer producer = Verifiable(valuePrefix: 7);
        Assert.Equal("7.0", producer.GetValue(0));
        Assert.Equal("7.42", producer.GetValue(42));
    }

    [Fact]
    public void GetKeyIsNoneWithoutRepeatingKeys()
    {
        VerifiableProducer producer = Verifiable();
        Assert.Null(producer.GetKey());
        Assert.Null(producer.GetKey());
    }

    [Fact]
    public void GetKeyWrapsAroundAtRepeatingKeys()
    {
        VerifiableProducer producer = Verifiable(repeatingKeys: 3);
        Assert.Equal(
            new[] { "0", "1", "2", "0", "1" }, Enumerable.Range(0, 5).Select(_ => producer.GetKey()).ToArray());
    }

    [Fact]
    public void GetKeyCounterWrapsOnOverflow()
    {
        VerifiableProducer producer = Verifiable(repeatingKeys: 0);
        producer.KeyCounter = int.MaxValue;
        Assert.Equal("2147483647", producer.GetKey());
        Assert.Equal("-2147483648", producer.GetKey());
    }

    [Fact]
    public void SendIncrementsSentAndAckedViaCallback()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock);
        Assert.Equal((0L, 0L), (producer.NumSent, producer.NumAcked));
        producer.Send("k", "0");
        mock.Flush();
        Assert.Equal((1L, 1L), (producer.NumSent, producer.NumAcked));
        producer.Send(null, "1");
        mock.Flush();
        Assert.Equal((2L, 2L), (producer.NumSent, producer.NumAcked));
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_success","key":"k","value":"0","topic":"t","partition":0,"offset":0}""",
                """{"name":"producer_send_success","key":null,"value":"1","topic":"t","partition":0,"offset":1}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void SendFailureFromTheCallbackIsAFailedSend()
    {
        RecordingProducer mock = MockProducer(autoComplete: false);
        VerifiableProducer producer = Verifiable(mock);
        producer.Send("k", "0");
        Assert.True(mock.Mock.ErrorNext(new Confluent.Kafka.Common.Errors.TimeoutException("Expiring 1 record(s)")));
        mock.Flush();
        Assert.Equal((1L, 0L), (producer.NumSent, producer.NumAcked));
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_error","key":"k","value":"0","topic":"t","exception":"class Confluent.Kafka.Common.Errors.TimeoutException","message":"Expiring 1 record(s)"}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void SendFailureRaisedBySendIsAFailedSend()
    {
        RecordingProducer mock = MockProducer();
        mock.Mock.SetSendException(new KafkaException("send failed"));
        VerifiableProducer producer = Verifiable(mock);
        producer.Send(null, "0");
        Assert.Equal((1L, 0L), (producer.NumSent, producer.NumAcked));
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_error","key":null,"value":"0","topic":"t","exception":"class Confluent.Kafka.Common.KafkaException","message":"send failed"}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void CreateTimeIsTheRecordTimestampAndAdvances()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock, createTime: 1_000);
        producer.StartTime -= 5;
        producer.Send(null, "0");
        producer.Send(null, "1");
        long?[] timestamps = mock.History().Select(record => record.Timestamp()).ToArray();
        Assert.Equal(2, timestamps.Length);
        Assert.Equal(1_000, timestamps[0]);
        Assert.True(timestamps[1] >= 1_005, timestamps[1].ToString());
        Assert.True(producer.CreateTime >= timestamps[1] + 5, producer.CreateTime.ToString());
    }

    [Fact]
    public void RecordWithoutCreateTimeHasNoTimestamp()
    {
        RecordingProducer mock = MockProducer();
        Verifiable(mock).Send("k", "0");
        ProducerRecord<string, string> record = mock.History()[0];
        Assert.Equal(("t", (int?)null, (long?)null), (record.Topic(), record.Partition(), record.Timestamp()));
        Assert.Equal(("k", "0"), (record.Key(), record.Value()));
    }

    [Fact]
    public void NegativeCreateTimeIsAFailedSendNotCounted()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock, createTime: -5);
        producer.Send(null, "0");
        Assert.Equal((0L, 0), (producer.NumSent, mock.History().Count));
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_error","key":null,"value":"0","topic":"t","exception":"class Confluent.Kafka.IllegalArgumentException","message":"Invalid timestamp: -5. Timestamp should always be non-negative or null."}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void RunProducesMaxMessages()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock, maxMessages: 3);
        producer.Run(new ThroughputThrottler(-1, 0));
        mock.Flush();
        Assert.Equal(new[] { "0", "1", "2" }, mock.History().Select(record => record.Value()));
        Assert.Equal(
            new[] { "startup_complete", "producer_send_success", "producer_send_success", "producer_send_success" },
            Lines().Select(Name));
        Assert.Equal((3L, 3L), (producer.NumSent, producer.NumAcked));
    }

    [Fact]
    public void RunValuesCarryThePrefixAndKeysRepeat()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock, maxMessages: 3, valuePrefix: 2, repeatingKeys: 2);
        producer.Run(new ThroughputThrottler(-1, 0));
        Assert.Equal(
            new (string?, string?)[] { ("0", "2.0"), ("1", "2.1"), ("0", "2.2") },
            mock.History().Select(record => (record.Key(), record.Value())));
    }

    [Fact]
    public void RunStopsWhenStopProducingIsSet()
    {
        RecordingProducer mock = MockProducer();
        VerifiableProducer producer = Verifiable(mock);
        producer.StopProducing = true;
        producer.Run(new ThroughputThrottler(-1, 0));
        Assert.Empty(mock.History());
        Assert.Equal(new[] { "startup_complete" }, Lines().Select(Name));
    }

    [Fact]
    public void CloseClosesTheProducerAndPrintsShutdownComplete()
    {
        RecordingProducer mock = MockProducer();
        Verifiable(mock).Close();
        Assert.True(mock.Mock.Closed());
        Assert.Equal(new[] { """{"name":"shutdown_complete"}""" }, LinesWithoutTimestamp());
    }

    [Fact]
    public void LifecycleEventsPutTimestampThenNameFirst()
    {
        VerifiableProducer.StartupComplete(_output);
        VerifiableProducer.ShutdownComplete(_output);
        Assert.Equal(
            new[] { """{"name":"startup_complete"}""", """{"name":"shutdown_complete"}""" }, LinesWithoutTimestamp());
    }

    [Fact]
    public void SuccessfulSendFieldsInJavaOrder()
    {
        var metadata = new RecordMetadata(new TopicPartition("t", 2), 40, 2, -1, 1, 1);
        VerifiableProducer.SuccessfulSend(_output, "k", "v", metadata);
        Assert.Equal(
            new[] { """{"name":"producer_send_success","key":"k","value":"v","topic":"t","partition":2,"offset":42}""" },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void FailedSendFieldsInJavaOrder()
    {
        VerifiableProducer.FailedSend(_output, null, "v", "t", new KafkaException("boom"));
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_error","key":null,"value":"v","topic":"t","exception":"class Confluent.Kafka.Common.KafkaException","message":"boom"}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void FailedSendWithNoMessageIsNull()
    {
        VerifiableProducer.FailedSend(_output, "k", "v", "t", new KafkaException());
        Assert.Equal(
            new[]
            {
                """{"name":"producer_send_error","key":"k","value":"v","topic":"t","exception":"class Confluent.Kafka.Common.KafkaException","message":null}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void ToolDataFieldsInJavaOrder()
    {
        VerifiableProducer.ToolData(_output, 10, 9, 100, 4.5);
        VerifiableProducer.ToolData(_output, 10, 8, 100, 50.0);
        VerifiableProducer.ToolData(_output, 0, 0, -1, 0.0);
        Assert.Equal(
            new[]
            {
                """{"name":"tool_data","sent":10,"acked":9,"target_throughput":100,"avg_throughput":4.5}""",
                """{"name":"tool_data","sent":10,"acked":8,"target_throughput":100,"avg_throughput":50.0}""",
                """{"name":"tool_data","sent":0,"acked":0,"target_throughput":-1,"avg_throughput":0.0}""",
            },
            LinesWithoutTimestamp());
    }

    [Fact]
    public void ParseArgsMinimal()
    {
        (Namespace res, Dictionary<string, object?> props) = Parse("--topic", "t", "--bootstrap-server", "b:9092");
        Assert.Equal(
            ("t", "b:9092", -1, -1, -1),
            (res.GetString("topic"), res.GetString("bootstrapServer"), res.GetInt("maxMessages"),
                res.GetInt("throughput"), res.GetInt("acks")));
        Assert.Equal(
            ((long?)-1, (int?)null, (int?)null),
            (res.GetLong("createTime"), res.GetInt("valuePrefix"), res.GetInt("repeatingKeys")));
        Assert.Equal(
            new Dictionary<string, object?> { ["bootstrap.servers"] = "b:9092", ["acks"] = "-1", ["retries"] = "0" },
            props);
    }

    [Fact]
    public void ParseArgsEveryOption()
    {
        (Namespace res, Dictionary<string, object?> props) = Parse(
            "--topic", "t", "--bootstrap-server", "b", "--max-messages", "5", "--throughput", "100", "--acks", "1",
            "--message-create-time", "1234", "--value-prefix", "3", "--repeating-keys", "4");
        Assert.Equal(
            ((int?)5, (int?)100, (long?)1234, (int?)3, (int?)4),
            (res.GetInt("maxMessages"), res.GetInt("throughput"), res.GetLong("createTime"),
                res.GetInt("valuePrefix"), res.GetInt("repeatingKeys")));
        Assert.Equal("1", props["acks"]);
    }

    [Fact]
    public void ParseArgsValidAcksChoices()
    {
        foreach (string acks in new[] { "0", "1", "-1" })
        {
            (Namespace res, Dictionary<string, object?> props) =
                Parse("--topic", "t", "--bootstrap-server", "b", "--acks", acks);
            Assert.Equal((int.Parse(acks, CultureInfo.InvariantCulture), (object?)acks), (res.GetInt("acks")!.Value, props["acks"]));
        }
    }

    [Fact]
    public void ParseArgsInvalidAcksChoice()
    {
        Assert.Equal(
            "argument --acks: invalid choice: 5 (choose from 0, 1, -1)",
            ParseError("--topic", "t", "--bootstrap-server", "b", "--acks", "5"));
    }

    [Fact]
    public void ParseArgsMissingTopic()
    {
        Assert.Equal("the following arguments are required: --topic", ParseError("--bootstrap-server", "b"));
    }

    [Fact]
    public void ParseArgsMissingBootstrapServer()
    {
        Assert.Equal("one of the arguments --bootstrap-server is required", ParseError("--topic", "t"));
    }

    [Fact]
    public void ParseArgsMissingValue()
    {
        Assert.Equal("argument --topic: expected one argument", ParseError("--topic"));
    }

    [Fact]
    public void ParseArgsNotAnInteger()
    {
        Assert.Equal(
            "argument --max-messages: invalid int value: 'x'",
            ParseError("--topic", "t", "--bootstrap-server", "b", "--max-messages", "x"));
    }

    [Fact]
    public void ParseArgsUnrecognizedArgument()
    {
        Assert.Equal(
            "unrecognized arguments: --bogus", ParseError("--topic", "t", "--bootstrap-server", "b", "--bogus"));
    }

    [Fact]
    public void ParseArgsConfigFilesAreMutuallyExclusive()
    {
        string config = WriteConfig(string.Empty);
        Assert.Equal(
            "Options --producer.config and --command-config are mutually exclusive.",
            ParseError(
                "--topic", "t", "--bootstrap-server", "b", "--producer.config", config, "--command-config", config));
    }

    [Fact]
    public void CommandConfigOverridesTheCommandLine()
    {
        string config = WriteConfig(
            "# written by the system test\nacks=1\nretries = 5\nlinger.ms: 10\nclient.id   tool\n");
        (_, Dictionary<string, object?> props) =
            Parse("--topic", "t", "--bootstrap-server", "b", "--command-config", config);
        Assert.Equal(
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = "b",
                ["acks"] = "1",
                ["retries"] = "5",
                ["linger.ms"] = "10",
                ["client.id"] = "tool",
            },
            props);
    }

    [Fact]
    public void DeprecatedProducerConfigStillLoads()
    {
        string config = WriteConfig("linger.ms=10\n");
        (_, Dictionary<string, object?> props) =
            Parse("--topic", "t", "--bootstrap-server", "b", "--producer.config", config);
        Assert.Equal("10", props["linger.ms"]);
        Assert.Equal(
            "Option --producer.config has been deprecated and will be removed in a future version. "
            + "Use --command-config instead.\n",
            _stdout.ToString());
    }

    [Fact]
    public void UnreadableConfigFileIsAnArgumentError()
    {
        string missing = Path.Combine(_tmpPath, "missing.properties");
        Assert.Contains(
            missing,
            ParseError("--topic", "t", "--bootstrap-server", "b", "--command-config", missing),
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateFromArgsBuildsAKafkaProducer()
    {
        VerifiableProducer producer = VerifiableProducer.CreateFromArgs(
            VerifiableProducer.ArgParser(),
            new[] { "--topic", "t", "--bootstrap-server", "localhost:1", "--message-create-time", "-1", "--throughput", "7" },
            _output);
        try
        {
            Assert.Equal(("t", 7L, (long?)null), (producer.Topic, producer.Throughput, producer.CreateTime));
        }
        finally
        {
            producer.Producer.Close();
        }
    }

    [Fact]
    public async Task ScriptWithoutArgumentsPrintsHelp()
    {
        (int exitCode, string stdout, _) = await ToolProcess.RunAsync(Tool);
        Assert.Equal(0, exitCode);
        Assert.StartsWith("usage: verifiable-producer", stdout, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScriptWithBadArgumentsExits1()
    {
        (int exitCode, string stdout, string stderr) = await ToolProcess.RunAsync(Tool, "--topic", "t");
        Assert.Equal(1, exitCode);
        Assert.Equal(string.Empty, stdout);
        Assert.EndsWith(
            "verifiable-producer: error: one of the arguments --bootstrap-server is required\n",
            stderr,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScriptReportsUnackedSendsThenItsSummary()
    {
        string config = WriteConfig("max.block.ms=200\n");
        (int exitCode, string stdout, string stderr) = await ToolProcess.RunAsync(
            Tool, "--topic", "t", "--bootstrap-server", "localhost:1", "--max-messages", "2", "--command-config", config);
        Assert.True(exitCode == 0, stderr);
        List<JsonElement> lines = Lines(stdout);
        Assert.Equal(
            new[] { "startup_complete", "producer_send_error", "producer_send_error", "shutdown_complete", "tool_data" },
            lines.Select(Name));
        Assert.Equal(new[] { "0", "1" }, lines.Skip(1).Take(2).Select(line => line.GetProperty("value").GetString()));
        JsonElement summary = lines[^1];
        Assert.Equal(
            (2L, 0L, -1L),
            (summary.GetProperty("sent").GetInt64(), summary.GetProperty("acked").GetInt64(),
                summary.GetProperty("target_throughput").GetInt64()));
    }

    [UnixFact]
    public async Task ScriptShutsDownCleanlyOnSigterm()
    {
        string config = WriteConfig("max.block.ms=200\n");
        using ToolProcess process = ToolProcess.Start(
            Tool, "--topic", "t", "--bootstrap-server", "localhost:1", "--throughput", "0", "--command-config", config);
        var names = new List<string>();
        while (names.Count < 3)
        {
            names.Add(Name(await process.ReadLineAsync()));
        }

        Assert.Equal(new[] { "startup_complete", "producer_send_error", "producer_send_error" }, names);
        process.Signal("TERM");
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();

        Assert.True(exitCode == 0, stderr);
        List<JsonElement> lines = Lines(stdout);
        Assert.Equal(new[] { "shutdown_complete", "tool_data" }, lines.Select(Name));
        Assert.Equal(
            (2L, 0L, 0L),
            (lines[1].GetProperty("sent").GetInt64(), lines[1].GetProperty("acked").GetInt64(),
                lines[1].GetProperty("target_throughput").GetInt64()));
    }

    [UnixFact]
    public async Task ScriptSigtermReleasesABlockedSend()
    {
        string config = WriteConfig("max.block.ms=60000\n");
        using ToolProcess process = ToolProcess.Start(
            Tool, "--topic", "t", "--bootstrap-server", "localhost:1", "--command-config", config);
        Assert.Equal("startup_complete", Name(await process.ReadLineAsync()));
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var signalled = Stopwatch.StartNew();
        process.Signal("TERM");
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();
        TimeSpan shutdown = signalled.Elapsed;

        Assert.True(exitCode == 0, stderr);
        Assert.True(shutdown < TimeSpan.FromSeconds(20), stderr);
        List<JsonElement> lines = Lines(stdout);
        List<string> names = lines.Select(Name).ToList();
        Assert.Equal(
            new[] { "producer_send_error", "shutdown_complete", "tool_data" }, names.OrderBy(n => n, StringComparer.Ordinal));
        Assert.True(names.IndexOf("shutdown_complete") < names.IndexOf("tool_data"), string.Join(",", names));
        JsonElement error = lines[names.IndexOf("producer_send_error")];
        Assert.Equal(
            ("0", "Producer closed while send in progress"),
            (error.GetProperty("value").GetString(), error.GetProperty("message").GetString()));
        JsonElement summary = lines[names.IndexOf("tool_data")];
        Assert.Equal((1L, 0L), (summary.GetProperty("sent").GetInt64(), summary.GetProperty("acked").GetInt64()));
    }

    private static string Name(string line) => Name(JsonDocument.Parse(line).RootElement);

    private static string Name(JsonElement line) => line.GetProperty("name").GetString()!;

    private static List<JsonElement> Lines(string stdout) =>
        stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .ToList();

    private static string WithoutTimestamp(string line)
    {
        Assert.Matches(s_timestamp, line);
        return "{" + s_timestamp.Replace(line, string.Empty);
    }

    private (Namespace Res, Dictionary<string, object?> Props) Parse(params string[] args) =>
        VerifiableProducer.ParseArgs(VerifiableProducer.ArgParser(), args, _output);

    private string ParseError(params string[] args) =>
        Assert.Throws<ArgumentParserException>(() => Parse(args)).Message;

    private RecordingProducer MockProducer(bool autoComplete = true)
    {
        var producer = new RecordingProducer(autoComplete);
        _producers.Add(producer);
        return producer;
    }

    private VerifiableProducer Verifiable(
        RecordingProducer? producer = null,
        int throughput = -1,
        int maxMessages = -1,
        int? valuePrefix = null,
        long? createTime = null,
        int? repeatingKeys = null) =>
        new VerifiableProducer(
            producer ?? MockProducer(), _output, "t", throughput, maxMessages, valuePrefix, createTime, repeatingKeys);

    private List<JsonElement> Lines() => Lines(_stdout.ToString());

    private List<string> LinesWithoutTimestamp() =>
        _stdout.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(WithoutTimestamp).ToList();

    private string WriteConfig(string contents)
    {
        string path = Path.Combine(_tmpPath, "p.properties");
        File.WriteAllText(path, contents);
        return path;
    }
}
