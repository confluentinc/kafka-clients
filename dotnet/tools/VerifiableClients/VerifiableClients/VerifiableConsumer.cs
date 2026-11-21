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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Tools;

/// <summary>
/// Java's <c>org.apache.kafka.tools.VerifiableConsumer</c>: consumes a topic and prints a JSON line for each
/// rebalance, poll and offset commit, the lines Kafka's system tests read, with Java's event names, fields and field
/// order. It is its own rebalance listener and commit callback, as Java's.
/// </summary>
internal sealed class VerifiableConsumer : IOffsetCommitCallback, IConsumerRebalanceListener
{
    private const string LoggerName = "verifiable_consumer";

    private const string RangeAssignor = "org.apache.kafka.clients.consumer.RangeAssignor";

    private const string RoundRobinAssignor = "org.apache.kafka.clients.consumer.RoundRobinAssignor";

    // Java polls with Duration.ofMillis(Long.MAX_VALUE), more than a TimeSpan holds: TimeSpan.MaxValue is the
    // longest poll here.
    private static readonly TimeSpan s_pollTimeout = TimeSpan.MaxValue;

    // startup_complete stays visible at least this long before shutdown_complete, which Java does not do: the system
    // tests count a consumer as started only between the two, and one fenced on its first heartbeat would end too
    // fast for them to see.
    private static readonly TimeSpan s_minStartupVisible = TimeSpan.FromSeconds(3);

    private readonly JsonOutput _output;

    private readonly TimeSpan _minStartupVisible;

    private readonly Func<TimeSpan> _monotonic;

    private readonly Action<TimeSpan> _sleep;

    internal VerifiableConsumer(
        IConsumer<string, string> consumer,
        JsonOutput output,
        string topic,
        int maxMessages,
        bool useAutoCommit,
        bool useAsyncCommit,
        bool verbose,
        int closeTimeoutMs)
        : this(
            consumer,
            output,
            topic,
            maxMessages,
            useAutoCommit,
            useAsyncCommit,
            verbose,
            closeTimeoutMs,
            s_minStartupVisible,
            () => TimeSpan.FromMilliseconds(Environment.TickCount64),
            Thread.Sleep)
    {
    }

    internal VerifiableConsumer(
        IConsumer<string, string> consumer,
        JsonOutput output,
        string topic,
        int maxMessages,
        bool useAutoCommit,
        bool useAsyncCommit,
        bool verbose,
        int closeTimeoutMs,
        TimeSpan minStartupVisible,
        Func<TimeSpan> monotonic,
        Action<TimeSpan> sleep)
    {
        Consumer = consumer;
        _output = output;
        Topic = topic;
        MaxMessages = maxMessages;
        UseAutoCommit = useAutoCommit;
        UseAsyncCommit = useAsyncCommit;
        Verbose = verbose;
        CloseTimeoutMs = closeTimeoutMs;
        _minStartupVisible = minStartupVisible;
        _monotonic = monotonic;
        _sleep = sleep;
    }

    internal IConsumer<string, string> Consumer { get; }

    internal string Topic { get; }

    internal int MaxMessages { get; }

    internal bool UseAutoCommit { get; }

    internal bool UseAsyncCommit { get; }

    internal bool Verbose { get; }

    internal int CloseTimeoutMs { get; }

    internal int ConsumedMessages { get; set; }

    internal static int Main(string[] args)
    {
        try
        {
            return Run(args);
        }
        catch (Exception e)
        {
            Console.Error.Write(e + "\n");
            return 1;
        }
    }

    internal static ArgumentParser ArgParser()
    {
        var parser = new ArgumentParser(
            "verifiable-consumer",
            "This tool consumes messages from a specific topic and emits consumer events (e.g. group rebalances, "
            + "received messages, and offsets committed) as JSON objects to STDOUT.");

        ArgumentParser.MutuallyExclusiveGroup connectionGroup = parser.AddMutuallyExclusiveGroup(
            "Connection Group", "Group of arguments for connection to brokers", required: true);
        parser.AddArgument(
            "--bootstrap-server",
            dest: "bootstrapServer",
            metavar: "HOST1:PORT1[,HOST2:PORT2[...]]",
            help: "The server(s) to connect to. Comma-separated list of Kafka brokers in the form "
                + "HOST1:PORT1,HOST2:PORT2,...",
            group: connectionGroup);
        parser.AddArgument("--topic", required: true, metavar: "TOPIC", help: "Consumes messages from this topic.");
        parser.AddArgument(
            "--group-protocol",
            defaultValue: ConsumerConfig.DEFAULT_GROUP_PROTOCOL,
            dest: "groupProtocol",
            metavar: "GROUP-PROTOCOL",
            help: "Group protocol (must be one of " + string.Join(", ", Enum.GetNames<GroupProtocol>()) + ")");
        parser.AddArgument(
            "--group-remote-assignor",
            dest: "groupRemoteAssignor",
            metavar: "GROUP-REMOTE-ASSIGNOR",
            help: "Group remote assignor; only used if the group protocol is " + nameof(GroupProtocol.CONSUMER));
        parser.AddArgument(
            "--group-id",
            required: true,
            dest: "groupId",
            metavar: "GROUP-ID",
            help: "The group id of the consumer group");
        parser.AddArgument(
            "--group-instance-id",
            dest: "groupInstanceId",
            metavar: "GROUP-INSTANCE-ID",
            help: "A unique identifier of the consumer instance");
        parser.AddArgument(
            "--max-messages",
            type: typeof(int),
            defaultValue: -1,
            dest: "maxMessages",
            metavar: "MAX-MESSAGES",
            help: "Consume this many messages. If -1 (the default), the consumer will consume until the process is "
                + "killed externally");
        parser.AddArgument(
            "--session-timeout",
            type: typeof(int),
            dest: "sessionTimeout",
            metavar: "TIMEOUT-MS",
            help: "Set the consumer's session timeout, note that this configuration is not supported when group "
                + "protocol is consumer");
        parser.AddArgument("--verbose", storeTrue: true, help: "Enable to log individual consumed records");
        parser.AddArgument(
            "--enable-autocommit",
            dest: "useAutoCommit",
            storeTrue: true,
            help: "Enable offset auto-commit on consumer");
        parser.AddArgument(
            "--close-timeout",
            type: typeof(int),
            defaultValue: 30000,
            dest: "closeTimeout",
            metavar: "CLOSE-TIMEOUT-MS",
            help: "Timeout in milliseconds for closing the consumer (default: %(default)s)");
        parser.AddArgument(
            "--reset-policy",
            defaultValue: "earliest",
            dest: "resetPolicy",
            metavar: "RESET-POLICY",
            help: "Set reset policy (must be either 'earliest', 'latest', or 'none')");
        parser.AddArgument(
            "--assignment-strategy",
            defaultValue: RangeAssignor,
            dest: "assignmentStrategy",
            metavar: "ASSIGNMENT-STRATEGY",
            help: "Set assignment strategy (e.g. " + RoundRobinAssignor + "); only used if the group protocol is "
                + nameof(GroupProtocol.CLASSIC));
        parser.AddArgument(
            "--consumer.config",
            metavar: "CONFIG-FILE",
            help: "(DEPRECATED) Consumer config properties file. This option will be removed in a future version. "
                + "Use --command-config instead");
        parser.AddArgument(
            "--command-config",
            dest: "commandConfigFile",
            metavar: "CONFIG-FILE",
            help: "Config properties file (config options shared with command line parameters will be overridden).");
        return parser;
    }

    internal static (Namespace Res, Dictionary<string, object?> ConsumerProps) ParseArgs(
        ArgumentParser parser, IReadOnlyList<string> args, JsonOutput output)
    {
        Namespace res = parser.ParseArgs(args);
        string? configFile = res.GetString("consumer.config");
        string? commandConfigFile = res.GetString("commandConfigFile");
        if (configFile is not null && commandConfigFile is not null)
        {
            throw new ArgumentParserException("Options --consumer.config and --command-config are mutually exclusive.");
        }

        var consumerProps = new Dictionary<string, object?>();
        if (configFile is not null)
        {
            output.Println(
                "Option --consumer.config has been deprecated and will be removed in a future version. "
                + "Use --command-config instead.");
            Merge(consumerProps, Utils.LoadProps(configFile));
        }

        if (commandConfigFile is not null)
        {
            Merge(consumerProps, Utils.LoadProps(commandConfigFile));
        }

        GroupProtocol groupProtocol = GroupProtocolMethods.Of(res.GetString("groupProtocol")!);
        consumerProps["group.protocol"] = groupProtocol.ToString();
        if (groupProtocol == GroupProtocol.CONSUMER)
        {
            string? groupRemoteAssignor = res.GetString("groupRemoteAssignor");
            if (groupRemoteAssignor is not null)
            {
                consumerProps["group.remote.assignor"] = groupRemoteAssignor;
            }
        }
        else
        {
            consumerProps["partition.assignment.strategy"] = res.GetString("assignmentStrategy");
        }

        if (res.GetInt("sessionTimeout") is { } sessionTimeout)
        {
            consumerProps["session.timeout.ms"] = sessionTimeout.ToString(CultureInfo.InvariantCulture);
        }

        consumerProps["group.id"] = res.GetString("groupId");
        if (res.GetString("groupInstanceId") is { } groupInstanceId)
        {
            consumerProps["group.instance.id"] = groupInstanceId;
        }

        consumerProps["bootstrap.servers"] = res.GetString("bootstrapServer");
        consumerProps["enable.auto.commit"] = res.GetBoolean("useAutoCommit");
        consumerProps["auto.offset.reset"] = res.GetString("resetPolicy");
        return (res, consumerProps);
    }

    internal static VerifiableConsumer CreateFromArgs(ArgumentParser parser, IReadOnlyList<string> args, JsonOutput output)
    {
        (Namespace res, Dictionary<string, object?> props) = ParseArgs(parser, args, output);
        var consumer = KafkaConsumer<string, string>.Create(props, new StringDeserializer(), new StringDeserializer());
        return new VerifiableConsumer(
            consumer,
            output,
            res.GetString("topic")!,
            res.GetInt("maxMessages")!.Value,
            res.GetBoolean("useAutoCommit"),
            false,
            res.GetBoolean("verbose"),
            res.GetInt("closeTimeout")!.Value);
    }

    internal static void StartupComplete(JsonOutput output) => output.Print("startup_complete");

    internal static void ShutdownRequested(JsonOutput output) => output.Print("shutdown_requested");

    internal static void ShutdownComplete(JsonOutput output) => output.Print("shutdown_complete");

    internal static void PartitionsRevoked(JsonOutput output, IReadOnlyCollection<TopicPartition> partitions) =>
        output.Print("partitions_revoked", writer => WritePartitions(writer, partitions));

    internal static void PartitionsAssigned(JsonOutput output, IReadOnlyCollection<TopicPartition> partitions) =>
        output.Print("partitions_assigned", writer => WritePartitions(writer, partitions));

    internal static void RecordsConsumed(
        JsonOutput output,
        int count,
        IReadOnlyList<(TopicPartition Partition, int Count, long MinOffset, long MaxOffset)> partitionSummaries) =>
        output.Print("records_consumed", writer =>
        {
            writer.WriteNumber("count", count);
            writer.WriteStartArray("partitions");
            foreach ((TopicPartition partition, int partitionCount, long minOffset, long maxOffset) in partitionSummaries)
            {
                writer.WriteStartObject();
                WritePartitionData(writer, partition);
                writer.WriteNumber("count", partitionCount);
                writer.WriteNumber("minOffset", minOffset);
                writer.WriteNumber("maxOffset", maxOffset);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        });

    internal static void RecordData(JsonOutput output, ConsumerRecord<string, string> record) =>
        output.Print("record_data", writer =>
        {
            writer.WriteString("key", record.Key());
            writer.WriteString("value", record.Value());
            writer.WriteString("topic", record.Topic());
            writer.WriteNumber("partition", record.Partition());
            writer.WriteNumber("offset", record.Offset());
        });

    internal static void OffsetsCommitted(
        JsonOutput output, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, string? error, bool success) =>
        output.Print("offsets_committed", writer =>
        {
            writer.WriteStartArray("offsets");
            foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
            {
                writer.WriteStartObject();
                WritePartitionData(writer, entry.Key);
                writer.WriteNumber("offset", entry.Value.Offset());
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            if (error is not null)
            {
                writer.WriteString("error", error);
            }

            writer.WriteBoolean("success", success);
        });

    internal bool HasMessageLimit() => MaxMessages >= 0;

    internal bool IsFinished() => HasMessageLimit() && ConsumedMessages >= MaxMessages;

    internal Dictionary<TopicPartition, OffsetAndMetadata> OnRecordsReceived(ConsumerRecords<string, string> records)
    {
        var offsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
        var summaries = new List<(TopicPartition Partition, int Count, long MinOffset, long MaxOffset)>();
        foreach (TopicPartition tp in records.Partitions())
        {
            IReadOnlyList<ConsumerRecord<string, string>> partitionRecords = records.Records(tp);

            if (HasMessageLimit() && ConsumedMessages + partitionRecords.Count > MaxMessages)
            {
                partitionRecords = partitionRecords.Take(MaxMessages - ConsumedMessages).ToList();
            }

            if (partitionRecords.Count == 0)
            {
                continue;
            }

            long minOffset = partitionRecords[0].Offset();
            long maxOffset = partitionRecords[partitionRecords.Count - 1].Offset();

            offsets[tp] = new OffsetAndMetadata(maxOffset + 1);
            summaries.Add((tp, partitionRecords.Count, minOffset, maxOffset));

            if (Verbose)
            {
                foreach (ConsumerRecord<string, string> record in partitionRecords)
                {
                    RecordData(_output, record);
                }
            }

            ConsumedMessages = unchecked(ConsumedMessages + partitionRecords.Count);
            if (IsFinished())
            {
                break;
            }
        }

        RecordsConsumed(_output, records.Count(), summaries);
        return offsets;
    }

    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
    {
        bool success = true;
        string? error = null;
        if (exception is not null)
        {
            success = false;
            // Java's getMessage(), null when there is none; KafkaException() has an empty Message.
            error = exception.Message.Length == 0 ? null : exception.Message;
        }

        // The client hands an asynchronous commit's callback null offsets when the commit failed or had none, where
        // Java's would dereference them: reported as no offsets.
        OffsetsCommitted(_output, offsets ?? new Dictionary<TopicPartition, OffsetAndMetadata>(), error, success);
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        PartitionsAssigned(_output, partitions);

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
        PartitionsRevoked(_output, partitions);

    internal void CommitSync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        try
        {
            Consumer.Commit(offsets);
            OnComplete(offsets, null);
        }
        catch (WakeupException)
        {
            CommitSync(offsets);
            throw;
        }
        catch (FencedInstanceIdException)
        {
            throw;
        }
        catch (Exception e)
        {
            OnComplete(offsets, e);
        }
    }

    internal void Run()
    {
        TimeSpan started = _monotonic();
        try
        {
            StartupComplete(_output);
            Consumer.Subscribe(new[] { Topic }, this);

            while (!IsFinished())
            {
                ConsumerRecords<string, string> records = Consumer.Poll(s_pollTimeout);
                Dictionary<TopicPartition, OffsetAndMetadata> offsets = OnRecordsReceived(records);

                if (!UseAutoCommit)
                {
                    if (UseAsyncCommit)
                    {
                        Consumer.CommitAsync(offsets, this);
                    }
                    else
                    {
                        CommitSync(offsets);
                    }
                }
            }
        }
        catch (WakeupException)
        {
            Log.Debug(LoggerName, "Caught WakeupException because consumer is shutdown, ignore and terminate.");
        }
        catch (Exception e)
        {
            Log.Error(LoggerName, "Error during processing, terminating consumer process: ", e);
        }
        finally
        {
            // Java lets a failed close end run() without shutdown_complete; here it is logged and the shutdown still
            // announced, as by the Python and Rust tools.
            try
            {
                Consumer.Close(CloseOptions.Timeout(TimeSpan.FromMilliseconds(CloseTimeoutMs)));
            }
            catch (Exception e)
            {
                Log.Error(LoggerName, "Error closing consumer: ", e);
            }

            TimeSpan remaining = _minStartupVisible - (_monotonic() - started);
            if (remaining > TimeSpan.Zero)
            {
                _sleep(remaining);
            }

            ShutdownComplete(_output);
        }
    }

    internal void Close()
    {
        try
        {
            ShutdownRequested(_output);
        }
        finally
        {
            Consumer.Wakeup();
        }
    }

    private static void WritePartitions(Utf8JsonWriter writer, IReadOnlyCollection<TopicPartition> partitions)
    {
        writer.WriteStartArray("partitions");
        foreach (TopicPartition tp in partitions)
        {
            writer.WriteStartObject();
            WritePartitionData(writer, tp);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WritePartitionData(Utf8JsonWriter writer, TopicPartition tp)
    {
        writer.WriteString("topic", tp.Topic());
        writer.WriteNumber("partition", tp.Partition());
    }

    private static void Merge(Dictionary<string, object?> props, Dictionary<string, string> loaded)
    {
        foreach (KeyValuePair<string, string> entry in loaded)
        {
            props[entry.Key] = entry.Value;
        }
    }

    private static int Run(string[] args)
    {
        Log.Configure();
        ArgumentParser parser = ArgParser();
        if (args.Length == 0)
        {
            parser.PrintHelp();
            return 0;
        }

        var output = new JsonOutput(new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false)));
        VerifiableConsumer consumer;
        try
        {
            consumer = CreateFromArgs(parser, args, output);
        }
        catch (HelpScreenException)
        {
            return 0;
        }
        catch (ArgumentParserException e)
        {
            parser.HandleError(e);
            return 1;
        }

        var hook = new ShutdownHook(consumer.Close, "verifiable-consumer-shutdown-hook");
        try
        {
            consumer.Run();
        }
        finally
        {
            hook.Finish();
        }

        return 0;
    }
}
