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
using System.Text;
using System.Text.Json;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Tools;

/// <summary>
/// Java's <c>org.apache.kafka.tools.VerifiableProducer</c>: produces increasing integers to a topic and prints a
/// JSON line for each send, the lines Kafka's system tests read, with Java's event names, fields and field order.
/// </summary>
internal sealed class VerifiableProducer
{
    private readonly JsonOutput _output;

    private readonly long _maxMessages;

    private readonly int? _valuePrefix;

    private readonly int? _repeatingKeys;

    private volatile bool _stopProducing;

    internal VerifiableProducer(
        IProducer<string, string> producer,
        JsonOutput output,
        string topic,
        int throughput,
        int maxMessages,
        int? valuePrefix,
        long? createTime,
        int? repeatingKeys)
    {
        Producer = producer;
        _output = output;
        Topic = topic;
        Throughput = throughput;
        _maxMessages = maxMessages;
        _valuePrefix = valuePrefix;
        CreateTime = createTime;
        StartTime = Utils.NowMillis();
        _repeatingKeys = repeatingKeys;
    }

    internal IProducer<string, string> Producer { get; }

    internal string Topic { get; }

    internal long Throughput { get; }

    internal long NumAcked { get; private set; }

    internal long NumSent { get; private set; }

    internal bool StopProducing
    {
        get => _stopProducing;
        set => _stopProducing = value;
    }

    internal long? CreateTime { get; private set; }

    internal long StartTime { get; set; }

    internal int KeyCounter { get; set; }

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
            "verifiable-producer",
            "This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each "
            + "\"send\" request, making externally visible which messages have been acked and which have not.");

        parser.AddArgument("--topic", required: true, metavar: "TOPIC", help: "Produce messages to this topic.");
        ArgumentParser.MutuallyExclusiveGroup connectionGroup = parser.AddMutuallyExclusiveGroup(
            "Connection Group", "Group of arguments for connection to brokers", required: true);
        parser.AddArgument(
            "--bootstrap-server",
            dest: "bootstrapServer",
            metavar: "HOST1:PORT1[,HOST2:PORT2[...]]",
            help: "REQUIRED: The server(s) to connect to. Comma-separated list of Kafka brokers in the form "
                + "HOST1:PORT1,HOST2:PORT2,...",
            group: connectionGroup);
        parser.AddArgument(
            "--max-messages",
            type: typeof(int),
            defaultValue: -1,
            dest: "maxMessages",
            metavar: "MAX-MESSAGES",
            help: "Produce this many messages. If -1, produce messages until the process is killed externally.");
        parser.AddArgument(
            "--throughput",
            type: typeof(int),
            defaultValue: -1,
            metavar: "THROUGHPUT",
            help: "If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.");
        parser.AddArgument(
            "--acks",
            type: typeof(int),
            defaultValue: -1,
            choices: new object[] { 0, 1, -1 },
            metavar: "ACKS",
            help: "Acks required on each produced message. See Kafka docs on acks for details.");
        parser.AddArgument(
            "--producer.config",
            metavar: "CONFIG-FILE",
            help: "(DEPRECATED) Producer config properties file. This option will be removed in a future version. "
                + "Use --command-config instead.");
        parser.AddArgument(
            "--message-create-time",
            type: typeof(long),
            defaultValue: -1L,
            dest: "createTime",
            metavar: "CREATE-TIME",
            help: "Send messages with creation time starting at the arguments value, in milliseconds since epoch");
        parser.AddArgument(
            "--value-prefix",
            type: typeof(int),
            dest: "valuePrefix",
            metavar: "VALUE-PREFIX",
            help: "If specified, each produced value will have this prefix with a dot separator");
        parser.AddArgument(
            "--repeating-keys",
            type: typeof(int),
            dest: "repeatingKeys",
            metavar: "REPEATING-KEYS",
            help: "If specified, each produced record will have a key starting at 0 increment by 1 up to the number "
                + "specified (exclusive), then the key is set to 0 again");
        parser.AddArgument(
            "--command-config",
            dest: "commandConfigFile",
            metavar: "CONFIG-FILE",
            help: "Config properties file (config options shared with command line parameters will be overridden).");
        return parser;
    }

    internal static (Namespace Res, Dictionary<string, object?> ProducerProps) ParseArgs(
        ArgumentParser parser, IReadOnlyList<string> args, JsonOutput output)
    {
        Namespace res = parser.ParseArgs(args);
        string? configFile = res.GetString("producer.config");
        string? commandConfigFile = res.GetString("commandConfigFile");
        if (configFile is not null && commandConfigFile is not null)
        {
            throw new ArgumentParserException("Options --producer.config and --command-config are mutually exclusive.");
        }

        var producerProps = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = res.GetString("bootstrapServer"),
            ["acks"] = res.GetInt("acks")!.Value.ToString(CultureInfo.InvariantCulture),
            ["retries"] = "0",
        };
        if (configFile is not null)
        {
            output.Println(
                "Option --producer.config has been deprecated and will be removed in a future version. "
                + "Use --command-config instead.");
            Merge(producerProps, Utils.LoadProps(configFile));
        }

        if (commandConfigFile is not null)
        {
            Merge(producerProps, Utils.LoadProps(commandConfigFile));
        }

        return (res, producerProps);
    }

    internal static VerifiableProducer CreateFromArgs(ArgumentParser parser, IReadOnlyList<string> args, JsonOutput output)
    {
        (Namespace res, Dictionary<string, object?> props) = ParseArgs(parser, args, output);
        long createTime = res.GetLong("createTime")!.Value;
        var producer = new KafkaProducer<string, string>(props, new StringSerializer(), new StringSerializer());
        return new VerifiableProducer(
            producer,
            output,
            res.GetString("topic")!,
            res.GetInt("throughput")!.Value,
            res.GetInt("maxMessages")!.Value,
            res.GetInt("valuePrefix"),
            createTime == -1 ? null : createTime,
            res.GetInt("repeatingKeys"));
    }

    internal static void StartupComplete(JsonOutput output) => output.Print("startup_complete");

    internal static void ShutdownComplete(JsonOutput output) => output.Print("shutdown_complete");

    internal static void SuccessfulSend(JsonOutput output, string? key, string value, RecordMetadata recordMetadata) =>
        output.Print("producer_send_success", writer =>
        {
            writer.WriteString("key", key);
            writer.WriteString("value", value);
            writer.WriteString("topic", recordMetadata.Topic());
            writer.WriteNumber("partition", recordMetadata.Partition());
            writer.WriteNumber("offset", recordMetadata.Offset());
        });

    internal static void FailedSend(JsonOutput output, string? key, string value, string topic, Exception e) =>
        output.Print("producer_send_error", writer =>
        {
            writer.WriteString("key", key);
            writer.WriteString("value", value);
            writer.WriteString("topic", topic);
            // Java's getClass().toString() of this exception's .NET class, which cannot name a Java class.
            writer.WriteString("exception", "class " + e.GetType().FullName);
            // Java's getMessage(), null when there is none; KafkaException() has an empty Message.
            writer.WriteString("message", e.Message.Length == 0 ? null : e.Message);
        });

    internal static void ToolData(JsonOutput output, long sent, long acked, long targetThroughput, double avgThroughput) =>
        output.Print("tool_data", writer =>
        {
            writer.WriteNumber("sent", sent);
            writer.WriteNumber("acked", acked);
            writer.WriteNumber("target_throughput", targetThroughput);
            JsonOutput.WriteNumber(writer, "avg_throughput", avgThroughput);
        });

    internal void Send(string? key, string value)
    {
        ProducerRecord<string, string> record;
        try
        {
            if (CreateTime is { } createTime)
            {
                record = new ProducerRecord<string, string>(Topic, null, createTime, key, value);
                CreateTime = createTime + (Utils.NowMillis() - StartTime);
            }
            else
            {
                record = new ProducerRecord<string, string>(Topic, key, value);
            }
        }
        catch (Exception e)
        {
            // Java builds the record outside its try, so a negative create time ends its producing thread; here,
            // as in Python and Rust, it is a failed send, not counted as sent.
            FailedSend(_output, key, value, Topic, e);
            return;
        }

        NumSent++;
        try
        {
            Producer.Send(record, new PrintInfoCallback(this, key, value));
        }
        catch (Exception e)
        {
            FailedSend(_output, key, value, Topic, e);
        }
    }

    internal string GetValue(long val) => _valuePrefix is { } valuePrefix
        ? valuePrefix.ToString(CultureInfo.InvariantCulture) + "." + val.ToString(CultureInfo.InvariantCulture)
        : val.ToString(CultureInfo.InvariantCulture);

    internal string? GetKey()
    {
        string? key = null;
        if (_repeatingKeys is { } repeatingKeys)
        {
            key = KeyCounter.ToString(CultureInfo.InvariantCulture);
            KeyCounter = unchecked(KeyCounter + 1);
            if (KeyCounter == repeatingKeys)
            {
                KeyCounter = 0;
            }
        }

        return key;
    }

    internal void Close()
    {
        Producer.Close();
        ShutdownComplete(_output);
    }

    internal void Run(ThroughputThrottler throttler)
    {
        StartupComplete(_output);
        long maxMessages = _maxMessages < 0 ? long.MaxValue : _maxMessages;
        for (long i = 0; i < maxMessages; i++)
        {
            if (StopProducing)
            {
                break;
            }

            long sendStartMs = Utils.NowMillis();
            Send(GetKey(), GetValue(i));
            if (throttler.ShouldThrottle(i, sendStartMs))
            {
                throttler.Throttle();
            }
        }
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
        VerifiableProducer producer;
        try
        {
            producer = CreateFromArgs(parser, args, output);
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

        long startMs = Utils.NowMillis();
        var throttler = new ThroughputThrottler(producer.Throughput, startMs);

        // Java's shutdown hook body, run by the hook thread at a signal or by the main thread at the end, whichever
        // comes first; the other waits for it.
        object finishLock = new object();
        bool finished = false;

        void CloseAndSummarize()
        {
            lock (finishLock)
            {
                if (finished)
                {
                    return;
                }

                finished = true;
                producer.Close();
                long elapsedMs = Utils.NowMillis() - startMs;
                // Java's division gives Infinity (NaN with nothing acked) when start and stop share a millisecond;
                // this tool writes 0.0 then, as Python's does.
                double avgThroughput = elapsedMs != 0 ? 1000 * (producer.NumAcked / (double)elapsedMs) : 0.0;
                ToolData(output, producer.NumSent, producer.NumAcked, producer.Throughput, avgThroughput);
            }
        }

        void Stop()
        {
            producer.StopProducing = true;
            throttler.Wakeup();
            // The hook closes the producer, as Java's, so a send blocked on metadata or buffer space fails at once
            // instead of holding the shutdown for up to max.block.ms.
            CloseAndSummarize();
        }

        var hook = new ShutdownHook(Stop, "verifiable-producer-shutdown-hook");
        try
        {
            producer.Run(throttler);
            CloseAndSummarize();
            return 0;
        }
        finally
        {
            hook.Finish();
        }
    }

    /// <summary>Java's <c>PrintInfoCallback</c>: exactly one line per completed send.</summary>
    private sealed class PrintInfoCallback : ICallback
    {
        private readonly VerifiableProducer _producer;

        private readonly string? _key;

        private readonly string _value;

        internal PrintInfoCallback(VerifiableProducer producer, string? key, string value)
        {
            _producer = producer;
            _key = key;
            _value = value;
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            lock (_producer._output.Lock)
            {
                if (exception is null)
                {
                    _producer.NumAcked++;
                    SuccessfulSend(_producer._output, _key, _value, metadata);
                }
                else
                {
                    FailedSend(_producer._output, _key, _value, _producer.Topic, exception);
                }
            }
        }
    }
}
