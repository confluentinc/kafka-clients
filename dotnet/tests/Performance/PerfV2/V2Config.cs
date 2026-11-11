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

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// Builds ckd's librdkafka-form producer and consumer configs, the v2 half of Python's <c>configuration_from_env</c>
/// and <c>_LibrdkafkaConsumer</c>: <c>message.max.bytes</c>, <c>queue.buffering.max.kbytes</c>, the librdkafka SASL
/// keys and <c>fetch.message.max.bytes</c> stand for the Java forms.
/// </summary>
internal static class V2Config
{
    private const int DefaultBatchSize = 1024 * 1024;
    private const int MaxRequestSizeCap = 8 * 1024 * 1024;
    private const int DefaultBufferMemoryMib = 32;
    private const int DefaultMaxPollRecords = 500;

    internal static Dictionary<string, string> BuildProducerConfig()
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
        };

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            conf[kv.Key] = kv.Value;
        }

        if (PerfEnv.Has("BOOTSTRAP_SERVERS"))
        {
            conf["bootstrap.servers"] = PerfEnv.GetString("BOOTSTRAP_SERVERS", conf["bootstrap.servers"]);
        }

        bool useDefaults = PerfEnv.GetBool("USE_DEFAULTS", false);
        if (!useDefaults)
        {
            conf["acks"] = PerfEnv.GetString("ACKS", "all");

            int batchSize = PerfEnv.Has("BATCH_SIZE") ? PerfEnv.ParseInt("BATCH_SIZE") * 1024 : DefaultBatchSize;
            conf["batch.size"] = Str(batchSize);

            // Capped at 8 MiB whether set or not, as the C, Rust, Java and Python harnesses do.
            long maxRequestSize = PerfEnv.Has("MAX_REQUEST_SIZE")
                ? (long)PerfEnv.ParseInt("MAX_REQUEST_SIZE") * 1024
                : (long)batchSize * 64;
            conf["message.max.bytes"] = Str((int)Math.Min(maxRequestSize, MaxRequestSizeCap));

            conf["compression.type"] = PerfEnv.GetString("COMPRESSION_TYPE", "none");
            conf["enable.idempotence"] = PerfEnv.GetString("ENABLE_IDEMPOTENCE", "false");

            if (PerfEnv.Has("MAX_IN_FLIGHT"))
            {
                conf["max.in.flight.requests.per.connection"] = PerfEnv.GetString("MAX_IN_FLIGHT", string.Empty);
            }

            // The same buffer as the Rust core's buffer.memory (32 MiB unless BUFFER_MEMORY says otherwise), with no
            // message cap, so the two clients queue the same bytes before a send has to wait.
            long bufferMemoryMib = PerfEnv.Has("BUFFER_MEMORY") ? PerfEnv.ParseInt("BUFFER_MEMORY") : DefaultBufferMemoryMib;
            conf["queue.buffering.max.kbytes"] = (bufferMemoryMib * 1024).ToString(CultureInfo.InvariantCulture);
            conf["queue.buffering.max.messages"] = "2147483647";

            conf["linger.ms"] = PerfEnv.GetString("LINGER_MS", "5");

            // CRC-32 like the v3 client's default, so partition verification compares alike; Python sets it only for
            // the sync v2 producer, here both modes get it.
            if (PerfEnv.GetString("SKIP_PARTITIONER_OVERRIDE", "0") != "1")
            {
                conf["partitioner"] = "consistent_random";
            }
        }

        return conf;
    }

    internal static Dictionary<string, string> BuildConsumerConfig(ConsumerBenchmarkConfig cfg)
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = cfg.BootstrapServers,
            ["group.id"] = cfg.GroupId,
            ["group.protocol"] = "consumer",
            ["client.id"] = "librdkafka-consumer-perf",
            ["auto.offset.reset"] = "latest",
            ["enable.auto.commit"] = "true",
        };

        // As Python's _librdkafka_tuning, under librdkafka's names; an unset setting keeps librdkafka's default. No
        // max.poll.records: librdkafka has none, and the harness reads ConsumeBatch records per poll instead.
        if (cfg.FetchMinBytes is int fetchMinBytes)
        {
            conf["fetch.min.bytes"] = Str(fetchMinBytes);
        }

        if (cfg.MaxPartitionFetchBytes is int maxPartitionFetchBytes)
        {
            conf["fetch.message.max.bytes"] = Str(maxPartitionFetchBytes);
        }

        if (cfg.FetchMaxWaitMs is int fetchMaxWaitMs)
        {
            conf["fetch.wait.max.ms"] = Str(fetchMaxWaitMs);
        }

        if (!cfg.UseDefaults)
        {
            conf["check.crcs"] = "false";
        }

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            conf[kv.Key] = kv.Value;
        }

        return conf;
    }

    /// <summary>
    /// The most records one batch poll returns, as Python's <c>_librdkafka_batch</c>: <c>MAX_POLL_RECORDS</c>, else
    /// 500 (the Java and Rust clients' default <c>max.poll.records</c>) under <c>USE_DEFAULTS=True</c>, else
    /// <c>CONSUMER_BATCH_SIZE</c>.
    /// </summary>
    internal static int ConsumeBatch(ConsumerBenchmarkConfig cfg) =>
        cfg.MaxPollRecords ?? (cfg.UseDefaults ? DefaultMaxPollRecords : cfg.BatchSize);

    internal static void PrintProducerConfiguration(IReadOnlyDictionary<string, string> conf, int keySize, int valueSize, bool doVerify)
    {
        Console.WriteLine($"Key size: {keySize} bytes");
        Console.WriteLine($"Value size: {valueSize} bytes");
        Console.WriteLine($"Verify: {doVerify}");
        Console.WriteLine("Producer configuration:");
        foreach (KeyValuePair<string, string> kv in conf)
        {
            bool secret = kv.Key == "sasl.password";
            Console.WriteLine($"  {kv.Key}: {(secret ? "<hidden>" : kv.Value)}");
        }
    }

    private static string Str(int value) => value.ToString(CultureInfo.InvariantCulture);
}
