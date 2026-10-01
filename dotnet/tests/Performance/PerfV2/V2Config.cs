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
/// Builds the <b>librdkafka-form</b> client config dictionaries for ckd — the v2 half of Python's
/// <c>configuration_from_env(v2=True)</c> (producer) and <c>_LibrdkafkaConsumer</c> config (consumer). It
/// mirrors V3's <c>V3Config</c> but restores every Python <c>if v2: ...</c> config fork (§1.2):
/// <list type="bullet">
///   <item><c>max.request.size</c> → <c>message.max.bytes</c>.</item>
///   <item><c>buffer.memory</c> → <c>queue.buffering.max.kbytes</c> + <c>queue.buffering.max.messages=2147483647</c>.</item>
///   <item>SASL in librdkafka form (<c>sasl.username</c> / <c>sasl.password</c>), never the Java <c>sasl.jaas.config</c>.</item>
///   <item><c>partitioner=consistent_random</c> set explicitly so partition verification matches the v3
///     Rust client's actual default (CRC-32, <c>design/current/partitioner.md</c>), not Java's murmur2
///     (Python <c>v2_producer</c>, <c>producer_performance_test.py:512-520</c>).</item>
///   <item>consumer <c>max.partition.fetch.bytes</c> → <c>fetch.message.max.bytes</c>; no <c>max.poll.records</c>
///     (librdkafka batches via <c>Consume</c>).</item>
/// </list>
/// Every value is a string (ckd's builders take <c>IEnumerable&lt;KeyValuePair&lt;string,string&gt;&gt;</c>).
/// </summary>
internal static class V2Config
{
    private const int DefaultBatchSize = 1024 * 1024;
    private const int MaxRequestSizeCap = 8 * 1024 * 1024;

    /// <summary>Builds the producer config (librdkafka form) from the environment (§5.2 / §1.2).</summary>
    internal static Dictionary<string, string> BuildProducerConfig()
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
        };

        // v2/ckd uses librdkafka-form SASL keys (sasl.username / sasl.password), NOT sasl.jaas.config.
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
            // acks=all, hardcoded like the C/Java perf tests (D7; ckd's default is also all, set explicitly).
            conf["acks"] = "all";

            int batchSize = PerfEnv.Has("BATCH_SIZE") ? PerfEnv.GetInt("BATCH_SIZE", 0) * 1024 : DefaultBatchSize;
            conf["batch.size"] = Str(batchSize);

            int maxRequestSize = PerfEnv.Has("MAX_REQUEST_SIZE")
                ? PerfEnv.GetInt("MAX_REQUEST_SIZE", 0) * 1024
                : Math.Min(batchSize * 64, MaxRequestSizeCap);
            // librdkafka form: message.max.bytes (NOT the Java max.request.size).
            conf["message.max.bytes"] = Str(maxRequestSize);

            conf["compression.type"] = PerfEnv.GetString("COMPRESSION_TYPE", "none");
            conf["enable.idempotence"] = PerfEnv.GetString("ENABLE_IDEMPOTENCE", "false");

            if (PerfEnv.Has("MAX_IN_FLIGHT"))
            {
                conf["max.in.flight.requests.per.connection"] = PerfEnv.GetString("MAX_IN_FLIGHT", string.Empty);
            }

            if (PerfEnv.Has("BUFFER_MEMORY"))
            {
                // librdkafka form: BUFFER_MEMORY (MiB) -> bytes -> queue.buffering.max.kbytes (KiB),
                // plus queue.buffering.max.messages at its max (Python configuration_from_env v2).
                long bufferMemoryBytes = (long)PerfEnv.GetInt("BUFFER_MEMORY", 0) * 1024 * 1024;
                conf["queue.buffering.max.kbytes"] = (bufferMemoryBytes / 1024).ToString(CultureInfo.InvariantCulture);
                conf["queue.buffering.max.messages"] = "2147483647";
            }

            conf["linger.ms"] = PerfEnv.GetString("LINGER_MS", "5");

            // Match the v3 Rust client's default partitioner so end-of-run partition verification is
            // apples-to-apples. The Rust client defaults to CRC-32 (KeyHasher::Crc32), matching
            // librdkafka's own consistent_random default rather than the Java client's murmur2 — see
            // design/current/partitioner.md. Since consistent_random is already librdkafka's own
            // default, this is explicit but redundant (Python v2_producer,
            // producer_performance_test.py:512-520). Python sets this only for the sync v2_producer; we
            // set it in the shared v2 producer config so the async path is comparable too (harmless — it
            // only selects the target partition).
            conf["partitioner"] = "consistent_random";
        }

        return conf;
    }

    /// <summary>Builds the consumer config (librdkafka form) from a parsed <see cref="ConsumerBenchmarkConfig"/> (§6.2 / §1.2).</summary>
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

        if (!cfg.UseDefaults)
        {
            conf["fetch.min.bytes"] = Str(cfg.FetchMinBytes);
            // librdkafka form: fetch.message.max.bytes (the same input the v3 consumer maps to
            // max.partition.fetch.bytes — Python _LibrdkafkaConsumer). No max.poll.records: librdkafka
            // batches via Consume(num_messages), not a poll-records cap.
            conf["fetch.message.max.bytes"] = Str(cfg.MaxPartitionFetchBytes);
            conf["check.crcs"] = "false";
        }

        // v2/ckd uses librdkafka-form SASL keys regardless of MODE.
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            conf[kv.Key] = kv.Value;
        }

        return conf;
    }

    /// <summary>Prints the producer configuration, hiding secrets (Python <c>print_configuration</c>).</summary>
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
