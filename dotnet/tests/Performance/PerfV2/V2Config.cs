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
            conf["acks"] = "all";

            int batchSize = PerfEnv.Has("BATCH_SIZE") ? PerfEnv.ParseInt("BATCH_SIZE") * 1024 : DefaultBatchSize;
            conf["batch.size"] = Str(batchSize);

            int maxRequestSize = PerfEnv.Has("MAX_REQUEST_SIZE")
                ? PerfEnv.ParseInt("MAX_REQUEST_SIZE") * 1024
                : Math.Min(batchSize * 64, MaxRequestSizeCap);
            conf["message.max.bytes"] = Str(maxRequestSize);

            conf["compression.type"] = PerfEnv.GetString("COMPRESSION_TYPE", "none");
            conf["enable.idempotence"] = PerfEnv.GetString("ENABLE_IDEMPOTENCE", "false");

            if (PerfEnv.Has("MAX_IN_FLIGHT"))
            {
                conf["max.in.flight.requests.per.connection"] = PerfEnv.GetString("MAX_IN_FLIGHT", string.Empty);
            }

            if (PerfEnv.Has("BUFFER_MEMORY"))
            {
                long bufferMemoryBytes = (long)PerfEnv.ParseInt("BUFFER_MEMORY") * 1024 * 1024;
                conf["queue.buffering.max.kbytes"] = (bufferMemoryBytes / 1024).ToString(CultureInfo.InvariantCulture);
                conf["queue.buffering.max.messages"] = "2147483647";
            }

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

        if (!cfg.UseDefaults)
        {
            conf["fetch.min.bytes"] = Str(cfg.FetchMinBytes);
            // No max.poll.records: librdkafka batches through Consume(num_messages).
            conf["fetch.message.max.bytes"] = Str(cfg.MaxPartitionFetchBytes);
            conf["check.crcs"] = "false";
        }

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            conf[kv.Key] = kv.Value;
        }

        return conf;
    }

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
