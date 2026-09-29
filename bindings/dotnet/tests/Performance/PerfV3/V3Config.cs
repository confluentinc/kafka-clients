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

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// Builds the <b>Java-form</b> client config dictionaries for our binding — the v3 half of Python's
/// <c>configuration_from_env(v2=False)</c> (producer) and <c>_RustConsumer</c> config (consumer). Every
/// value is a string (the binding takes <c>IReadOnlyDictionary&lt;string,string&gt;</c>); SASL keys are
/// Java-form (<c>sasl.jaas.config</c>), never the librdkafka <c>sasl.username</c>/<c>sasl.password</c> form.
/// </summary>
internal static class V3Config
{
    private const int DefaultBatchSize = 1024 * 1024;
    private const int MaxRequestSizeCap = 8 * 1024 * 1024;

    /// <summary>Builds the producer config (Java form) from the environment (§5.2).</summary>
    internal static Dictionary<string, string> BuildProducerConfig()
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
        };

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
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
            // acks=all, hardcoded like the C/Java perf tests (D7).
            conf["acks"] = "all";

            int batchSize = PerfEnv.Has("BATCH_SIZE") ? PerfEnv.GetInt("BATCH_SIZE", 0) * 1024 : DefaultBatchSize;
            conf["batch.size"] = Str(batchSize);

            int maxRequestSize = PerfEnv.Has("MAX_REQUEST_SIZE")
                ? PerfEnv.GetInt("MAX_REQUEST_SIZE", 0) * 1024
                : Math.Min(batchSize * 64, MaxRequestSizeCap);
            conf["max.request.size"] = Str(maxRequestSize);

            conf["compression.type"] = PerfEnv.GetString("COMPRESSION_TYPE", "none");
            conf["enable.idempotence"] = PerfEnv.GetString("ENABLE_IDEMPOTENCE", "false");

            if (PerfEnv.Has("MAX_IN_FLIGHT"))
            {
                conf["max.in.flight.requests.per.connection"] = PerfEnv.GetString("MAX_IN_FLIGHT", string.Empty);
            }

            if (PerfEnv.Has("BUFFER_MEMORY"))
            {
                long bufferMemory = (long)PerfEnv.GetInt("BUFFER_MEMORY", 0) * 1024 * 1024;
                conf["buffer.memory"] = bufferMemory.ToString(CultureInfo.InvariantCulture);
            }

            conf["linger.ms"] = PerfEnv.GetString("LINGER_MS", "5");
        }

        return conf;
    }

    /// <summary>Builds the consumer config (Java form) from a parsed <see cref="ConsumerBenchmarkConfig"/> (§6.2).</summary>
    internal static Dictionary<string, string> BuildConsumerConfig(ConsumerBenchmarkConfig cfg)
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = cfg.BootstrapServers,
            ["group.id"] = cfg.GroupId,
            ["group.protocol"] = "consumer",
            ["client.id"] = "rust-consumer-perf",
            ["auto.offset.reset"] = "latest",
            ["enable.auto.commit"] = "true",
        };

        if (!cfg.UseDefaults)
        {
            conf["fetch.min.bytes"] = Str(cfg.FetchMinBytes);
            conf["max.partition.fetch.bytes"] = Str(cfg.MaxPartitionFetchBytes);
            conf["max.poll.records"] = Str(cfg.BatchSize + 500);
            conf["check.crcs"] = "false";
        }

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
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
            bool secret = kv.Key == "sasl.jaas.config" || kv.Key == "sasl.password";
            Console.WriteLine($"  {kv.Key}: {(secret ? "<hidden>" : kv.Value)}");
        }
    }

    private static string Str(int value) => value.ToString(CultureInfo.InvariantCulture);
}
