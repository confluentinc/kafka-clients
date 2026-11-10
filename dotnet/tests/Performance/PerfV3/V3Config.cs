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
/// Builds this binding's Java-form producer and consumer configs, the v3 half of Python's <c>configuration_from_env</c>
/// and <c>_RustConsumer</c>; SASL goes as <c>sasl.jaas.config</c>.
/// </summary>
internal static class V3Config
{
    private const int DefaultBatchSize = 1024 * 1024;
    private const int MaxRequestSizeCap = 8 * 1024 * 1024;

    // The core's chunk size: below it the core falls back from incremental to full.
    private const int IncrementalMinBatchSize = 16 * 1024;

    private const string AllocationStrategyKey = "buffer.memory.allocation.strategy";

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
            conf["acks"] = PerfEnv.GetString("ACKS", "all");

            int batchSize = PerfEnv.Has("BATCH_SIZE") ? PerfEnv.ParseInt("BATCH_SIZE") * 1024 : DefaultBatchSize;
            conf["batch.size"] = Str(batchSize);

            // Capped at 8 MiB whether set or not, as the C, Rust, Java and Python harnesses do.
            long maxRequestSize = PerfEnv.Has("MAX_REQUEST_SIZE")
                ? (long)PerfEnv.ParseInt("MAX_REQUEST_SIZE") * 1024
                : (long)batchSize * 64;
            conf["max.request.size"] = Str((int)Math.Min(maxRequestSize, MaxRequestSizeCap));

            conf["compression.type"] = PerfEnv.GetString("COMPRESSION_TYPE", "none");
            conf["enable.idempotence"] = PerfEnv.GetString("ENABLE_IDEMPOTENCE", "false");

            if (PerfEnv.Has("MAX_IN_FLIGHT"))
            {
                conf["max.in.flight.requests.per.connection"] = PerfEnv.GetString("MAX_IN_FLIGHT", string.Empty);
            }

            if (PerfEnv.Has("BUFFER_MEMORY"))
            {
                long bufferMemory = (long)PerfEnv.ParseInt("BUFFER_MEMORY") * 1024 * 1024;
                conf["buffer.memory"] = bufferMemory.ToString(CultureInfo.InvariantCulture);
            }

            conf["linger.ms"] = PerfEnv.GetString("LINGER_MS", "5");
            conf[AllocationStrategyKey] = AllocationStrategy(conf["compression.type"], batchSize);
        }

        return conf;
    }

    /// <summary>
    /// The effective <c>buffer.memory.allocation.strategy</c> of a producer config: <c>BUFFER_MEMORY_ALLOCATION_STRATEGY</c>
    /// when set, else <c>incremental</c> where the core runs it and <c>full</c> where it does not (it refuses
    /// incremental with compression and falls back to full below a 16 KiB batch). A config that leaves the key unset
    /// (<c>USE_DEFAULTS</c>, as the Rust harness does) runs the core's default, <c>full</c>.
    /// </summary>
    internal static string EffectiveAllocationStrategy(IReadOnlyDictionary<string, string> conf) =>
        conf.TryGetValue(AllocationStrategyKey, out string? strategy) ? strategy : "full";

    private static string AllocationStrategy(string compressionType, int batchSize)
    {
        string? explicitStrategy = PerfEnv.GetStringOrNull("BUFFER_MEMORY_ALLOCATION_STRATEGY");
        if (!string.IsNullOrEmpty(explicitStrategy))
        {
            return explicitStrategy;
        }

        bool coreRunsIncremental = string.Equals(compressionType, "none", StringComparison.OrdinalIgnoreCase)
            && batchSize >= IncrementalMinBatchSize;
        return coreRunsIncremental ? "incremental" : "full";
    }

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

        // As Python's _rust_tuning; an unset setting keeps the core's default. No check.crcs override: the core keeps
        // Java's default true, and Python sets false for the librdkafka consumer only.
        if (cfg.FetchMinBytes is int fetchMinBytes)
        {
            conf["fetch.min.bytes"] = Str(fetchMinBytes);
        }

        if (cfg.MaxPartitionFetchBytes is int maxPartitionFetchBytes)
        {
            conf["max.partition.fetch.bytes"] = Str(maxPartitionFetchBytes);
        }

        if (cfg.FetchMaxWaitMs is int fetchMaxWaitMs)
        {
            conf["fetch.max.wait.ms"] = Str(fetchMaxWaitMs);
        }

        if (cfg.MaxPollRecords is int maxPollRecords)
        {
            conf["max.poll.records"] = Str(maxPollRecords);
        }
        else if (!cfg.UseDefaults)
        {
            conf["max.poll.records"] = Str(cfg.BatchSize);
        }

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
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
            bool secret = kv.Key == "sasl.jaas.config" || kv.Key == "sasl.password";
            Console.WriteLine($"  {kv.Key}: {(secret ? "<hidden>" : kv.Value)}");
        }
    }

    internal static Dictionary<string, object?> ClientConfigs(IReadOnlyDictionary<string, string> conf)
    {
        var configs = new Dictionary<string, object?>(conf.Count);
        foreach (KeyValuePair<string, string> kv in conf)
        {
            configs[kv.Key] = kv.Value;
        }

        return configs;
    }

    private static string Str(int value) => value.ToString(CultureInfo.InvariantCulture);
}
