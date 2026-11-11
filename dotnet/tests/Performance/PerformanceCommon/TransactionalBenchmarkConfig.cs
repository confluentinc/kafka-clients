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
using System.Globalization;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The <c>MODE=transactional</c> settings: the producer benchmark's own settings plus the transaction ones, read as
/// Python's <c>transactional_producer_performance_test.py</c> reads them.
/// </summary>
public sealed class TransactionalBenchmarkConfig
{
    /// <summary>The source records seeded when nothing sizes the seed, per second of the run plus 30 s.</summary>
    private const long DefaultSeedRate = 125_000;

    private TransactionalBenchmarkConfig(ProducerBenchmarkConfig producer)
    {
        Producer = producer;
    }

    public ProducerBenchmarkConfig Producer { get; }

    public string BootstrapServers { get; private set; } = "localhost:9092";

    public string TxnMode { get; private set; } = "produce";

    public bool Eos => TxnMode == "eos";

    public int RecordsPerTransaction { get; private set; } = 100;

    public double AbortRate { get; private set; }

    public int NumTransactionalProducers { get; private set; } = 1;

    public string TransactionalId { get; private set; } = "perf-txn";

    public string? SourceTopic { get; private set; }

    public string GroupId { get; private set; } = string.Empty;

    public string? KafkaBin { get; private set; }

    public int SeedThroughput { get; private set; } = -1;

    public long? SeedCount { get; private set; }

    public static TransactionalBenchmarkConfig FromEnv()
    {
        var config = new TransactionalBenchmarkConfig(ProducerBenchmarkConfig.FromEnv())
        {
            BootstrapServers = PerfEnv.GetString("BOOTSTRAP_SERVERS", "localhost:9092"),
            TxnMode = PerfEnv.GetString("TXN_MODE", "produce"),
            RecordsPerTransaction = Math.Max(1, PerfEnv.GetInt("RECORDS_PER_TRANSACTION", 100)),
            AbortRate = Math.Clamp(GetDouble("ABORT_RATE", 0.0), 0.0, 1.0),
            NumTransactionalProducers = Math.Max(1, PerfEnv.GetInt("NUM_TRANSACTIONAL_PRODUCERS", 1)),
            TransactionalId = PerfEnv.GetString("TRANSACTIONAL_ID", "perf-txn"),
            SourceTopic = NullIfEmpty(PerfEnv.GetStringOrNull("SOURCE_TOPIC")),
            KafkaBin = NullIfEmpty(PerfEnv.GetStringOrNull("KAFKA_BIN")),
            SeedThroughput = PerfEnv.GetInt("SEED_THROUGHPUT", -1),
        };

        config.GroupId = PerfEnv.GetString("GROUP_ID", config.TransactionalId + "-eos-consumer");
        if (PerfEnv.GetStringOrNull("SEED_COUNT") is { Length: > 0 })
        {
            config.SeedCount = PerfEnv.GetLong("SEED_COUNT", 1);
        }

        return config;
    }

    /// <summary>
    /// The records seeded into the source topic: <c>SEED_COUNT</c> when set, else <c>NUM_MESSAGES</c> when positive,
    /// else enough to outlast the run at <c>SEED_THROUGHPUT</c> (or a generous default rate).
    /// </summary>
    public long SeedRecordCount()
    {
        if (SeedCount is long seedCount)
        {
            return Math.Max(1, seedCount);
        }

        if (Producer.NumMessages > 0)
        {
            return Producer.NumMessages;
        }

        long rate = SeedThroughput > 0 ? SeedThroughput : DefaultSeedRate;
        return Math.Max(1, rate * (Producer.TestDurationSeconds + 30L));
    }

    private static double GetDouble(string name, double fallback)
    {
        string? value = PerfEnv.GetStringOrNull(name);
        return string.IsNullOrEmpty(value) ? fallback : double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
