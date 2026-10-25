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
using System.IO;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// The collection of every engine-level test, never run in parallel: the benchmark configs are read only from the
/// process-global environment, which a parallel test would leak into another's config or into a smoke's child process.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PerfEngineCollection : ICollectionFixture<PerfEngineFixture>
{
    public const string Name = "PerfEngine";
}

/// <summary>
/// Saves every perf environment variable and the current directory, points the current directory at a temp directory
/// (where <see cref="Metrics"/> and the consumer summary write their files), and restores both on dispose.
/// </summary>
public sealed class PerfEngineFixture : IDisposable
{
    private static readonly string[] s_managedVariables =
    {
        "ASYNC", "AWAIT_ACCEPTED", "BOOTSTRAP_SERVERS", "CLIENT_VERSION", "COMPRESSION_TYPE", "CONSUMER_BATCH_SIZE",
        "CREATE_TOPIC", "DO_VERIFY", "FETCH_MIN_BYTES", "GROUP_ID", "INTERVAL_SECONDS", "JOIN_TIMEOUT_SECONDS",
        "KAFKA_BIN", "KEY_SIZE", "LIMIT_RPS", "MAX_PARTITION_FETCH_BYTES",
        "NUM_MESSAGES", "P99_LIMIT_MS", "PARTITIONS", "PERF_LRK_STATS", "POLL_SINGLE", "POLL_TIMEOUT_MS",
        "READINESS_MIN_RECORDS", "READINESS_TIMEOUT_SECONDS", "RESULTS_FILE", "SETTLE_TIMEOUT_SECONDS",
        "SKIP_PARTITIONER_OVERRIDE", "TEST_DURATION_SECONDS", "THROUGHPUT", "TOPIC_NAME", "USE_DEFAULTS",
        "VALUE_SIZE", "WARMUP_SECONDS",
    };

    private readonly Dictionary<string, string?> _saved = new(StringComparer.Ordinal);
    private readonly string _originalDirectory;
    private readonly string _workDirectory;

    public PerfEngineFixture()
    {
        _originalDirectory = Directory.GetCurrentDirectory();
        foreach (string name in s_managedVariables)
        {
            _saved[name] = Environment.GetEnvironmentVariable(name);
        }

        _workDirectory = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "perf-engine-tests-" + Guid.NewGuid().ToString("N"))).FullName;
        Directory.SetCurrentDirectory(_workDirectory);
    }

    public static void Apply(IReadOnlyDictionary<string, string> values)
    {
        foreach (string name in s_managedVariables)
        {
            Environment.SetEnvironmentVariable(name, null);
        }

        foreach (KeyValuePair<string, string> pair in values)
        {
            Environment.SetEnvironmentVariable(pair.Key, pair.Value);
        }
    }

    public void Dispose()
    {
        foreach (KeyValuePair<string, string?> pair in _saved)
        {
            Environment.SetEnvironmentVariable(pair.Key, pair.Value);
        }

        Directory.SetCurrentDirectory(_originalDirectory);

        try
        {
            Directory.Delete(_workDirectory, recursive: true);
        }
        catch (Exception)
        {
            // Best-effort cleanup of the temp working directory.
        }
    }
}
