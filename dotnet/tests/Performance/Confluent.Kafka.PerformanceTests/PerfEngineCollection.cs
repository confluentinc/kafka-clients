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
/// The collection every engine-level test belongs to. <c>DisableParallelization</c> is mandatory, not
/// tidiness: <see cref="ConsumerBenchmarkConfig"/> / <see cref="ProducerBenchmarkConfig"/> have private
/// constructors and private setters, so the only way to configure them is to set environment variables and
/// call <c>FromEnv()</c> — and both the environment and the current directory are PROCESS-global. Left
/// parallel, one test's <c>NUM_MESSAGES</c> would leak into another's config, and worse into the perf
/// smoke's child process (<c>ProcessStartInfo.Environment</c> is pre-populated from this process at
/// construction time, so a concurrently-mutated variable would land in the launched benchmark).
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PerfEngineCollection : ICollectionFixture<PerfEngineFixture>
{
    /// <summary>The collection name, referenced by each engine test class's <c>[Collection]</c>.</summary>
    public const string Name = "PerfEngine";
}

/// <summary>
/// Saves the process-global state the engine tests mutate — every perf environment variable they touch,
/// plus the current directory — and restores it on dispose. The current directory is pointed at a fresh
/// temp directory for the duration, because <see cref="Metrics"/>'s public constructor truncates and opens
/// <c>metrics.jsonl</c> in the current directory and <c>ConsumerBenchmark</c>'s summary writes
/// <c>results.json</c> beside it; neither belongs in the repo. (There is an <c>internal Metrics(TextWriter)</c>
/// constructor marked "used by tests", but PerformanceCommon grants no <c>InternalsVisibleTo</c> to this
/// project, so it is not reachable from here.)
/// </summary>
public sealed class PerfEngineFixture : IDisposable
{
    // Every variable ConsumerBenchmarkConfig.FromEnv or ProducerBenchmarkConfig.FromEnv reads. Apply()
    // clears the whole set before applying a test's values, so one test can never inherit another's.
    private static readonly string[] s_managedVariables =
    {
        "ASYNC", "BOOTSTRAP_SERVERS", "CLIENT_VERSION", "CONSUMER_BATCH_SIZE", "CREATE_TOPIC",
        "DO_VERIFY", "FETCH_MIN_BYTES", "GROUP_ID", "INTERVAL_SECONDS", "JOIN_TIMEOUT_SECONDS",
        "KAFKA_BIN", "KEY_SIZE", "LIMIT_RPS", "MAX_PARTITION_FETCH_BYTES", "NUM_MESSAGES",
        "P99_LIMIT_MS", "PARTITIONS", "POLL_SINGLE", "POLL_TIMEOUT_MS", "READINESS_MIN_RECORDS",
        "READINESS_TIMEOUT_SECONDS", "SETTLE_TIMEOUT_SECONDS", "TEST_DURATION_SECONDS", "THROUGHPUT",
        "TOPIC_NAME", "USE_DEFAULTS", "VALUE_SIZE", "WARMUP_SECONDS",
    };

    private readonly Dictionary<string, string?> _saved = new(StringComparer.Ordinal);
    private readonly string _originalDirectory;
    private readonly string _workDirectory;

    /// <summary>Captures the environment + current directory and switches to a temp working directory.</summary>
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

    /// <summary>
    /// Clears every managed variable, then applies <paramref name="values"/> — so the argument is a test's
    /// COMPLETE environment, not a delta on whatever ran before it.
    /// </summary>
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

    /// <summary>Restores the captured environment and current directory, and removes the temp directory.</summary>
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
