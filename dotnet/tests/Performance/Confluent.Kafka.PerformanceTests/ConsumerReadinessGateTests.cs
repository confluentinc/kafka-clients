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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Covers <c>ConsumerBenchmark</c>'s readiness gate and measurement against scripted backends, with no broker; every
/// case runs both the sync (<c>Run</c>) and the async (<c>RunAsync</c>) engine.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ConsumerReadinessGateTests
{
    private const string SmokeTopic = "readiness-gate-topic";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Gate_WhenEnoughMeasurableRecords_Measures(bool asyncMode) =>
        GateWhenEnoughMeasurableRecordsMeasuresAsync(asyncMode);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Gate_WhenNoMeasurableRecords_ReturnsSetupNotReady(bool asyncMode) =>
        GateWhenNoMeasurableRecordsAsync(asyncMode);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Gate_WhenTimestampsUnusable_ReturnsSetupNotReady(bool asyncMode) =>
        GateWhenTimestampsUnusableAsync(asyncMode);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Gate_CountsOnlyMeasurableRecords(bool asyncMode) =>
        GateCountsOnlyMeasurableRecordsAsync(asyncMode);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Measure_CountsEveryRecordAndWritesResults(bool asyncMode) =>
        MeasureCountsEveryRecordAndWritesResultsAsync(asyncMode);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Measure_WhenResultsFileUnwritable_Fails(bool asyncMode) =>
        MeasureWhenResultsFileUnwritableFailsAsync(asyncMode);

    private static async Task GateWhenEnoughMeasurableRecordsMeasuresAsync(bool asyncMode)
    {
        // The gate's 20 records are warmup. The run is unbounded and timed, so counted they would make the total 45.
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            Fresh(20),
            Fresh(25),
        };

        (ConsumerBenchmarkResult result, PollScript driver) = await RunEngineAsync(
            asyncMode,
            script,
            readinessMinRecords: 20,
            readinessTimeoutSeconds: 5,
            numMessages: 0,
            extraEnv: new Dictionary<string, string>(StringComparer.Ordinal) { ["TEST_DURATION_SECONDS"] = "1" }).ConfigureAwait(false);

        Assert.False(result.SetupNotReady);
        Assert.False(result.AssignmentFailed);
        Assert.Equal(25, result.MessagesMeasured);
        Assert.Equal(1, driver.CloseCount);
    }

    private static async Task GateWhenNoMeasurableRecordsAsync(bool asyncMode)
    {
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
        };

        (ConsumerBenchmarkResult result, PollScript driver) = await RunEngineAsync(
            asyncMode, script, readinessMinRecords: 20, readinessTimeoutSeconds: 1, numMessages: 5).ConfigureAwait(false);

        Assert.True(result.SetupNotReady);
        Assert.False(result.AssignmentFailed);
        Assert.Equal(0, result.MessagesMeasured);
        Assert.Equal(0, result.P99);

        Assert.Equal(1, driver.CloseCount);
    }

    private static async Task GateWhenTimestampsUnusableAsync(bool asyncMode)
    {
        // 50 records carry no CreateTime (-1) and 50 a timestamp 60 s ahead, a negative latency: none is measurable.
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            WithTimestamp(50, -1),
            WithTimestamp(50, NowMs() + 60_000),
        };

        (ConsumerBenchmarkResult result, _) = await RunEngineAsync(
            asyncMode, script, readinessMinRecords: 20, readinessTimeoutSeconds: 1, numMessages: 5).ConfigureAwait(false);

        Assert.True(result.SetupNotReady);
        Assert.Equal(0, result.MessagesMeasured);
    }

    private static async Task GateCountsOnlyMeasurableRecordsAsync(bool asyncMode)
    {
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            Fresh(19),
            WithTimestamp(200, -1),
        };

        (ConsumerBenchmarkResult result, _) = await RunEngineAsync(
            asyncMode, script, readinessMinRecords: 20, readinessTimeoutSeconds: 1, numMessages: 5).ConfigureAwait(false);

        Assert.True(result.SetupNotReady);
        Assert.Equal(0, result.MessagesMeasured);
    }

    private static async Task MeasureCountsEveryRecordAndWritesResultsAsync(bool asyncMode)
    {
        // Measured: 2 records 60 s in the future (a negative latency, clamped into bucket 0), 2 with no CreateTime
        // (throughput only) and 2 a second old.
        long now = NowMs();
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            Fresh(20),
            WithTimestamp(2, now + 60_000),
            WithTimestamp(2, -1),
            WithTimestamp(2, now - 1_000),
        };
        string path = Path.Combine(Directory.GetCurrentDirectory(), "consumer-results-" + Guid.NewGuid().ToString("N") + ".json");

        (ConsumerBenchmarkResult result, _) = await RunEngineAsync(
            asyncMode,
            script,
            readinessMinRecords: 20,
            readinessTimeoutSeconds: 5,
            numMessages: 6,
            extraEnv: new Dictionary<string, string>(StringComparer.Ordinal) { ["RESULTS_FILE"] = path }).ConfigureAwait(false);

        Assert.False(result.SetupNotReady);
        Assert.False(result.ResultsWriteFailed);
        Assert.Equal(6, result.MessagesMeasured);

        using JsonDocument results = JsonDocument.Parse(File.ReadAllText(path));
        JsonElement root = results.RootElement;
        Assert.Equal("consumer", root.GetProperty("test").GetString());
        Assert.Equal(asyncMode ? "dotnet-rust-async-consumer" : "dotnet-rust-consumer", root.GetProperty("client").GetString());
        Assert.Equal(6, root.GetProperty("messages_measured").GetInt64());

        // min and max are exact: the negative latency is kept in min, while its bucket is 0.
        JsonElement latency = root.GetProperty("latency_ms");
        Assert.True(latency.GetProperty("min").GetInt64() <= -59_000);
        Assert.True(latency.GetProperty("max").GetInt64() >= 1_000);
        Assert.Equal(0, latency.GetProperty("p50").GetInt64());
        Assert.True(latency.GetProperty("p99").GetInt64() >= 1_000);

        foreach (string key in new[] { "rss_avg_mb", "cpu_avg_pct", "rss_avg_kib" })
        {
            Assert.Equal(JsonValueKind.Number, root.GetProperty(key).ValueKind);
        }
    }

    private static async Task MeasureWhenResultsFileUnwritableFailsAsync(bool asyncMode)
    {
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            Fresh(20),
            Fresh(5),
        };
        string path = Path.Combine("no-such-directory", "results.json");

        (ConsumerBenchmarkResult result, _) = await RunEngineAsync(
            asyncMode,
            script,
            readinessMinRecords: 20,
            readinessTimeoutSeconds: 5,
            numMessages: 5,
            extraEnv: new Dictionary<string, string>(StringComparer.Ordinal) { ["RESULTS_FILE"] = path }).ConfigureAwait(false);

        Assert.Equal(5, result.MessagesMeasured);
        Assert.True(result.ResultsWriteFailed);
    }

    private static async Task<(ConsumerBenchmarkResult Result, PollScript Driver)> RunEngineAsync(
        bool asyncMode,
        IReadOnlyList<IReadOnlyList<PolledRecord>> script,
        int readinessMinRecords,
        int readinessTimeoutSeconds,
        long numMessages,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["TOPIC_NAME"] = SmokeTopic,
            ["ASYNC"] = asyncMode ? "True" : "False",
            ["WARMUP_SECONDS"] = "0",
            ["TEST_DURATION_SECONDS"] = "30",
            ["INTERVAL_SECONDS"] = "1",
            ["JOIN_TIMEOUT_SECONDS"] = "5",
            ["SETTLE_TIMEOUT_SECONDS"] = "5",
            ["NUM_MESSAGES"] = numMessages.ToString(CultureInfo.InvariantCulture),
            ["READINESS_MIN_RECORDS"] = readinessMinRecords.ToString(CultureInfo.InvariantCulture),
            ["READINESS_TIMEOUT_SECONDS"] = readinessTimeoutSeconds.ToString(CultureInfo.InvariantCulture),
        };
        if (extraEnv is not null)
        {
            foreach (KeyValuePair<string, string> pair in extraEnv)
            {
                env[pair.Key] = pair.Value;
            }
        }

        PerfEngineFixture.Apply(env);

        ConsumerBenchmarkConfig config = ConsumerBenchmarkConfig.FromEnv();
        var driver = new PollScript(script);
        using Metrics metrics = Metrics.ForConsumer();

        if (asyncMode)
        {
            using var backend = new ScriptedAsyncConsumerBackend(driver);
            ConsumerBenchmarkResult asyncResult = await ConsumerBenchmark.RunAsync(config, backend, metrics).ConfigureAwait(false);
            return (asyncResult, driver);
        }

        using var syncBackend = new ScriptedConsumerBackend(driver);
        return (ConsumerBenchmark.Run(config, syncBackend, metrics), driver);
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static IReadOnlyList<PolledRecord> Fresh(int count) => WithTimestamp(count, NowMs());

    private static IReadOnlyList<PolledRecord> WithTimestamp(int count, long timestampMs)
    {
        var records = new PolledRecord[count];
        for (int i = 0; i < count; i++)
        {
            records[i] = new PolledRecord(timestampMs, 10);
        }

        return records;
    }

    /// <summary>
    /// Replays a fixed sequence of poll results, then returns empty batches forever, and counts closes.
    /// Shared by the sync and async backends so both engines see the identical script.
    /// </summary>
    private sealed class PollScript
    {
        private readonly Queue<IReadOnlyList<PolledRecord>> _batches;

        internal PollScript(IReadOnlyList<IReadOnlyList<PolledRecord>> batches) =>
            _batches = new Queue<IReadOnlyList<PolledRecord>>(batches);

        internal int CloseCount { get; private set; }

        internal IReadOnlyList<PolledRecord> Next()
        {
            // A real poll blocks; the 1 ms sleep keeps an exhausted script from spinning the gate's deadline loop.
            Thread.Sleep(1);
            return _batches.Count > 0 ? _batches.Dequeue() : Array.Empty<PolledRecord>();
        }

        internal void Close() => CloseCount++;
    }

    /// <summary>A synchronous consumer backend that replays its <see cref="PollScript"/>.</summary>
    private sealed class ScriptedConsumerBackend : IConsumerBackend
    {
        private readonly PollScript _script;

        internal ScriptedConsumerBackend(PollScript script) => _script = script;

        public void Subscribe(string topic)
        {
        }

        public bool Assigned() => true;

        public IReadOnlyList<PolledRecord> PollBatch() => _script.Next();

        public IReadOnlyList<PolledRecord> PollSingle() => PollBatch();

        public void Close() => _script.Close();

        public void Dispose()
        {
        }
    }

    /// <summary>An asynchronous consumer backend that replays its <see cref="PollScript"/>.</summary>
    private sealed class ScriptedAsyncConsumerBackend : IAsyncConsumerBackend
    {
        private readonly PollScript _script;

        internal ScriptedAsyncConsumerBackend(PollScript script) => _script = script;

        public Task Subscribe(string topic) => Task.CompletedTask;

        public bool Assigned() => true;

        public Task<IReadOnlyList<PolledRecord>> PollBatch() => Task.FromResult(_script.Next());

        public Task<IReadOnlyList<PolledRecord>> PollSingle() => PollBatch();

        public Task Close()
        {
            _script.Close();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }
}
