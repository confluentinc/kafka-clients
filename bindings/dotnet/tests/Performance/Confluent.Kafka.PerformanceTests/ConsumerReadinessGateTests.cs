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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Covers <c>ConsumerBenchmark</c>'s readiness gate (M13/P3 item A) against scripted backends — no broker,
/// no Docker, no client. Every case runs BOTH engines, sync (<c>Run</c>) and async (<c>RunAsync</c>), so the
/// two code paths cannot drift.
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

    // ---- cases -------------------------------------------------------------------------------------

    private static async Task GateWhenEnoughMeasurableRecordsMeasuresAsync(bool asyncMode)
    {
        // 20 measurable records satisfy the gate, then 5 more are measured (NUM_MESSAGES=5 ends the run).
        // The gate's 20 are warmup and must NOT appear in MessagesMeasured — that assertion is the point.
        var script = new List<IReadOnlyList<PolledRecord>>
        {
            Array.Empty<PolledRecord>(),
            Array.Empty<PolledRecord>(),
            Fresh(20),
            Fresh(5),
        };

        (ConsumerBenchmarkResult result, PollScript driver) = await RunEngineAsync(
            asyncMode, script, readinessMinRecords: 20, readinessTimeoutSeconds: 5, numMessages: 5).ConfigureAwait(false);

        Assert.False(result.SetupNotReady);
        Assert.False(result.AssignmentFailed);
        Assert.Equal(5, result.MessagesMeasured);
        Assert.Equal(1, driver.CloseCount);
    }

    private static async Task GateWhenNoMeasurableRecordsAsync(bool asyncMode)
    {
        // Nothing ever arrives: the settle loop sees its two empty polls, then the gate waits out its
        // budget. This is the "slow feeder" case the gate exists for.
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

        // The early return must still close the backend — the run is over, not paused.
        Assert.Equal(1, driver.CloseCount);
    }

    private static async Task GateWhenTimestampsUnusableAsync(bool asyncMode)
    {
        // Records DO flow, but none is measurable: 50 carry no CreateTime (-1) and 50 carry a timestamp
        // 60 s in the future, which yields a NEGATIVE latency. That second shape is exactly the macOS
        // cross-clock failure (item H) — the consumer drops every record and measures nothing — so this
        // case doubles as executable documentation of H's root cause.
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
        // 219 records received, only 19 of them measurable, threshold 20. If the gate counted RECEIVED
        // records it would open; it must count only measurable ones and time out instead.
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

    // ---- harness -----------------------------------------------------------------------------------

    private static async Task<(ConsumerBenchmarkResult Result, PollScript Driver)> RunEngineAsync(
        bool asyncMode,
        IReadOnlyList<IReadOnlyList<PolledRecord>> script,
        int readinessMinRecords,
        int readinessTimeoutSeconds,
        long numMessages)
    {
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal)
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
        });

        ConsumerBenchmarkConfig config = ConsumerBenchmarkConfig.FromEnv();
        var driver = new PollScript(script);
        using var metrics = new Metrics();

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

    /// <summary>A batch of <paramref name="count"/> records stamped now — measurable (latency >= 0).</summary>
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
            // A real poll blocks up to POLL_TIMEOUT_MS. Sleeping 1 ms keeps an exhausted script from
            // spinning the readiness gate's deadline loop hot for its whole budget.
            Thread.Sleep(1);
            return _batches.Count > 0 ? _batches.Dequeue() : Array.Empty<PolledRecord>();
        }

        internal void Close() => CloseCount++;
    }

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
