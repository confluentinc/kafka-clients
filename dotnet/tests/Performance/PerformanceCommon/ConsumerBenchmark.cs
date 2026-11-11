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
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>The consumer benchmark's outcome, from which the exe chooses its exit code.</summary>
public sealed class ConsumerBenchmarkResult
{
    public const int SetupNotReadyExitCode = 2;

    internal ConsumerBenchmarkResult(
        bool assignmentFailed, bool setupNotReady, long messagesMeasured, long p99, bool resultsWriteFailed = false)
    {
        AssignmentFailed = assignmentFailed;
        SetupNotReady = setupNotReady;
        MessagesMeasured = messagesMeasured;
        P99 = p99;
        ResultsWriteFailed = resultsWriteFailed;
    }

    public bool AssignmentFailed { get; }

    public bool SetupNotReady { get; }

    public long MessagesMeasured { get; }

    public long P99 { get; }

    /// <summary>The run measured records but could not write its results file.</summary>
    public bool ResultsWriteFailed { get; }
}

/// <summary>
/// The client-free consumer end-to-end latency engine, Python's <c>run</c> / <c>run_async</c> over one shared
/// <see cref="Measurement"/>: a readiness gate waits for measurable records before the timed window, and a run whose
/// pipeline never goes live returns <see cref="ConsumerBenchmarkResult.SetupNotReady"/>, to be retried.
/// </summary>
public static class ConsumerBenchmark
{
    public static ConsumerBenchmarkResult Run(ConsumerBenchmarkConfig config, IConsumerBackend backend, Metrics metrics)
    {
        PrintHeader(config);
        backend.Subscribe(config.Topic);
        Func<IReadOnlyList<PolledRecord>> poll = config.PollSingle ? backend.PollSingle : backend.PollBatch;

        double joinStart = Monotonic();
        while (!backend.Assigned())
        {
            poll();
            if (Monotonic() - joinStart >= config.JoinTimeoutSeconds)
            {
                Console.Error.WriteLine("ERROR: timed out waiting for assignment");
                CloseQuietly(backend);
                return new ConsumerBenchmarkResult(assignmentFailed: true, setupNotReady: false, 0, 0);
            }
        }

        Console.WriteLine($">>> assigned after {Monotonic() - joinStart:F1}s");

        double settleDeadline = Monotonic() + config.SettleTimeoutSeconds;
        int empties = 0;
        while (empties < 2 && Monotonic() < settleDeadline)
        {
            empties = poll().Count > 0 ? 0 : empties + 1;
        }

        Console.WriteLine(">>> at live edge");

        LoadDriver? loadDriver = null;
        try
        {
            loadDriver = LoadDriver.MaybeStart(config);

            if (!AwaitPipelineReady(config, poll))
            {
                return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: true, 0, 0);
            }

            var measurement = new Measurement(config, metrics);
            measurement.Begin();
            double measuredDuration;
            try
            {
                while (!PerfSignals.Terminating)
                {
                    measurement.ProcessPoll(poll());
                    if (measurement.TimeLimitReached() || measurement.NoDataTimeout())
                    {
                        break;
                    }
                }
            }
            catch (MeasurementDone)
            {
            }
            finally
            {
                measuredDuration = measurement.MeasuredDuration();
                metrics.SetMeasurementEnd(Metrics.NowMs());
                loadDriver?.Stop();
                loadDriver = null;
                metrics.StopCollecting();
            }

            // Before the close, so a failing close cannot discard the results.
            return Summarize(config, measurement, measuredDuration, metrics);
        }
        finally
        {
            // As Python, every exit stops the load driver.
            loadDriver?.Stop();
            CloseQuietly(backend);
        }
    }

    public static async Task<ConsumerBenchmarkResult> RunAsync(ConsumerBenchmarkConfig config, IAsyncConsumerBackend backend, Metrics metrics)
    {
        PrintHeader(config);
        await backend.Subscribe(config.Topic).ConfigureAwait(false);
        Func<Task<IReadOnlyList<PolledRecord>>> poll = config.PollSingle ? backend.PollSingle : backend.PollBatch;

        double joinStart = Monotonic();
        while (!backend.Assigned())
        {
            await poll().ConfigureAwait(false);
            if (Monotonic() - joinStart >= config.JoinTimeoutSeconds)
            {
                Console.Error.WriteLine("ERROR: timed out waiting for assignment");
                await CloseQuietlyAsync(backend).ConfigureAwait(false);
                return new ConsumerBenchmarkResult(assignmentFailed: true, setupNotReady: false, 0, 0);
            }
        }

        Console.WriteLine($">>> assigned after {Monotonic() - joinStart:F1}s");

        double settleDeadline = Monotonic() + config.SettleTimeoutSeconds;
        int empties = 0;
        while (empties < 2 && Monotonic() < settleDeadline)
        {
            IReadOnlyList<PolledRecord> got = await poll().ConfigureAwait(false);
            empties = got.Count > 0 ? 0 : empties + 1;
        }

        Console.WriteLine(">>> at live edge");

        LoadDriver? loadDriver = null;
        try
        {
            loadDriver = LoadDriver.MaybeStart(config);

            if (!await AwaitPipelineReadyAsync(config, poll).ConfigureAwait(false))
            {
                return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: true, 0, 0);
            }

            var measurement = new Measurement(config, metrics);
            measurement.Begin();
            double measuredDuration;
            try
            {
                while (!PerfSignals.Terminating)
                {
                    measurement.ProcessPoll(await poll().ConfigureAwait(false));
                    if (measurement.TimeLimitReached() || measurement.NoDataTimeout())
                    {
                        break;
                    }
                }
            }
            catch (MeasurementDone)
            {
            }
            finally
            {
                measuredDuration = measurement.MeasuredDuration();
                metrics.SetMeasurementEnd(Metrics.NowMs());
                loadDriver?.Stop();
                loadDriver = null;
                metrics.StopCollecting();
            }

            // Before the close, so a failing close cannot discard the results.
            return Summarize(config, measurement, measuredDuration, metrics);
        }
        finally
        {
            // As Python, every exit stops the load driver.
            loadDriver?.Stop();
            await CloseQuietlyAsync(backend).ConfigureAwait(false);
        }
    }

    private static bool AwaitPipelineReady(ConsumerBenchmarkConfig config, Func<IReadOnlyList<PolledRecord>> poll)
    {
        double deadline = Monotonic() + config.ReadinessTimeoutSeconds;
        long measurable = 0;
        long received = 0;
        while (measurable < config.ReadinessMinRecords && Monotonic() < deadline)
        {
            foreach (PolledRecord record in poll())
            {
                received++;
                if (IsMeasurable(record.TimestampMs))
                {
                    measurable++;
                }
            }
        }

        return ReadinessOk(config, measurable, received);
    }

    private static async Task<bool> AwaitPipelineReadyAsync(ConsumerBenchmarkConfig config, Func<Task<IReadOnlyList<PolledRecord>>> poll)
    {
        double deadline = Monotonic() + config.ReadinessTimeoutSeconds;
        long measurable = 0;
        long received = 0;
        while (measurable < config.ReadinessMinRecords && Monotonic() < deadline)
        {
            foreach (PolledRecord record in await poll().ConfigureAwait(false))
            {
                received++;
                if (IsMeasurable(record.TimestampMs))
                {
                    measurable++;
                }
            }
        }

        return ReadinessOk(config, measurable, received);
    }

    private static bool ReadinessOk(ConsumerBenchmarkConfig config, long measurable, long received)
    {
        if (measurable >= config.ReadinessMinRecords)
        {
            Console.WriteLine($">>> pipeline live ({measurable} records confirmed); measuring");
            return true;
        }

        Console.Error.WriteLine(
            $"SETUP_NOT_READY: {measurable}/{config.ReadinessMinRecords} measurable records " +
            $"({received} received) in {config.ReadinessTimeoutSeconds}s at live edge");
        return false;
    }

    // Python's _measurable_latency_ms: the gate needs a CreateTime and a non-negative latency.
    private static bool IsMeasurable(long tsMs) => tsMs > 0 && Metrics.NowMs() - tsMs >= 0;

    // Python's _close_consumer: a close error is a warning, which neither discards the results nor fails the run.
    private static void CloseQuietly(IConsumerBackend backend)
    {
        try
        {
            backend.Close();
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"WARNING: closing the consumer failed: {e.GetType().Name}: {e.Message}");
        }
    }

    private static async Task CloseQuietlyAsync(IAsyncConsumerBackend backend)
    {
        try
        {
            await backend.Close().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"WARNING: closing the consumer failed: {e.GetType().Name}: {e.Message}");
        }
    }

    private static void PrintHeader(ConsumerBenchmarkConfig config)
    {
        string mode = config.AsyncMode ? "async" : "sync";
        string pollMode = config.PollSingle ? "single" : "batch";
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"Consumer E2E Latency Benchmark - CLIENT_VERSION={config.ClientVersion} ({mode}, poll={pollMode})");
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"Bootstrap: {config.BootstrapServers}  Topic: {config.Topic}  Group: {config.GroupId}");
        Console.WriteLine($"Warmup: {config.WarmupSeconds}s  Measure: {config.TestDurationSeconds}s  Interval: {config.IntervalSeconds}s  Poll: {config.PollTimeoutMs}ms");
        Console.WriteLine(new string('=', 72));
    }

    private static ConsumerBenchmarkResult Summarize(
        ConsumerBenchmarkConfig config, Measurement m, double duration, Metrics metrics)
    {
        long[] hist = m.LatencyHist;
        long p50 = LatencyHistogram.PercentileFromHist(hist, 0.50);
        long p90 = LatencyHistogram.PercentileFromHist(hist, 0.90);
        long p95 = LatencyHistogram.PercentileFromHist(hist, 0.95);
        long p99 = LatencyHistogram.PercentileFromHist(hist, 0.99);
        long p999 = LatencyHistogram.PercentileFromHist(hist, 0.999);

        // Exact, never from the clamped buckets.
        double avg = m.LatencySamples > 0 ? (double)m.LatencySumMs / m.LatencySamples : 0.0;
        long min = m.LatencyMinMs ?? 0;
        long max = m.LatencyMaxMs;

        long measured = m.MeasuredMessages;
        double thrMsg = duration > 0 ? measured / duration : 0.0;
        double thrMib = duration > 0 ? (measured * (double)config.MessageSize) / (1024.0 * 1024.0) / duration : 0.0;

        ExternalMetricsAggregations agg = metrics.ExternalMetricsAggregations();
        bool external = agg.TotalExternalMetrics > 0;
        double cpu = external ? agg.AverageCpu : 0.0;
        double rssMb = external ? agg.AverageRss / (1024.0 * 1024.0) : 0.0;

        Console.WriteLine();
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"SUMMARY - CLIENT_VERSION={config.ClientVersion} (warmup excluded)");
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"Measured messages: {measured}");
        Console.WriteLine($"Duration:          {duration:F2} s");
        Console.WriteLine($"Throughput:        {thrMsg:F0} msg/s  ({thrMib:F2} MiB/s)");
        Console.WriteLine($"E2E latency (ms):  min={min} avg={avg:F2} p50={p50} p90={p90} p95={p95} p99={p99} p99.9={p999} max={max}");
        if (external)
        {
            Console.WriteLine($"CPU (% one core):  avg={cpu:F1}");
            Console.WriteLine($"RSS (MB):          avg={rssMb:F1}");
        }

        Console.WriteLine(new string('=', 72));

        // Not written when nothing was measured, as Python: zeros would read as a result.
        bool writeFailed = false;
        if (measured > 0)
        {
            var json = new StringBuilder(512);
            json.Append("{\n");
            json.Append("  \"test\": \"consumer\",\n");
            json.Append("  \"client\": ").Append(PerfFormat.JsonString(ClientName(config))).Append(",\n");
            json.Append("  \"client_version\": ").Append(PerfFormat.JsonString(config.ClientVersion)).Append(",\n");
            json.Append("  \"topic\": ").Append(PerfFormat.JsonString(config.Topic)).Append(",\n");
            json.Append("  \"messages_measured\": ").Append(PerfFormat.Num(measured)).Append(",\n");
            json.Append("  \"duration_s\": ").Append(PerfFormat.JsonFloat(duration)).Append(",\n");
            json.Append("  \"throughput_msg_s\": ").Append(PerfFormat.JsonFloat(thrMsg)).Append(",\n");
            json.Append("  \"throughput_mib_s\": ").Append(PerfFormat.JsonFloat(thrMib)).Append(",\n");
            json.Append("  \"latency_ms\": {\n");
            json.Append("    \"min\": ").Append(PerfFormat.Num(min)).Append(",\n");
            json.Append("    \"avg\": ").Append(PerfFormat.JsonFloat(avg)).Append(",\n");
            json.Append("    \"p50\": ").Append(PerfFormat.Num(p50)).Append(",\n");
            json.Append("    \"p90\": ").Append(PerfFormat.Num(p90)).Append(",\n");
            json.Append("    \"p95\": ").Append(PerfFormat.Num(p95)).Append(",\n");
            json.Append("    \"p99\": ").Append(PerfFormat.Num(p99)).Append(",\n");
            json.Append("    \"p999\": ").Append(PerfFormat.Num(p999)).Append(",\n");
            json.Append("    \"max\": ").Append(PerfFormat.Num(max)).Append('\n');
            json.Append("  },\n");

            // rss_avg_mb as the other consumer harnesses' summaries, rss_avg_kib as every harness's results.json;
            // all three are written, 0 without a sample.
            json.Append("  \"rss_avg_mb\": ").Append(PerfFormat.JsonFloat(rssMb)).Append(",\n");
            json.Append("  \"cpu_avg_pct\": ").Append(PerfFormat.JsonFloat(cpu)).Append(",\n");
            json.Append("  \"rss_avg_kib\": ").Append(PerfFormat.JsonFloat(rssMb * 1024.0)).Append('\n');
            json.Append('}');

            writeFailed = !TryWriteResults(config.ResultsFile, json.ToString());
        }

        return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: false, measured, p99, writeFailed);
    }

    /// <summary>The <c>client</c> of results.json: backend, then mode, as Python's <c>_client_name</c>.</summary>
    private static string ClientName(ConsumerBenchmarkConfig config) =>
        "dotnet-" + (config.ClientVersion == "2" ? "librdkafka" : "rust") + (config.AsyncMode ? "-async" : string.Empty) + "-consumer";

    // Unlike Python, which only logs it, a failed write fails the run.
    private static bool TryWriteResults(string path, string contents)
    {
        try
        {
            File.WriteAllText(path, contents);
            Console.WriteLine($"Results summary written to: {path}");
            return true;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            Console.Error.WriteLine($"Could not write {path}: {e.Message}");
            return false;
        }
    }

    private static double Monotonic() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;

    /// <summary>Thrown when the measured-message cap is reached, Python's <c>_Done</c>.</summary>
    private sealed class MeasurementDone : Exception
    {
    }

    /// <summary>
    /// The loop state <see cref="Run"/> and <see cref="RunAsync"/> share, Python's <c>_Measurement</c>: the warmup to
    /// measure switch, the latency histogram, the exact latency sum, min and max, and the stop conditions.
    /// </summary>
    private sealed class Measurement
    {
        private readonly ConsumerBenchmarkConfig _cfg;
        private readonly Metrics _metrics;
        private double? _consumeStart;
        private bool _warmupComplete;
        private double? _measureStart;
        private double _noDataDeadline;

        internal Measurement(ConsumerBenchmarkConfig cfg, Metrics metrics)
        {
            _cfg = cfg;
            _metrics = metrics;
            LatencyHist = LatencyHistogram.New(LatencyHistogram.ConsumerMaxLatencyMs);
            _warmupComplete = cfg.WarmupSeconds <= 0;
        }

        internal long[] LatencyHist { get; }

        internal long MeasuredMessages { get; private set; }

        internal long LatencySamples { get; private set; }

        internal long LatencySumMs { get; private set; }

        internal long? LatencyMinMs { get; private set; }

        // Starts at 0, as Python's latency_max_ms.
        internal long LatencyMaxMs { get; private set; }

        internal void Begin()
        {
            // Primed before the sampler starts, so its first sample covers only measured time.
            _metrics.ResetCpuBaseline();
            _metrics.StartCollecting(_cfg.IntervalSeconds);
            if (_warmupComplete)
            {
                _measureStart = Monotonic();
                _metrics.SetMeasurementStart(Metrics.NowMs());
            }

            _noDataDeadline = Monotonic() + ConsumerBenchmarkConfig.NoDataBudgetSeconds;
        }

        /// <summary>
        /// Feeds one poll's records, reading the clocks once when the poll returned, as Python's <c>run</c> passes
        /// <c>poll_now_ms</c>: the harness's own per-record work is not counted as latency. The latency is the wall
        /// clock minus the record's CreateTime, which is wall clock too.
        /// </summary>
        internal void ProcessPoll(IReadOnlyList<PolledRecord> records)
        {
            if (records.Count == 0)
            {
                return;
            }

            long pollNowMs = Metrics.NowMs();
            double now = Monotonic();
            for (int i = 0; i < records.Count; i++)
            {
                ProcessRecord(records[i], pollNowMs, now);
            }
        }

        internal bool TimeLimitReached() =>
            _warmupComplete && _measureStart is double start && Monotonic() - start >= _cfg.TestDurationSeconds;

        internal bool NoDataTimeout()
        {
            if (!_warmupComplete && _consumeStart is null && Monotonic() >= _noDataDeadline)
            {
                Console.Error.WriteLine("ERROR: no records within 120s (is something producing?)");
                return true;
            }

            return false;
        }

        internal double MeasuredDuration() => _measureStart is double start ? Monotonic() - start : 0.0;

        // Python's process_record: the warmup is timed from the first record.
        private void ProcessRecord(PolledRecord record, long pollNowMs, double now)
        {
            _consumeStart ??= now;

            if (!_warmupComplete)
            {
                if (now - _consumeStart.Value >= _cfg.WarmupSeconds)
                {
                    _warmupComplete = true;
                    _measureStart = now;

                    // Excludes the warmup's CPU from the first measured sample.
                    _metrics.ResetCpuBaseline();
                    _metrics.SetMeasurementStart(Metrics.NowMs());
                    Console.WriteLine($">>> warmup complete ({_cfg.WarmupSeconds}s); measuring");
                }

                return;
            }

            // Every measured record counts towards throughput; only one with a CreateTime has a latency.
            _metrics.AddBytes(record.NBytes);
            _metrics.AddMessages(1);
            MeasuredMessages++;
            if (record.TimestampMs > 0)
            {
                long latency = pollNowMs - record.TimestampMs;

                // A negative latency (a clock step) lands in bucket 0 and is still counted.
                LatencyHistogram.Record(LatencyHist, latency);
                LatencySumMs += latency;
                if (LatencyMinMs is not long min || latency < min)
                {
                    LatencyMinMs = latency;
                }

                if (latency > LatencyMaxMs)
                {
                    LatencyMaxMs = latency;
                }

                _metrics.AddLatency(latency);
                LatencySamples++;
            }

            if (_cfg.NumMessages > 0 && MeasuredMessages >= _cfg.NumMessages)
            {
                throw new MeasurementDone();
            }
        }
    }
}
