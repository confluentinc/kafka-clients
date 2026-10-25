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
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>The consumer benchmark's outcome, from which the exe chooses its exit code.</summary>
public sealed class ConsumerBenchmarkResult
{
    public const int SetupNotReadyExitCode = 2;

    internal ConsumerBenchmarkResult(bool assignmentFailed, bool setupNotReady, long messagesMeasured, long p99)
    {
        AssignmentFailed = assignmentFailed;
        SetupNotReady = setupNotReady;
        MessagesMeasured = messagesMeasured;
        P99 = p99;
    }

    public bool AssignmentFailed { get; }

    public bool SetupNotReady { get; }

    public long MessagesMeasured { get; }

    public long P99 { get; }
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
                backend.Close();
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

        LoadDriver? loadDriver = LoadDriver.MaybeStart(config);

        if (!AwaitPipelineReady(config, poll))
        {
            // Unlike Python, which leaks its spawn_producer process here, stop the load driver.
            loadDriver?.Stop();
            backend.Close();
            return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: true, 0, 0);
        }

        var measurement = new Measurement(config, metrics);
        measurement.Begin();
        try
        {
            while (!PerfSignals.Terminating)
            {
                foreach (PolledRecord record in poll())
                {
                    measurement.ProcessRecord(record.TimestampMs, record.NBytes);
                }

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
            double measuredDuration = measurement.MeasuredDuration();
            metrics.SetMeasurementEnd(Metrics.NowMs());
            loadDriver?.Stop();
            backend.Close();
            measurement.SetFinalDuration(measuredDuration);
        }

        return Summarize(config, measurement, metrics);
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
                await backend.Close().ConfigureAwait(false);
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

        LoadDriver? loadDriver = LoadDriver.MaybeStart(config);

        if (!await AwaitPipelineReadyAsync(config, poll).ConfigureAwait(false))
        {
            // Unlike Python, which leaks its spawn_producer process here, stop the load driver.
            loadDriver?.Stop();
            await backend.Close().ConfigureAwait(false);
            return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: true, 0, 0);
        }

        var measurement = new Measurement(config, metrics);
        measurement.Begin();
        try
        {
            while (!PerfSignals.Terminating)
            {
                foreach (PolledRecord record in await poll().ConfigureAwait(false))
                {
                    measurement.ProcessRecord(record.TimestampMs, record.NBytes);
                }

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
            double measuredDuration = measurement.MeasuredDuration();
            metrics.SetMeasurementEnd(Metrics.NowMs());
            loadDriver?.Stop();
            await backend.Close().ConfigureAwait(false);
            measurement.SetFinalDuration(measuredDuration);
        }

        return Summarize(config, measurement, metrics);
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
                if (MeasurableLatencyMs(record.TimestampMs) is not null)
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
                if (MeasurableLatencyMs(record.TimestampMs) is not null)
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

    private static long? MeasurableLatencyMs(long tsMs)
    {
        if (tsMs > 0)
        {
            long latency = Metrics.NowMs() - tsMs;
            if (latency >= 0)
            {
                return latency;
            }
        }

        return null;
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

    private static ConsumerBenchmarkResult Summarize(ConsumerBenchmarkConfig config, Measurement m, Metrics metrics)
    {
        long[] hist = m.LatencyHist;
        long p50 = LatencyHistogram.PercentileFromHist(hist, 0.50);
        long p90 = LatencyHistogram.PercentileFromHist(hist, 0.90);
        long p95 = LatencyHistogram.PercentileFromHist(hist, 0.95);
        long p99 = LatencyHistogram.PercentileFromHist(hist, 0.99);
        long p999 = LatencyHistogram.PercentileFromHist(hist, 0.999);

        long total = 0;
        long weighted = 0;
        long min = 0;
        long max = 0;
        bool sawMin = false;
        for (int ms = 0; ms < hist.Length; ms++)
        {
            long c = hist[ms];
            if (c == 0)
            {
                continue;
            }

            total += c;
            weighted += ms * c;
            if (!sawMin)
            {
                min = ms;
                sawMin = true;
            }

            max = ms;
        }

        double avg = total > 0 ? (double)weighted / total : 0.0;
        double duration = m.FinalDurationSeconds;
        double thrMsg = duration > 0 ? m.MeasuredMessages / duration : 0.0;
        double thrMib = duration > 0 ? (m.MeasuredMessages * (double)config.MessageSize) / (1024.0 * 1024.0) / duration : 0.0;

        // rss_avg_mb (MB), as Python's consumer summary; the producer harnesses report rss_avg_kib.
        ExternalMetricsAggregations agg = metrics.ExternalMetricsAggregations();
        (double Cpu, double RssMb)? external = agg.TotalExternalMetrics > 0
            ? (agg.AverageCpu, agg.AverageRss / (1024.0 * 1024.0))
            : null;

        Console.WriteLine();
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"SUMMARY - CLIENT_VERSION={config.ClientVersion} (warmup excluded)");
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"Measured messages: {m.MeasuredMessages}");
        Console.WriteLine($"Duration:          {duration:F2} s");
        Console.WriteLine($"Throughput:        {thrMsg:F0} msg/s  ({thrMib:F2} MiB/s)");
        Console.WriteLine($"E2E latency (ms):  min={min} avg={avg:F2} p50={p50} p90={p90} p95={p95} p99={p99} p99.9={p999} max={max}");
        if (external is (double cpu, double rssMb))
        {
            Console.WriteLine($"CPU (% one core):  avg={cpu:F1}");
            Console.WriteLine($"RSS (MB):          avg={rssMb:F1}");
        }

        Console.WriteLine(new string('=', 72));

        WriteResultsJson(config, m.MeasuredMessages, duration, thrMsg, thrMib, min, avg, p50, p90, p95, p99, p999, max, external);
        return new ConsumerBenchmarkResult(assignmentFailed: false, setupNotReady: false, m.MeasuredMessages, p99);
    }

    private static void WriteResultsJson(
        ConsumerBenchmarkConfig config,
        long measuredMessages,
        double duration,
        double thrMsg,
        double thrMib,
        long min,
        double avg,
        long p50,
        long p90,
        long p95,
        long p99,
        long p999,
        long max,
        (double Cpu, double RssMb)? external)
    {
        var sb = new StringBuilder(512);
        sb.Append("{\n");
        sb.Append("  \"client_version\": \"").Append(config.ClientVersion).Append("\",\n");
        sb.Append("  \"topic\": \"").Append(config.Topic).Append("\",\n");
        sb.Append("  \"messages_measured\": ").Append(measuredMessages.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("  \"duration_s\": ").Append(PerfFormat.JsonFloat(duration)).Append(",\n");
        sb.Append("  \"throughput_msg_s\": ").Append(PerfFormat.JsonFloat(thrMsg)).Append(",\n");
        sb.Append("  \"throughput_mib_s\": ").Append(PerfFormat.JsonFloat(thrMib)).Append(",\n");
        sb.Append("  \"latency_ms\": {\n");
        sb.Append("    \"min\": ").Append(min.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"avg\": ").Append(PerfFormat.JsonFloat(avg)).Append(",\n");
        sb.Append("    \"p50\": ").Append(p50.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p90\": ").Append(p90.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p95\": ").Append(p95.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p99\": ").Append(p99.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p999\": ").Append(p999.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"max\": ").Append(max.ToString(CultureInfo.InvariantCulture)).Append('\n');
        if (external is (double cpu, double rssMb))
        {
            sb.Append("  },\n");
            sb.Append("  \"cpu_avg_pct\": ").Append(PerfFormat.JsonFloat(cpu)).Append(",\n");
            sb.Append("  \"rss_avg_mb\": ").Append(PerfFormat.JsonFloat(rssMb)).Append('\n');
        }
        else
        {
            sb.Append("  }\n");
        }

        sb.Append("}");

        try
        {
            File.WriteAllText("results.json", sb.ToString());
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            // Non-fatal, as in Python: the summary is already on stdout.
        }
    }

    private static double Monotonic() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;

    /// <summary>Thrown when the measured-message cap is reached, Python's <c>_Done</c>.</summary>
    private sealed class MeasurementDone : Exception
    {
    }

    /// <summary>
    /// The loop state <see cref="Run"/> and <see cref="RunAsync"/> share, Python's <c>_Measurement</c>: the warmup to
    /// measure switch, the latency histogram and the stop conditions.
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
            LatencyHist = LatencyHistogram.New();
            _warmupComplete = cfg.WarmupSeconds <= 0;
        }

        internal long[] LatencyHist { get; }

        internal long MeasuredMessages { get; private set; }

        internal double FinalDurationSeconds { get; private set; }

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

        internal void ProcessRecord(long tsMs, int nbytes)
        {
            _consumeStart ??= Monotonic();
            double now = Monotonic();

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

            if (MeasurableLatencyMs(tsMs) is long latency)
            {
                LatencyHistogram.Record(LatencyHist, latency);
                _metrics.AddLatency(latency);
                _metrics.AddBytes(nbytes);
                _metrics.AddMessages(1);
                MeasuredMessages++;
            }

            if (_cfg.NumMessages > 0 && MeasuredMessages >= _cfg.NumMessages)
            {
                throw new MeasurementDone();
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

        internal void SetFinalDuration(double seconds) => FinalDurationSeconds = seconds;
    }
}
