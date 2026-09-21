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

/// <summary>The consumer benchmark summary — the fields written to <c>results.json</c> and used for the exit code (§6.3).</summary>
public sealed class ConsumerBenchmarkResult
{
    /// <summary>
    /// Process exit code meaning "the setup never came up" — the pipeline was not live at the edge, so
    /// nothing was measured and the run should be RETRIED rather than reported as a failure. The C# analog
    /// of <c>consumer_performance_test.py</c>'s <c>SETUP_NOT_READY_RC</c>, shared so the two
    /// <c>ConsumerMain</c>s and the in-suite smoke's retry loop all agree on the value.
    /// </summary>
    public const int SetupNotReadyExitCode = 2;

    internal ConsumerBenchmarkResult(bool assignmentFailed, bool setupNotReady, long messagesMeasured, long p99)
    {
        AssignmentFailed = assignmentFailed;
        SetupNotReady = setupNotReady;
        MessagesMeasured = messagesMeasured;
        P99 = p99;
    }

    /// <summary>Whether the run aborted waiting for a partition assignment (Python returns <c>None</c> — exit 1).</summary>
    public bool AssignmentFailed { get; }

    /// <summary>
    /// Whether the readiness gate never saw enough measurable records (Python's <c>SETUP_NOT_READY</c>
    /// sentinel — exit <see cref="SetupNotReadyExitCode"/>, retryable, NOT a benchmark failure).
    /// </summary>
    public bool SetupNotReady { get; }

    /// <summary>Number of measured (warmup-excluded) records (Python <c>messages_measured</c>).</summary>
    public long MessagesMeasured { get; }

    /// <summary>The measured p99 latency in ms (for the <c>P99_LIMIT_MS</c> gate).</summary>
    public long P99 { get; }
}

/// <summary>
/// The client-agnostic consumer end-to-end-latency engine — the C# analog of
/// <c>consumer_performance_test.py</c>'s <c>run</c> (sync) / <c>run_async</c> (async), sharing one
/// <see cref="Measurement"/> so the two paths compute identical stats. Per-record e2e latency is
/// <c>now_ms - record.Timestamp</c>, recorded only when the record carries a positive timestamp and the
/// latency is non-negative.
/// </summary>
/// <remarks>
/// <para>
/// Topic (re)creation is <b>not</b> performed here (D6 — .NET ships no in-harness AdminClient; provisioning
/// is external), so the engine never calls a <c>recreate_topic</c> analog. When <c>KAFKA_BIN</c> is set it
/// self-spawns <c>kafka-producer-perf-test.sh</c> as the load driver (mirroring Python's <c>spawn_producer</c>);
/// otherwise an external producer must feed the topic.
/// </para>
/// <para>
/// Before the timed window opens, a <b>readiness gate</b> waits for
/// <see cref="ConsumerBenchmarkConfig.ReadinessMinRecords"/> records with a usable, non-negative latency
/// to actually flow (Python's <c>_readiness_ok</c>). "Caught up to the live edge" is not the same as
/// "data is flowing" — a feeder still starting up in a container leaves the measured window empty, which
/// would be reported as a product failure. Records consumed by the gate are warmup and are NOT measured.
/// When the gate expires the run returns <see cref="ConsumerBenchmarkResult.SetupNotReady"/> so the caller
/// can exit <see cref="ConsumerBenchmarkResult.SetupNotReadyExitCode"/> and be retried.
/// </para>
/// <para>
/// <b>Deliberate divergence from Python</b> (recorded per <c>definition-of-done.md</c> §7): the gate's
/// early return also stops the load driver. Python leaks its <c>spawn_producer</c> subprocess on this path
/// (its <c>finally</c> block lives inside the measurement <c>try</c> it never enters); in .NET that would
/// be a real orphaned process, so <see cref="LoadDriver.Stop"/> is called before returning.
/// </remarks>
public static class ConsumerBenchmark
{
    /// <summary>Runs the <b>sync</b> consumer benchmark (Python <c>run</c>) and returns its stats.</summary>
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
            // Deliberate divergence from Python (see the type remarks): stop the load driver we started
            // rather than leaking it. Metrics.StopCollecting is a no-op here — StartCollecting only runs
            // in Measurement.Begin, which this early return precedes — so no sampler thread is stranded.
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
            // num_messages reached.
        }
        finally
        {
            double measuredDuration = measurement.MeasuredDuration();
            metrics.SetMeasurementEnd(Metrics.NowMs());
            loadDriver?.Stop();
            backend.Close();
            measurement.SetFinalDuration(measuredDuration);
        }

        return Summarize(config, measurement);
    }

    /// <summary>Runs the <b>async</b> consumer benchmark (Python <c>run_async</c>) and returns its stats.</summary>
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
            // Same deliberate divergence + StopCollecting note as the sync path above.
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
            // num_messages reached.
        }
        finally
        {
            double measuredDuration = measurement.MeasuredDuration();
            metrics.SetMeasurementEnd(Metrics.NowMs());
            loadDriver?.Stop();
            await backend.Close().ConfigureAwait(false);
            measurement.SetFinalDuration(measuredDuration);
        }

        return Summarize(config, measurement);
    }

    /// <summary>
    /// Blocks until <see cref="ConsumerBenchmarkConfig.ReadinessMinRecords"/> measurable records have
    /// flowed, or the readiness budget expires — Python's gate loop plus <c>_readiness_ok</c>. Returns
    /// <see langword="false"/> when the pipeline never went live.
    /// </summary>
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

    /// <summary>Async counterpart of <see cref="AwaitPipelineReady"/> (Python's <c>run_async</c> gate).</summary>
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

    /// <summary>Logs and reports the gate outcome — Python's <c>_readiness_ok</c>, message-for-message.</summary>
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

    /// <summary>
    /// The e2e latency in ms when the record carries a usable CreateTime and the latency is non-negative,
    /// else <see langword="null"/> (Python's <c>_measurable_latency_ms</c>). Shared by the readiness gate
    /// and <see cref="Measurement.ProcessRecord"/> so the two can never disagree about what "measurable"
    /// means. A negative latency is what a cross-clock boundary (a container VM whose clock drifts from
    /// the host) produces, and it is dropped rather than recorded.
    /// </summary>
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

    private static ConsumerBenchmarkResult Summarize(ConsumerBenchmarkConfig config, Measurement m)
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

        Console.WriteLine();
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"SUMMARY - CLIENT_VERSION={config.ClientVersion} (warmup excluded)");
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"Measured messages: {m.MeasuredMessages}");
        Console.WriteLine($"Duration:          {duration:F2} s");
        Console.WriteLine($"Throughput:        {thrMsg:F0} msg/s  ({thrMib:F2} MiB/s)");
        Console.WriteLine($"E2E latency (ms):  min={min} avg={avg:F2} p50={p50} p90={p90} p95={p95} p99={p99} p99.9={p999} max={max}");
        Console.WriteLine(new string('=', 72));

        WriteResultsJson(config, m.MeasuredMessages, duration, thrMsg, thrMib, min, avg, p50, p90, p95, p99, p999, max);
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
        long max)
    {
        var sb = new StringBuilder(512);
        sb.Append("{\n");
        sb.Append("  \"client_version\": \"").Append(config.ClientVersion).Append("\",\n");
        sb.Append("  \"topic\": \"").Append(config.Topic).Append("\",\n");
        sb.Append("  \"messages_measured\": ").Append(measuredMessages.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("  \"duration_s\": ").Append(JsonFloat(duration)).Append(",\n");
        sb.Append("  \"throughput_msg_s\": ").Append(JsonFloat(thrMsg)).Append(",\n");
        sb.Append("  \"throughput_mib_s\": ").Append(JsonFloat(thrMib)).Append(",\n");
        sb.Append("  \"latency_ms\": {\n");
        sb.Append("    \"min\": ").Append(min.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"avg\": ").Append(JsonFloat(avg)).Append(",\n");
        sb.Append("    \"p50\": ").Append(p50.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p90\": ").Append(p90.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p95\": ").Append(p95.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p99\": ").Append(p99.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"p999\": ").Append(p999.ToString(CultureInfo.InvariantCulture)).Append(",\n");
        sb.Append("    \"max\": ").Append(max.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("  }\n");
        sb.Append("}");

        try
        {
            File.WriteAllText("results.json", sb.ToString());
        }
        catch (IOException)
        {
            // Non-fatal: the summary is already on stdout (Python swallows the same OSError).
        }
    }

    // Rounds to 2 decimals (banker's rounding, matching Python round()) and renders like Python's json:
    // an integral value keeps a trailing ".0" (round(10.0, 2) -> 10.0 -> "10.0").
    private static string JsonFloat(double value)
    {
        double rounded = Math.Round(value, 2, MidpointRounding.ToEven);
        if (rounded == Math.Floor(rounded) && Math.Abs(rounded) < 1e15)
        {
            return ((long)rounded).ToString(CultureInfo.InvariantCulture) + ".0";
        }

        return rounded.ToString("0.##", CultureInfo.InvariantCulture);
    }

    private static double Monotonic() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;

    /// <summary>Raised when the measured-message cap is reached (Python's <c>_Done</c> control-flow exception).</summary>
    private sealed class MeasurementDone : Exception
    {
    }

    /// <summary>
    /// Shared loop state for the sync <see cref="Run"/> and async <see cref="RunAsync"/> — the C# analog of
    /// Python's <c>_Measurement</c>, so the warmup→measure transition, latency histogram, and
    /// termination semantics live in one place.
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
