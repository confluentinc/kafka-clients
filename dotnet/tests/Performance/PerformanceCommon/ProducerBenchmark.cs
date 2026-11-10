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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>The producer benchmark's outcome, which drives the exe's cooldown, verification and exit code.</summary>
public sealed class ProducerBenchmarkResult
{
    internal ProducerBenchmarkResult(long warmupSent, long measuredSent, bool latencyBudgetExceeded, bool warmupFailed, bool runFailed)
    {
        WarmupSent = warmupSent;
        MeasuredSent = measuredSent;
        LatencyBudgetExceeded = latencyBudgetExceeded;
        WarmupFailed = warmupFailed;
        RunFailed = runFailed;
    }

    public long WarmupSent { get; }

    public long MeasuredSent { get; }

    public bool LatencyBudgetExceeded { get; }

    public bool WarmupFailed { get; }

    public bool RunFailed { get; }
}

/// <summary>
/// The client-free producer engine, Python's <c>main</c> and <c>async_main</c>: each measured send reports to a
/// delivery callback that stamps its latency at the ack; after the loop the engine flushes, waits up to 30 s for the
/// remaining callbacks, checks the counts and writes the summary.
/// </summary>
public static class ProducerBenchmark
{
    private static readonly TimeSpan s_drainTimeout = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_drainPollInterval = TimeSpan.FromMilliseconds(10);

    // A counted run also stops once its measured time reaches TEST_DURATION_SECONDS plus this slack.
    private const int CountedRunCapSlackSeconds = 10;

    // A client that rejects this many sends in a row with no successful send in between cannot reach a meaningful
    // measurement, so the run stops as failed, as in the C and Rust harnesses.
    private const long MaxConsecutiveSendRejections = 1000;

    public static ProducerBenchmarkResult RunSync(
        IProducerBackend backend,
        ProducerBenchmarkConfig config,
        Metrics metrics,
        PerfMessage[] messages,
        CancellationToken cancellationToken)
    {
        long warmupSent = 0;

        if (config.WarmupSeconds > 0)
        {
            Console.WriteLine($"Warming up for {config.WarmupSeconds} seconds ...");
            long warmupEnd = Stopwatch.GetTimestamp() + SecondsToTicks(config.WarmupSeconds);
            int i = 0;
            while (Stopwatch.GetTimestamp() < warmupEnd)
            {
                PerfMessage message = messages[i % messages.Length];
                try
                {
                    PerfRecordMetadata meta = backend.Send(config.TopicName, message.Key, message.Value).Get();
                    Verify(meta, config);
                    warmupSent++;
                }
                catch (Exception)
                {
                    Console.WriteLine("Warmup failed due to message verification error");
                    return WarmupFailed(warmupSent);
                }

                Thread.Sleep(100);
                i++;
            }
        }

        var deliveries = new DeliveryStats(metrics, config);
        metrics.ResetCpuBaseline();
        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        long consecutiveRejections = 0;
        bool capped = false;
        bool rejectionsFailed = false;
        while (ContinueSending(config, messagesSent, cancellationToken))
        {
            if (TimeBoundReached(config, firstTicks, ref capped))
            {
                break;
            }

            PerfMessage message = messages[messagesSent % messages.Length];
            long startTicks = Stopwatch.GetTimestamp();
            try
            {
                backend.Send(config.TopicName, message.Key, message.Value, deliveries, startTicks);
            }
            catch (Exception ex)
            {
                // Never accepted: not counted, and no callback will come for it.
                ReportSendError(ex, cancellationToken);
                if (TooManyRejections(ref consecutiveRejections))
                {
                    rejectionsFailed = true;
                    break;
                }

                continue;
            }

            consecutiveRejections = 0;
            messagesSent++;
            SleepSeconds(PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks)), cancellationToken);
        }

        // Only when not interrupted, as Python's main.
        if (!cancellationToken.IsCancellationRequested)
        {
            SleepSeconds(PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks), final: true), cancellationToken);
            try
            {
                backend.Flush(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception e)
            {
                Console.WriteLine($"Producer flush failed: {e.Message}");
            }

            long drainEnd = Stopwatch.GetTimestamp() + (long)(s_drainTimeout.TotalSeconds * Stopwatch.Frequency);
            while (deliveries.Completed < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
            {
                Thread.Sleep(s_drainPollInterval);
            }
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementEnd(afterMs);
        var run = new MeasuredRun(messagesSent, capped, rejectionsFailed, firstTicks, afterTicks, afterMs);
        return Finish(deliveries, metrics, config, warmupSent, run, cancellationToken);
    }

    public static async Task<ProducerBenchmarkResult> RunAsync(
        IAsyncProducerBackend backend,
        ProducerBenchmarkConfig config,
        Metrics metrics,
        PerfMessage[] messages,
        CancellationToken cancellationToken)
    {
        long warmupSent = 0;

        if (config.WarmupSeconds > 0)
        {
            Console.WriteLine($"Warming up for {config.WarmupSeconds} seconds ...");
            long warmupEnd = Stopwatch.GetTimestamp() + SecondsToTicks(config.WarmupSeconds);
            int i = 0;
            while (Stopwatch.GetTimestamp() < warmupEnd)
            {
                PerfMessage message = messages[i % messages.Length];
                try
                {
                    Task<PerfRecordMetadata> delivery = await backend.Send(config.TopicName, message.Key, message.Value).ConfigureAwait(false);
                    PerfRecordMetadata meta = await delivery.ConfigureAwait(false);
                    Verify(meta, config);
                    warmupSent++;
                }
                catch (Exception)
                {
                    Console.WriteLine("Warmup failed due to message verification error");
                    return WarmupFailed(warmupSent);
                }

                await Task.Delay(100).ConfigureAwait(false);

                i++;
            }
        }

        var deliveries = new DeliveryStats(metrics, config);
        Console.WriteLine($"AWAIT_ACCEPTED: {(config.AwaitAccepted ? "True" : "False")}");
        metrics.ResetCpuBaseline();
        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        long consecutiveRejections = 0;
        bool capped = false;
        bool rejectionsFailed = false;
        try
        {
            while (ContinueSending(config, messagesSent, cancellationToken))
            {
                if (TimeBoundReached(config, firstTicks, ref capped))
                {
                    break;
                }

                PerfMessage message = messages[messagesSent % messages.Length];
                long startTicks = Stopwatch.GetTimestamp();
                try
                {
                    ValueTask accepted = backend.Send(config.TopicName, message.Key, message.Value, deliveries, startTicks);
                    if (config.AwaitAccepted)
                    {
                        // Waits for acceptance only, where the client applies its backpressure.
                        await accepted.ConfigureAwait(false);
                    }
                    else if (!accepted.IsCompletedSuccessfully)
                    {
                        // Counted as sent: a later acceptance failure is a failed delivery.
                        _ = ReportFailedAcceptance(accepted, deliveries, startTicks);
                    }
                }
                catch (Exception ex)
                {
                    // Never accepted: not counted, and no callback will come for it.
                    ReportSendError(ex, cancellationToken);
                    if (TooManyRejections(ref consecutiveRejections))
                    {
                        rejectionsFailed = true;
                        break;
                    }

                    continue;
                }

                consecutiveRejections = 0;
                messagesSent++;
                double wait = PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks));
                if (wait > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), cancellationToken).ConfigureAwait(false);
                }
            }

            double finalWait = PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks), final: true);
            if (finalWait > 0 && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(finalWait), cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C ends the loop, as Python's terminating flag does; the summary still runs.
        }

        try
        {
            await backend.Flush(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Producer flush failed: {e.Message}");
        }

        long drainEnd = Stopwatch.GetTimestamp() + (long)(s_drainTimeout.TotalSeconds * Stopwatch.Frequency);
        while (deliveries.Completed < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
        {
            await Task.Delay(s_drainPollInterval).ConfigureAwait(false);
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementEnd(afterMs);
        var run = new MeasuredRun(messagesSent, capped, rejectionsFailed, firstTicks, afterTicks, afterMs);
        return Finish(deliveries, metrics, config, warmupSent, run, cancellationToken);
    }

    /// <summary>
    /// Seconds to sleep after <paramref name="messagesSent"/> measured sends (zero or less: none), as Python's
    /// <c>pacing_wait_s</c>: at every checkpoint the sender sleeps until <c>first send + sends / LIMIT_RPS</c>;
    /// <paramref name="final"/>, after the last send, waits off the checkpoint too, as the C harness does, so a fast
    /// client cannot end a counted run early.
    /// </summary>
    public static double PacingWaitSeconds(ProducerBenchmarkConfig config, long messagesSent, double elapsedSeconds, bool final = false)
    {
        if (config.LimitRps is not int rps || (!final && messagesSent % config.PacingCheckpoint != 0))
        {
            return 0.0;
        }

        return (messagesSent / (double)rps) - elapsedSeconds;
    }

    private static ProducerBenchmarkResult WarmupFailed(long warmupSent) =>
        new ProducerBenchmarkResult(warmupSent, 0, latencyBudgetExceeded: false, warmupFailed: true, runFailed: true);

    private static bool ContinueSending(ProducerBenchmarkConfig config, long messagesSent, CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }

        return config.NumMessages <= 0 || messagesSent < config.NumMessages;
    }

    private static void ReportSendError(Exception ex, CancellationToken token)
    {
        if (!token.IsCancellationRequested)
        {
            Console.WriteLine($"Send error: {ex.Message}");
        }
    }

    private static async Task ReportFailedAcceptance(ValueTask accepted, IPerfDeliveryCallback deliveries, long startTicks)
    {
        try
        {
            await accepted.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            deliveries.OnCompletion(startTicks, default, ex);
        }
    }

    private static bool Verify(PerfRecordMetadata meta, ProducerBenchmarkConfig config)
    {
        if (!config.DoVerify)
        {
            return true;
        }

        // As Python: verify_message (v2) wants a timestamp > 0, verify_record_metadata (v3) >= 0.
        bool timestampOk = config.ClientVersion == "2" ? meta.Timestamp > 0 : meta.Timestamp >= 0;
        return meta.Offset >= 0 && meta.Partition >= 0 && meta.Topic == config.TopicName && timestampOk;
    }

    private static ProducerBenchmarkResult Finish(
        DeliveryStats deliveries,
        Metrics metrics,
        ProducerBenchmarkConfig config,
        long warmupSent,
        MeasuredRun run,
        CancellationToken token)
    {
        if (run.Capped)
        {
            Console.WriteLine(
                $"capped: sent {run.MessagesSent} of {config.NumMessages} records in " +
                $"{TicksToSeconds(run.AfterTicks - run.FirstTicks).ToString("F2", CultureInfo.InvariantCulture)} s " +
                $"(TEST_DURATION_SECONDS + {CountedRunCapSlackSeconds} s)");
        }

        // One consistent view: a callback can still arrive after the drain gave up.
        DeliverySnapshot snapshot = deliveries.Snapshot();
        bool countsFailed = CheckCompletion(snapshot, config, run, token);
        (bool budgetExceeded, bool resultsWritten) = PrintSummary(snapshot, metrics, config, run);
        bool runFailed = countsFailed || !resultsWritten || run.RejectionsFailed;
        return new ProducerBenchmarkResult(warmupSent, run.MessagesSent, budgetExceeded, warmupFailed: false, runFailed: runFailed);
    }

    // As Python's check_completion: a counted run is held to the records it sent, so the time cap's shortfall is
    // reported by the capped line, not failed.
    private static bool CheckCompletion(DeliverySnapshot snapshot, ProducerBenchmarkConfig config, MeasuredRun run, CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }

        bool failed = false;
        if (snapshot.Verified != snapshot.Completed)
        {
            Console.WriteLine($"Verified messages {snapshot.Verified} does not match completed messages {snapshot.Completed}");
            failed = true;
        }

        if (config.NumMessages > 0)
        {
            if (snapshot.Completed != run.MessagesSent)
            {
                Console.WriteLine($"Completed messages {snapshot.Completed} does not match produced messages {run.MessagesSent}");
                failed = true;
            }

            if (snapshot.Completed == 0)
            {
                Console.WriteLine(run.Capped ? "No messages completed before the time cap" : "No messages completed");
                failed = true;
            }
        }

        return failed;
    }

    // The rates and messages_measured count the successful deliveries, the latency min/avg/max are exact and the
    // percentiles come from the floor-ms histogram.
    private static (bool BudgetExceeded, bool ResultsWritten) PrintSummary(
        DeliverySnapshot snapshot,
        Metrics metrics,
        ProducerBenchmarkConfig config,
        MeasuredRun run)
    {
        double totalTimeS = TicksToSeconds(run.AfterTicks - run.FirstTicks);
        double totalTimeMs = totalTimeS * 1000.0;
        long succeeded = snapshot.Succeeded;
        double messageRate = totalTimeS > 0 ? succeeded / totalTimeS : 0.0;
        ExternalMetricsAggregations agg = metrics.ExternalMetricsAggregations();

        Console.WriteLine($"End time: {run.AfterMs} ms");
        Console.WriteLine($"Duration: {Inv(totalTimeMs)} ms");

        if (agg.TotalExternalMetrics > 0)
        {
            double averageCpu = agg.AverageCpu;
            double averageRss = agg.AverageRss / 1024.0;
            Console.WriteLine($"Average CPU: {averageCpu:F2} %");
            Console.WriteLine($"Average RSS: {averageRss:F2} KiB");
            Console.WriteLine($"CPU Efficiency: {messageRate / (averageCpu > 0 ? averageCpu : 1):F2} msg/(s * 1% CPU)");
            Console.WriteLine($"Memory Efficiency: {messageRate / (averageRss > 0 ? averageRss : 1):F2} msg/(s * KB RSS)");
        }
        else
        {
            Console.WriteLine("No external metrics collected");
        }

        double mibRate = totalTimeS > 0 ? (succeeded * (double)config.MessageSize) / (1024.0 * 1024.0) / totalTimeS : 0.0;
        double minLatency = succeeded > 0 ? TicksToMs(snapshot.MinLatencyTicks) : 0.0;
        double averageLatency = succeeded > 0 ? TicksToMs(snapshot.TotalLatencyTicks) / succeeded : 0.0;
        double maxLatency = succeeded > 0 ? TicksToMs(snapshot.MaxLatencyTicks) : 0.0;
        Console.WriteLine($"Average time: {(succeeded > 0 ? totalTimeMs / succeeded : 0.0):F2} ms");
        Console.WriteLine($"Average rate msg/s: {messageRate:F2} msg/s");
        Console.WriteLine($"Average rate MiB/s: {mibRate:F2} MiB/s");
        Console.WriteLine($"Average latency: {averageLatency:F2} ms");
        Console.WriteLine($"Max latency: {maxLatency:F2} ms");

        long[] hist = snapshot.LatencyHist;
        long p50 = LatencyHistogram.PercentileFromHist(hist, 0.50);
        long p90 = LatencyHistogram.PercentileFromHist(hist, 0.90);
        long p95 = LatencyHistogram.PercentileFromHist(hist, 0.95);
        long p99 = LatencyHistogram.PercentileFromHist(hist, 0.99);
        long p999 = LatencyHistogram.PercentileFromHist(hist, 0.999);
        Console.WriteLine($"p50 latency: {p50} ms");
        Console.WriteLine($"p90 latency: {p90} ms");
        Console.WriteLine($"p99 latency: {p99} ms");
        Console.WriteLine($"p999 latency: {p999} ms");

        bool external = agg.TotalExternalMetrics > 0;
        string client = (config.ClientVersion == "2" ? "dotnet-librdkafka" : "dotnet-rust") + (config.Async ? "-async" : string.Empty);
        var json = new StringBuilder(512);
        json.Append("{\n");
        json.Append("  \"test\": \"producer\",\n");
        json.Append("  \"client\": ").Append(PerfFormat.JsonString(client)).Append(",\n");
        json.Append("  \"topic\": ").Append(PerfFormat.JsonString(config.TopicName)).Append(",\n");
        json.Append("  \"messages_measured\": ").Append(PerfFormat.Num(succeeded)).Append(",\n");
        json.Append("  \"duration_s\": ").Append(PerfFormat.JsonFloat(totalTimeS)).Append(",\n");
        json.Append("  \"throughput_msg_s\": ").Append(PerfFormat.JsonFloat(messageRate)).Append(",\n");
        json.Append("  \"throughput_mib_s\": ").Append(PerfFormat.JsonFloat(mibRate)).Append(",\n");
        json.Append("  \"latency_ms\": {\n");
        json.Append("    \"min\": ").Append(PerfFormat.JsonFloat(minLatency)).Append(",\n");
        json.Append("    \"avg\": ").Append(PerfFormat.JsonFloat(averageLatency)).Append(",\n");
        json.Append("    \"p50\": ").Append(PerfFormat.Num(p50)).Append(",\n");
        json.Append("    \"p90\": ").Append(PerfFormat.Num(p90)).Append(",\n");
        json.Append("    \"p95\": ").Append(PerfFormat.Num(p95)).Append(",\n");
        json.Append("    \"p99\": ").Append(PerfFormat.Num(p99)).Append(",\n");
        json.Append("    \"p999\": ").Append(PerfFormat.Num(p999)).Append(",\n");
        json.Append("    \"max\": ").Append(PerfFormat.JsonFloat(maxLatency)).Append('\n');
        json.Append("  },\n");
        json.Append("  \"cpu_avg_pct\": ").Append(PerfFormat.JsonFloat(external ? agg.AverageCpu : 0.0)).Append(",\n");
        json.Append("  \"rss_avg_kib\": ").Append(PerfFormat.JsonFloat(external ? agg.AverageRss / 1024.0 : 0.0));
        if (config.BufferMemoryAllocationStrategy is string strategy)
        {
            json.Append(",\n  \"buffer_memory_allocation_strategy\": ").Append(PerfFormat.JsonString(strategy));
        }

        json.Append("\n}");

        bool resultsWritten = TryWriteResults(config.ResultsFile, json.ToString());

        if (config.P99LimitMs > 0 && p99 > config.P99LimitMs)
        {
            Console.WriteLine($"p99 latency {p99} ms exceeds budget {config.P99LimitMs} ms");
            return (true, resultsWritten);
        }

        return (false, resultsWritten);
    }

    // Failing to write the results file fails the run, as in Python.
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
            Console.WriteLine($"Failed to write {path}: {e.Message}");
            return false;
        }
    }

    private static void SleepSeconds(double seconds, CancellationToken token)
    {
        if (seconds <= 0)
        {
            return;
        }

        int ms = (int)Math.Min(int.MaxValue, seconds * 1000.0);
        token.WaitHandle.WaitOne(ms);
    }

    // Checked before every send, rejected retries included, as the C and Rust harnesses do: a counted run stops at
    // TEST_DURATION_SECONDS plus the slack (capped), an unbounded one at TEST_DURATION_SECONDS.
    private static bool TimeBoundReached(ProducerBenchmarkConfig config, long firstTicks, ref bool capped)
    {
        double elapsed = ElapsedSeconds(firstTicks);
        if (config.NumMessages > 0)
        {
            if (elapsed < config.TestDurationSeconds + CountedRunCapSlackSeconds)
            {
                return false;
            }

            capped = true;
            return true;
        }

        if (elapsed < config.TestDurationSeconds)
        {
            return false;
        }

        Console.WriteLine($"Test duration reached, {elapsed.ToString("F2", CultureInfo.InvariantCulture)} seconds. Interrupting...\n");
        return true;
    }

    private static bool TooManyRejections(ref long consecutiveRejections)
    {
        if (++consecutiveRejections < MaxConsecutiveSendRejections)
        {
            return false;
        }

        Console.WriteLine($"{consecutiveRejections} consecutive sends rejected with no successful send in between; stopping the run as failed");
        return true;
    }

    private static double ElapsedSeconds(long firstTicks) => TicksToSeconds(Stopwatch.GetTimestamp() - firstTicks);

    private static double TicksToSeconds(long ticks) => ticks / (double)Stopwatch.Frequency;

    private static double TicksToMs(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    private static long SecondsToTicks(int seconds) => (long)seconds * Stopwatch.Frequency;

    private static string Inv(double value) => value.ToString(CultureInfo.InvariantCulture);

    private static void TryWriteLine(string line)
    {
        try
        {
            Console.WriteLine(line);
        }
        catch (Exception)
        {
            // A delivery callback must not throw: a line the console cannot take is lost.
        }
    }

    /// <summary>The measured send loop's outcome and the interval's end, which the summary and checks read.</summary>
    private readonly struct MeasuredRun
    {
        internal MeasuredRun(long messagesSent, bool capped, bool rejectionsFailed, long firstTicks, long afterTicks, long afterMs)
        {
            MessagesSent = messagesSent;
            Capped = capped;
            RejectionsFailed = rejectionsFailed;
            FirstTicks = firstTicks;
            AfterTicks = afterTicks;
            AfterMs = afterMs;
        }

        internal long MessagesSent { get; }

        internal bool Capped { get; }

        internal bool RejectionsFailed { get; }

        internal long FirstTicks { get; }

        internal long AfterTicks { get; }

        internal long AfterMs { get; }
    }

    /// <summary>The counters at one instant, copied out of <see cref="DeliveryStats"/> under its lock.</summary>
    private readonly struct DeliverySnapshot
    {
        internal DeliverySnapshot(
            long[] latencyHist,
            long verified,
            long completed,
            long succeeded,
            long minLatencyTicks,
            long maxLatencyTicks,
            long totalLatencyTicks)
        {
            LatencyHist = latencyHist;
            Verified = verified;
            Completed = completed;
            Succeeded = succeeded;
            MinLatencyTicks = minLatencyTicks;
            MaxLatencyTicks = maxLatencyTicks;
            TotalLatencyTicks = totalLatencyTicks;
        }

        internal long[] LatencyHist { get; }

        internal long Verified { get; }

        internal long Completed { get; }

        internal long Succeeded { get; }

        internal long MinLatencyTicks { get; }

        internal long MaxLatencyTicks { get; }

        internal long TotalLatencyTicks { get; }
    }

    /// <summary>
    /// The measured interval's accounting, fed by the delivery callbacks (Python's <c>record_delivery</c> and
    /// <c>record_completion_async</c>); some clients report on several threads at once, so the counters share one
    /// lock. Latencies are <see cref="Stopwatch"/> ticks from the send to the ack.
    /// </summary>
    private sealed class DeliveryStats : IPerfDeliveryCallback
    {
        private readonly object _lock = new object();
        private readonly Metrics _metrics;
        private readonly ProducerBenchmarkConfig _config;
        private readonly long[] _latencyHist = LatencyHistogram.New();
        private long _verified;
        private long _completed;
        private long _succeeded;
        private long _minLatencyTicks = long.MaxValue;
        private long _maxLatencyTicks;
        private long _totalLatencyTicks;

        internal DeliveryStats(Metrics metrics, ProducerBenchmarkConfig config)
        {
            _metrics = metrics;
            _config = config;
        }

        internal long Completed
        {
            get
            {
                lock (_lock)
                {
                    return _completed;
                }
            }
        }

        public void OnCompletion(long sendStartTicks, PerfRecordMetadata metadata, Exception? exception)
        {
            // Stamped before the lock, so the latency is the time to the ack.
            long latencyTicks = Stopwatch.GetTimestamp() - sendStartTicks;
            string? line = null;
            lock (_lock)
            {
                try
                {
                    if (exception is not null)
                    {
                        // A failed record counts as completed, is not verified (which fails the run) and adds no
                        // latency or throughput.
                        line = $"Produce call resulted in exception: {exception.Message}";
                    }
                    else
                    {
                        if (Verify(metadata, _config))
                        {
                            _verified++;
                        }

                        double latencyMs = TicksToMs(latencyTicks);
                        _metrics.AddLatency(latencyMs);
                        LatencyHistogram.Record(_latencyHist, latencyMs);
                        _metrics.AddMessages(1);
                        _metrics.AddBytes(_config.MessageSize);
                        _minLatencyTicks = Math.Min(_minLatencyTicks, latencyTicks);
                        _maxLatencyTicks = Math.Max(_maxLatencyTicks, latencyTicks);
                        _totalLatencyTicks += latencyTicks;
                        _succeeded++;
                    }

                    // Counted last, and in the catch too: every reported record settles once.
                    _completed++;
                }
                catch (Exception e)
                {
                    _completed++;
                    line = $"Delivery accounting failed: {e}";
                }
            }

            if (line is not null)
            {
                TryWriteLine(line);
            }
        }

        internal DeliverySnapshot Snapshot()
        {
            lock (_lock)
            {
                return new DeliverySnapshot(
                    (long[])_latencyHist.Clone(),
                    _verified,
                    _completed,
                    _succeeded,
                    _succeeded > 0 ? _minLatencyTicks : 0,
                    _maxLatencyTicks,
                    _totalLatencyTicks);
            }
        }
    }
}
