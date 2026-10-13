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

/// <summary>The producer benchmark outcome the per-client exe needs for cooldown / VERIFY_CONSUMED / exit code.</summary>
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

    /// <summary>Records sent (and awaited) during warmup — excluded from the measurement, counted for VERIFY_CONSUMED.</summary>
    public long WarmupSent { get; }

    /// <summary>Records sent during the measured interval (Python <c>measured_sent</c>).</summary>
    public long MeasuredSent { get; }

    /// <summary>Whether the measured p99 exceeded <c>P99_LIMIT_MS</c> (the run must exit non-zero).</summary>
    public bool LatencyBudgetExceeded { get; }

    /// <summary>Whether warmup aborted on a send or delivery error (no measured interval ran).</summary>
    public bool WarmupFailed { get; }

    /// <summary>
    /// Whether the run must exit non-zero for a reason other than the latency budget (Python
    /// <c>run_failed</c>): the warmup failed, the results file could not be written, or — unless the run was
    /// interrupted — not every completed record verified, or not every requested record completed. The
    /// summary and the results file are still produced in the last two cases.
    /// </summary>
    public bool RunFailed { get; }
}

/// <summary>
/// The client-agnostic producer measured-loop engine — the C# analog of <c>producer_performance_test.py</c>'s
/// <c>main</c> (sync) and <c>async_main</c> (async), driven through <see cref="IProducerBackend"/> /
/// <see cref="IAsyncProducerBackend"/> so it runs identically over any client.
/// </summary>
/// <remarks>
/// Both paths send each measured record with a delivery callback and record it there, when the client
/// reports the acknowledgement, as every other producer harness does (Python, Rust, C, Java). The latency is
/// stamped at the ack, so it never includes the time a recorder took to reach the record. A failed record
/// counts as completed but adds no latency or throughput. When the send loop ends the engine flushes the
/// producer and waits up to 30 s for the remaining callbacks, then checks the counts, prints the summary and
/// writes the results file. Both paths share the accounting, the checks and the summary, so they report
/// identically.
/// </remarks>
public static class ProducerBenchmark
{
    // How long the engine waits, after the flush, for the remaining delivery callbacks (Python: 30 s).
    private static readonly TimeSpan s_drainTimeout = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_drainPollInterval = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Runs the sync producer benchmark, a port of Python's <c>main</c>. Warmup sends are waited inline and
    /// never recorded. Each measured send registers a delivery callback that records the record. If the run
    /// was not interrupted, the loop's end is followed by a flush and a wait for the remaining callbacks.
    /// </summary>
    public static ProducerBenchmarkResult RunSync(
        IProducerBackend backend,
        ProducerBenchmarkConfig config,
        Metrics metrics,
        PerfMessage[] messages,
        CancellationToken cancellationToken)
    {
        long warmupSent = 0;

        // Warmup — each send waited inline, then a 0.1 s sleep; never recorded (Python).
        if (config.WarmupSeconds > 0)
        {
            Console.WriteLine($"Warming up for {config.WarmupSeconds} seconds ...");
            long warmupEnd = Stopwatch.GetTimestamp() + SecondsToTicks(config.WarmupSeconds);
            int i = 0;
            while (Stopwatch.GetTimestamp() < warmupEnd && !cancellationToken.IsCancellationRequested)
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

        var deliveries = new DeliveryStats(metrics, config, countQueueFull: false);
        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        while (ContinueSending(config, messagesSent, cancellationToken))
        {
            PerfMessage message = messages[messagesSent % messages.Length];
            long startMs = Metrics.NowMs();
            try
            {
                backend.Send(config.TopicName, message.Key, message.Value, deliveries, startMs);
            }
            catch (Exception ex)
            {
                // A send the client never accepted is not counted, and no callback will come for it (Python's
                // loop swallows the RuntimeError such a send raises; the Rust harness logs it).
                ReportSendError(ex, cancellationToken);
                if (DurationExceeded(config, firstTicks))
                {
                    break;
                }

                continue;
            }

            messagesSent++;
            SleepSeconds(PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks)), cancellationToken);
            if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
            {
                Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                break;
            }
        }

        // Python's main flushes and waits for the callbacks only when it was not interrupted.
        if (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                backend.Flush(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Interrupted during the flush: report what has completed.
            }
            catch (Exception e)
            {
                Console.WriteLine($"Producer flush failed: {e.Message}");
            }

            long drainEnd = Stopwatch.GetTimestamp() + (long)(s_drainTimeout.TotalSeconds * Stopwatch.Frequency);
            while (deliveries.Settled < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
            {
                Thread.Sleep(s_drainPollInterval);
            }
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        return Finish(deliveries, metrics, config, warmupSent, messagesSent, firstTicks, afterTicks, afterMs, cancellationToken);
    }

    /// <summary>
    /// Runs the <b>async (pipelined)</b> producer benchmark (Python <c>async_main</c>): each measured send
    /// registers a delivery callback that records the record, and the loop waits only for the client to
    /// accept a record (or not even that, with <c>AWAIT_ACCEPTED=False</c>) before sending the next.
    /// </summary>
    /// <remarks>
    /// <b>Cancellation is handled here, not propagated.</b> Two awaits are wired to
    /// <paramref name="cancellationToken"/> and throw on Ctrl-C: the warmup delay and the pacing delay. Both
    /// are caught, so the caller's tail (cooldown, Final CPU / RSS, "Done", the optional verification, the
    /// exit code) still runs, mirroring Python's <c>except CancelledError</c> in <c>async_main</c>. Like
    /// Python's <c>async_main</c>, and unlike the sync path, the flush and the wait for the remaining
    /// callbacks run even when the run was interrupted.
    /// </remarks>
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
            while (Stopwatch.GetTimestamp() < warmupEnd && !cancellationToken.IsCancellationRequested)
            {
                PerfMessage message = messages[i % messages.Length];
                try
                {
                    // Both stages: the accepted stage, then the delivery it yields.
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

                try
                {
                    await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C during warmup: leave the warmup loop, do NOT propagate (see the
                    // cancellation note on this method).
                    break;
                }

                i++;
            }
        }

        var deliveries = new DeliveryStats(metrics, config, countQueueFull: true);
        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        metrics.SetMeasurementStart(beforeMs);
        if (!config.AwaitAccepted)
        {
            Console.WriteLine("Not awaiting send acceptance (AWAIT_ACCEPTED=False)");
        }

        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        try
        {
            while (ContinueSending(config, messagesSent, cancellationToken))
            {
                PerfMessage message = messages[messagesSent % messages.Length];
                long startMs = Metrics.NowMs();
                try
                {
                    ValueTask accepted = backend.Send(config.TopicName, message.Key, message.Value, deliveries, startMs);
                    if (config.AwaitAccepted)
                    {
                        // Awaits ACCEPTANCE only (where the client applies its backpressure, as Java's send()
                        // blocks); the delivery is reported to the callback.
                        await accepted.ConfigureAwait(false);
                    }
                    else if (!accepted.IsCompletedSuccessfully)
                    {
                        // AWAIT_ACCEPTED=False: send the next record at once. This one is counted as sent
                        // below, so if its acceptance later fails it is reported as a failed delivery.
                        _ = ReportFailedAcceptance(accepted, deliveries, startMs);
                    }
                }
                catch (Exception ex)
                {
                    // A send the client never accepted is not counted, and no callback will come for it. The
                    // backend takes no token, so this is never the run's own cancellation.
                    ReportSendError(ex, cancellationToken);
                    if (DurationExceeded(config, firstTicks))
                    {
                        break;
                    }

                    continue;
                }

                messagesSent++;
                double wait = PacingWaitSeconds(config, messagesSent, ElapsedSeconds(firstTicks));
                if (wait > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), cancellationToken).ConfigureAwait(false);
                }

                if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
                {
                    Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C during a pacing delay. Python's async_main catches CancelledError and still reports;
            // falling through to the flush and the summary below does the same.
            Console.WriteLine("Main cancelled");
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
        while (deliveries.Settled < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
        {
            await Task.Delay(s_drainPollInterval).ConfigureAwait(false);
        }

        // QUEUE_FULL deliveries were never sent, so they are subtracted from the measured sent count.
        long queueFull = deliveries.QueueFull;
        long measuredSent = messagesSent - queueFull;
        if (queueFull > 0)
        {
            Console.WriteLine($"QUEUE_FULL errors: {queueFull} (subtracted from sent; measured_sent={measuredSent})");
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        return Finish(deliveries, metrics, config, warmupSent, measuredSent, firstTicks, afterTicks, afterMs, cancellationToken);
    }

    /// <summary>
    /// Seconds the sender sleeps after its <paramref name="messagesSent"/>th measured send, or a value
    /// <c>&lt;= 0</c> for none (Python <c>pacing_wait_s</c>). At every
    /// <see cref="ProducerBenchmarkConfig.PacingCheckpoint"/>th send it is the time left until
    /// <c>messagesSent / LIMIT_RPS</c> seconds after the first send; there is no wait between checkpoints,
    /// nor without a rate limit.
    /// </summary>
    /// <param name="config">The run's config; only its rate limit is read.</param>
    /// <param name="messagesSent">Measured sends so far, the current one included.</param>
    /// <param name="elapsedSeconds">Seconds since the first measured send.</param>
    public static double PacingWaitSeconds(ProducerBenchmarkConfig config, long messagesSent, double elapsedSeconds)
    {
        if (config.LimitRps is not int rps || messagesSent % config.PacingCheckpoint != 0)
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

    // A send counted as sent whose acceptance stage later failed: no callback will come for it, so it is
    // reported as a failed delivery, which keeps the drain's count converging.
    private static async Task ReportFailedAcceptance(ValueTask accepted, IPerfDeliveryCallback deliveries, long startMs)
    {
        try
        {
            await accepted.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            deliveries.OnCompletion(startMs, default, ex);
        }
    }

    private static bool Verify(PerfRecordMetadata meta, ProducerBenchmarkConfig config)
    {
        if (!config.DoVerify)
        {
            return true;
        }

        // Python's verify_message (v2/ckd) requires timestamp > 0; verify_record_metadata (v3) requires
        // timestamp >= 0. Preserved rather than smoothed to one shared bound.
        bool timestampOk = config.ClientVersion == "2" ? meta.Timestamp > 0 : meta.Timestamp >= 0;
        return meta.Offset >= 0 && meta.Partition >= 0 && meta.Topic == config.TopicName && timestampOk;
    }

    private static ProducerBenchmarkResult Finish(
        DeliveryStats deliveries,
        Metrics metrics,
        ProducerBenchmarkConfig config,
        long warmupSent,
        long measuredSent,
        long firstTicks,
        long afterTicks,
        long afterMs,
        CancellationToken token)
    {
        // One consistent view: a callback can still arrive after the drain gave up.
        DeliverySnapshot snapshot = deliveries.Snapshot();
        bool countsFailed = CheckCompletion(snapshot, config, token);
        (bool budgetExceeded, bool resultsWritten) = PrintSummary(snapshot, metrics, config, firstTicks, afterTicks, afterMs);
        return new ProducerBenchmarkResult(warmupSent, measuredSent, budgetExceeded, warmupFailed: false, runFailed: countsFailed || !resultsWritten);
    }

    // Python check_completion: unless interrupted, every completed record must have verified and, with a
    // message target, every requested record must have completed. The summary is printed either way.
    private static bool CheckCompletion(DeliverySnapshot snapshot, ProducerBenchmarkConfig config, CancellationToken token)
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

        if (config.NumMessages > 0 && snapshot.Completed != config.NumMessages)
        {
            Console.WriteLine($"Completed messages {snapshot.Completed} does not match produced messages {config.NumMessages}");
            failed = true;
        }

        return failed;
    }

    private static (bool BudgetExceeded, bool ResultsWritten) PrintSummary(
        DeliverySnapshot snapshot,
        Metrics metrics,
        ProducerBenchmarkConfig config,
        long firstTicks,
        long afterTicks,
        long afterMs)
    {
        metrics.SetMeasurementEnd(afterMs);
        double totalTimeMs = TicksToSeconds(afterTicks - firstTicks) * 1000.0;
        double totalTimeS = totalTimeMs / 1000.0;
        long completed = snapshot.Completed;
        double messageRate = totalTimeS > 0 ? completed / totalTimeS : 0.0;
        ExternalMetricsAggregations agg = metrics.ExternalMetricsAggregations();

        Console.WriteLine($"End time: {afterMs} ms");
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

        double mibRate = totalTimeS > 0 ? (completed * (double)config.MessageSize) / (1024.0 * 1024.0) / totalTimeS : 0.0;
        double averageLatency = completed > 0 ? (double)snapshot.TotalLatencyMs / completed : 0.0;
        Console.WriteLine($"Average time: {(completed > 0 ? totalTimeMs / completed : 0.0):F2} ms");
        Console.WriteLine($"Average rate msg/s: {messageRate:F2} msg/s");
        Console.WriteLine($"Average rate MiB/s: {mibRate:F2} MiB/s");
        Console.WriteLine($"Average latency: {averageLatency:F2} ms");
        Console.WriteLine($"Max latency: {(double)snapshot.MaxLatencyMs:F2} ms");

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

        // The machine-readable summary: the same file name, keys and latency_ms shape as every other producer
        // harness (and the consumer harness's latency_ms); "client" names the implementation that wrote it.
        long min = 0;
        for (int ms = 0; ms < hist.Length; ms++)
        {
            if (hist[ms] != 0)
            {
                min = ms;
                break;
            }
        }

        bool external = agg.TotalExternalMetrics > 0;
        string client = (config.ClientVersion == "2" ? "dotnet-librdkafka" : "dotnet-rust") + (config.Async ? "-async" : string.Empty);
        var json = new StringBuilder(512);
        json.Append("{\n");
        json.Append("  \"test\": \"producer\",\n");
        json.Append("  \"client\": ").Append(PerfFormat.JsonString(client)).Append(",\n");
        json.Append("  \"topic\": ").Append(PerfFormat.JsonString(config.TopicName)).Append(",\n");
        json.Append("  \"messages_measured\": ").Append(PerfFormat.Num(completed)).Append(",\n");
        json.Append("  \"duration_s\": ").Append(PerfFormat.JsonFloat(totalTimeS)).Append(",\n");
        json.Append("  \"throughput_msg_s\": ").Append(PerfFormat.JsonFloat(messageRate)).Append(",\n");
        json.Append("  \"throughput_mib_s\": ").Append(PerfFormat.JsonFloat(mibRate)).Append(",\n");
        json.Append("  \"latency_ms\": {\n");
        json.Append("    \"min\": ").Append(PerfFormat.Num(min)).Append(",\n");
        json.Append("    \"avg\": ").Append(PerfFormat.JsonFloat(averageLatency)).Append(",\n");
        json.Append("    \"p50\": ").Append(PerfFormat.Num(p50)).Append(",\n");
        json.Append("    \"p90\": ").Append(PerfFormat.Num(p90)).Append(",\n");
        json.Append("    \"p95\": ").Append(PerfFormat.Num(p95)).Append(",\n");
        json.Append("    \"p99\": ").Append(PerfFormat.Num(p99)).Append(",\n");
        json.Append("    \"p999\": ").Append(PerfFormat.Num(p999)).Append(",\n");
        json.Append("    \"max\": ").Append(PerfFormat.Num(snapshot.MaxLatencyMs)).Append('\n');
        json.Append("  },\n");
        json.Append("  \"cpu_avg_pct\": ").Append(PerfFormat.JsonFloat(external ? agg.AverageCpu : 0.0)).Append(",\n");
        json.Append("  \"rss_avg_kib\": ").Append(PerfFormat.JsonFloat(external ? agg.AverageRss / 1024.0 : 0.0)).Append('\n');
        json.Append('}');

        bool resultsWritten = TryWriteResults(config.ResultsFile, json.ToString());

        if (config.P99LimitMs > 0 && p99 > config.P99LimitMs)
        {
            Console.WriteLine($"p99 latency {p99} ms exceeds budget {config.P99LimitMs} ms");
            return (true, resultsWritten);
        }

        return (false, resultsWritten);
    }

    // The results file is the run's primary artifact, so failing to write it fails the run rather than letting
    // it exit 0 without one (Python).
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

    private static bool DurationExceeded(ProducerBenchmarkConfig config, long firstTicks)
    {
        // exceeded_seconds: 10 when bounded by a message count, 1 otherwise (Python).
        int exceededSeconds = config.NumMessages > 0 ? 10 : 1;
        return ElapsedSeconds(firstTicks) > config.TestDurationSeconds + exceededSeconds;
    }

    private static double ElapsedSeconds(long firstTicks) => TicksToSeconds(Stopwatch.GetTimestamp() - firstTicks);

    private static double TicksToSeconds(long ticks) => ticks / (double)Stopwatch.Frequency;

    private static long SecondsToTicks(int seconds) => (long)seconds * Stopwatch.Frequency;

    private static string Inv(double value) => value.ToString(CultureInfo.InvariantCulture);

    private static bool IsQueueFull(Exception ex)
    {
        string s = ex.Message.ToLowerInvariant();
        return s.Contains("queue_full") || s.Contains("queue full");
    }

    private static void TryWriteLine(string line)
    {
        try
        {
            Console.WriteLine(line);
        }
        catch (Exception)
        {
            // A delivery callback must not throw; a console that cannot be written to loses the line.
        }
    }

    /// <summary>The counters at one instant, copied out of <see cref="DeliveryStats"/> under its lock.</summary>
    private readonly struct DeliverySnapshot
    {
        internal DeliverySnapshot(long[] latencyHist, long verified, long completed, long maxLatencyMs, long totalLatencyMs)
        {
            LatencyHist = latencyHist;
            Verified = verified;
            Completed = completed;
            MaxLatencyMs = maxLatencyMs;
            TotalLatencyMs = totalLatencyMs;
        }

        internal long[] LatencyHist { get; }

        internal long Verified { get; }

        internal long Completed { get; }

        internal long MaxLatencyMs { get; }

        internal long TotalLatencyMs { get; }
    }

    /// <summary>
    /// The measured interval's accounting, fed by the delivery callbacks (Python's <c>record_delivery</c> and
    /// <c>record_delivery_async</c>). The callbacks run on the client's own threads, and for some clients on
    /// more than one at once, so every counter is updated under one lock.
    /// </summary>
    private sealed class DeliveryStats : IPerfDeliveryCallback
    {
        private readonly object _lock = new object();
        private readonly Metrics _metrics;
        private readonly ProducerBenchmarkConfig _config;
        private readonly bool _countQueueFull;
        private readonly long[] _latencyHist = LatencyHistogram.New();
        private long _verified;
        private long _completed;
        private long _queueFull;
        private long _maxLatencyMs;
        private long _totalLatencyMs;

        // countQueueFull: whether a QUEUE_FULL failure is kept out of the completed count (async only, as in
        // Python, where the sync path counts every failure as completed).
        internal DeliveryStats(Metrics metrics, ProducerBenchmarkConfig config, bool countQueueFull)
        {
            _metrics = metrics;
            _config = config;
            _countQueueFull = countQueueFull;
        }

        /// <summary>Records reported so far, completed or QUEUE_FULL: what the drain waits on.</summary>
        internal long Settled
        {
            get
            {
                lock (_lock)
                {
                    return _completed + _queueFull;
                }
            }
        }

        internal long QueueFull
        {
            get
            {
                lock (_lock)
                {
                    return _queueFull;
                }
            }
        }

        public void OnCompletion(long sendStartMs, PerfRecordMetadata metadata, Exception? exception)
        {
            // Stamped first, so the latency is the time to the ack and not to this record's turn at the lock.
            long latencyMs = Metrics.NowMs() - sendStartMs;
            string? line = null;
            lock (_lock)
            {
                try
                {
                    if (exception is not null)
                    {
                        // QUEUE_FULL is the one outcome kept out of the completed count: the record was never
                        // sent, so it is subtracted from the sent count instead. Not logged, since it can occur
                        // tens of thousands of times.
                        if (_countQueueFull && IsQueueFull(exception))
                        {
                            _queueFull++;
                            return;
                        }

                        // A failed record counts as completed (below) but adds no latency or throughput.
                        line = $"Produce call resulted in exception: {exception.Message}";
                    }
                    else
                    {
                        if (Verify(metadata, _config))
                        {
                            _verified++;
                        }

                        _metrics.AddLatency(latencyMs);
                        LatencyHistogram.Record(_latencyHist, latencyMs);
                        _metrics.AddMessages(1);
                        _metrics.AddBytes(_config.MessageSize);
                        _maxLatencyMs = Math.Max(_maxLatencyMs, latencyMs);
                        _totalLatencyMs += latencyMs;
                    }

                    // Counted last and even if the accounting above throws, so every record settles exactly once
                    // and the drain never waits out its deadline for a record that was reported.
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
                return new DeliverySnapshot((long[])_latencyHist.Clone(), _verified, _completed, _maxLatencyMs, _totalLatencyMs);
            }
        }
    }
}
