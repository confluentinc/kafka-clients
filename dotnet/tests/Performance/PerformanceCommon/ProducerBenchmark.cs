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
                // Never accepted: not counted, and no callback will come for it.
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

        // Only when not interrupted, as Python's main.
        if (!cancellationToken.IsCancellationRequested)
        {
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
            while (deliveries.Settled < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
            {
                Thread.Sleep(s_drainPollInterval);
            }
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        return Finish(deliveries, metrics, config, warmupSent, messagesSent, firstTicks, afterTicks, afterMs, cancellationToken);
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
                        // Waits for acceptance only, where the client applies its backpressure.
                        await accepted.ConfigureAwait(false);
                    }
                    else if (!accepted.IsCompletedSuccessfully)
                    {
                        // Counted as sent: a later acceptance failure is a failed delivery.
                        _ = ReportFailedAcceptance(accepted, deliveries, startMs);
                    }
                }
                catch (Exception ex)
                {
                    // Never accepted: not counted, and no callback will come for it.
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
        while (deliveries.Settled < messagesSent && Stopwatch.GetTimestamp() < drainEnd)
        {
            await Task.Delay(s_drainPollInterval).ConfigureAwait(false);
        }

        // QUEUE_FULL records were never sent, so they are subtracted from the sent count.
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

        // As Python: verify_message (v2) wants a timestamp > 0, verify_record_metadata (v3) >= 0.
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

    private static bool DurationExceeded(ProducerBenchmarkConfig config, long firstTicks)
    {
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
            // A delivery callback must not throw: a line the console cannot take is lost.
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
    /// <c>record_completion_async</c>); some clients report on several threads at once, so the counters share one
    /// lock.
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

        // Only the async path keeps QUEUE_FULL out of the completed count, as in Python.
        internal DeliveryStats(Metrics metrics, ProducerBenchmarkConfig config, bool countQueueFull)
        {
            _metrics = metrics;
            _config = config;
            _countQueueFull = countQueueFull;
        }

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
            // Stamped before the lock, so the latency is the time to the ack.
            long latencyMs = Metrics.NowMs() - sendStartMs;
            string? line = null;
            lock (_lock)
            {
                try
                {
                    if (exception is not null)
                    {
                        // Never sent, so subtracted from the sent count; too frequent to log.
                        if (_countQueueFull && IsQueueFull(exception))
                        {
                            _queueFull++;
                            return;
                        }

                        // A failed record counts as completed but adds no latency or throughput.
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
                return new DeliverySnapshot((long[])_latencyHist.Clone(), _verified, _completed, _maxLatencyMs, _totalLatencyMs);
            }
        }
    }
}
