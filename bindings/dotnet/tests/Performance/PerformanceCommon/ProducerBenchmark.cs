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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>The producer benchmark outcome the per-client exe needs for cooldown / VERIFY_CONSUMED / exit code.</summary>
public sealed class ProducerBenchmarkResult
{
    internal ProducerBenchmarkResult(long warmupSent, long measuredSent, bool latencyBudgetExceeded, bool warmupFailed)
    {
        WarmupSent = warmupSent;
        MeasuredSent = measuredSent;
        LatencyBudgetExceeded = latencyBudgetExceeded;
        WarmupFailed = warmupFailed;
    }

    /// <summary>Records sent (and awaited) during warmup — excluded from the measurement, counted for VERIFY_CONSUMED.</summary>
    public long WarmupSent { get; }

    /// <summary>Records sent during the measured interval (Python <c>measured_sent</c>).</summary>
    public long MeasuredSent { get; }

    /// <summary>Whether the measured p99 exceeded <c>P99_LIMIT_MS</c> (the run must exit non-zero).</summary>
    public bool LatencyBudgetExceeded { get; }

    /// <summary>Whether warmup aborted on a verification error (no measured interval ran).</summary>
    public bool WarmupFailed { get; }
}

/// <summary>
/// The client-agnostic producer measured-loop engine — the C# analog of <c>producer_performance_test.py</c>'s
/// <c>main</c> (sync) and <c>async_main</c> (async), driven through <see cref="IProducerBackend"/> /
/// <see cref="IAsyncProducerBackend"/> so it runs identically over any client.
/// </summary>
/// <remarks>
/// <b>The key .NET deviation (PLAN §5.1 / D5).</b> The sync path is a <b>serial blocking</b> measurement
/// (<c>startMs = now; meta = Send(record); latencyMs = now - startMs; record</c>) — there is no future to
/// pipeline, so no bounded queue and no recorder task (and never a fake <c>Task.Run</c> pipeline, which
/// would measure threadpool overhead). The async path <b>is</b> pipelined: the send loop pushes each
/// <see cref="Task"/> onto a bounded <see cref="Channel"/> (a blocking write is the backpressure) and a
/// separate recorder task awaits each in order. Both share the verify + metrics + summary code so the two
/// paths report identically.
/// </remarks>
public static class ProducerBenchmark
{
    /// <summary>
    /// Runs the <b>sync (serial-blocking)</b> producer benchmark (Python <c>main</c>). Warmup sends are
    /// awaited inline and never recorded; the measured loop blocks on each send and records it in place.
    /// </summary>
    public static ProducerBenchmarkResult RunSync(
        IProducerBackend backend,
        ProducerBenchmarkConfig config,
        Metrics metrics,
        PerfMessage[] messages,
        CancellationToken cancellationToken)
    {
        var stats = new RunStats(config);
        long warmupSent = 0;

        // Warmup — inline, never fed to the recorder/histogram (PLAN §5.1).
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
                    PerfRecordMetadata meta = backend.Send(config.TopicName, message.Key, message.Value);
                    Verify(meta, config);
                    warmupSent++;
                }
                catch (Exception)
                {
                    Console.WriteLine("Warmup failed due to message verification error");
                    return new ProducerBenchmarkResult(warmupSent, 0, false, warmupFailed: true);
                }

                Thread.Sleep(100);
                i++;
            }
        }

        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        long nextCheckTicks = firstTicks + Stopwatch.Frequency;
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        while (ContinueSending(config, messagesSent, cancellationToken))
        {
            PerfMessage message = messages[messagesSent % messages.Length];
            long startMs = Metrics.NowMs();
            PerfRecordMetadata meta = backend.Send(config.TopicName, message.Key, message.Value);
            long latencyMs = Metrics.NowMs() - startMs;
            RecordCompleted(stats, metrics, config, meta, latencyMs);
            messagesSent++;

            ApplyRateLimit(config, messagesSent, ref nextCheckTicks, cancellationToken);
            if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
            {
                Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                break;
            }
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        bool budgetExceeded = FinishAndSummarize(stats, metrics, config, messagesSent, firstTicks, afterTicks, afterMs, cancellationToken);
        return new ProducerBenchmarkResult(warmupSent, messagesSent, budgetExceeded, warmupFailed: false);
    }

    /// <summary>
    /// Runs the <b>async (pipelined)</b> producer benchmark (Python <c>async_main</c>): the send loop pushes
    /// each delivery <see cref="Task"/> onto a bounded channel (backpressure) and a recorder task awaits them.
    /// </summary>
    public static async Task<ProducerBenchmarkResult> RunAsync(
        IAsyncProducerBackend backend,
        ProducerBenchmarkConfig config,
        Metrics metrics,
        PerfMessage[] messages,
        CancellationToken cancellationToken)
    {
        var stats = new RunStats(config);
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
                    PerfRecordMetadata meta = await backend.Send(config.TopicName, message.Key, message.Value).ConfigureAwait(false);
                    Verify(meta, config);
                    warmupSent++;
                }
                catch (Exception)
                {
                    Console.WriteLine("Warmup failed due to message verification error");
                    return new ProducerBenchmarkResult(warmupSent, 0, false, warmupFailed: true);
                }

                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                i++;
            }
        }

        // max 2 GiB of in-flight messages in the queue (bounded → backpressure), matching Python.
        int capacity = (int)Math.Min(int.MaxValue, (2L * 1024 * 1024 * 1024) / Math.Max(1, config.MessageSize));
        var channel = Channel.CreateBounded<(Task<PerfRecordMetadata> Task, long StartMs)>(
            new BoundedChannelOptions(capacity) { SingleReader = true, SingleWriter = true });

        long queueFull = 0;
        Task recorderTask = Task.Run(async () =>
        {
            await foreach ((Task<PerfRecordMetadata> task, long startMs) in channel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                PerfRecordMetadata meta;
                try
                {
                    meta = await task.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    continue;
                }
                catch (Exception ex)
                {
                    if (IsQueueFull(ex))
                    {
                        Interlocked.Increment(ref queueFull);
                    }
                    else
                    {
                        Console.WriteLine($"Produce call resulted in exception: {ex.Message}");
                    }

                    continue;
                }

                long latencyMs = Metrics.NowMs() - startMs;
                RecordCompleted(stats, metrics, config, meta, latencyMs);
            }
        });

        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        long nextCheckTicks = firstTicks + Stopwatch.Frequency;
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        while (ContinueSending(config, messagesSent, cancellationToken))
        {
            PerfMessage message = messages[messagesSent % messages.Length];
            long startMs = Metrics.NowMs();
            Task<PerfRecordMetadata> task = backend.Send(config.TopicName, message.Key, message.Value);
            await channel.Writer.WriteAsync((task, startMs), cancellationToken).ConfigureAwait(false);
            messagesSent++;

            await ApplyRateLimitAsync(config, messagesSent, () => nextCheckTicks, v => nextCheckTicks = v, cancellationToken).ConfigureAwait(false);
            if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
            {
                Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                break;
            }
        }

        channel.Writer.Complete();
        await recorderTask.ConfigureAwait(false);

        long measuredSent = messagesSent - Interlocked.Read(ref queueFull);
        if (queueFull > 0)
        {
            Console.WriteLine($"QUEUE_FULL errors: {queueFull} (subtracted from sent; measured_sent={measuredSent})");
        }

        long afterMs = Metrics.NowMs();
        long afterTicks = Stopwatch.GetTimestamp();
        bool budgetExceeded = FinishAndSummarize(stats, metrics, config, messagesSent, firstTicks, afterTicks, afterMs, cancellationToken);
        return new ProducerBenchmarkResult(warmupSent, measuredSent, budgetExceeded, warmupFailed: false);
    }

    private static bool ContinueSending(ProducerBenchmarkConfig config, long messagesSent, CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return false;
        }

        return config.NumMessages <= 0 || messagesSent < config.NumMessages;
    }

    private static void RecordCompleted(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, PerfRecordMetadata meta, long latencyMs)
    {
        if (Verify(meta, config))
        {
            stats.Verified++;
        }

        stats.Completed++;
        metrics.AddLatency(latencyMs);
        LatencyHistogram.Record(stats.LatencyHist, latencyMs);
        metrics.AddMessages(1);
        metrics.AddBytes(config.MessageSize);
        if (latencyMs > stats.MaxLatencyMs)
        {
            stats.MaxLatencyMs = latencyMs;
        }

        stats.TotalLatencyMs += latencyMs;
    }

    private static bool Verify(PerfRecordMetadata meta, ProducerBenchmarkConfig config)
    {
        if (!config.DoVerify)
        {
            return true;
        }

        return meta.Offset >= 0 && meta.Partition >= 0 && meta.Topic == config.TopicName && meta.Timestamp >= 0;
    }

    private static bool FinishAndSummarize(
        RunStats stats,
        Metrics metrics,
        ProducerBenchmarkConfig config,
        long messagesSent,
        long firstTicks,
        long afterTicks,
        long afterMs,
        CancellationToken token)
    {
        bool terminating = token.IsCancellationRequested;
        if (stats.Verified != stats.Completed)
        {
            if (!terminating)
            {
                Console.WriteLine($"Verified messages {stats.Verified} does not match completed messages {stats.Completed}");
            }

            return false;
        }

        if (config.NumMessages > 0 && stats.Completed != config.NumMessages)
        {
            if (!terminating)
            {
                Console.WriteLine($"Completed messages {stats.Completed} does not match produced messages {config.NumMessages}");
            }

            return false;
        }

        return PrintSummary(stats, metrics, config, firstTicks, afterTicks, afterMs);
    }

    private static bool PrintSummary(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, long firstTicks, long afterTicks, long afterMs)
    {
        metrics.SetMeasurementEnd(afterMs);
        double totalTimeMs = TicksToSeconds(afterTicks - firstTicks) * 1000.0;
        double totalTimeS = totalTimeMs / 1000.0;
        double messageRate = totalTimeS > 0 ? stats.Completed / totalTimeS : 0.0;
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

        Console.WriteLine($"Average time: {totalTimeMs / stats.Completed:F2} ms");
        Console.WriteLine($"Average rate msg/s: {messageRate:F2} msg/s");
        Console.WriteLine($"Average rate MiB/s: {(stats.Completed * (double)config.MessageSize) / (1024.0 * 1024.0) / totalTimeS:F2} MiB/s");
        Console.WriteLine($"Average latency: {(double)stats.TotalLatencyMs / stats.Completed:F2} ms");
        Console.WriteLine($"Max latency: {(double)stats.MaxLatencyMs:F2} ms");

        long p50 = LatencyHistogram.PercentileFromHist(stats.LatencyHist, 0.50);
        long p90 = LatencyHistogram.PercentileFromHist(stats.LatencyHist, 0.90);
        long p99 = LatencyHistogram.PercentileFromHist(stats.LatencyHist, 0.99);
        long p999 = LatencyHistogram.PercentileFromHist(stats.LatencyHist, 0.999);
        Console.WriteLine($"p50 latency: {p50} ms");
        Console.WriteLine($"p90 latency: {p90} ms");
        Console.WriteLine($"p99 latency: {p99} ms");
        Console.WriteLine($"p999 latency: {p999} ms");

        if (config.P99LimitMs > 0 && p99 > config.P99LimitMs)
        {
            Console.WriteLine($"p99 latency {p99} ms exceeds budget {config.P99LimitMs} ms");
            return true;
        }

        return false;
    }

    private static void ApplyRateLimit(ProducerBenchmarkConfig config, long messagesSent, ref long nextCheckTicks, CancellationToken token)
    {
        if (config.LimitRps is not int rps || rps <= 0 || messagesSent % rps != 0)
        {
            return;
        }

        long now = Stopwatch.GetTimestamp();
        if (now < nextCheckTicks)
        {
            double waitSeconds = (nextCheckTicks - now) / (double)Stopwatch.Frequency;
            SleepSeconds(waitSeconds, token);
        }

        nextCheckTicks += Stopwatch.Frequency;
    }

    private static async Task ApplyRateLimitAsync(ProducerBenchmarkConfig config, long messagesSent, Func<long> getNext, Action<long> setNext, CancellationToken token)
    {
        if (config.LimitRps is not int rps || rps <= 0 || messagesSent % rps != 0)
        {
            return;
        }

        long nextCheckTicks = getNext();
        long now = Stopwatch.GetTimestamp();
        if (now < nextCheckTicks)
        {
            double waitSeconds = (nextCheckTicks - now) / (double)Stopwatch.Frequency;
            if (waitSeconds > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(waitSeconds), token).ConfigureAwait(false);
            }
        }

        setNext(nextCheckTicks + Stopwatch.Frequency);
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

    private sealed class RunStats
    {
        internal RunStats(ProducerBenchmarkConfig config)
        {
            _ = config;
            LatencyHist = LatencyHistogram.New();
        }

        internal long[] LatencyHist { get; }

        internal long Verified { get; set; }

        internal long Completed { get; set; }

        internal long MaxLatencyMs { get; set; }

        internal long TotalLatencyMs { get; set; }
    }
}
