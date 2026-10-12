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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
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
/// Both paths pipeline, as Python's do. The sync path sends on the calling thread and puts each send's
/// <see cref="PerfSendHandle"/> on a bounded <see cref="BlockingCollection{T}"/> (a blocking <c>Add</c> is the
/// backpressure); a recorder thread waits the handles in send order. The async path pushes each delivery
/// <see cref="Task"/> onto a bounded <see cref="Channel"/> (a blocking write is the backpressure), and a
/// recorder task awaits them in order. Both share the verify + metrics + summary code so the two paths report
/// identically.
/// </remarks>
public static class ProducerBenchmark
{
    // The getter of a send that threw before the client accepted it: Get() rethrows the original failure.
    private static readonly Func<object, PerfRecordMetadata> s_rethrowSendFailure = static state =>
    {
        ((ExceptionDispatchInfo)state).Throw();
        return default;
    };

    /// <summary>
    /// Runs the sync producer benchmark, a port of Python's <c>main</c>. Warmup sends are waited inline and
    /// never recorded. In the measured loop each send returns a <see cref="PerfSendHandle"/>, which goes onto a
    /// queue bounded at 2 GiB of messages; a recorder thread waits the handles in send order and records each
    /// one, a failed one included (Python's <c>record_completed_calls</c>). When the loop ends, the recorder
    /// drains the queue and is joined before the summary.
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

        // Warmup — each send waited inline, then a 0.1 s sleep; never fed to the recorder/histogram (Python).
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
                    return new ProducerBenchmarkResult(warmupSent, 0, false, warmupFailed: true);
                }

                Thread.Sleep(100);
                i++;
            }
        }

        // max 2 GiB of in-flight messages in the queue (bounded → backpressure), matching Python.
        int capacity = (int)Math.Min(int.MaxValue, (2L * 1024 * 1024 * 1024) / Math.Max(1, config.MessageSize));
        using var produceCalls = new BlockingCollection<(PerfSendHandle Handle, long StartMs)>(capacity);
        var recorder = new Thread(() =>
        {
            // Python waits each queued call in order with get(timeout=1) until main clears its loop flag, then
            // drains the rest with get_nowait(). CompleteAdding below is that flag; the consuming enumerable
            // drains the queue and ends once it is empty, with no idle get(timeout=1) at the end.
            foreach ((PerfSendHandle handle, long startMs) in produceCalls.GetConsumingEnumerable())
            {
                RecordCompletedCall(stats, metrics, config, handle, startMs);
            }
        })
        {
            IsBackground = true,
            Name = "perf-producer-recorder",
        };
        recorder.Start();

        long beforeMs = Metrics.NowMs();
        long firstTicks = Stopwatch.GetTimestamp();
        long nextCheckTicks = firstTicks + RateLimitSliceTicks(config);
        metrics.SetMeasurementStart(beforeMs);
        Console.WriteLine($"Starting measured interval at {beforeMs} ms: {DateTime.UtcNow:o}");

        long messagesSent = 0;
        try
        {
            while (ContinueSending(config, messagesSent, cancellationToken))
            {
                PerfMessage message = messages[messagesSent % messages.Length];
                long startMs = Metrics.NowMs();
                PerfSendHandle handle;
                try
                {
                    handle = backend.Send(config.TopicName, message.Key, message.Value);
                }
                catch (Exception ex)
                {
                    // A send that fails before the client accepts it (Java's send() throwing): queue its
                    // failure so the recorder counts it, in order, like a failed delivery — as RunAsync does.
                    // Python's loop only swallows the RuntimeError a send raises once its signal handler has
                    // closed the producer; .NET's signal handling cancels the token instead.
                    handle = new PerfSendHandle(s_rethrowSendFailure, ExceptionDispatchInfo.Capture(ex));
                }

                try
                {
                    produceCalls.Add((handle, startMs), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C while the queue is full: stop sending, as Python's loop does on `terminating`.
                    // The handle in hand is not counted, like RunAsync's canceled channel write.
                    break;
                }

                messagesSent++;

                ApplyRateLimit(config, messagesSent, ref nextCheckTicks, cancellationToken);
                if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
                {
                    Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                    break;
                }
            }
        }
        finally
        {
            // Python: t, record_completed_calls_loop = record_completed_calls_loop, None; t.join()
            produceCalls.CompleteAdding();
            recorder.Join();
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
    /// <remarks>
    /// <b>Cancellation is handled here, not propagated.</b> Three awaits in this method are wired to
    /// <paramref name="cancellationToken"/> and throw on Ctrl-C: the warmup delay, the channel write, and
    /// the rate-limit delay. Letting that escape skipped everything after the send loop — the in-flight
    /// queue was never closed out, the recorder task was left waiting forever, and the caller's whole tail
    /// (cooldown, Final CPU / RSS, "Done", the optional verification, the exit code) never ran, so the one
    /// thing Ctrl-C is for — "stop, but tell me what you measured" — was the one thing it did not do. Both
    /// guarded regions therefore catch <see cref="OperationCanceledException"/> and fall through to
    /// teardown, mirroring Python's <c>except CancelledError</c> in <c>async_main</c>. The SYNC sibling
    /// has one token-wired wait, its queue's <c>Add</c>, which it catches before falling through to its own
    /// drain and join; its <c>SleepSeconds</c> uses <c>token.WaitHandle.WaitOne</c>, which returns rather
    /// than throwing.
    /// </remarks>
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
                    // Both stages: the accepted stage, then the delivery it yields.
                    Task<PerfRecordMetadata> delivery = await backend.Send(config.TopicName, message.Key, message.Value).ConfigureAwait(false);
                    PerfRecordMetadata meta = await delivery.ConfigureAwait(false);
                    Verify(meta, config);
                    warmupSent++;
                }
                catch (Exception)
                {
                    Console.WriteLine("Warmup failed due to message verification error");
                    return new ProducerBenchmarkResult(warmupSent, 0, false, warmupFailed: true);
                }

                try
                {
                    await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C during warmup: leave the warmup loop, do NOT propagate (see the
                    // cancellation note on this method). The channel and recorder task do not exist
                    // yet, so this region needs its own guard.
                    break;
                }

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
        long nextCheckTicks = firstTicks + RateLimitSliceTicks(config);
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
                Task<PerfRecordMetadata> task;
                try
                {
                    ValueTask<Task<PerfRecordMetadata>> send = backend.Send(config.TopicName, message.Key, message.Value);
                    if (config.AwaitAccepted)
                    {
                        // Awaits ACCEPTANCE only (the first stage, where the client applies its
                        // backpressure, as Java's send() blocks); the delivery is awaited by the recorder.
                        task = await send.ConfigureAwait(false);
                    }
                    else
                    {
                        // AWAIT_ACCEPTED=False: do not wait for acceptance. An accepted send is unwrapped in
                        // place; a pending or failed one becomes a single task that completes with the
                        // delivery, or fails with the acceptance failure, which the recorder counts the same
                        // way as a failure thrown by the call itself (the catch below).
                        task = send.IsCompletedSuccessfully ? send.Result : send.AsTask().Unwrap();
                    }
                }
                catch (Exception ex)
                {
                    // A send that fails before it is accepted (Java's send() throwing): hand the failure to
                    // the recorder so it is counted exactly like a failed delivery. The backend takes no
                    // token, so this is never the run's own cancellation.
                    task = Task.FromException<PerfRecordMetadata>(ex);
                }

                await channel.Writer.WriteAsync((task, startMs), cancellationToken).ConfigureAwait(false);
                messagesSent++;

                await ApplyRateLimitAsync(config, messagesSent, () => nextCheckTicks, v => nextCheckTicks = v, cancellationToken).ConfigureAwait(false);
                if (messagesSent % 10000 == 0 && DurationExceeded(config, firstTicks))
                {
                    Console.WriteLine($"Test duration reached, {ElapsedSeconds(firstTicks):F2} seconds. Interrupting...\n");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C mid-run. Python's async_main catches CancelledError at exactly this point
            // (producer_performance_test.py:958-961), cleans up its recorder task, prints
            // "Main cancelled" and RETURNS normally so the module-level tail — cooldown, Final CPU,
            // Final RSS, "Done", the optional consumed-message verification, the exit code — still
            // runs. Falling through here does the same: the writer is completed and the recorder
            // awaited just below. Completing the writer is the .NET analogue of Python's
            // record_task.cancel() and is strictly better, since the recorder drains what is already
            // queued instead of discarding it. ApplyRateLimitAsync's Task.Delay sits inside this
            // region and needs no separate guard.
            Console.WriteLine("Main cancelled");
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

    // Python record_completed_calls: wait the call and verify it, then count it as completed with its latency.
    // A failed call is logged and counted but left unverified, and the run continues.
    private static void RecordCompletedCall(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, PerfSendHandle handle, long startMs)
    {
        PerfRecordMetadata meta;
        try
        {
            meta = handle.Get();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Produce call resulted in exception: {ex.Message}");
            RecordFailed(stats, metrics, config, Metrics.NowMs() - startMs);
            return;
        }

        RecordCompleted(stats, metrics, config, meta, Metrics.NowMs() - startMs);
    }

    private static void RecordCompleted(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, PerfRecordMetadata meta, long latencyMs)
    {
        if (Verify(meta, config))
        {
            stats.Verified++;
        }

        RecordCompletion(stats, metrics, config, latencyMs);
    }

    // A produce call that threw still counts as completed with recorded latency, but is NOT verified —
    // mirroring producer_performance_test.py:632-648 (record_completed_calls increments completed and
    // records latency even when produce_call.result() raises).
    private static void RecordFailed(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, long latencyMs)
    {
        RecordCompletion(stats, metrics, config, latencyMs);
    }

    private static void RecordCompletion(RunStats stats, Metrics metrics, ProducerBenchmarkConfig config, long latencyMs)
    {
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

        // Python's verify_message (v2/ckd) requires timestamp > 0; verify_record_metadata (v3) requires
        // timestamp >= 0. Preserved rather than smoothed to one shared bound (producer_performance_test.py
        // :234 vs :248).
        bool timestampOk = config.ClientVersion == "2" ? meta.Timestamp > 0 : meta.Timestamp >= 0;
        return meta.Offset >= 0 && meta.Partition >= 0 && meta.Topic == config.TopicName && timestampOk;
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

        if (stats.Completed == 0)
        {
            Console.WriteLine("No messages completed; skipping summary.");
            return false;
        }

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

    // The limiter is a quota per pacing slice (Python: check the clock every limit_rps messages, i.e. a
    // one-second slice): send LimitRpsSliceMessages at full speed, then sleep until the slice's scheduled
    // end. The schedule is absolute (nextCheckTicks advances by a fixed amount whether or not we slept), so
    // an oversleep or a stall is caught up over the following slices and the long-run rate is LIMIT_RPS.
    // LIMIT_RPS_SLICE_MS shrinks the slice so the offered load is smooth enough to measure below saturation.
    private static void ApplyRateLimit(ProducerBenchmarkConfig config, long messagesSent, ref long nextCheckTicks, CancellationToken token)
    {
        if (config.LimitRps is not int rps || rps <= 0 || messagesSent % config.LimitRpsSliceMessages != 0)
        {
            return;
        }

        long now = Stopwatch.GetTimestamp();
        if (now < nextCheckTicks)
        {
            double waitSeconds = (nextCheckTicks - now) / (double)Stopwatch.Frequency;
            SleepSeconds(waitSeconds, token);
        }

        nextCheckTicks += RateLimitSliceTicks(config);
    }

    private static async Task ApplyRateLimitAsync(ProducerBenchmarkConfig config, long messagesSent, Func<long> getNext, Action<long> setNext, CancellationToken token)
    {
        if (config.LimitRps is not int rps || rps <= 0 || messagesSent % config.LimitRpsSliceMessages != 0)
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

        setNext(nextCheckTicks + RateLimitSliceTicks(config));
    }

    /// <summary>
    /// Stopwatch ticks one pacing slice is scheduled to take: <c>LimitRpsSliceMessages / LIMIT_RPS</c>
    /// seconds, computed from the rounded slice count so the long-run rate is exactly <c>LIMIT_RPS</c>.
    /// One second when the slice is the default (the original per-second quota) or no limit is set.
    /// </summary>
    private static long RateLimitSliceTicks(ProducerBenchmarkConfig config) =>
        config.LimitRps is int rps && rps > 0
            ? Stopwatch.Frequency * config.LimitRpsSliceMessages / rps
            : Stopwatch.Frequency;

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
