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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// <c>ProducerBenchmark.RunSync</c>, the port of Python's <c>main()</c>: the send loop queues each send's
/// <see cref="PerfSendHandle"/> on a queue bounded at 2 GiB of messages, and a recorder thread waits the handles
/// in send order, counting a failed one and carrying on (Python's <c>record_completed_calls</c>). Warmup sends
/// are waited inline. The fake backend scripts each send's handle, so every case is deterministic.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ProducerSyncPipelineTests
{
    private const string Topic = "sync-pipeline-topic";

    // Every wait here is bounded: the run as a whole, and each gate a scripted Get() parks on.
    private static readonly TimeSpan s_returnBudget = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_gateBudget = TimeSpan.FromSeconds(10);

    // How long the send count must stay put for the send loop to count as blocked on the full queue.
    private static readonly TimeSpan s_settle = TimeSpan.FromMilliseconds(500);

    private static readonly Func<object, PerfRecordMetadata> s_invoke = static state => ((Func<PerfRecordMetadata>)state)();

    [Fact]
    public Task RunSync_RecordsCompletionsInSendOrder() => RecordsInSendOrderAsync();

    [Theory]
    [InlineData(0, 1073741824, 2)]
    [InlineData(536870912, 536870912, 2)] // the key counts toward the message size
    [InlineData(0, 715827883, 2)] // rounded down, as Python's //
    [InlineData(0, 536870912, 4)]
    public Task RunSync_QueueCapacity_IsTwoGiBOverMessageSize(int keySize, int valueSize, int expectedCapacity) =>
        QueueCapacityAsync(keySize, valueSize, expectedCapacity);

    [Fact]
    public Task RunSync_FailedGet_IsCountedAndTheRunContinues() => FailedGetAsync();

    [Fact]
    public Task RunSync_SendThatThrows_IsCountedLikeAFailedGet() => SendThatThrowsAsync();

    [Fact]
    public Task RunSync_Warmup_WaitsEachSendInline() => WarmupAsync();

    // ---- cases -------------------------------------------------------------------------------------

    private static async Task RecordsInSendOrderAsync()
    {
        Apply(numMessages: 5);
        using var fifthSent = new ManualResetEventSlim();
        var returned = new ConcurrentQueue<int>();
        bool firstGetTimedOut = false;
        var backend = new ScriptedProducerBackend(n =>
        {
            if (n == 5)
            {
                fifthSent.Set();
            }

            return Handle(() =>
            {
                // The first send is delivered only once the fifth is made, so the run completes in time only
                // if the loop keeps sending while the recorder waits on the first handle.
                if (n == 1 && !fifthSent.Wait(s_gateBudget))
                {
                    firstGetTimedOut = true;
                    throw new TimeoutException("the fifth send was never made");
                }

                returned.Enqueue(n);
                return Delivered(n);
            });
        });

        ProducerBenchmarkResult result = await RunSyncAsync(backend).ConfigureAwait(false);

        Assert.False(firstGetTimedOut, "the send loop stopped sending while the first send's handle was being waited");

        // The handles that were ready are waited only after the first one returns: in send order.
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, returned.ToArray());
        Assert.Equal(5, backend.SendCount);
        Assert.Equal(5, result.MeasuredSent);
    }

    private static async Task QueueCapacityAsync(int keySize, int valueSize, int expectedCapacity)
    {
        int numMessages = expectedCapacity + 6;
        Apply(numMessages: numMessages, keySize: keySize, valueSize: valueSize);
        var firstGetEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var returned = new ConcurrentQueue<int>();
        var backend = new ScriptedProducerBackend(n => Handle(() =>
        {
            if (n == 1)
            {
                firstGetEntered.TrySetResult(true);
                if (!release.Wait(s_gateBudget))
                {
                    throw new TimeoutException("the test never released the first send's handle");
                }
            }

            returned.Enqueue(n);
            return Delivered(n);
        }));

        using var metrics = new Metrics();
        Task<ProducerBenchmarkResult> run = StartRunSync(backend, metrics);

        // With the recorder parked on the first handle, the loop fills the queue and makes one more send, whose
        // handle it cannot queue: the first send, a full queue, and the one in hand.
        int blockedAt = expectedCapacity + 2;
        try
        {
            await WithinGateBudget(firstGetEntered.Task, "the recorder never waited the first send's handle").ConfigureAwait(false);
            await WaitUntil(
                () => backend.SendCount >= blockedAt,
                () => $"the send loop stopped at {backend.SendCount} sends, short of {blockedAt}: the queue holds fewer than {expectedCapacity}").ConfigureAwait(false);
            await Task.Delay(s_settle).ConfigureAwait(false);

            int settled = backend.SendCount;
            Assert.True(
                settled == blockedAt,
                $"the send loop reached {settled} sends while the recorder was parked, past {blockedAt}: the queue holds more than {expectedCapacity}");
        }
        finally
        {
            release.Set();
        }

        ProducerBenchmarkResult result = await FinishWithinBudget(run).ConfigureAwait(false);

        Assert.Equal(Enumerable.Range(1, numMessages).ToArray(), returned.ToArray());
        Assert.Equal(numMessages, backend.SendCount);
        Assert.Equal(numMessages, result.MeasuredSent);
    }

    private static async Task FailedGetAsync()
    {
        Apply(numMessages: 5, doVerify: true);
        var waited = new ConcurrentQueue<int>();
        var backend = new ScriptedProducerBackend(n => Handle(() =>
        {
            waited.Enqueue(n);
            return n == 3 ? throw new InvalidOperationException("delivery failed (fake)") : Delivered(n);
        }));

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // The failed third send is logged and counted, and the recorder goes on to wait the fourth and fifth.
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, waited.ToArray());
        Assert.Equal(5, result.MeasuredSent);
        Assert.Equal(1, Occurrences(output, "Produce call resulted in exception: delivery failed (fake)"));

        // Counted as completed but not verified, so the summary is withheld.
        Assert.Equal(1, Occurrences(output, "Verified messages 4 does not match completed messages 5"));
    }

    private static async Task SendThatThrowsAsync()
    {
        Apply(numMessages: 5, doVerify: true);
        var waited = new ConcurrentQueue<int>();
        var backend = new ScriptedProducerBackend(n =>
        {
            if (n == 3)
            {
                throw new InvalidOperationException("send rejected (fake)");
            }

            return Handle(() =>
            {
                waited.Enqueue(n);
                Console.WriteLine($"fake get {n}");
                return Delivered(n);
            });
        });

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // A send that throws before the client accepts the record is queued as a failed handle, and the loop
        // goes on sending.
        Assert.Equal(5, backend.SendCount);
        Assert.Equal(new[] { 1, 2, 4, 5 }, waited.ToArray());
        Assert.Equal(5, result.MeasuredSent);
        Assert.Equal(1, Occurrences(output, "Produce call resulted in exception: send rejected (fake)"));
        Assert.Equal(1, Occurrences(output, "Verified messages 4 does not match completed messages 5"));

        // The recorder reaches the failure in its send position: after the second send, before the fourth.
        int second = output.IndexOf("fake get 2", StringComparison.Ordinal);
        int failure = output.IndexOf("Produce call resulted in exception: send rejected (fake)", StringComparison.Ordinal);
        int fourth = output.IndexOf("fake get 4", StringComparison.Ordinal);
        Assert.True(second >= 0 && second < failure && failure < fourth, $"recorded out of send order: {output}");
    }

    private static async Task WarmupAsync()
    {
        const int measured = 3;
        Apply(numMessages: measured, warmupSeconds: 1, doVerify: true);
        var events = new ConcurrentQueue<string>();
        var backend = new ScriptedProducerBackend(n =>
        {
            events.Enqueue($"S{n}");
            return Handle(() =>
            {
                events.Enqueue($"G{n}");
                return Delivered(n);
            });
        });

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // Each warmup iteration sleeps 0.1 s after its send, so a 1 s warmup makes about ten sends. Without the
        // sleep the instant fake would make hundreds of thousands.
        int warmup = (int)result.WarmupSent;
        Assert.InRange(warmup, 1, 15);
        string[] log = events.ToArray();
        Assert.Equal(2 * (warmup + measured), log.Length);

        // Each warmup send is waited before the next one is made.
        for (int i = 1; i <= warmup; i++)
        {
            Assert.Equal($"S{i}", log[(2 * i) - 2]);
            Assert.Equal($"G{i}", log[(2 * i) - 1]);
        }

        // Then the measured sends, each waited once by the recorder, in send order.
        string[] measuredSends = Enumerable.Range(warmup + 1, measured).Select(n => $"S{n}").ToArray();
        string[] measuredGets = Enumerable.Range(warmup + 1, measured).Select(n => $"G{n}").ToArray();
        Assert.Equal(measuredSends, log.Skip(2 * warmup).Where(e => e[0] == 'S').ToArray());
        Assert.Equal(measuredGets, log.Skip(2 * warmup).Where(e => e[0] == 'G').ToArray());
        Assert.Equal(measured, result.MeasuredSent);
        Assert.Equal(warmup + measured, backend.SendCount);

        // The warmup sends are not recorded: the counts match NUM_MESSAGES, so the summary is printed.
        Assert.Equal(1, Occurrences(output, "Warming up for 1 seconds ..."));
        Assert.DoesNotContain("does not match", output, StringComparison.Ordinal);
        Assert.Contains("p50 latency:", output, StringComparison.Ordinal);
    }

    // ---- harness -----------------------------------------------------------------------------------

    private static void Apply(long numMessages, int warmupSeconds = 0, int keySize = 0, int valueSize = 16, bool doVerify = false)
    {
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["TOPIC_NAME"] = Topic,
            ["ASYNC"] = "False",
            ["KEY_SIZE"] = keySize.ToString(CultureInfo.InvariantCulture),
            ["VALUE_SIZE"] = valueSize.ToString(CultureInfo.InvariantCulture),
            ["WARMUP_SECONDS"] = warmupSeconds.ToString(CultureInfo.InvariantCulture),
            ["TEST_DURATION_SECONDS"] = "600",
            ["NUM_MESSAGES"] = numMessages.ToString(CultureInfo.InvariantCulture),
            ["DO_VERIFY"] = doVerify ? "True" : "False",
        });
    }

    private static async Task<(ProducerBenchmarkResult Result, string Output)> RunSyncCapturingOutputAsync(IProducerBackend backend)
    {
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        TextWriter original = Console.Out;
        Console.SetOut(output);
        try
        {
            ProducerBenchmarkResult result = await RunSyncAsync(backend).ConfigureAwait(false);
            return (result, output.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    private static async Task<ProducerBenchmarkResult> RunSyncAsync(IProducerBackend backend)
    {
        using var metrics = new Metrics();
        return await FinishWithinBudget(StartRunSync(backend, metrics)).ConfigureAwait(false);
    }

    // RunSync blocks its caller for the whole run, so it gets a thread of its own and the test waits for it
    // within a budget.
    private static Task<ProducerBenchmarkResult> StartRunSync(IProducerBackend backend, Metrics metrics)
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        // Small messages whatever KEY_SIZE and VALUE_SIZE say: the engine reads the configured sizes only for the
        // queue bound and the byte metrics, and the capacity cases set them to hundreds of megabytes.
        PerfMessage[] messages = MessageGenerator.Generate(keySize: 0, valueSize: 16, count: 4);
        return Task.Factory.StartNew(
            () => ProducerBenchmark.RunSync(backend, config, metrics, messages, CancellationToken.None),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private static async Task<ProducerBenchmarkResult> FinishWithinBudget(Task<ProducerBenchmarkResult> run)
    {
        Task finished = await Task.WhenAny(run, Task.Delay(s_returnBudget)).ConfigureAwait(false);
        Assert.True(
            ReferenceEquals(finished, run),
            $"ProducerBenchmark.RunSync did not return within {s_returnBudget.TotalSeconds} s.");

        return await run.ConfigureAwait(false);
    }

    private static async Task WithinGateBudget(Task task, string failure)
    {
        Task finished = await Task.WhenAny(task, Task.Delay(s_gateBudget)).ConfigureAwait(false);
        Assert.True(ReferenceEquals(finished, task), failure);
    }

    private static async Task WaitUntil(Func<bool> condition, Func<string> failure)
    {
        var waited = Stopwatch.StartNew();
        while (!condition())
        {
            if (waited.Elapsed >= s_gateBudget)
            {
                Assert.Fail(failure());
            }

            await Task.Delay(10).ConfigureAwait(false);
        }
    }

    private static int Occurrences(string text, string value)
    {
        int count = 0;
        for (int i = text.IndexOf(value, StringComparison.Ordinal); i >= 0; i = text.IndexOf(value, i + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static PerfSendHandle Handle(Func<PerfRecordMetadata> get) => new PerfSendHandle(s_invoke, get);

    private static PerfRecordMetadata Delivered(int n) => new PerfRecordMetadata(Topic, 0, n, 1);

    /// <summary>A sync producer backend whose handle for each send is scripted by the test.</summary>
    private sealed class ScriptedProducerBackend : IProducerBackend
    {
        private readonly Func<int, PerfSendHandle> _send;
        private int _sendCount;

        internal ScriptedProducerBackend(Func<int, PerfSendHandle> send)
        {
            _send = send;
        }

        internal int SendCount => Volatile.Read(ref _sendCount);

        public PerfSendHandle Send(string topic, byte[]? key, byte[]? value) => _send(Interlocked.Increment(ref _sendCount));

        public void Close()
        {
        }

        public void Dispose()
        {
        }
    }
}
