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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Guards <c>ProducerBenchmark.RunAsync</c>'s interrupt handling by cancelling its token directly: the warmup runs to
/// its end whatever the token, as Python's ignores its termination flag, and a cancellation at the pacing delay ends
/// the loop; either way the run reaches the drain and the report, not escaping <c>RunAsync</c>.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ProducerCancellationTests
{
    private const string SmokeTopic = "cancellation-topic";
    private static readonly TimeSpan s_returnBudget = TimeSpan.FromSeconds(30);

    [Fact]
    public Task RunAsync_WithPreCancelledToken_RunsTheWarmupThenSendsNothing() =>
        PreCancelledTokenRunsTheWarmupAsync();

    [Fact]
    public Task RunAsync_CancelledDuringWarmup_RunsTheWarmupToItsEnd() =>
        CancelledDuringWarmupRunsToItsEndAsync();

    [Fact]
    public Task RunAsync_CancelledMidRun_CountsTheAcceptedSends() =>
        CancelledMidRunCountsAcceptedSendsAsync();

    [Fact]
    public Task RunAsync_CancelledDuringAPacingWait_FlushesAndReports() =>
        CancelledDuringPacingFlushesAndReportsAsync();

    private static async Task PreCancelledTokenRunsTheWarmupAsync()
    {
        // Each warmup send waits 0.1 s after it, so a 1 s warmup makes about ten sends.
        Apply(warmupSeconds: 1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var backend = new ScriptedProducerBackend();

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.InRange(result.WarmupSent, 2, 15);
        Assert.Equal(0, result.MeasuredSent);
        Assert.False(result.WarmupFailed);
        Assert.Equal(result.WarmupSent, backend.SendCount);
    }

    private static async Task CancelledDuringWarmupRunsToItsEndAsync()
    {
        // Cancelled on the first warmup send; the warmup still runs its whole second.
        Apply(warmupSeconds: 1);
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 1);

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.InRange(result.WarmupSent, 2, 15);
        Assert.Equal(0, result.MeasuredSent);
        Assert.False(result.WarmupFailed);
        Assert.Equal(result.WarmupSent, backend.SendCount);
    }

    private static async Task CancelledMidRunCountsAcceptedSendsAsync()
    {
        // Cancelled in the measured loop, unpaced; the 5th send was accepted first, so it counts, as in Python.
        Apply(warmupSeconds: 0);
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 5);

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(5, backend.SendCount);
        Assert.Equal(5, result.MeasuredSent);
        Assert.False(result.WarmupFailed);

        Assert.False(result.RunFailed);
    }

    private static async Task CancelledDuringPacingFlushesAndReportsAsync()
    {
        // LIMIT_RPS=1 makes every send a checkpoint with a 1 s wait, so the interrupt lands on the pacing delay;
        // s_returnBudget turns a regression into a failure, not a hung suite.
        Apply(warmupSeconds: 0, limitRps: "1");
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 1);

        (ProducerBenchmarkResult result, string output) = await RunAsyncCapturingOutput(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(1, backend.SendCount);
        Assert.Equal(1, result.MeasuredSent);
        Assert.Equal(1, backend.FlushCount);
        Assert.False(result.RunFailed);
        Assert.Contains("p99 latency: ", output, StringComparison.Ordinal);
        Assert.DoesNotContain("Main cancelled", output, StringComparison.Ordinal);
    }

    private static void Apply(int warmupSeconds, string? limitRps = null)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["TOPIC_NAME"] = SmokeTopic,
            ["ASYNC"] = "True",
            ["KEY_SIZE"] = "0",
            ["VALUE_SIZE"] = "16",
            ["WARMUP_SECONDS"] = warmupSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["TEST_DURATION_SECONDS"] = "600",
            ["NUM_MESSAGES"] = "0",
            ["DO_VERIFY"] = "False",
        };
        if (limitRps is not null)
        {
            values["LIMIT_RPS"] = limitRps;
        }

        PerfEngineFixture.Apply(values);
    }

    private static async Task<ProducerBenchmarkResult> RunAsync(IAsyncProducerBackend backend, CancellationToken token)
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();
        PerfMessage[] messages = MessageGenerator.Generate(config.KeySize, config.ValueSize, count: 4);
        using var metrics = new Metrics();

        Task<ProducerBenchmarkResult> run = ProducerBenchmark.RunAsync(backend, config, metrics, messages, token);
        Task finished = await Task.WhenAny(run, Task.Delay(s_returnBudget)).ConfigureAwait(false);
        Assert.True(
            ReferenceEquals(finished, run),
            $"ProducerBenchmark.RunAsync did not return within {s_returnBudget.TotalSeconds} s after cancellation.");

        return await run.ConfigureAwait(false);
    }

    private static async Task<(ProducerBenchmarkResult Result, string Output)> RunAsyncCapturingOutput(
        IAsyncProducerBackend backend,
        CancellationToken token)
    {
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        TextWriter original = Console.Out;
        Console.SetOut(TextWriter.Synchronized(output));
        try
        {
            ProducerBenchmarkResult result = await RunAsync(backend, token).ConfigureAwait(false);
            return (result, output.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    /// <summary>
    /// A producer backend whose sends are accepted and delivered immediately, and which fires the supplied
    /// <see cref="CancellationTokenSource"/> on its Nth send — so the cancellation lands at a chosen,
    /// deterministic point in RunAsync rather than racing the loop.
    /// </summary>
    private sealed class ScriptedProducerBackend : IAsyncProducerBackend
    {
        private readonly CancellationTokenSource? _cts;
        private readonly int _cancelOnSend;
        private int _sendCount;

        internal ScriptedProducerBackend()
            : this(null, 0)
        {
        }

        internal ScriptedProducerBackend(CancellationTokenSource? cts, int cancelOnSend)
        {
            _cts = cts;
            _cancelOnSend = cancelOnSend;
        }

        private int _flushCount;

        internal int SendCount => Volatile.Read(ref _sendCount);

        internal int FlushCount => Volatile.Read(ref _flushCount);

        public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value) =>
            new ValueTask<Task<PerfRecordMetadata>>(Task.FromResult(Next(topic)));

        public ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
        {
            callback.OnCompletion(sendStartMs, Next(topic), null);
            return default;
        }

        public Task Flush(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _flushCount);
            return Task.CompletedTask;
        }

        private PerfRecordMetadata Next(string topic)
        {
            int n = Interlocked.Increment(ref _sendCount);
            if (_cts is not null && _cancelOnSend > 0 && n == _cancelOnSend)
            {
                _cts.Cancel();
            }

            return new PerfRecordMetadata(topic, 0, n, 1);
        }

        public Task Close() => Task.CompletedTask;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }
}
