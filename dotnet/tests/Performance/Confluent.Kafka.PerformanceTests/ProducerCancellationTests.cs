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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Regression guards for <c>ProducerBenchmark.RunAsync</c>'s interrupt handling (M13/P3 item D). Before
/// the fix, a cancellation raised at one of the method's token-wired awaits escaped RunAsync and then Main,
/// skipping the drain and the caller's whole reporting tail. Two awaits are token-wired now: the warmup
/// delay and the rate limiter's pacing delay.
/// These drive the CancellationToken directly, which is what makes them a usable guard at all: exercising
/// a real OS signal would need a subprocess-plus-kill test, and PerfSignals' handlers
/// (<c>Console.CancelKeyPress</c> plus a <c>PosixSignalRegistration</c> pair for SIGINT/SIGTERM) are
/// themselves untested by that shape — driving the token directly is the mock-testable substitute.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ProducerCancellationTests
{
    private const string SmokeTopic = "cancellation-topic";
    private static readonly TimeSpan s_returnBudget = TimeSpan.FromSeconds(30);

    [Fact]
    public Task RunAsync_WithPreCancelledToken_ReturnsWithoutThrowing() =>
        PreCancelledTokenReturnsAsync();

    [Fact]
    public Task RunAsync_CancelledDuringWarmup_ReturnsWithoutThrowing() =>
        CancelledDuringWarmupReturnsAsync();

    [Fact]
    public Task RunAsync_CancelledMidRun_CountsTheAcceptedSends() =>
        CancelledMidRunCountsAcceptedSendsAsync();

    [Fact]
    public Task RunAsync_CancelledDuringAPacingWait_ReturnsWithoutThrowing() =>
        CancelledDuringPacingReturnsAsync();

    // ---- cases -------------------------------------------------------------------------------------

    private static async Task PreCancelledTokenReturnsAsync()
    {
        // A token already cancelled on entry: both loop guards refuse the first iteration, so nothing is
        // sent and nothing throws. Documents the contract rather than the bug (this path was already
        // safe) — the two cases below are the ones that used to escape.
        Apply(warmupSeconds: 5);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var backend = new ScriptedProducerBackend();

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(0, result.WarmupSent);
        Assert.Equal(0, result.MeasuredSent);
        Assert.False(result.WarmupFailed);
        Assert.Equal(0, backend.SendCount);
    }

    private static async Task CancelledDuringWarmupReturnsAsync()
    {
        // Cancelled inside warmup, so the interrupt lands on `await Task.Delay(100, cancellationToken)`.
        // That is the FIRST guarded region; before the fix this threw straight out of RunAsync.
        Apply(warmupSeconds: 30);
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 1);

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(1, result.WarmupSent);
        Assert.Equal(0, result.MeasuredSent);
        Assert.False(result.WarmupFailed);
        Assert.Equal(1, backend.SendCount);
    }

    private static async Task CancelledMidRunCountsAcceptedSendsAsync()
    {
        // Cancelled during the measured loop, with no rate limit: the loop sees the token before its next
        // send. The 5th send was accepted before that, so it is counted, as Python's async_main counts it.
        Apply(warmupSeconds: 0);
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 5);

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(5, backend.SendCount);
        Assert.Equal(5, result.MeasuredSent);
        Assert.False(result.WarmupFailed);

        // Interrupted, so the count checks are skipped: the run does not fail.
        Assert.False(result.RunFailed);
    }

    private static async Task CancelledDuringPacingReturnsAsync()
    {
        // LIMIT_RPS=1 makes every send a checkpoint and asks for a 1 s wait after the first, so the interrupt
        // lands on `await Task.Delay(wait, cancellationToken)`. That is the SECOND guarded region, and the
        // important one: falling through must still flush, wait for the callbacks and report what was
        // measured. s_returnBudget turns a regression into a failure instead of a hung suite.
        Apply(warmupSeconds: 0, limitRps: "1");
        using var cts = new CancellationTokenSource();
        var backend = new ScriptedProducerBackend(cts, cancelOnSend: 1);

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(1, backend.SendCount);
        Assert.Equal(1, result.MeasuredSent);
        Assert.Equal(1, backend.FlushCount);
        Assert.False(result.RunFailed);
    }

    // ---- harness -----------------------------------------------------------------------------------

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
