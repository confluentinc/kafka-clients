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
/// <c>AWAIT_ACCEPTED</c>: whether <c>ProducerBenchmark.RunAsync</c>'s send loop waits for a send's first
/// stage (the client accepting the record) before the next send. Each mode has a test that fails under the
/// other: by default every send finds the previous acceptance complete; with <c>False</c> the loop keeps
/// sending while acceptances are still pending, and would hang here if it awaited them.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ProducerAcceptanceModeTests
{
    private const string Topic = "acceptance-topic";

    // A queue-full failure message. The recorder recognizes QUEUE_FULL by the message text alone
    // (ProducerBenchmark.IsQueueFull), so the exception type does not matter here.
    private const string QueueFullMessage = "Local: Queue full";

    private static readonly TimeSpan s_returnBudget = TimeSpan.FromSeconds(30);

    [Fact]
    public Task RunAsync_ByDefault_AwaitsAcceptanceBeforeTheNextSend() =>
        DefaultAwaitsAcceptanceAsync();

    [Fact]
    public Task RunAsync_NotAwaitingAcceptance_KeepsSendingWhileAcceptanceIsPending() =>
        NotAwaitingKeepsSendingAsync();

    [Fact]
    public Task RunAsync_NotAwaitingAcceptance_CountsBothQueueFullShapesAsQueueFull() =>
        NotAwaitingCountsQueueFullAsync();

    [Theory]
    [InlineData(null, true)]
    [InlineData("True", true)]
    [InlineData("False", false)]
    public void FromEnv_ParsesAwaitAccepted(string? value, bool expected)
    {
        Apply(awaitAccepted: value);

        Assert.Equal(expected, ProducerBenchmarkConfig.FromEnv().AwaitAccepted);
    }

    [Theory]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("Falase")]
    public void FromEnv_RejectsAMisspeltAwaitAccepted(string value)
    {
        Apply(awaitAccepted: value);

        ArgumentException ex = Assert.Throws<ArgumentException>(() => ProducerBenchmarkConfig.FromEnv());
        Assert.Equal($"AWAIT_ACCEPTED must be True or False, not '{value}'", ex.Message);
    }

    // ---- cases -------------------------------------------------------------------------------------

    private static async Task DefaultAwaitsAcceptanceAsync()
    {
        Apply(awaitAccepted: null);
        using var cts = new CancellationTokenSource();
        var acceptances = new List<Task>();
        bool previousAcceptedAtEverySend = true;
        var backend = new StagedProducerBackend(n =>
        {
            if (n > 1 && !acceptances[n - 2].IsCompleted)
            {
                previousAcceptedAtEverySend = false;
            }

            if (n == 5)
            {
                cts.Cancel();
            }

            Task<Task<PerfRecordMetadata>> acceptance = AcceptAfterAsync(TimeSpan.FromMilliseconds(50), Delivered(n));
            acceptances.Add(acceptance);
            return new ValueTask<Task<PerfRecordMetadata>>(acceptance);
        });

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.True(previousAcceptedAtEverySend, "a send started before the previous send was accepted");
        Assert.Equal(5, backend.SendCount);

        // The 5th send cancels the run, so its channel write throws and only the first four are counted.
        Assert.Equal(4, result.MeasuredSent);
    }

    private static async Task NotAwaitingKeepsSendingAsync()
    {
        Apply(awaitAccepted: "False");
        using var cts = new CancellationTokenSource();
        var acceptances = new List<TaskCompletionSource<Task<PerfRecordMetadata>>>();
        int pendingAtFifthSend = -1;
        var backend = new StagedProducerBackend(n =>
        {
            var acceptance = new TaskCompletionSource<Task<PerfRecordMetadata>>(TaskCreationOptions.RunContinuationsAsynchronously);
            acceptances.Add(acceptance);
            if (n == 5)
            {
                pendingAtFifthSend = 0;
                for (int i = 0; i < 4; i++)
                {
                    if (!acceptances[i].Task.IsCompleted)
                    {
                        pendingAtFifthSend++;
                    }
                }

                // Stop the run, then let every record be accepted and delivered so the recorder drains.
                cts.Cancel();
                for (int i = 0; i < acceptances.Count; i++)
                {
                    acceptances[i].TrySetResult(Delivered(i + 1));
                }
            }

            return new ValueTask<Task<PerfRecordMetadata>>(acceptance.Task);
        });

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(4, pendingAtFifthSend);
        Assert.Equal(5, backend.SendCount);
        Assert.Equal(4, result.MeasuredSent);
    }

    private static async Task NotAwaitingCountsQueueFullAsync()
    {
        // A queue-full failure reaches the loop in one of two shapes: the call itself throws, or a send
        // whose acceptance is still pending fails later. Both must be counted as QUEUE_FULL and subtracted.
        Apply(awaitAccepted: "False");
        using var cts = new CancellationTokenSource();
        TaskCompletionSource<Task<PerfRecordMetadata>>? second = null;
        var backend = new StagedProducerBackend(n =>
        {
            switch (n)
            {
                case 2:
                    // Still pending when the loop moves on; fails at the next send.
                    second = new TaskCompletionSource<Task<PerfRecordMetadata>>(TaskCreationOptions.RunContinuationsAsynchronously);
                    return new ValueTask<Task<PerfRecordMetadata>>(second.Task);
                case 3:
                    second!.TrySetException(new InvalidOperationException(QueueFullMessage));
                    break;
                case 4:
                    throw new InvalidOperationException(QueueFullMessage);
                case 6:
                    cts.Cancel();
                    break;
            }

            return new ValueTask<Task<PerfRecordMetadata>>(Delivered(n));
        });

        ProducerBenchmarkResult result = await RunAsync(backend, cts.Token).ConfigureAwait(false);

        // Sends 1-5 reach the channel (the 6th cancels the run); sends 2 and 4 are QUEUE_FULL.
        Assert.Equal(6, backend.SendCount);
        Assert.Equal(3, result.MeasuredSent);
    }

    // ---- harness -----------------------------------------------------------------------------------

    private static void Apply(string? awaitAccepted)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["TOPIC_NAME"] = Topic,
            ["ASYNC"] = "True",
            ["KEY_SIZE"] = "0",
            ["VALUE_SIZE"] = "16",
            ["WARMUP_SECONDS"] = "0",
            ["TEST_DURATION_SECONDS"] = "600",
            ["NUM_MESSAGES"] = "0",
            ["DO_VERIFY"] = "False",
        };
        if (awaitAccepted is not null)
        {
            values["AWAIT_ACCEPTED"] = awaitAccepted;
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
            $"ProducerBenchmark.RunAsync did not return within {s_returnBudget.TotalSeconds} s.");

        return await run.ConfigureAwait(false);
    }

    private static Task<PerfRecordMetadata> Delivered(int n) =>
        Task.FromResult(new PerfRecordMetadata(Topic, 0, n, 1));

    private static async Task<Task<PerfRecordMetadata>> AcceptAfterAsync(TimeSpan delay, Task<PerfRecordMetadata> delivery)
    {
        await Task.Delay(delay).ConfigureAwait(false);
        return delivery;
    }

    /// <summary>A producer backend whose two send stages are scripted per send by the test.</summary>
    private sealed class StagedProducerBackend : IAsyncProducerBackend
    {
        private readonly Func<int, ValueTask<Task<PerfRecordMetadata>>> _send;
        private int _sendCount;

        internal StagedProducerBackend(Func<int, ValueTask<Task<PerfRecordMetadata>>> send)
        {
            _send = send;
        }

        internal int SendCount => Volatile.Read(ref _sendCount);

        public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value) =>
            _send(Interlocked.Increment(ref _sendCount));

        public Task Close() => Task.CompletedTask;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }
}
