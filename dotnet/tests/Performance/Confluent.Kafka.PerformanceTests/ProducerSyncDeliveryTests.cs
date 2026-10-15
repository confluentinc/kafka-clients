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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// <c>ProducerBenchmark.RunSync</c>, the port of Python's <c>main()</c>: each measured send registers a
/// delivery callback that records the record when the client reports it (Python's <c>record_delivery</c>). A
/// failed record counts as completed with no latency; a send that throws is not counted at all. After the loop
/// the engine flushes and waits for the remaining callbacks, checks the counts, prints the summary and writes
/// <c>results.json</c>. Warmup sends are waited inline. The fake backend scripts each send, so every case is
/// deterministic.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class ProducerSyncDeliveryTests
{
    private const string Topic = "sync-delivery-topic";

    // Every wait here is bounded.
    private static readonly TimeSpan s_returnBudget = TimeSpan.FromSeconds(30);

    private static readonly Func<object, PerfRecordMetadata> s_invoke = static state => ((Func<PerfRecordMetadata>)state)();

    private static readonly string[] s_resultKeys =
    {
        "test", "client", "topic", "messages_measured", "duration_s", "throughput_msg_s", "throughput_mib_s",
        "latency_ms", "cpu_avg_pct", "rss_avg_kib",
    };

    private static readonly string[] s_latencyKeys = { "min", "avg", "p50", "p90", "p95", "p99", "p999", "max" };

    [Fact]
    public Task RunSync_WaitsForLateCallbacks_AndStampsLatencyInTheCallback() => LateCallbacksAsync();

    [Fact]
    public Task RunSync_FailedDelivery_IsCompleted_AndFailsTheVerifyCheck() => FailedDeliveryAsync();

    [Fact]
    public Task RunSync_SendThatThrows_IsNotCounted() => SendThatThrowsAsync();

    [Fact]
    public Task RunSync_Warmup_WaitsEachSendInline() => WarmupAsync();

    [Fact]
    public Task RunSync_WarmupFailure_FailsTheRun() => WarmupFailureAsync();

    [Theory]
    [InlineData("3", "dotnet-rust")]
    [InlineData("2", "dotnet-librdkafka")]
    public Task RunSync_WritesResultsJson_WithTheSharedKeys(string clientVersion, string client) =>
        ResultsJsonAsync(clientVersion, client);

    [Fact]
    public Task RunSync_NothingCompleted_StillWritesAValidResultsJson() => NothingCompletedAsync();

    [Fact]
    public Task RunSync_UnwritableResultsFile_FailsTheRun() => UnwritableResultsAsync();

    // ---- cases -------------------------------------------------------------------------------------

    private static async Task LateCallbacksAsync()
    {
        const int delayMs = 300;
        Apply(numMessages: 5);
        var pending = new ConcurrentQueue<(IPerfDeliveryCallback Callback, long StartMs, int N)>();
        var backend = new ScriptedProducerBackend(
            onSend: (n, callback, startMs) => pending.Enqueue((callback, startMs, n)),
            onFlush: () =>
            {
                // Nothing is delivered until the flush, and then only after a delay and in reverse order, so the
                // run reports every record only if it waits for the callbacks after the flush.
                var deliverer = new Thread(() =>
                {
                    Thread.Sleep(delayMs);
                    foreach ((IPerfDeliveryCallback callback, long startMs, int n) in pending.Reverse())
                    {
                        callback.OnCompletion(startMs, Delivered(n), null);
                    }
                })
                { IsBackground = true };
                deliverer.Start();
            });

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        Assert.Equal(1, backend.FlushCount);
        Assert.Equal(5, result.MeasuredSent);
        Assert.False(result.RunFailed, output);
        Assert.DoesNotContain("does not match", output, StringComparison.Ordinal);

        // Every record was reported, and its latency runs to the callback, not to the send's return.
        using JsonDocument results = ReadResults("results.json");
        Assert.Equal(5, results.RootElement.GetProperty("messages_measured").GetInt64());
        Assert.True(
            results.RootElement.GetProperty("latency_ms").GetProperty("min").GetInt64() >= delayMs - 50,
            $"the latency was not stamped in the callback: {results.RootElement}");
    }

    private static async Task FailedDeliveryAsync()
    {
        Apply(numMessages: 5, doVerify: true);
        var backend = new ScriptedProducerBackend(onSend: (n, callback, startMs) =>
        {
            if (n == 3)
            {
                callback.OnCompletion(startMs, default, new InvalidOperationException("delivery failed (fake)"));
            }
            else
            {
                callback.OnCompletion(startMs, Delivered(n), null);
            }
        });

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // The failed third record is logged and counted as completed, so the counts match NUM_MESSAGES, but it
        // did not verify: the run fails, and the summary is still printed.
        Assert.Equal(5, result.MeasuredSent);
        Assert.Equal(1, Occurrences(output, "Produce call resulted in exception: delivery failed (fake)"));
        Assert.Equal(1, Occurrences(output, "Verified messages 4 does not match completed messages 5"));
        Assert.DoesNotContain("Completed messages", output, StringComparison.Ordinal);
        Assert.True(result.RunFailed);
        Assert.Contains("p50 latency:", output, StringComparison.Ordinal);

        using JsonDocument results = ReadResults("results.json");
        Assert.Equal(5, results.RootElement.GetProperty("messages_measured").GetInt64());
    }

    private static async Task SendThatThrowsAsync()
    {
        Apply(numMessages: 5, doVerify: true);
        var backend = new ScriptedProducerBackend(onSend: (n, callback, startMs) =>
        {
            if (n == 3)
            {
                throw new InvalidOperationException("send rejected (fake)");
            }

            callback.OnCompletion(startMs, Delivered(n), null);
        });

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // A send the client never accepted is logged and not counted: the loop makes a sixth send to reach
        // NUM_MESSAGES, and the five it counted all completed and verified.
        Assert.Equal(6, backend.SendCount);
        Assert.Equal(5, result.MeasuredSent);
        Assert.Equal(1, Occurrences(output, "Send error: send rejected (fake)"));
        Assert.DoesNotContain("Produce call resulted in exception", output, StringComparison.Ordinal);
        Assert.DoesNotContain("does not match", output, StringComparison.Ordinal);
        Assert.False(result.RunFailed);
    }

    private static async Task WarmupAsync()
    {
        const int measured = 3;
        Apply(numMessages: measured, warmupSeconds: 1, doVerify: true);
        var events = new ConcurrentQueue<string>();
        var backend = new ScriptedProducerBackend(
            onSend: (n, callback, startMs) =>
            {
                events.Enqueue($"C{n}");
                callback.OnCompletion(startMs, Delivered(n), null);
            },
            onWarmupSend: n =>
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
        Assert.Equal((2 * warmup) + measured, log.Length);

        // Each warmup send is waited before the next one is made.
        for (int i = 1; i <= warmup; i++)
        {
            Assert.Equal($"S{i}", log[(2 * i) - 2]);
            Assert.Equal($"G{i}", log[(2 * i) - 1]);
        }

        // Then the measured sends, each with a delivery callback.
        Assert.Equal(Enumerable.Range(warmup + 1, measured).Select(n => $"C{n}").ToArray(), log.Skip(2 * warmup).ToArray());
        Assert.Equal(measured, result.MeasuredSent);
        Assert.Equal(warmup + measured, backend.SendCount);

        // The warmup sends are not recorded: the counts match NUM_MESSAGES.
        Assert.Equal(1, Occurrences(output, "Warming up for 1 seconds ..."));
        Assert.DoesNotContain("does not match", output, StringComparison.Ordinal);
        Assert.False(result.RunFailed);
        Assert.Contains("p50 latency:", output, StringComparison.Ordinal);
    }

    private static async Task WarmupFailureAsync()
    {
        Apply(numMessages: 3, warmupSeconds: 5);
        var backend = new ScriptedProducerBackend(
            onSend: (n, callback, startMs) => callback.OnCompletion(startMs, Delivered(n), null),
            onWarmupSend: n => Handle(() => throw new InvalidOperationException("warmup delivery failed (fake)")));

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        Assert.True(result.WarmupFailed);
        Assert.True(result.RunFailed);
        Assert.Equal(0, result.MeasuredSent);
        Assert.Equal(1, backend.SendCount);
        Assert.Equal(1, Occurrences(output, "Warmup failed due to message verification error"));
    }

    private static async Task ResultsJsonAsync(string clientVersion, string client)
    {
        const string path = "custom-results.json";
        Apply(numMessages: 4, clientVersion: clientVersion, resultsFile: path);
        File.Delete(path);
        var backend = new ScriptedProducerBackend(onSend: (n, callback, startMs) => callback.OnCompletion(startMs, Delivered(n), null));

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        Assert.False(result.RunFailed, output);
        Assert.Equal(1, Occurrences(output, $"Results summary written to: {path}"));

        // The same keys, in the same order, as every other producer harness's results.json.
        using JsonDocument results = ReadResults(path);
        JsonElement root = results.RootElement;
        Assert.Equal(s_resultKeys, root.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal(s_latencyKeys, root.GetProperty("latency_ms").EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal("producer", root.GetProperty("test").GetString());
        Assert.Equal(client, root.GetProperty("client").GetString());
        Assert.Equal(Topic, root.GetProperty("topic").GetString());
        Assert.Equal(4, root.GetProperty("messages_measured").GetInt64());
    }

    private static async Task NothingCompletedAsync()
    {
        // A run interrupted before its first send still writes a parseable results.json, with zeros rather than
        // a division by zero.
        Apply(numMessages: 5);
        var backend = new ScriptedProducerBackend(onSend: (n, callback, startMs) => callback.OnCompletion(startMs, Delivered(n), null));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend, cts.Token).ConfigureAwait(false);

        Assert.Equal(0, backend.SendCount);
        Assert.Equal(0, backend.FlushCount);
        Assert.Equal(0, result.MeasuredSent);

        // Interrupted, so the count checks are skipped and the run does not fail.
        Assert.False(result.RunFailed, output);
        Assert.Contains("Average latency: 0.00 ms", output, StringComparison.Ordinal);

        string text = File.ReadAllText("results.json");
        Assert.Contains("\"messages_measured\": 0,", text, StringComparison.Ordinal);
        Assert.Contains("\"throughput_msg_s\": 0.0,", text, StringComparison.Ordinal);
        Assert.Contains("\"avg\": 0.0,", text, StringComparison.Ordinal);
        Assert.Contains("\"max\": 0\n", text, StringComparison.Ordinal);
        using JsonDocument results = JsonDocument.Parse(text);
        Assert.Equal(s_resultKeys, results.RootElement.EnumerateObject().Select(p => p.Name).ToArray());
    }

    private static async Task UnwritableResultsAsync()
    {
        string path = Path.Combine("no-such-directory", "results.json");
        Apply(numMessages: 3, resultsFile: path);
        var backend = new ScriptedProducerBackend(onSend: (n, callback, startMs) => callback.OnCompletion(startMs, Delivered(n), null));

        (ProducerBenchmarkResult result, string output) = await RunSyncCapturingOutputAsync(backend).ConfigureAwait(false);

        // The results file is the run's artifact: failing to write it fails the run although every record
        // completed.
        Assert.Equal(3, result.MeasuredSent);
        Assert.Equal(1, Occurrences(output, $"Failed to write {path}: "));
        Assert.DoesNotContain("Results summary written to", output, StringComparison.Ordinal);
        Assert.True(result.RunFailed);
    }

    // ---- harness -----------------------------------------------------------------------------------

    private static void Apply(
        long numMessages,
        int warmupSeconds = 0,
        bool doVerify = false,
        string clientVersion = "3",
        string? resultsFile = null)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["TOPIC_NAME"] = Topic,
            ["ASYNC"] = "False",
            ["CLIENT_VERSION"] = clientVersion,
            ["KEY_SIZE"] = "0",
            ["VALUE_SIZE"] = "16",
            ["WARMUP_SECONDS"] = warmupSeconds.ToString(CultureInfo.InvariantCulture),
            ["TEST_DURATION_SECONDS"] = "600",
            ["NUM_MESSAGES"] = numMessages.ToString(CultureInfo.InvariantCulture),
            ["DO_VERIFY"] = doVerify ? "True" : "False",
        };
        if (resultsFile is not null)
        {
            values["RESULTS_FILE"] = resultsFile;
        }

        PerfEngineFixture.Apply(values);
    }

    private static async Task<(ProducerBenchmarkResult Result, string Output)> RunSyncCapturingOutputAsync(
        IProducerBackend backend,
        CancellationToken token = default)
    {
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        TextWriter original = Console.Out;
        Console.SetOut(TextWriter.Synchronized(output));
        try
        {
            using var metrics = new Metrics();
            ProducerBenchmarkResult result = await FinishWithinBudget(StartRunSync(backend, metrics, token)).ConfigureAwait(false);
            return (result, output.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    // RunSync blocks its caller for the whole run, so it gets a thread of its own and the test waits for it
    // within a budget.
    private static Task<ProducerBenchmarkResult> StartRunSync(IProducerBackend backend, Metrics metrics, CancellationToken token)
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();
        PerfMessage[] messages = MessageGenerator.Generate(config.KeySize, config.ValueSize, count: 4);
        return Task.Factory.StartNew(
            () => ProducerBenchmark.RunSync(backend, config, metrics, messages, token),
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

    private static JsonDocument ReadResults(string path) => JsonDocument.Parse(File.ReadAllText(path));

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

    /// <summary>
    /// A sync producer backend whose sends are scripted by the test: the measured send hands the test the
    /// engine's callback to report the delivery (inline or later, from any thread), and the warmup send
    /// returns a scripted handle. Both count toward <see cref="SendCount"/>, numbered in call order.
    /// </summary>
    private sealed class ScriptedProducerBackend : IProducerBackend
    {
        private readonly Action<int, IPerfDeliveryCallback, long> _onSend;
        private readonly Func<int, PerfSendHandle>? _onWarmupSend;
        private readonly Action? _onFlush;
        private int _sendCount;
        private int _flushCount;

        internal ScriptedProducerBackend(
            Action<int, IPerfDeliveryCallback, long> onSend,
            Func<int, PerfSendHandle>? onWarmupSend = null,
            Action? onFlush = null)
        {
            _onSend = onSend;
            _onWarmupSend = onWarmupSend;
            _onFlush = onFlush;
        }

        internal int SendCount => Volatile.Read(ref _sendCount);

        internal int FlushCount => Volatile.Read(ref _flushCount);

        public PerfSendHandle Send(string topic, byte[]? key, byte[]? value)
        {
            int n = Interlocked.Increment(ref _sendCount);
            return _onWarmupSend is null
                ? throw new InvalidOperationException("this case has no warmup")
                : _onWarmupSend(n);
        }

        public void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs) =>
            _onSend(Interlocked.Increment(ref _sendCount), callback, sendStartMs);

        public void Flush(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _flushCount);
            _onFlush?.Invoke();
        }

        public void Close()
        {
        }

        public void Dispose()
        {
        }
    }
}
