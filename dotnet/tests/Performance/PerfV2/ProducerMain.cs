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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// The <c>MODE=producer</c> entry for the v2 (ckd) baseline — the C# analog of
/// <c>producer_performance_test.py</c>'s <c>__main__</c> with <c>CLIENT_VERSION=2</c>: generate the cycled
/// messages, build the librdkafka-form producer config, start the metrics sampler, run the sync (serial)
/// or async (pipelined) engine over the ckd backend, cool down (GC + await), then return the exit code
/// (non-zero if the p99 budget was exceeded).
/// </summary>
/// <remarks>
/// The v2 baseline is manual-comparison only (not the p99 gate — D10). <c>VERIFY_CONSUMED</c> is a v3
/// correctness feature (PLAN deliverable #2/#3): it needs a consumer, and this exe references only ckd —
/// so when it is requested the run logs that it is unsupported on the v2 baseline and skips it, rather
/// than adding a second ckd client path.
/// </remarks>
internal static class ProducerMain
{
    internal static async Task<int> Run()
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();
        string bootstrapServers = PerfEnv.GetString("BOOTSTRAP_SERVERS", "localhost:9092");

        // Optionally start from a clean topic before producing (Python's main(): recreate_topic runs
        // regardless of CLIENT_VERSION, right at the top).
        if (config.CreateTopic)
        {
            TopicProvisioning.RecreateTopic(bootstrapServers, config.TopicName, config.Partitions);
        }

        if (PerfEnv.GetBool("VERIFY_CONSUMED", false))
        {
            Console.WriteLine("VERIFY_CONSUMED is not implemented for the v2 (ckd) baseline (manual comparison only); skipping");
        }

        PerfMessage[] messages = MessageGenerator.Generate(config.KeySize, config.ValueSize, MessageGenerator.DefaultCount);

        Dictionary<string, string> producerConfig = V2Config.BuildProducerConfig();
        V2Config.PrintProducerConfiguration(producerConfig, config.KeySize, config.ValueSize, config.DoVerify);

        PrintProducingBanner(config);

        using var metrics = new Metrics();
        metrics.StartCollecting(1);

        ProducerBenchmarkResult result = await RunBenchmark(config, producerConfig, metrics, messages).ConfigureAwait(false);

        // Cooldown — keep sampling through a short window so the post-test windows land in metrics.jsonl
        // (excluded from the averages, since the measured interval has ended). Matches Python's __main__.
        if (!PerfSignals.Terminating)
        {
            Console.WriteLine("Performing garbage collection...");
            GC.Collect();
        }

        Console.WriteLine("Waiting for final metrics collection...");
        if (!PerfSignals.Terminating)
        {
            Thread.Sleep(ProducerBenchmarkConfig.PostTestAwaitSeconds * 1000);
        }

        (double lastCpu, double lastRss) = metrics.ExternalMetricsLastValues();
        Console.WriteLine($"Final CPU: {lastCpu:F2} %");
        Console.WriteLine($"Final RSS: {lastRss / 1024:F2} KiB");
        metrics.StopCollecting();
        Console.WriteLine("Done");

        return result.LatencyBudgetExceeded ? 1 : 0;
    }

    private static async Task<ProducerBenchmarkResult> RunBenchmark(
        ProducerBenchmarkConfig config,
        Dictionary<string, string> producerConfig,
        Metrics metrics,
        PerfMessage[] messages)
    {
        if (config.Async)
        {
            Console.WriteLine("Running async producer performance test v2 (confluent-kafka-dotnet)...");
            ApplyDefaultAsyncQueueSizing(producerConfig, config);
            var backend = new V2AsyncProducerBackend(producerConfig);
            try
            {
                return await ProducerBenchmark.RunAsync(backend, config, metrics, messages, PerfSignals.Token).ConfigureAwait(false);
            }
            finally
            {
                await CloseQuietlyAsync(backend).ConfigureAwait(false);
                await backend.DisposeAsync().ConfigureAwait(false);
            }
        }

        Console.WriteLine("Running sync producer performance test v2 (confluent-kafka-dotnet)...");
        var syncBackend = new V2SyncProducerBackend(producerConfig);
        try
        {
            return ProducerBenchmark.RunSync(syncBackend, config, metrics, messages, PerfSignals.Token);
        }
        finally
        {
            CloseQuietly(syncBackend);
            syncBackend.Dispose();
        }
    }

    /// <summary>
    /// Sizes the librdkafka send queue to the run — Python's <c>AsyncCompatibleProducer.__init__</c>
    /// (v2-async only; the sync path has no such default and relies on <c>producer.poll()</c>-driven
    /// backpressure instead). Without this, an unsized queue defaults to librdkafka's 100,000-message /
    /// 1,048,576 KiB caps, hits <c>QUEUE_FULL</c> far sooner than Python's v2-async baseline does, making
    /// the two not comparable. Skipped for a key already set by the <c>BUFFER_MEMORY</c> block above —
    /// same precedence as Python's dict merge, where the explicit config overrides these defaults.
    /// </summary>
    private static void ApplyDefaultAsyncQueueSizing(Dictionary<string, string> producerConfig, ProducerBenchmarkConfig config)
    {
        if (!producerConfig.ContainsKey("queue.buffering.max.messages"))
        {
            long numMessagesConf = config.NumMessages > 0 ? Math.Min(config.NumMessages, int.MaxValue) : int.MaxValue;
            producerConfig["queue.buffering.max.messages"] = numMessagesConf.ToString(CultureInfo.InvariantCulture);
        }

        if (!producerConfig.ContainsKey("queue.buffering.max.kbytes"))
        {
            long totalSize = config.NumMessages * config.MessageSize;
            long totalSizeConf = totalSize > 0 ? Math.Min(totalSize, int.MaxValue) : int.MaxValue;
            producerConfig["queue.buffering.max.kbytes"] = totalSizeConf.ToString(CultureInfo.InvariantCulture);
        }
    }

    private static void CloseQuietly(IProducerBackend backend)
    {
        try
        {
            backend.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine($"Producer close failed: {e.Message}");
        }
    }

    private static async Task CloseQuietlyAsync(IAsyncProducerBackend backend)
    {
        try
        {
            await backend.Close().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Producer close failed: {e.Message}");
        }
    }

    private static void PrintProducingBanner(ProducerBenchmarkConfig config)
    {
        if (config.LimitRps is null)
        {
            if (config.NumMessages > 0)
            {
                Console.WriteLine($"Producing {config.NumMessages} messages at max rate");
            }
            else
            {
                Console.WriteLine($"Producing messages at max rate for {config.TestDurationSeconds} seconds");
            }
        }
        else
        {
            Console.WriteLine($"Producing {config.NumMessages} messages at {config.LimitRps} msg/s");
        }
    }
}
