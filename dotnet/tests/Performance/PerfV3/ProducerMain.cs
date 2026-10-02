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

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// The <c>MODE=producer</c> entry — the C# analog of <c>producer_performance_test.py</c>'s <c>__main__</c>:
/// generate the cycled messages, (optionally) capture the VERIFY_CONSUMED baseline, start the metrics
/// sampler, run the sync (serial) or async (pipelined) engine over the v3 backend, cool down (GC + await),
/// then run the optional consume-all verification and return the exit code (non-zero if the p99 budget was
/// exceeded).
/// </summary>
internal static class ProducerMain
{
    internal static async Task<int> Run()
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();
        bool verifyConsumed = PerfEnv.GetBool("VERIFY_CONSUMED", false);
        string bootstrapServers = PerfEnv.GetString("BOOTSTRAP_SERVERS", "localhost:9092");

        // Optionally start from a clean topic before producing (Python's main(): recreate_topic before
        // the baseline-offset capture below).
        if (config.CreateTopic)
        {
            TopicProvisioning.RecreateTopic(bootstrapServers, config.TopicName, config.Partitions);
        }

        PerfMessage[] messages = MessageGenerator.Generate(config.KeySize, config.ValueSize, MessageGenerator.DefaultCount);

        Dictionary<string, string> producerConfig = V3Config.BuildProducerConfig();
        V3Config.PrintProducerConfiguration(producerConfig, config.KeySize, config.ValueSize, config.DoVerify);

        PrintProducingBanner(config);

        using var metrics = new Metrics();
        metrics.StartCollecting(1);

        Dictionary<int, long>? baseline = null;
        if (verifyConsumed)
        {
            try
            {
                baseline = VerifyConsumed.CaptureBaseline(bootstrapServers, config.TopicName);
                long preExisting = 0;
                foreach (long v in baseline.Values)
                {
                    preExisting += v;
                }

                Console.WriteLine($"Baseline: topic {config.TopicName} has {preExisting} pre-existing messages across {baseline.Count} partitions; verifier will start from these offsets");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Baseline capture failed: {e.Message}. Verification will be skipped.");
                baseline = null;
            }
        }

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

        int exitCode = 0;
        if (verifyConsumed && !PerfSignals.Terminating && baseline is not null)
        {
            long expected = result.WarmupSent + result.MeasuredSent;
            Console.WriteLine($"Verifying consumed messages from topic '{config.TopicName}' starting at baseline offsets (expected = {result.WarmupSent} warmup + {result.MeasuredSent} measured = {expected})");
            try
            {
                exitCode = VerifyConsumed.VerifyConsumedMessages(bootstrapServers, config.TopicName, baseline, expected, config.KeySize > 0);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Verification failed with exception: {e.Message}");
                exitCode = 1;
            }
        }
        else if (!verifyConsumed)
        {
            Console.WriteLine("Consumer verification skipped (VERIFY_CONSUMED=False)");
        }
        else if (PerfSignals.Terminating)
        {
            Console.WriteLine("Consumer verification skipped (terminated)");
        }
        else
        {
            Console.WriteLine("Consumer verification skipped (baseline capture failed)");
        }

        if (result.LatencyBudgetExceeded)
        {
            exitCode = 1;
        }

        return exitCode;
    }

    private static async Task<ProducerBenchmarkResult> RunBenchmark(
        ProducerBenchmarkConfig config,
        Dictionary<string, string> producerConfig,
        Metrics metrics,
        PerfMessage[] messages)
    {
        if (config.Async)
        {
            Console.WriteLine("Running async producer performance test v3 (confluent-kafka-rust)...");
            var backend = new V3AsyncProducerBackend(producerConfig);
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

        Console.WriteLine("Running sync producer performance test v3 (confluent-kafka-rust)...");
        var syncBackend = new V3SyncProducerBackend(producerConfig);
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
