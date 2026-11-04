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
/// The <c>MODE=producer</c> entry of the v2 (ckd) baseline, as Python's <c>__main__</c> with <c>CLIENT_VERSION=2</c>:
/// it runs the sync or async engine over the ckd producer, cools down and returns the exit code; <c>VERIFY_CONSUMED</c>
/// is skipped, as this exe has no consumer.
/// </summary>
internal static class ProducerMain
{
    internal static async Task<int> Run()
    {
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();
        string bootstrapServers = PerfEnv.GetString("BOOTSTRAP_SERVERS", "localhost:9092");

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

        // The cooldown keeps sampling so the post-test windows reach metrics.jsonl, outside the averages, as Python.
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

        return result.LatencyBudgetExceeded || result.RunFailed ? 1 : 0;
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

    // As Python's AsyncCompatibleProducer (v2 async): size the send queue to the run unless BUFFER_MEMORY set the key.
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
