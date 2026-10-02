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
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// The <c>MODE=consumer</c> entry — the C# analog of <c>consumer_performance_test.py</c>'s <c>main</c>:
/// build the v3 consumer config, run the sync or async e2e-latency engine (which writes <c>results.json</c>
/// and <c>metrics.jsonl</c>), and return the exit code — non-zero on assignment timeout, an exceeded p99
/// budget, or zero measured messages.
/// </summary>
internal static class ConsumerMain
{
    internal static async Task<int> Run()
    {
        ConsumerBenchmarkConfig config = ConsumerBenchmarkConfig.FromEnv();

        // Optionally start from a clean topic before consuming (Python's run()/run_async(): recreate_topic
        // before build_consumer). Runs here, before the shared ConsumerBenchmark engine, because an
        // AdminClient is client-specific and ConsumerBenchmark carries no client dependency (M13/P1 D8).
        if (config.CreateTopic)
        {
            TopicProvisioning.RecreateTopic(config.BootstrapServers, config.Topic, config.Partitions);
        }

        Dictionary<string, string> consumerConfig = V3Config.BuildConsumerConfig(config);

        using var metrics = new Metrics();
        ConsumerBenchmarkResult result;
        if (config.AsyncMode)
        {
            var backend = new V3AsyncConsumerBackend(consumerConfig, config.PollTimeoutMs);
            try
            {
                result = await ConsumerBenchmark.RunAsync(config, backend, metrics).ConfigureAwait(false);
            }
            finally
            {
                await backend.DisposeAsync().ConfigureAwait(false);
            }
        }
        else
        {
            using var backend = new V3SyncConsumerBackend(consumerConfig, config.PollTimeoutMs);
            result = ConsumerBenchmark.Run(config, backend, metrics);
        }

        metrics.StopCollecting();

        // The readiness gate never saw the pipeline go live (feeder still starting up, or nothing
        // measurable arriving). Distinct from a benchmark failure: the launcher retries this rc.
        // Checked FIRST — Python's main() tests the SETUP_NOT_READY sentinel ahead of the None
        // (assignment-failure) case and the p99 / no-messages gates.
        if (result.SetupNotReady)
        {
            return ConsumerBenchmarkResult.SetupNotReadyExitCode;
        }

        if (result.AssignmentFailed)
        {
            return 1;
        }

        if (config.P99LimitMs > 0 && result.P99 > config.P99LimitMs)
        {
            Console.Error.WriteLine($"FAIL: p99 latency {result.P99} ms exceeds budget {config.P99LimitMs} ms");
            return 1;
        }

        if (result.MessagesMeasured <= 0)
        {
            Console.Error.WriteLine("FAIL: no messages measured");
            return 1;
        }

        return 0;
    }
}
