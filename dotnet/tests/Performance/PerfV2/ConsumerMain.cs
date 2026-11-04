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

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// The <c>MODE=consumer</c> entry of the v2 (ckd) baseline, as Python's consumer <c>main</c> with
/// <c>CLIENT_VERSION=2</c>: it runs the sync or async engine over the ckd consumer and returns the exit code.
/// </summary>
internal static class ConsumerMain
{
    internal static async Task<int> Run()
    {
        ConsumerBenchmarkConfig config = ConsumerBenchmarkConfig.FromEnv();

        if (config.CreateTopic)
        {
            TopicProvisioning.RecreateTopic(config.BootstrapServers, config.Topic, config.Partitions);
        }

        Dictionary<string, string> consumerConfig = V2Config.BuildConsumerConfig(config);

        using var metrics = new Metrics();
        ConsumerBenchmarkResult result;
        if (config.AsyncMode)
        {
            var backend = new V2AsyncConsumerBackend(consumerConfig, config.PollTimeoutMs, config.BatchSize);
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
            using var backend = new V2SyncConsumerBackend(consumerConfig, config.PollTimeoutMs, config.BatchSize);
            result = ConsumerBenchmark.Run(config, backend, metrics);
        }

        metrics.StopCollecting();

        // Checked first, as Python's main does: the launcher retries this exit code.
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
