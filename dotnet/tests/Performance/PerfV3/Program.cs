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
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// The v3 perf executable: <c>MODE</c> selects the producer or the consumer benchmark, and the exit code carries the
/// verdict the launcher and the perf smoke gate on.
/// </summary>
internal static class Program
{
    // Exit codes: 0 clean, 1 a benchmark failure, 2 setup not ready (the smoke retries it, so nothing else returns it),
    // 64 a usage error, 130 interrupted.
    private const int InterruptedExitCode = 130;

    private const int UsageExitCode = 64;

    private static async Task<int> Main()
    {
        PerfSignals.Install();

        string mode = PerfEnv.GetString("MODE", "producer");
        try
        {
            switch (mode)
            {
                case "producer":
                    return await ProducerMain.Run().ConfigureAwait(false);
                case "consumer":
                    return await ConsumerMain.Run().ConfigureAwait(false);
                default:
                    Console.Error.WriteLine($"Unknown MODE '{mode}' (expected 'producer' or 'consumer')");
                    return UsageExitCode;
            }
        }
        catch (OperationCanceledException)
        {
            // A safety net: the engines handle their own cancellation, so a stray one exits 130 instead of crashing.
            Console.Error.WriteLine("Interrupted; exiting.");
            return InterruptedExitCode;
        }
    }
}
