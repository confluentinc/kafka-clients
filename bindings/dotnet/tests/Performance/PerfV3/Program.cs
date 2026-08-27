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
/// The v3 perf executable entry point. <c>MODE={producer,consumer}</c> selects the benchmark (D8 — the
/// client dimension is the exe boundary, so producer/consumer collapse to a <c>MODE</c> env inside one
/// exe). The process exit code carries the p99-budget / no-data verdict so the launcher (Makefile /
/// in-suite xUnit) can gate on it.
/// </summary>
internal static class Program
{
    private static async Task<int> Main()
    {
        PerfSignals.Install();

        string mode = PerfEnv.GetString("MODE", "producer");
        switch (mode)
        {
            case "producer":
                return await ProducerMain.Run().ConfigureAwait(false);
            case "consumer":
                return await ConsumerMain.Run().ConfigureAwait(false);
            default:
                Console.Error.WriteLine($"Unknown MODE '{mode}' (expected 'producer' or 'consumer')");
                return 2;
        }
    }
}
