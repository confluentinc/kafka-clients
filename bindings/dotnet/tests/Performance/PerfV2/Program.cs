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

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// The v2 (ckd) perf executable entry point. <c>MODE={producer,consumer}</c> selects the benchmark (D8 —
/// the client dimension is the exe boundary, so producer/consumer collapse to a <c>MODE</c> env inside one
/// exe). The process exit code carries the p99-budget / no-data verdict so the launcher (Makefile) can
/// gate on it. This exe is the v2 baseline; the in-suite Docker smoke gates v3 only (D10).
/// </summary>
internal static class Program
{
    private static async Task<int> Main()
    {
        // This exe IS client version 2 (the exe boundary is the client dimension — PLAN §1.1.1). Pin
        // CLIENT_VERSION=2 up front so every downstream read is authoritative regardless of how the exe
        // was launched: the consumer results.json client_version and the default group.id
        // (benchmark-2-<epoch>) both derive from it (Python selects v2 via this same env var; here the
        // exe encodes it). Set before any config is parsed.
        Environment.SetEnvironmentVariable("CLIENT_VERSION", "2");

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
