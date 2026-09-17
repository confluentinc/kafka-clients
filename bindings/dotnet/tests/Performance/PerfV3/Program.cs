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
    // The exit-code number space, stated once so every arm of Main can be checked against it:
    //   0   clean run
    //   1   a real benchmark failure (assignment timeout, p99 budget exceeded, nothing measured)
    //   2   ConsumerBenchmarkResult.SetupNotReadyExitCode — the setup never came up; RETRY me. The
    //       in-suite smoke's retry loop keys on this value, so nothing else may return it.
    //   64  EX_USAGE (sysexits.h) — the invocation itself was wrong.
    //   130 128 + SIGINT, the conventional shell code for an interrupted process.
    private const int InterruptedExitCode = 130;

    // An unknown MODE is a usage error, not a readiness problem. It returned 2 before M13/P3 gave that
    // value a meaning; leaving it there made a MODE typo in consumer mode get retried three times and
    // reported as "consumer perf run failed (rc=2) after 3 attempt(s)" — pointing the reader at the
    // feeder when the real message ("Unknown MODE ...") was on stderr of a run that never touched the
    // broker.
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
            // Safety net only (M13/P3 item D). The engines catch their own cancellation and run their
            // teardown, so this should be unreachable; it exists so that threading the termination
            // token through some future await can never again turn Ctrl-C into an unhandled-exception
            // crash dump with no exit code, no final metrics and no cleanup. Main has no other handler.
            Console.Error.WriteLine("Interrupted; exiting.");
            return InterruptedExitCode;
        }
    }
}
