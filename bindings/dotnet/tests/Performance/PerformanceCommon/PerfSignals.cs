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
using System.Threading;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Process-wide termination signal — the C# analog of the perf tests' module-level <c>terminating</c> /
/// <c>_terminating</c> flag set by a SIGINT/SIGTERM handler. A Ctrl-C (or SIGTERM) flips
/// <see cref="Terminating"/> and cancels <see cref="Token"/> so the send / measure loops stop cleanly and
/// the run skips its cooldown + verification (matching Python's <c>if not terminating</c> guards).
/// </summary>
public static class PerfSignals
{
    private static readonly CancellationTokenSource s_cts = new CancellationTokenSource();

    /// <summary>Whether termination has been requested (Python's module <c>terminating</c> global).</summary>
    public static bool Terminating => s_cts.IsCancellationRequested;

    /// <summary>A cancellation token flipped on termination — passed to the async delays / send loops.</summary>
    public static CancellationToken Token => s_cts.Token;

    /// <summary>Installs the Ctrl-C / SIGTERM handler (idempotent-friendly; call once at process start).</summary>
    public static void Install()
    {
        Console.CancelKeyPress += (_, e) =>
        {
            // Let the process shut down gracefully rather than aborting immediately.
            e.Cancel = true;
            RequestTermination();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => RequestTermination();
    }

    /// <summary>Requests termination (flips <see cref="Terminating"/> / cancels <see cref="Token"/>).</summary>
    public static void RequestTermination()
    {
        if (!s_cts.IsCancellationRequested)
        {
            s_cts.Cancel();
        }
    }
}
