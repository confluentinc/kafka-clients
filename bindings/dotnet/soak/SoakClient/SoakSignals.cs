// Copyright 2026 Confluent Inc.
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
//
// PROVENANCE: forked from bindings/dotnet/tests/Performance/PerformanceCommon/PerfSignals.cs
// (see MetricPrimitives.cs's header for why the soak forks rather than references), with a
// callback added: the soak's SIGINT/SIGTERM handler must also drive the shutdown watchdog
// and the client's own stop path, which the perf harness's flag-only shape cannot express.

using System;
using System.Threading;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Process-wide termination signal — the analog of <c>soakclient.py</c>'s
/// <c>signal_handler</c>. Ctrl-C or SIGTERM flips <see cref="Terminating"/> and runs the
/// installed callback exactly once.
/// </summary>
internal static class SoakSignals
{
    private static readonly object s_lock = new object();
    private static int s_fired;
    private static Action? s_onSignal;

    /// <summary>Whether termination has been requested.</summary>
    internal static bool Terminating => Volatile.Read(ref s_fired) != 0;

    /// <summary>
    /// Installs the Ctrl-C / SIGTERM handler. <paramref name="onSignal"/> runs on the
    /// signalling thread and must be non-blocking (Python's handler only writes one line
    /// and sets two flags, for the same reason).
    /// </summary>
    internal static void Install(Action onSignal)
    {
        lock (s_lock)
        {
            s_onSignal = onSignal;
        }

        Console.CancelKeyPress += (_, e) =>
        {
            // Shut down gracefully rather than aborting immediately.
            e.Cancel = true;
            RequestTermination();
        };

        // SIGTERM — run.sh's stop_child() sends one, and this is how .NET surfaces it.
        // ⚠ This handler is deliberately the ONLY ProcessExit handler the soak installs,
        // and it does nothing but set a flag and invoke a non-blocking callback. That is
        // what makes the shutdown watchdog's Environment.Exit safe (Environment.Exit runs
        // ProcessExit handlers on the calling thread, so a blocking one would wedge the
        // very path that exists to un-wedge a wedged shutdown).
        AppDomain.CurrentDomain.ProcessExit += (_, _) => RequestTermination();
    }

    /// <summary>Requests termination; the callback runs at most once per process.</summary>
    internal static void RequestTermination()
    {
        if (Interlocked.Exchange(ref s_fired, 1) != 0)
        {
            return;
        }

        Action? callback;
        lock (s_lock)
        {
            callback = s_onSignal;
        }

        callback?.Invoke();
    }
}
