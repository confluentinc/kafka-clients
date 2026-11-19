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

using System;
using System.Threading;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Process-wide termination signal, as Python's soak <c>signal_handler</c>: Ctrl-C or SIGTERM sets
/// <see cref="Terminating"/> and runs the installed callback once.
/// </summary>
internal static class SoakSignals
{
    private static readonly object s_lock = new object();
    private static int s_fired;
    private static Action? s_onSignal;

    internal static bool Terminating => Volatile.Read(ref s_fired) != 0;

    // onSignal runs on the signalling thread and must not block; Python's handler only sets flags.
    internal static void Install(Action onSignal)
    {
        lock (s_lock)
        {
            s_onSignal = onSignal;
        }

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            RequestTermination();
        };

        // SIGTERM. The soak's only ProcessExit handler, and non-blocking: the shutdown watchdog's
        // Environment.Exit runs it on the watchdog's thread, where blocking would wedge the exit.
        AppDomain.CurrentDomain.ProcessExit += (_, _) => RequestTermination();
    }

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
