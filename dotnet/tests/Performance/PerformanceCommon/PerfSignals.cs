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
using System.Runtime.InteropServices;
using System.Threading;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Process-wide termination signal — the C# analog of the perf tests' module-level <c>terminating</c> /
/// <c>_terminating</c> flag, set by <c>signal_handler</c>/<c>_install_signal_handlers</c>'s
/// <c>signal.signal(SIGINT, ...)</c> + <c>signal.signal(SIGTERM, ...)</c> pair. Flipping
/// <see cref="Terminating"/> and cancelling <see cref="Token"/> lets the send / measure loops stop
/// cleanly and the run skip its cooldown + verification (matching Python's <c>if not terminating</c>
/// guards).
/// </summary>
public static class PerfSignals
{
    private static readonly CancellationTokenSource s_cts = new CancellationTokenSource();

    /// <summary>Whether termination has been requested (Python's module <c>terminating</c> global).</summary>
    public static bool Terminating => s_cts.IsCancellationRequested;

    /// <summary>A cancellation token flipped on termination — passed to the async delays / send loops.</summary>
    public static CancellationToken Token => s_cts.Token;

    /// <summary>
    /// Installs the termination handlers (idempotent-friendly; call once at process start). Covers BOTH
    /// signals Python installs (<c>signal.SIGINT</c> + <c>signal.SIGTERM</c>), via two independent
    /// mechanisms:
    /// <list type="bullet">
    /// <item><c>Console.CancelKeyPress</c> — .NET's own SIGINT-equivalent, kept for the interactive-tty
    /// case it already covered.</item>
    /// <item><see cref="PosixSignalRegistration"/> for BOTH <see cref="PosixSignal.SIGINT"/> and
    /// <see cref="PosixSignal.SIGTERM"/> — the fix for the gap <c>CancelKeyPress</c> alone left open: a
    /// delivered SIGTERM (<c>docker stop</c>, a CI cancel, a plain <c>kill</c>) never reached
    /// <c>CancelKeyPress</c> at all, and a delivered SIGINT on a non-tty process (redirected/closed
    /// stdin — the common case for a benchmark launched by a script or container) is undocumented and
    /// not guaranteed to reach it either. <c>PosixSignalRegistration</c> is available cross-platform for
    /// these two signals specifically (mapped to the Windows console-control equivalent there), so it
    /// needs no <c>#if</c> platform guard. <c>ctx.Cancel = true</c> suppresses the runtime's own default
    /// terminate-the-process action so OUR graceful shutdown path runs instead — matching Python's
    /// handler, which replaces the interpreter's default action the same way.</item>
    /// </list>
    /// Both mechanisms funnel into the same <see cref="RequestTermination"/>, which is idempotent, so
    /// a signal that happens to reach both (an interactive Ctrl-C) is harmless.
    /// </summary>
    public static void Install()
    {
        Console.CancelKeyPress += (_, e) =>
        {
            // Let the process shut down gracefully rather than aborting immediately.
            e.Cancel = true;
            RequestTerminationFromSignal();
        };

        PosixSignalRegistration.Create(PosixSignal.SIGINT, HandlePosixSignal);
        PosixSignalRegistration.Create(PosixSignal.SIGTERM, HandlePosixSignal);

        // NOT RequestTerminationFromSignal: ProcessExit fires on every process exit, including a
        // perfectly normal completion with no signal involved — printing Python's signal message here
        // would fire on every clean run, not just an interrupted one.
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

    /// <summary>
    /// The actual-signal path (Ctrl-C / SIGINT / SIGTERM): prints Python's <c>signal_handler</c> message
    /// before requesting termination. Kept separate from <see cref="RequestTermination"/> so a normal,
    /// un-signaled exit (which still runs <c>ProcessExit</c>) never prints it.
    /// </summary>
    private static void RequestTerminationFromSignal()
    {
        if (!s_cts.IsCancellationRequested)
        {
            // Python's signal_handler writes this via a raw fd write rather than `print`, because
            // re-entering the normal I/O path from inside a signal handler can raise a reentrant-call
            // error; Console.Write from a PosixSignalRegistration callback (which runs on a normal
            // thread-pool thread, not the actual signal-handling context) has no such restriction.
            Console.Out.Write("Termination signal received, shutting down...\n");
        }

        RequestTermination();
    }

    private static void HandlePosixSignal(PosixSignalContext context)
    {
        context.Cancel = true;
        RequestTerminationFromSignal();
    }
}
