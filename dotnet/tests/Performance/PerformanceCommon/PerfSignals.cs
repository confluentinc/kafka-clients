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
/// The process-wide termination flag, Python's <c>terminating</c> set by its SIGINT and SIGTERM handlers: the send
/// and measure loops stop, and the run skips its cooldown and verification.
/// </summary>
public static class PerfSignals
{
    private static readonly CancellationTokenSource s_cts = new CancellationTokenSource();

    public static bool Terminating => s_cts.IsCancellationRequested;

    public static CancellationToken Token => s_cts.Token;

    public static void Install()
    {
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            RequestTerminationFromSignal();
        };

        // CancelKeyPress alone misses a SIGTERM and a SIGINT sent to a process without a terminal.
        PosixSignalRegistration.Create(PosixSignal.SIGINT, HandlePosixSignal);
        PosixSignalRegistration.Create(PosixSignal.SIGTERM, HandlePosixSignal);

        // Not RequestTerminationFromSignal: ProcessExit also fires on a clean exit, which must not
        // print the signal message.
        AppDomain.CurrentDomain.ProcessExit += (_, _) => RequestTermination();
    }

    public static void RequestTermination()
    {
        if (!s_cts.IsCancellationRequested)
        {
            s_cts.Cancel();
        }
    }

    private static void RequestTerminationFromSignal()
    {
        if (!s_cts.IsCancellationRequested)
        {
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
