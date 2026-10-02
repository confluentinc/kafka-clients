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
using System.Globalization;
using System.Threading;

namespace Confluent.Kafka.Soak;

/// <summary>Log severities, mirroring <c>soakclient.py</c>'s <c>--log-level</c> values.</summary>
internal enum SoakLogLevel
{
    /// <summary>Per-record detail; off by default.</summary>
    Debug = 0,

    /// <summary>The default level: lifecycle, status lines, the SUMMARY verdict.</summary>
    Info = 1,

    /// <summary>Something a run should survive but an operator should see (a disconnect, a duplicate).</summary>
    Warning = 2,

    /// <summary>A failure that was counted rather than fatal.</summary>
    Error = 3,

    /// <summary>A failure that ends the run.</summary>
    Fatal = 4,
}

/// <summary>
/// The soak's stdout logger. Deliberately dependency-free — an operational tool whose
/// output is read by <c>tail -f</c> on an EC2 box needs one line format and nothing else,
/// and adding a logging framework here would be a second external dependency beyond the
/// OpenTelemetry one PLAN D12 already calls out.
/// <para>
/// The line format mirrors the Python soak's
/// <c>'%(asctime)-15s %(levelname)-8s [%(threadName)s] %(message)s'</c>, so the two
/// clients' logs read identically side by side. Writes are serialized on one lock:
/// the producer loop, the consumer loop, the delivery continuations and the sampler all
/// log, and interleaved partial lines would be unreadable.
/// </para>
/// </summary>
internal sealed class SoakLogger
{
    private readonly object _lock = new object();
    private readonly SoakLogLevel _minimumLevel;

    /// <summary>Creates a logger emitting <paramref name="minimumLevel"/> and above.</summary>
    internal SoakLogger(SoakLogLevel minimumLevel)
    {
        _minimumLevel = minimumLevel;
    }

    /// <summary>
    /// Parses a level name (case-insensitive), falling back to <see cref="SoakLogLevel.Info"/>
    /// — a log level is never worth failing a two-week run over.
    /// </summary>
    internal static SoakLogLevel ParseLevel(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return SoakLogLevel.Info;
        }

        return name!.ToUpperInvariant() switch
        {
            "DEBUG" => SoakLogLevel.Debug,
            "INFO" => SoakLogLevel.Info,
            "WARNING" or "WARN" => SoakLogLevel.Warning,
            "ERROR" => SoakLogLevel.Error,
            "FATAL" or "CRITICAL" => SoakLogLevel.Fatal,
            _ => SoakLogLevel.Info,
        };
    }

    /// <summary>Logs at <see cref="SoakLogLevel.Debug"/>.</summary>
    internal void Debug(string message) => Log(SoakLogLevel.Debug, message);

    /// <summary>Logs at <see cref="SoakLogLevel.Info"/>.</summary>
    internal void Info(string message) => Log(SoakLogLevel.Info, message);

    /// <summary>Logs at <see cref="SoakLogLevel.Warning"/>.</summary>
    internal void Warning(string message) => Log(SoakLogLevel.Warning, message);

    /// <summary>Logs at <see cref="SoakLogLevel.Error"/>.</summary>
    internal void Error(string message) => Log(SoakLogLevel.Error, message);

    /// <summary>Logs at <see cref="SoakLogLevel.Fatal"/>.</summary>
    internal void Fatal(string message) => Log(SoakLogLevel.Fatal, message);

    private void Log(SoakLogLevel level, string message)
    {
        if (level < _minimumLevel)
        {
            return;
        }

        string line = string.Format(
            CultureInfo.InvariantCulture,
            "{0,-15} {1,-8} [{2}] {3}",
            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss,fff", CultureInfo.InvariantCulture),
            level.ToString().ToUpperInvariant(),
            Thread.CurrentThread.Name ?? "main",
            message);

        lock (_lock)
        {
            Console.Out.WriteLine(line);
            Console.Out.Flush();
        }
    }
}
