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

/// <summary>Log severities, as the Python soak's <c>--log-level</c> values.</summary>
internal enum SoakLogLevel
{
    Debug = 0,

    Info = 1,

    Warning = 2,

    Error = 3,

    Fatal = 4,
}

/// <summary>
/// The soak's dependency-free stdout logger, in the Python soak's line format
/// (<c>%(asctime)-15s %(levelname)-8s [%(threadName)s] %(message)s</c>); one lock keeps
/// lines whole.
/// </summary>
internal sealed class SoakLogger
{
    private readonly object _lock = new object();
    private readonly SoakLogLevel _minimumLevel;

    internal SoakLogger(SoakLogLevel minimumLevel)
    {
        _minimumLevel = minimumLevel;
    }

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

    internal void Debug(string message) => Log(SoakLogLevel.Debug, message);

    internal void Info(string message) => Log(SoakLogLevel.Info, message);

    internal void Warning(string message) => Log(SoakLogLevel.Warning, message);

    internal void Error(string message) => Log(SoakLogLevel.Error, message);

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
