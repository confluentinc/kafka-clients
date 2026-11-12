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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Temporary, for the macOS CI hang investigation: when <c>CONFLUENT_KAFKA_HANG_TRACE_DIR</c> names a folder, appends
/// one flushed line per event to <c>dotnet-trace-&lt;pid&gt;.log</c> there, in the format of the core's
/// <c>rust-trace-&lt;pid&gt;.log</c>: milliseconds since the epoch, the thread's name, <c>handle=</c> and the client's
/// view in hex (the value the core's close lines print), and the event.
/// </summary>
internal static class HangTrace
{
    private static readonly object s_gate = new object();

    private static readonly StreamWriter? s_writer = Open();

    internal static bool Enabled => s_writer != null;

    internal static void Write(IntPtr handle, string text)
    {
        if (s_writer == null)
        {
            return;
        }

        string thread = (Thread.CurrentThread.Name ?? "unnamed").Replace(' ', '_');
        string line = string.Format(
            CultureInfo.InvariantCulture,
            "{0} {1} handle={2:x} {3}",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            thread,
            unchecked((ulong)handle.ToInt64()),
            text);
        try
        {
            lock (s_gate)
            {
                s_writer.WriteLine(line);
            }
        }
        catch (Exception)
        {
            // A trace that cannot be written must not change what the client does.
        }
    }

    private static StreamWriter? Open()
    {
        try
        {
            string? directory = Environment.GetEnvironmentVariable("CONFLUENT_KAFKA_HANG_TRACE_DIR");
            if (string.IsNullOrEmpty(directory))
            {
                return null;
            }

            Directory.CreateDirectory(directory);
            string path = Path.Combine(
                directory,
                string.Format(CultureInfo.InvariantCulture, "dotnet-trace-{0}.log", Process.GetCurrentProcess().Id));
            FileStream stream = new FileStream(
                path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
            return new StreamWriter(stream, new UTF8Encoding(false)) { AutoFlush = true };
        }
        catch (Exception)
        {
            return null;
        }
    }
}
