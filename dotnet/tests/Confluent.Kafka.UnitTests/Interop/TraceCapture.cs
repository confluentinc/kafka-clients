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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Captures the lines written to <see cref="Trace"/> while it is registered, for a test to read those carrying its own
/// marker.
/// </summary>
internal sealed class TraceCapture : TraceListener
{
    private readonly StringBuilder _pending = new StringBuilder();

    private readonly List<string> _lines = new List<string>();

    private TraceCapture()
    {
    }

    internal static TraceCapture Start()
    {
        var capture = new TraceCapture();
        Trace.Listeners.Add(capture);
        return capture;
    }

    internal List<string> LinesContaining(string marker)
    {
        lock (_lines)
        {
            return _lines.Where(line => line.IndexOf(marker, StringComparison.Ordinal) >= 0).ToList();
        }
    }

    public override void Write(string? message)
    {
        lock (_lines)
        {
            _pending.Append(message);
        }
    }

    public override void WriteLine(string? message)
    {
        lock (_lines)
        {
            _pending.Append(message);
            _lines.Add(_pending.ToString());
            _pending.Clear();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Trace.Listeners.Remove(this);
        }

        base.Dispose(disposing);
    }
}
