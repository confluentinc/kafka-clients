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

using System.Diagnostics;

namespace Confluent.Kafka.Performance;

/// <summary>Samples the process's resident set size in bytes, Python's <c>MemoryBucket</c>.</summary>
internal sealed class MemorySampler
{
    private readonly Process _process = Process.GetCurrentProcess();

    internal double Sample()
    {
        _process.Refresh();
        return _process.WorkingSet64;
    }
}

/// <summary>
/// Samples the process's CPU utilization since the previous sample, Python's <c>CPUBucket</c> (psutil's
/// <c>cpu_percent</c>): not divided by the core count, so it can exceed 100 %; a first sample without a baseline is
/// <c>0.0</c>.
/// </summary>
internal sealed class CpuSampler
{
    private readonly Process _process = Process.GetCurrentProcess();

    // Sample runs on the collector thread, ResetBaseline on the measuring thread.
    private readonly object _lock = new object();
    private bool _hasBaseline;
    private double _lastCpuSeconds;
    private long _lastWallTimestamp;

    internal double Sample()
    {
        lock (_lock)
        {
            _process.Refresh();
            double cpuSeconds = _process.TotalProcessorTime.TotalSeconds;
            long wallTimestamp = Stopwatch.GetTimestamp();

            if (!_hasBaseline)
            {
                _hasBaseline = true;
                _lastCpuSeconds = cpuSeconds;
                _lastWallTimestamp = wallTimestamp;
                return 0.0;
            }

            double wallSeconds = (wallTimestamp - _lastWallTimestamp) / (double)Stopwatch.Frequency;
            double cpuDeltaSeconds = cpuSeconds - _lastCpuSeconds;
            _lastCpuSeconds = cpuSeconds;
            _lastWallTimestamp = wallTimestamp;

            return wallSeconds > 0 ? cpuDeltaSeconds / wallSeconds * 100.0 : 0.0;
        }
    }

    internal void ResetBaseline()
    {
        lock (_lock)
        {
            _process.Refresh();
            _lastCpuSeconds = _process.TotalProcessorTime.TotalSeconds;
            _lastWallTimestamp = Stopwatch.GetTimestamp();
            _hasBaseline = true;
        }
    }
}
