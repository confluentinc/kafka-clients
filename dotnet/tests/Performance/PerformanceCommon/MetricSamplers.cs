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

/// <summary>
/// Resident-set-size sampler — the <c>psutil</c> <c>memory_info().rss</c> analog
/// (<c>performance_common.py</c> <c>MemoryBucket</c>). Sampled once per rollover; the value is
/// <see cref="Process.WorkingSet64"/> in bytes.
/// </summary>
internal sealed class MemorySampler
{
    private readonly Process _process = Process.GetCurrentProcess();

    /// <summary>Returns the current process resident set size in bytes.</summary>
    internal double Sample()
    {
        _process.Refresh();
        return _process.WorkingSet64;
    }
}

/// <summary>
/// CPU-utilization sampler — the <c>psutil</c> <c>Process.cpu_percent()</c> analog
/// (<c>performance_common.py</c> <c>CPUBucket</c>). Each sample returns utilization since the previous
/// sample as <c>Δ(TotalProcessorTime) / Δ(wall-clock) * 100</c> — deliberately <b>not</b> divided by
/// <c>ProcessorCount</c>, so (like psutil) it can exceed 100 % on a multi-core box. The first sample has
/// no baseline and returns <c>0.0</c> (mirroring psutil's first-call behavior).
/// </summary>
internal sealed class CpuSampler
{
    private readonly Process _process = Process.GetCurrentProcess();
    private bool _hasBaseline;
    private double _lastCpuSeconds;
    private long _lastWallTimestamp;

    /// <summary>Returns CPU utilization (%) since the previous call; the first call returns <c>0.0</c>.</summary>
    internal double Sample()
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
