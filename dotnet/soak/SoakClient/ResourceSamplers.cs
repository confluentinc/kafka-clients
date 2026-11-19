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
using System.Diagnostics;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Resident-set-size sampler (psutil's <c>memory_info().rss</c>), feeding the JSONL <c>rss</c>
/// bucket; forked from the perf suite's <c>MetricSamplers</c>, as <see cref="CpuSampler"/>.
/// </summary>
internal sealed class MemorySampler
{
    private readonly Process _process = Process.GetCurrentProcess();

    internal double Sample()
    {
        // Process caches its counters; without Refresh() every window reports the value
        // read at construction.
        _process.Refresh();
        return _process.WorkingSet64;
    }
}

/// <summary>
/// CPU-utilization sampler (psutil's <c>cpu_percent()</c>), feeding the JSONL <c>cpu</c> bucket:
/// processor time over wall-clock since the last sample, not divided by the core count, so it can
/// exceed 100 % as psutil's does; the first sample returns <c>0.0</c>.
/// </summary>
internal sealed class CpuSampler
{
    private readonly Process _process = Process.GetCurrentProcess();
    private bool _hasBaseline;
    private double _lastCpuSeconds;
    private long _lastWallTimestamp;

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

/// <summary>
/// One reading of the process-resource gauges; the first has no CPU deltas, as Python's
/// <c>get_rusage()</c> skips <c>calc_rusage_deltas</c> until a previous reading exists.
/// </summary>
internal readonly struct ResourceSample
{
    internal ResourceSample(bool hasCpuDeltas, double userCpuPercent, double systemCpuPercent, double rssMiB, double maxRssMiB, double gcHeapMiB, double gcHeapPeakMiB)
    {
        HasCpuDeltas = hasCpuDeltas;
        UserCpuPercent = userCpuPercent;
        SystemCpuPercent = systemCpuPercent;
        RssMiB = rssMiB;
        MaxRssMiB = maxRssMiB;
        GcHeapMiB = gcHeapMiB;
        GcHeapPeakMiB = gcHeapPeakMiB;
    }

    internal bool HasCpuDeltas { get; }

    internal double UserCpuPercent { get; }

    internal double SystemCpuPercent { get; }

    internal double RssMiB { get; }

    internal double MaxRssMiB { get; }

    internal double GcHeapMiB { get; }

    internal double GcHeapPeakMiB { get; }
}

/// <summary>
/// The soak's <c>get_rusage()</c> analog: user/system CPU deltas, RSS, an RSS maximum,
/// and the managed-heap size that separates .NET-side growth from native/Rust growth.
/// </summary>
internal sealed class ProcessResourceSampler
{
    private const double BytesPerMiB = 1024.0 * 1024.0;

    private readonly Process _process = Process.GetCurrentProcess();
    private bool _hasBaseline;
    private TimeSpan _lastUserTime;
    private TimeSpan _lastSystemTime;
    private long _lastWallTimestamp;
    private long _maxRssBytes;
    private long _gcHeapPeakBytes;

    internal double CurrentRssMiB()
    {
        _process.Refresh();
        return _process.WorkingSet64 / BytesPerMiB;
    }

    internal ResourceSample Sample()
    {
        // Process caches its counters: without Refresh() the CPU deltas are always zero and every
        // window reports the same RSS.
        _process.Refresh();

        TimeSpan userTime = _process.UserProcessorTime;
        TimeSpan systemTime = _process.PrivilegedProcessorTime;
        long wallTimestamp = Stopwatch.GetTimestamp();
        long rssBytes = _process.WorkingSet64;

        // Unlike Python's ru_maxrss, a running maximum of the sampled WorkingSet64
        // (PeakWorkingSet64 reads 0 on Unix), so a spike between two samples is missed.
        if (rssBytes > _maxRssBytes)
        {
            _maxRssBytes = rssBytes;
        }

        // Managed heap only, as Python's tracemalloc, so RSS climbing while this is flat means
        // native growth; no forced collection, which would perturb what is measured.
        long gcHeapBytes = GC.GetTotalMemory(false);
        if (gcHeapBytes > _gcHeapPeakBytes)
        {
            _gcHeapPeakBytes = gcHeapBytes;
        }

        bool hasDeltas = _hasBaseline;
        double userPercent = 0.0;
        double systemPercent = 0.0;
        if (_hasBaseline)
        {
            double wallSeconds = (wallTimestamp - _lastWallTimestamp) / (double)Stopwatch.Frequency;
            if (wallSeconds > 0)
            {
                userPercent = (userTime - _lastUserTime).TotalSeconds / wallSeconds * 100.0;
                systemPercent = (systemTime - _lastSystemTime).TotalSeconds / wallSeconds * 100.0;
            }
        }

        _hasBaseline = true;
        _lastUserTime = userTime;
        _lastSystemTime = systemTime;
        _lastWallTimestamp = wallTimestamp;

        return new ResourceSample(
            hasDeltas,
            userPercent,
            systemPercent,
            rssBytes / BytesPerMiB,
            _maxRssBytes / BytesPerMiB,
            gcHeapBytes / BytesPerMiB,
            _gcHeapPeakBytes / BytesPerMiB);
    }
}
