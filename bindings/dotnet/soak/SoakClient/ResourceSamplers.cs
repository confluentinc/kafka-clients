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
//
// PROVENANCE: MemorySampler / CpuSampler are forked from
// bindings/dotnet/tests/Performance/PerformanceCommon/MetricSamplers.cs — both are
// `internal` there, and the fork is deliberate for the reasons in MetricPrimitives.cs's
// header. ProcessResourceSampler has no perf-harness counterpart: it is the soak's own
// `get_rusage()` analog (soakclient.py), producing the cpu.* / memory.* gauges.

using System;
using System.Diagnostics;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Resident-set-size sampler — the <c>psutil</c> <c>memory_info().rss</c> analog, feeding
/// the JSONL <c>rss</c> bucket. The value is <see cref="Process.WorkingSet64"/> in bytes.
/// </summary>
internal sealed class MemorySampler
{
    private readonly Process _process = Process.GetCurrentProcess();

    /// <summary>Returns the current process resident set size in bytes.</summary>
    internal double Sample()
    {
        // Process caches its counters; without Refresh() every window reports the value
        // read at construction.
        _process.Refresh();
        return _process.WorkingSet64;
    }
}

/// <summary>
/// CPU-utilization sampler — the <c>psutil</c> <c>Process.cpu_percent()</c> analog,
/// feeding the JSONL <c>cpu</c> bucket. Each sample returns utilization since the
/// previous sample as <c>Δ(TotalProcessorTime) / Δ(wall-clock) * 100</c> — deliberately
/// <b>not</b> divided by <c>ProcessorCount</c>, so (like psutil) it can exceed 100 % on a
/// multi-core box. The first sample has no baseline and returns <c>0.0</c>.
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

/// <summary>
/// One reading of the soak's process-resource gauges. <see cref="HasCpuDeltas"/> is
/// false for the very first sample, which only establishes the baseline — mirroring
/// Python's <c>get_rusage()</c>, where <c>calc_rusage_deltas</c> is skipped until a
/// previous <c>rusage</c> exists.
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

    /// <summary>Whether <see cref="UserCpuPercent"/> / <see cref="SystemCpuPercent"/> carry a delta (false on the first sample).</summary>
    internal bool HasCpuDeltas { get; }

    /// <summary>User CPU as a percentage of wall-clock since the previous sample (<c>ru_utime</c> analog).</summary>
    internal double UserCpuPercent { get; }

    /// <summary>Privileged/system CPU as a percentage of wall-clock since the previous sample (<c>ru_stime</c> analog).</summary>
    internal double SystemCpuPercent { get; }

    /// <summary>Current resident set size, MiB.</summary>
    internal double RssMiB { get; }

    /// <summary>Highest resident set size observed BY THIS SAMPLER, MiB (see <see cref="ProcessResourceSampler"/>).</summary>
    internal double MaxRssMiB { get; }

    /// <summary>Managed-heap bytes as MiB — the <c>tracemalloc</c> analog.</summary>
    internal double GcHeapMiB { get; }

    /// <summary>Highest managed-heap reading observed so far, MiB — the <c>tracemalloc</c>-peak analog.</summary>
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

    /// <summary>Current resident set size in MiB (used for the two startup baselines).</summary>
    internal double CurrentRssMiB()
    {
        _process.Refresh();
        return _process.WorkingSet64 / BytesPerMiB;
    }

    /// <summary>Takes one reading, advancing the CPU baseline and the two running maxima.</summary>
    internal ResourceSample Sample()
    {
        // Process caches its counters (D9's ".NET trap to get right"): without this the
        // CPU deltas are always zero and every window reports the same RSS.
        _process.Refresh();

        TimeSpan userTime = _process.UserProcessorTime;
        TimeSpan systemTime = _process.PrivilegedProcessorTime;
        long wallTimestamp = Stopwatch.GetTimestamp();
        long rssBytes = _process.WorkingSet64;

        // ⚠ RECORDED DEVIATION from PLAN D9, which specified Process.PeakWorkingSet64 for
        // `memory.rss.max`. MEASURED on this branch's toolchain (.NET 10.0.302, macOS):
        // Process.PeakWorkingSet64 returns 0 while WorkingSet64 returns 38780928 — the
        // Unix implementation does not track a peak, so the gauge would read a flat 0
        // and the Python soak's `ru_maxrss` line would have no counterpart at all. A
        // running maximum of the sampled WorkingSet64 is therefore what is reported.
        // Note the consequence honestly: this is the peak OF THE SOAK'S OWN SAMPLES (one
        // per window), not the kernel's true high-water mark, so a spike entirely between
        // two samples is invisible to it.
        if (rssBytes > _maxRssBytes)
        {
            _maxRssBytes = rssBytes;
        }

        // GC.GetTotalMemory(false) is the direct tracemalloc analog: MANAGED HEAP ONLY,
        // so RSS climbing while this stays flat points at native/Rust growth — the soak's
        // headline question, which RSS alone cannot answer. `false` = do not force a
        // collection; a forced GC every 10 s would perturb the very allocation behaviour
        // under observation.
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
