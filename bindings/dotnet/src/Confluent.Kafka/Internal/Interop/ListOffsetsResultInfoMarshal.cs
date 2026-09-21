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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a <b>borrowed</b> <c>kafka_admin_ListOffsetsResultInfo_t</c> out into an owned
/// managed <see cref="ListOffsetsResult.ListOffsetsResultInfo"/>.
/// </summary>
/// <remarks>
/// ⚠⚠ <b>The leader epoch's absence is read from the accessor's BOOL RETURN, never from
/// the value.</b> Java's field is <c>Optional&lt;Integer&gt;</c>
/// (<c>ListOffsetsResult.java:74</c>) and the header is explicit: the accessor "writes the
/// leader epoch to <c>*out_epoch</c> and returns <c>true</c>, or returns <c>false</c> when
/// Java's <c>leaderEpoch()</c> is <c>Optional.empty()</c>". A negative epoch is a
/// <em>present</em> value, so a <c>-1</c>-means-absent reading would hand the caller
/// <see langword="null"/> for an epoch the broker actually reported. M15/P3's
/// <c>total_bytes</c> is the opposite convention — there <c>-1</c> genuinely is the
/// sentinel — which is exactly why the accessor's own documentation, not habit, decides.
/// </remarks>
internal static class ListOffsetsResultInfoMarshal
{
    /// <summary>
    /// The presence-and-value accessor's shape, injectable so a test can drive the
    /// <em>present</em> branch.
    /// </summary>
    /// <remarks>
    /// ⚠ The mock can only produce <c>Optional.empty()</c> — the Rust
    /// <c>MockAdminClient.list_offsets</c> builds every info with <c>None</c> — so without
    /// this seam the present branch would be unreachable and "reads the bool" would be
    /// untestable. The same A/B accommodation M15/P3 made for
    /// <c>DescribeClusterMarshal</c>'s authorized-operations gate.
    /// </remarks>
    /// <param name="info">The borrowed info pointer.</param>
    /// <param name="epoch">Receives the epoch when the return is <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when Java's <c>Optional</c> is present.</returns>
    internal delegate bool LeaderEpochAccessor(IntPtr info, out int epoch);

    /// <summary>Copies out one info value.</summary>
    /// <param name="info">The borrowed pointer, or <see cref="IntPtr.Zero"/>.</param>
    /// <param name="leaderEpoch">
    /// The presence accessor; defaults to the real P/Invoke. Overridden only by tests, to
    /// reach the branch the mock cannot produce.
    /// </param>
    /// <returns>An owned value, or <see langword="null"/> for a null pointer.</returns>
    internal static ListOffsetsResult.ListOffsetsResultInfo? CopyOut(
        IntPtr info, LeaderEpochAccessor? leaderEpoch = null)
    {
        if (info == IntPtr.Zero)
        {
            return null;
        }

        LeaderEpochAccessor presence = leaderEpoch ?? NativeMethods.ListOffsetsResultInfoLeaderEpoch;

        // ⚠ The RETURN decides presence; `epoch` is only meaningful when it is true.
        int? epochOrNull = presence(info, out int epoch) ? epoch : null;

        return new ListOffsetsResult.ListOffsetsResultInfo(
            NativeMethods.ListOffsetsResultInfoOffset(info),
            NativeMethods.ListOffsetsResultInfoTimestamp(info),
            epochOrNull);
    }
}
