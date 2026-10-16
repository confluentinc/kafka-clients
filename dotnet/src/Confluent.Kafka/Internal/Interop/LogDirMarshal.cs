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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the <c>describeLogDirs</c> / <c>describeReplicaLogDirs</c> value trees — a
/// <c>LogDirDescriptionMap_t</c> and its borrowed <c>LogDirDescription_t</c> children, or a
/// <c>ReplicaLogDirInfo_t</c> — into fully owned managed objects, so nothing survives the
/// value's own <c>_destroy</c>, which the per-key callback runs after this returns (ffi §B2
/// Category 4 / §B4).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>There is a borrowed <c>KafkaError</c> in this file, nested inside the value
/// tree.</b> <c>LogDirDescription_error</c> is <c>const</c> — borrowed from its map, never
/// destroyed, read with <see cref="KafkaException.FromBorrowedHandle"/> — and it is
/// <b>distinct</b> from the per-broker error the <c>describe_log_dirs</c> callback
/// delivers, which is owned and read by <c>KeyedResultMarshal.CompleteKey</c>, not here. A
/// reviewer scanning for "the error accessor" finds only one of the two.
/// </para>
/// <para>
/// ⚠ <b>The nested read is a recorded coverage gap, and that was MEASURED rather than
/// assumed.</b> A destroy-after-read injected at the nested site below changes nothing —
/// not because it is covered elsewhere, but because the pointer there is always NULL on
/// every broker-free path, which a control-positive confirmed by failing when it was null.
/// The measurement, and why the shape is nonetheless pinned, are written up once in
/// <c>AdminLogDirsMarshalTests.TheNestedDirectoryError_IsAFieldOnASuccessfulDescription</c>.
/// </para>
/// <para>
/// ⚠ <b>The two errors mean different things.</b> The per-broker one says <em>that broker's
/// query failed</em> and faults the key's <see cref="System.Threading.Tasks.Task"/>. The
/// one here is a <b>field on a successfully returned description</b> — the broker answered,
/// but that directory is offline (the header says so outright: "It is <em>not</em> the
/// per-broker error … the broker answered, but this particular directory is offline or
/// unreadable"). The task <b>succeeds</b>, carrying a description whose
/// <see cref="LogDirDescription.Error"/> is non-null; faulting it here would be a
/// behavioural divergence.
/// </para>
/// <para>
/// ⚠ <b><c>-1</c> IS a sentinel for the volume sizes, and is NOT one for the replica
/// fields.</b> <c>total_bytes</c> / <c>usable_bytes</c> return <c>-1</c> for "the broker did
/// not report it", which is Java's empty <c>OptionalLong</c> (<c>LogDirDescription.java:49</c>
/// maps <c>UNKNOWN_VOLUME_BYTES</c> to <c>OptionalLong.empty()</c>) — so they map to
/// <c>long?</c> with <c>-1</c> → <see langword="null"/>. <c>replica_size</c> and
/// <c>replica_offset_lag</c> also document <c>-1</c>, but only for an <em>out-of-range
/// index</em>, which the <c>replica_count</c>-bounded loop cannot reach; their values pass
/// through unchanged. ⚠ Contrast <c>deleteRecords</c>' low watermark, where <c>-1</c> is a
/// legitimate value and not a sentinel at all.
/// </para>
/// </remarks>
internal static class LogDirMarshal
{
    /// <summary>
    /// Java's <c>UNKNOWN_VOLUME_BYTES</c> — the value <c>total_bytes</c> /
    /// <c>usable_bytes</c> report when the broker did not supply a size.
    /// </summary>
    private const long UnknownVolumeBytes = -1L;

    /// <summary>
    /// Copies one <c>LogDirDescriptionMap_t</c> out: every log directory of one broker,
    /// keyed by path.
    /// </summary>
    /// <param name="map">
    /// The broker's log-dir map, owned by the per-broker callback, which destroys it after
    /// this returns.
    /// </param>
    /// <returns>The owned map.</returns>
    /// <exception cref="KafkaException">
    /// The callback delivered neither a map nor an error for a broker — unreachable in
    /// practice, since the header says exactly one of the two is non-null and
    /// <c>KeyedResultMarshal.CompleteKey</c> reads the error first.
    /// </exception>
    internal static IReadOnlyDictionary<string, LogDirDescription> CopyOutMap(IntPtr map)
    {
        if (map == IntPtr.Zero)
        {
            throw new KafkaException(
                "The describeLogDirs result carried neither log directories nor an error for a broker.");
        }

        int count = NativeMethods.LogDirDescriptionMapCount(map);
        Dictionary<string, LogDirDescription> directories =
            new Dictionary<string, LogDirDescription>(Math.Max(count, 0), StringComparer.Ordinal);
        for (int index = 0; index < count; index++)
        {
            string path = Utf8Marshal.PtrToString(NativeMethods.LogDirDescriptionMapGetKey(map, index))
                ?? string.Empty;
            IntPtr description = NativeMethods.LogDirDescriptionMapGetValue(map, index);
            if (description == IntPtr.Zero)
            {
                // Guarded by `count`, so unreachable; skipping is the safe reading.
                continue;
            }

            // Add, not the indexer: the ABI's keys come from a map and are documented sorted
            // by path, so a duplicate would be a core contract violation worth surfacing.
            directories.Add(path, CopyOutDescription(description));
        }

        return directories;
    }

    /// <summary>
    /// Copies one borrowed <c>LogDirDescription_t</c> out, replicas and all.
    /// </summary>
    /// <param name="description">The borrowed description pointer.</param>
    /// <returns>The owned description.</returns>
    internal static LogDirDescription CopyOutDescription(IntPtr description)
    {
        // ⚠ BORROWED — read, never destroy. Unlike the per-broker error this one is not owned
        // (see the class remarks); it is a FIELD on a description the broker returned
        // successfully, not a failure of the query.
        KafkaException? error =
            KafkaException.FromBorrowedHandle(NativeMethods.LogDirDescriptionError(description));

        long? totalBytes = VolumeBytes(NativeMethods.LogDirDescriptionTotalBytes(description));
        long? usableBytes = VolumeBytes(NativeMethods.LogDirDescriptionUsableBytes(description));

        int replicaCount = NativeMethods.LogDirDescriptionReplicaCount(description);
        Dictionary<TopicPartition, ReplicaInfo> replicas =
            new Dictionary<TopicPartition, ReplicaInfo>(Math.Max(replicaCount, 0));
        for (int index = 0; index < replicaCount; index++)
        {
            // The (topic, partition) pair reassembles into the shipped TopicPartition, whose
            // own IEquatable implementation is what keys this dictionary — no second
            // partition type, and no comparer that could disagree with it.
            TopicPartition partition = new TopicPartition(
                Utf8Marshal.PtrToString(NativeMethods.LogDirDescriptionReplicaTopic(description, index))
                    ?? string.Empty,
                NativeMethods.LogDirDescriptionReplicaPartition(description, index));

            replicas[partition] = new ReplicaInfo(
                NativeMethods.LogDirDescriptionReplicaSize(description, index),
                NativeMethods.LogDirDescriptionReplicaOffsetLag(description, index),
                NativeMethods.LogDirDescriptionReplicaIsFuture(description, index));
        }

        return new LogDirDescription(
            error,
            replicas,
            totalBytes,
            usableBytes,
            NativeMethods.LogDirDescriptionIsCordoned(description));
    }

    /// <summary>
    /// Copies one <c>ReplicaLogDirInfo_t</c> out — the per-key callback's own (owned,
    /// destroyed by the caller after this returns).
    /// </summary>
    /// <param name="info">The info pointer.</param>
    /// <returns>The owned info.</returns>
    /// <exception cref="KafkaException">
    /// <paramref name="info"/> is NULL. ⚠ On the per-key path a NULL info with a NULL error
    /// is <b>reachable</b> since M15/P13.3 — it is how the core reports an <em>absent</em>
    /// replica — but the per-key value reader
    /// (<see cref="AdminCallbacks.ReplicaLogDirInfoPerKeyValue"/>) turns that case into the
    /// absent-key fault <em>before</em> calling this, so this guard is reached only by a
    /// caller that bypasses it.
    /// </exception>
    internal static DescribeReplicaLogDirsResult.ReplicaLogDirInfo CopyOutReplicaInfo(IntPtr info)
    {
        if (info == IntPtr.Zero)
        {
            throw new KafkaException(
                "The describeReplicaLogDirs result carried neither log-directory information nor an error "
                    + "for a replica.");
        }

        // ⚠ Both directory strings are genuinely nullable and must round-trip as null: a
        // replica the broker does not host has no current dir, and a replica at rest has no
        // future dir. Neither becomes "".
        return new DescribeReplicaLogDirsResult.ReplicaLogDirInfo(
            Utf8Marshal.PtrToString(NativeMethods.ReplicaLogDirInfoCurrentReplicaLogDir(info)),
            NativeMethods.ReplicaLogDirInfoCurrentReplicaOffsetLag(info),
            Utf8Marshal.PtrToString(NativeMethods.ReplicaLogDirInfoFutureReplicaLogDir(info)),
            NativeMethods.ReplicaLogDirInfoFutureReplicaOffsetLag(info));
    }

    /// <summary>
    /// Java's <c>OptionalLong</c> mapping for a volume size: <c>-1</c> is
    /// <c>UNKNOWN_VOLUME_BYTES</c> and becomes <see langword="null"/>; every other value —
    /// <b>including <c>0</c></b> — passes through.
    /// </summary>
    /// <param name="bytes">The size the ABI reported.</param>
    /// <returns>The size, or <see langword="null"/> when it was not reported.</returns>
    /// <remarks>
    /// ⚠ Written as an equality test against the sentinel, deliberately, rather than as
    /// "negative means absent" or a falsy check: <c>0</c> is a real size and must survive.
    /// </remarks>
    internal static long? VolumeBytes(long bytes) => bytes == UnknownVolumeBytes ? null : bytes;
}
