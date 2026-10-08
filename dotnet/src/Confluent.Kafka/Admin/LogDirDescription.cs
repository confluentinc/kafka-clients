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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one log directory on one broker — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.LogDirDescription</c>
/// (<c>LogDirDescription.java:31</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b><see cref="Error"/> is a FIELD on a successfully-returned description, not a
/// failure of the query.</b> It means <em>this log directory</em> is offline or unreadable
/// while the broker's query itself succeeded — Java documents
/// <c>KafkaStorageException</c> ("the log directory is offline") and
/// <c>UnknownServerException</c> as its two values (<c>:54-59</c>). The per-broker
/// task in <see cref="DescribeLogDirsResult.Descriptions"/> therefore
/// <b>succeeds</b>, carrying a description whose <see cref="Error"/> is non-null. Faulting
/// the task here would be a behavioural divergence — the broker answered.
/// </para>
/// <para>
/// <b>Accessors are properties</b> (decision D18) — each is a pure managed field read.
/// </para>
/// </remarks>
public sealed class LogDirDescription
{
    /// <summary>
    /// Java's <c>UNKNOWN_VOLUME_BYTES</c> (<c>DescribeLogDirsResponse.java</c>, imported at
    /// <c>LogDirDescription.java:26</c>), the "did not report" volume size.
    /// </summary>
    private const long UnknownVolumeBytes = -1L;

    /// <summary>
    /// Initializes a description with unreported volume sizes and an uncordoned directory —
    /// Java's 2-arg constructor (<c>:38</c>), which passes <c>UNKNOWN_VOLUME_BYTES</c> for both
    /// sizes and <c>false</c> for <c>isCordoned</c>.
    /// </summary>
    /// <param name="error">
    /// The directory-level error, or <see langword="null"/> if the directory is healthy.
    /// </param>
    /// <param name="replicaInfos">The replicas hosted in this directory.</param>
    /// <exception cref="ArgumentNullException"><paramref name="replicaInfos"/> is null.</exception>
    public LogDirDescription(
        KafkaException? error, IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos)
        : this(error, replicaInfos, null, null, false)
    {
    }

    /// <summary>
    /// Initializes an uncordoned description — Java's 4-arg constructor (<c>:42</c>), which
    /// passes <c>false</c> for <c>isCordoned</c>.
    /// </summary>
    /// <param name="error">
    /// The directory-level error, or <see langword="null"/> if the directory is healthy.
    /// </param>
    /// <param name="replicaInfos">The replicas hosted in this directory.</param>
    /// <param name="totalBytes">
    /// The volume's total size, or <see langword="null"/> — or Java's <c>-1</c> — if the
    /// broker did not report it.
    /// </param>
    /// <param name="usableBytes">
    /// The volume's usable size, or <see langword="null"/> — or Java's <c>-1</c> — if the
    /// broker did not report it.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="replicaInfos"/> is null.</exception>
    public LogDirDescription(
        KafkaException? error,
        IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos,
        long? totalBytes,
        long? usableBytes)
        : this(error, replicaInfos, totalBytes, usableBytes, false)
    {
    }

    /// <summary>
    /// Initializes a description — Java's 5-arg constructor (<c>:46</c>).
    /// </summary>
    /// <param name="error">
    /// The directory-level error, or <see langword="null"/> if the directory is healthy.
    /// </param>
    /// <param name="replicaInfos">The replicas hosted in this directory.</param>
    /// <param name="totalBytes">
    /// The volume's total size, or <see langword="null"/> — or Java's <c>-1</c> — if the
    /// broker did not report it.
    /// </param>
    /// <param name="usableBytes">
    /// The volume's usable size, or <see langword="null"/> — or Java's <c>-1</c> — if the
    /// broker did not report it.
    /// </param>
    /// <param name="isCordoned">Whether this log directory is cordoned.</param>
    /// <remarks>
    /// <para>
    /// <b>Public, with Java's three arities</b> (2, 4 and 5 — <c>:38</c>, <c>:42</c>,
    /// <c>:46</c>), so a caller can build one for a fake or a test, as Java's public
    /// constructors allow; its siblings <see cref="ReplicaInfo"/> and
    /// <see cref="PartitionReassignment"/> are likewise publicly constructible.
    /// </para>
    /// <para>
    /// The sizes are <c>long?</c> because <see cref="TotalBytes"/> / <see cref="UsableBytes"/>
    /// are (see <see cref="TotalBytes"/>). Java's sentinel <c>-1</c>
    /// (<c>UNKNOWN_VOLUME_BYTES</c>) is accepted too and stored as <see langword="null"/>, as
    /// Java's constructor turns it into <c>OptionalLong.empty()</c> (<c>:49-50</c>), so a
    /// Java-ported <c>(…, -1, -1)</c> means the same here. Every other value, <c>0</c> and
    /// other negatives included, is kept as given, as Java keeps it.
    /// </para>
    /// <para>
    /// ⚠ A null <paramref name="replicaInfos"/> is rejected with
    /// <see cref="ArgumentNullException"/>, stricter than Java, which stores it and fails
    /// later, in <c>replicaInfos()</c>'s <c>unmodifiableMap(null)</c> (<c>:70</c>) — the
    /// <see cref="PartitionReassignment"/> precedent for a null collection argument.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="replicaInfos"/> is null.</exception>
    public LogDirDescription(
        KafkaException? error,
        IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos,
        long? totalBytes,
        long? usableBytes,
        bool isCordoned)
    {
        if (replicaInfos is null)
        {
            throw new ArgumentNullException(nameof(replicaInfos));
        }

        Error = error;

        // Java returns Collections.unmodifiableMap over the stored map (:69-70): a read-only
        // view, so the map cannot be changed through this property, while changes the caller
        // makes to its own map stay visible, as they do in Java.
        ReplicaInfos = replicaInfos is IDictionary<TopicPartition, ReplicaInfo> mutable
            ? new ReadOnlyDictionary<TopicPartition, ReplicaInfo>(mutable)
            : replicaInfos;
        TotalBytes = totalBytes == UnknownVolumeBytes ? null : totalBytes;
        UsableBytes = usableBytes == UnknownVolumeBytes ? null : usableBytes;
        IsCordoned = isCordoned;
    }

    /// <summary>
    /// The directory-level error, or <see langword="null"/> when the directory is healthy —
    /// Java's <c>error()</c> (<c>:61</c>), whose type is <c>ApiException</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="KafkaException"/> is this binding's established mapping for Java's
    /// <c>ApiException</c> (the M15/P1 <c>TopicMetadataAndConfig</c> precedent); no second
    /// exception type is introduced for it. ⚠ Non-null here does <b>not</b> mean the query
    /// failed — see the type remarks.
    /// </remarks>
    public KafkaException? Error { get; }

    /// <summary>
    /// The replicas hosted in this directory, keyed by topic partition — Java's
    /// <c>replicaInfos()</c> (<c>:69</c>).
    /// </summary>
    /// <remarks>
    /// A read-only view, as Java's <c>Collections.unmodifiableMap</c> is: casting it back to a
    /// mutable dictionary and changing it throws <see cref="NotSupportedException"/>, the .NET
    /// counterpart of Java's <c>UnsupportedOperationException</c>.
    /// </remarks>
    public IReadOnlyDictionary<TopicPartition, ReplicaInfo> ReplicaInfos { get; }

    /// <summary>
    /// The total size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>totalBytes()</c> (<c>:78</c>), an <c>OptionalLong</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>OptionalLong</c> has no netstandard2.0 equivalent, so the mapping is
    /// <c>long?</c>, and the ABI's <c>-1</c> — Java's <c>UNKNOWN_VOLUME_BYTES</c>, which its
    /// constructor turns into <c>OptionalLong.empty()</c> (<c>:49</c>) — becomes
    /// <see langword="null"/>. ⚠ <b>Contrast <see cref="DeletedRecords.LowWatermark"/>,
    /// where <c>-1</c> is a legitimate value and not a sentinel at all.</b> Here a volume
    /// size cannot legitimately be negative, and the header names <c>-1</c> as the "did not
    /// report" marker. <c>0</c> is a real size and stays <c>0</c>.
    /// </remarks>
    public long? TotalBytes { get; }

    /// <summary>
    /// The usable size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>usableBytes()</c> (<c>:87</c>).
    /// </summary>
    /// <remarks>
    /// <inheritdoc cref="TotalBytes" path="/remarks"/>
    /// </remarks>
    public long? UsableBytes { get; }

    /// <summary>
    /// Whether this log directory is cordoned — Java's <c>isCordoned()</c> (<c>:94</c>),
    /// a plain <c>boolean</c>.
    /// </summary>
    public bool IsCordoned { get; }

    /// <summary>
    /// A diagnostic rendering following Java's <c>toString()</c> (<c>:98</c>).
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        StringBuilder replicas = new StringBuilder("{");
        bool first = true;
        foreach (KeyValuePair<TopicPartition, ReplicaInfo> replica in ReplicaInfos)
        {
            if (!first)
            {
                replicas.Append(", ");
            }

            first = false;
            replicas.Append(replica.Key).Append('=').Append(replica.Value);
        }

        replicas.Append('}');

        return string.Format(
            CultureInfo.InvariantCulture,
            "LogDirDescription(replicaInfos={0}, error={1}, totalBytes={2}, usableBytes={3}, isCordoned={4})",
            replicas,
            Error is null ? "null" : Error.Message,
            TotalBytes is null ? "empty" : TotalBytes.Value.ToString(CultureInfo.InvariantCulture),
            UsableBytes is null ? "empty" : UsableBytes.Value.ToString(CultureInfo.InvariantCulture),
            // Java renders the Boolean lowercase; C#'s bool.ToString() would give "True"/"False".
            IsCordoned ? "true" : "false");
    }
}
