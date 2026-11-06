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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one log directory on one broker — Java's
/// <c>org.apache.kafka.clients.admin.LogDirDescription</c>.
/// </summary>
/// <remarks>
/// <see cref="Error()"/> is a field of a description the broker returned, not a failure of the
/// query: it means this log directory is offline or unreadable (Java's
/// <c>KafkaStorageException</c> or <c>UnknownServerException</c>). The per-broker future in
/// <see cref="DescribeLogDirsResult.Descriptions"/> and
/// <see cref="AsyncDescribeLogDirsResult.Descriptions"/> succeeds, as in Java.
/// </remarks>
public sealed class LogDirDescription
{
    private const long UnknownVolumeBytes = -1L;

    private readonly ApiException? _error;
    private readonly IReadOnlyDictionary<TopicPartition, ReplicaInfo> _replicaInfos;
    private readonly long? _totalBytes;
    private readonly long? _usableBytes;
    private readonly bool _isCordoned;

    /// <summary>
    /// Initializes a description with unreported volume sizes and an uncordoned directory —
    /// Java's 2-argument constructor, which passes <c>UNKNOWN_VOLUME_BYTES</c> for both sizes and
    /// <c>false</c> for <c>isCordoned</c>.
    /// </summary>
    /// <param name="error">
    /// The directory-level error, or <see langword="null"/> if the directory is healthy.
    /// </param>
    /// <param name="replicaInfos">The replicas hosted in this directory.</param>
    /// <exception cref="ArgumentNullException"><paramref name="replicaInfos"/> is null.</exception>
    public LogDirDescription(
        ApiException? error, IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos)
        : this(error, replicaInfos, null, null, false)
    {
    }

    /// <summary>
    /// Initializes an uncordoned description — Java's 4-argument constructor, which passes
    /// <c>false</c> for <c>isCordoned</c>.
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
        ApiException? error,
        IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos,
        long? totalBytes,
        long? usableBytes)
        : this(error, replicaInfos, totalBytes, usableBytes, false)
    {
    }

    /// <summary>
    /// Initializes a description — Java's 5-argument constructor.
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
    /// A size of <c>-1</c>, Java's <c>UNKNOWN_VOLUME_BYTES</c>, is stored as
    /// <see langword="null"/>, as Java's constructor turns it into <c>OptionalLong.empty()</c>;
    /// every other value, <c>0</c> and other negatives included, is kept as given.
    /// </para>
    /// <para>
    /// A null <paramref name="replicaInfos"/> throws, stricter than Java, which fails later in
    /// <c>replicaInfos()</c>: a recorded deviation.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="replicaInfos"/> is null.</exception>
    public LogDirDescription(
        ApiException? error,
        IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos,
        long? totalBytes,
        long? usableBytes,
        bool isCordoned)
    {
        if (replicaInfos is null)
        {
            throw new ArgumentNullException(nameof(replicaInfos));
        }

        _error = error;

        _replicaInfos = replicaInfos is IDictionary<TopicPartition, ReplicaInfo> mutable
            ? new ReadOnlyDictionary<TopicPartition, ReplicaInfo>(mutable)
            : replicaInfos;
        _totalBytes = totalBytes == UnknownVolumeBytes ? null : totalBytes;
        _usableBytes = usableBytes == UnknownVolumeBytes ? null : usableBytes;
        _isCordoned = isCordoned;
    }

    /// <summary>
    /// The directory-level error, or <see langword="null"/> when the directory is healthy —
    /// Java's <c>error()</c>.
    /// </summary>
    /// <returns>The directory-level error, or <see langword="null"/> when the directory is healthy.</returns>
    /// <remarks>
    /// Non-null does not mean the query failed: see the type remarks.
    /// </remarks>
    public ApiException? Error() => _error;

    /// <summary>
    /// The replicas hosted in this directory, keyed by topic partition — Java's
    /// <c>replicaInfos()</c>.
    /// </summary>
    /// <returns>The replicas hosted in this directory, keyed by topic partition.</returns>
    /// <remarks>
    /// A read-only view over the caller's map, as Java's <c>Collections.unmodifiableMap</c> is:
    /// the caller's own changes stay visible, and a change through a cast back to a mutable
    /// dictionary throws <see cref="NotSupportedException"/>.
    /// </remarks>
    public IReadOnlyDictionary<TopicPartition, ReplicaInfo> ReplicaInfos() => _replicaInfos;

    /// <summary>
    /// The total size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>totalBytes()</c>, an <c>OptionalLong</c>.
    /// </summary>
    /// <returns>The total size in bytes of the volume this directory is on, or <see langword="null"/> when the
    /// broker did not report it — Java's <c>totalBytes()</c>, an <c>OptionalLong</c>.</returns>
    /// <remarks>
    /// Java's <c>UNKNOWN_VOLUME_BYTES</c> (<c>-1</c>), which Java's constructor turns into
    /// <c>OptionalLong.empty()</c>, becomes <see langword="null"/>; <c>0</c> is a real size.
    /// Unlike <see cref="DeletedRecords.LowWatermark()"/>, where <c>-1</c> is a value.
    /// </remarks>
    public long? TotalBytes() => _totalBytes;

    /// <summary>
    /// The usable size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>usableBytes()</c>.
    /// </summary>
    /// <remarks>
    /// <inheritdoc cref="TotalBytes()" path="/remarks"/>
    /// </remarks>
    public long? UsableBytes() => _usableBytes;

    /// <summary>
    /// Whether this log directory is cordoned — Java's <c>isCordoned()</c>.
    /// </summary>
    /// <returns>Whether this log directory is cordoned.</returns>
    public bool IsCordoned() => _isCordoned;

    /// <summary>
    /// A diagnostic rendering following Java's <c>toString()</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        StringBuilder replicas = new StringBuilder("{");
        bool first = true;
        foreach (KeyValuePair<TopicPartition, ReplicaInfo> replica in _replicaInfos)
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
            _error is null ? "null" : _error.Message,
            _totalBytes is null ? "empty" : _totalBytes.Value.ToString(CultureInfo.InvariantCulture),
            _usableBytes is null ? "empty" : _usableBytes.Value.ToString(CultureInfo.InvariantCulture),
            _isCordoned ? "true" : "false");
    }
}
