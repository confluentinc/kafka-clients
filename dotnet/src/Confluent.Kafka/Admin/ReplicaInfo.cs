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

using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one replica in a log directory — Java's
/// <c>org.apache.kafka.clients.admin.ReplicaInfo</c>.
/// </summary>
public sealed class ReplicaInfo
{
    private readonly long _size;
    private readonly long _offsetLag;
    private readonly bool _isFuture;

    /// <summary>Initializes a replica description — Java's constructor.</summary>
    /// <param name="size">The total size of this replica's log segments, in bytes.</param>
    /// <param name="offsetLag">The replica's offset lag.</param>
    /// <param name="isFuture">Whether this is a future replica being moved in.</param>
    public ReplicaInfo(long size, long offsetLag, bool isFuture)
    {
        _size = size;
        _offsetLag = offsetLag;
        _isFuture = isFuture;
    }

    /// <summary>
    /// The total size of this replica's log segments in bytes — Java's <c>size()</c>. Does not
    /// include data in remote storage.
    /// </summary>
    /// <returns>The total size of this replica's log segments in bytes.</returns>
    public long Size() => _size;

    /// <summary>
    /// The lag of the log's end offset behind the partition's high watermark — or, for a
    /// future replica, behind the current replica's end offset. Java's <c>offsetLag()</c>.
    /// </summary>
    /// <returns>The lag of the log's end offset behind the partition's high watermark — or, for a future replica,
    /// behind the current replica's end offset.</returns>
    public long OffsetLag() => _offsetLag;

    /// <summary>
    /// Whether this replica was created by an <c>alterReplicaLogDirs</c> move and has not
    /// yet replaced the current replica — Java's <c>isFuture()</c>.
    /// </summary>
    /// <returns>Whether this replica was created by an <c>alterReplicaLogDirs</c> move and has not yet replaced the
    /// current replica.</returns>
    public bool IsFuture() => _isFuture;

    /// <summary>A diagnostic rendering — Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ReplicaInfo(size={0}, offsetLag={1}, isFuture={2})",
            Size(),
            OffsetLag(),
            IsFuture() ? "true" : "false");
}
