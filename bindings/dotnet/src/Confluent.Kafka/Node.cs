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

namespace Confluent.Kafka;

/// <summary>
/// Information about a Kafka node (broker) — the .NET realization of Java's
/// <c>org.apache.kafka.common.Node</c>. Appears as the leader and replica entries of a
/// <see cref="PartitionInfo"/>; the fields are owned copies read out of a borrowed
/// (Category-4) ABI <c>Node_t</c>, which is freed with its owning
/// <see cref="PartitionInfo"/> tree (ffi-marshalling.md §B2/§B3).
/// </summary>
/// <remarks>
/// A <c>sealed class</c> (not a <c>readonly struct</c>), matching the
/// <see cref="PartitionInfo"/> / <see cref="OffsetAndTimestamp"/> precedent — a result
/// payload read once, never a hot-path map key. Immutable. Carries
/// <see cref="ToString"/> only (no <c>IEquatable</c>): it is a query-result value, not a
/// dictionary key (the E1 value-type precedent). Value equality can be added later
/// non-breakingly if a concrete need arises.
/// </remarks>
public sealed class Node
{
    /// <summary>
    /// Initializes a new instance from the values copied out of the ABI element.
    /// </summary>
    /// <param name="id">The node (broker) id.</param>
    /// <param name="host">The node host name.</param>
    /// <param name="port">The node port.</param>
    /// <param name="rack">
    /// The rack the node belongs to, or <see langword="null"/> when absent (Java's
    /// nullable <c>rack()</c>).
    /// </param>
    internal Node(int id, string host, int port, string? rack)
    {
        Id = id;
        Host = host;
        Port = port;
        Rack = rack;
    }

    /// <summary>The node (broker) id.</summary>
    public int Id { get; }

    /// <summary>The node host name.</summary>
    public string Host { get; }

    /// <summary>The node port.</summary>
    public int Port { get; }

    /// <summary>
    /// The rack the node belongs to, or <see langword="null"/> when absent (Java's
    /// <c>rack()</c> is nullable).
    /// </summary>
    public string? Rack { get; }

    /// <summary>
    /// Returns a string of the form <c>"host:port (id: N rack: R)"</c>, mirroring Java's
    /// <c>Node.toString()</c>. An absent <see cref="Rack"/> renders as the literal
    /// <c>null</c>, as it does in Java.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0}:{1} (id: {2} rack: {3})",
            Host,
            Port,
            Id,
            Rack ?? "null");
}
