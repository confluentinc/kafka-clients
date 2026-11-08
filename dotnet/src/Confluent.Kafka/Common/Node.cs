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
using System.Globalization;

namespace Confluent.Kafka.Common;

/// <summary>A Kafka node (broker), Java's <c>Node</c>.</summary>
public sealed class Node
{
    private static readonly Node s_noNode = new Node(-1, string.Empty, -1);

    private readonly string _idString;

    /// <summary>Creates a node without a rack, Java's <c>Node(int, String, int)</c>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is null.</exception>
    public Node(int id, string host, int port)
        : this(id, host, port, rack: null, isFenced: false)
    {
    }

    /// <summary>Creates a node, Java's <c>Node(int, String, int, String)</c>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is null.</exception>
    public Node(int id, string host, int port, string? rack)
        : this(id, host, port, rack, isFenced: false)
    {
    }

    /// <summary>Creates a node, Java's <c>Node(int, String, int, String, boolean)</c>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is null.</exception>
    public Node(int id, string host, int port, string? rack, bool isFenced)
    {
        Id = id;
        _idString = id.ToString(CultureInfo.InvariantCulture);
        Host = host ?? throw new ArgumentNullException(nameof(host));
        Port = port;
        Rack = rack;
        IsFenced = isFenced;
    }

    /// <summary>The node with id <c>-1</c>, an empty host and port <c>-1</c>, Java's <c>Node.noNode()</c>.</summary>
    /// <returns>The same placeholder node on every call.</returns>
    public static Node NoNode() => s_noNode;

    /// <summary>
    /// Whether this node is empty, which may be the case if <see cref="NoNode"/> is used as a placeholder in a
    /// response payload with an error: its host is empty or its port negative.
    /// </summary>
    /// <returns>Whether the node is empty.</returns>
    public bool IsEmpty() => string.IsNullOrEmpty(Host) || Port < 0;

    /// <summary>The node id.</summary>
    public int Id { get; }

    /// <summary>
    /// The node id as a string, Java's <c>idString()</c>: the integer id goes over the wire, the string form
    /// identifies the node to the network client.
    /// </summary>
    /// <returns>The node id in decimal.</returns>
    public string IdString() => _idString;

    /// <summary>The node's host name.</summary>
    public string Host { get; }

    /// <summary>The node's port.</summary>
    public int Port { get; }

    /// <summary>Whether this node has a rack, Java's <c>hasRack()</c>.</summary>
    /// <returns>Whether <see cref="Rack"/> is not <see langword="null"/>.</returns>
    public bool HasRack() => Rack is not null;

    /// <summary>The node's rack, or <see langword="null"/> when it has none.</summary>
    public string? Rack { get; }

    /// <summary>Whether the node is fenced, Java's <c>isFenced()</c>.</summary>
    public bool IsFenced { get; }

    /// <summary>Value equality over the id, host, port, rack and fenced flag, Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same node.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is Node other
            && Id == other.Id
            && Port == other.Port
            && string.Equals(Host, other.Host, StringComparison.Ordinal)
            && string.Equals(Rack, other.Rack, StringComparison.Ordinal)
            && IsFenced == other.IsFenced);

    /// <summary>The hash of the fields <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int result = 31 + StringComparer.Ordinal.GetHashCode(Host);
            result = (31 * result) + Id;
            result = (31 * result) + Port;
            result = (31 * result) + (Rack is null ? 0 : StringComparer.Ordinal.GetHashCode(Rack));
            result = (31 * result) + IsFenced.GetHashCode();
            return result;
        }
    }

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0}:{1} (id: {2} rack: {3} isFenced: {4})",
            Host,
            Port,
            _idString,
            Rack ?? "null",
            IsFenced ? "true" : "false");
}
