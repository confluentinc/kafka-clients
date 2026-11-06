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
/// <remarks>Unlike Java's <c>Node</c>, it has no value equality.</remarks>
public sealed class Node
{
    private readonly int _id;
    private readonly string _host;
    private readonly int _port;
    private readonly string? _rack;
    private readonly bool _isFenced;

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
        _id = id;
        _host = host ?? throw new ArgumentNullException(nameof(host));
        _port = port;
        _rack = rack;
        _isFenced = isFenced;
    }

    /// <summary>The node id.</summary>
    /// <returns>The node id.</returns>
    public int Id() => _id;

    /// <summary>The node's host name.</summary>
    /// <returns>The node's host name.</returns>
    public string Host() => _host;

    /// <summary>The node's port.</summary>
    /// <returns>The node's port.</returns>
    public int Port() => _port;

    /// <summary>The node's rack, or <see langword="null"/> when it has none.</summary>
    /// <returns>The node's rack, or <see langword="null"/> when it has none.</returns>
    public string? Rack() => _rack;

    /// <summary>Whether the node is fenced, Java's <c>isFenced()</c>.</summary>
    /// <returns>Whether the node is fenced.</returns>
    public bool IsFenced() => _isFenced;

    /// <inheritdoc/>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0}:{1} (id: {2} rack: {3} isFenced: {4})",
            Host(),
            Port(),
            Id(),
            Rack() ?? "null",
            IsFenced() ? "true" : "false");
}
