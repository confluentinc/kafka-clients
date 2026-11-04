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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeFeaturesOptions</c>.
/// </summary>
public sealed class DescribeFeaturesOptions
{
    private int? _timeoutMs;
    private int? _nodeId;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs
    {
        get => _timeoutMs;
        set
        {
            _timeoutMs = value;
            Setters.Ran(nameof(TimeoutMs));
        }
    }

    /// <summary>
    /// The broker to query, or <see langword="null"/> while unset, when the client chooses — Java's
    /// <c>nodeId()</c>, an <c>OptionalInt</c>, and <c>nodeId(int)</c>.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// The value set is <see langword="null"/>: Java's setter takes an <c>int</c>, so a set node id cannot be
    /// unset again.
    /// </exception>
    public int? NodeId
    {
        get => _nodeId;
        set
        {
            _nodeId = value ?? throw new ArgumentNullException(nameof(value));
            Setters.Ran(nameof(NodeId));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
