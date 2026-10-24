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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing a consumer group's committed offsets — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class ListConsumerGroupOffsetsOptions
{
    private int? _timeoutMs;
    private bool _requireStable;

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
    /// Wait for any in-progress transaction on a partition to resolve before reporting its
    /// committed offset — Java's <c>requireStable()</c>. Defaults to
    /// <see langword="false"/>, as Java's explicitly initialized field does.
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/> the broker answers at once with the last
    /// committed offset, which an open transaction on the partition may still change.
    /// </remarks>
    public bool RequireStable
    {
        get => _requireStable;
        set
        {
            _requireStable = value;
            Setters.Ran(nameof(RequireStable));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
