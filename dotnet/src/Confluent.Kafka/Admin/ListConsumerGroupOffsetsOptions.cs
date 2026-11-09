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

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public ListConsumerGroupOffsetsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Wait for any in-progress transaction on a partition to resolve before reporting its
    /// committed offset — Java's <c>requireStable()</c>. Defaults to
    /// <see langword="false"/>, as Java's explicitly initialized field does.
    /// </summary>
    /// <returns>Wait for any in-progress transaction on a partition to resolve before reporting its committed
    /// offset — Java's <c>requireStable()</c>. Defaults to <see langword="false"/>, as Java's explicitly
    /// initialized field does.</returns>
    /// <remarks>
    /// While this is <see langword="false"/> the broker answers at once with the last
    /// committed offset, which an open transaction on the partition may still change.
    /// </remarks>
    public bool RequireStable() => _requireStable;

    /// <summary>Sets an optional requireStable flag.</summary>
    /// <param name="requireStable">Whether to wait for pending transactions to resolve. Defaults to
    /// <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ListConsumerGroupOffsetsOptions RequireStable(bool requireStable)
    {
        _requireStable = requireStable;
        Setters.Ran(nameof(RequireStable));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
