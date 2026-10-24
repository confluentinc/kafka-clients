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
/// Options for <see cref="IAdmin.ListTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListTopicsOptions</c>.
/// </summary>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks"/>
public sealed class ListTopicsOptions
{
    private int? _timeoutMs;

    private bool _listInternal;

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
    /// Include internal topics such as <c>__consumer_offsets</c> — Java's
    /// <c>listInternal(boolean)</c> / <c>shouldListInternal()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ListInternal
    {
        get => _listInternal;
        set
        {
            _listInternal = value;
            Setters.Ran(nameof(ListInternal));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();

    /// <summary>
    /// Value equality over <see cref="ListInternal"/> only, as Java's <c>equals</c>: two
    /// options that differ only in their timeout are equal.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two options ask for the same listing.</returns>
    public override bool Equals(object? obj) =>
        obj is ListTopicsOptions other && ListInternal == other.ListInternal;

    /// <summary>
    /// The hash of <see cref="ListInternal"/> alone — Java's <c>hashCode</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode() => ListInternal.GetHashCode();

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>, which leaves out the timeout.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        "ListTopicsOptions(listInternal=" + (ListInternal ? "true" : "false") + ")";
}
