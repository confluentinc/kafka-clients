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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for describing producers — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeProducersOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeProducersOptions
{
    private int? _timeoutMs;
    private int? _brokerId;

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
    /// The broker to query, or <see langword="null"/> — the default — to query each
    /// partition's leader. Java's <c>brokerId(int)</c> and <c>brokerId()</c>
    /// (an <c>OptionalInt</c> defaulting to <c>empty()</c>).
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// The value set is <see langword="null"/>: Java's setter takes an <c>int</c>, so a set broker id cannot be
    /// unset again.
    /// </exception>
    public int? BrokerId
    {
        get => _brokerId;
        set
        {
            _brokerId = value ?? throw new ArgumentNullException(nameof(value));
            Setters.Ran(nameof(BrokerId));
        }
    }

    /// <summary>
    /// Value equality over the broker id <em>and</em> the timeout — Java's <c>equals</c>,
    /// which includes <c>timeoutMs</c> (unlike
    /// <see cref="ListTransactionsOptions.Equals(object?)"/>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two options are equal.</returns>
    public override bool Equals(object? obj) =>
        obj is DescribeProducersOptions other
        && BrokerId == other.BrokerId
        && TimeoutMs == other.TimeoutMs;

    /// <summary>The hash of the broker id and the timeout — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return ((BrokerId?.GetHashCode() ?? 0) * 31) + (TimeoutMs?.GetHashCode() ?? 0);
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>: the broker id prints as
    /// <c>OptionalInt[3]</c> or <c>OptionalInt.empty</c>, the timeout as a number or
    /// <c>null</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "DescribeProducersOptions(brokerId={0}, timeoutMs={1})",
            BrokerId.HasValue
                ? "OptionalInt[" + BrokerId.Value.ToString(CultureInfo.InvariantCulture) + "]"
                : "OptionalInt.empty",
            TimeoutMs.HasValue ? TimeoutMs.Value.ToString(CultureInfo.InvariantCulture) : "null");

    internal OptionSetters Setters { get; } = new OptionSetters();
}
