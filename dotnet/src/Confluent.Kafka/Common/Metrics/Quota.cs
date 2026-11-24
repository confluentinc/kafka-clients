// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// An upper or lower bound for metrics, Java's <c>org.apache.kafka.common.metrics.Quota</c>. Two quotas are equal when
/// their bounds are equal as doubles (so <c>NaN</c> equals only the same instance) and both are upper or both lower
/// bounds, as in Java.
/// </summary>
/// <remarks>
/// The type shares its name with the namespace <c>Confluent.Kafka.Common.Quota</c>, so code inside another
/// <c>Confluent.Kafka.Common.*</c> namespace writes it <c>Metrics.Quota</c>.
/// </remarks>
public sealed class Quota : IEquatable<Quota>
{
    private readonly bool _upper;
    private readonly double _bound;

    /// <summary>Initializes a quota, as Java's <c>Quota(double bound, boolean upper)</c>.</summary>
    /// <param name="bound">The bound.</param>
    /// <param name="upper"><see langword="true"/> for an upper bound, <see langword="false"/> for a lower one.</param>
    public Quota(double bound, bool upper)
    {
        _bound = bound;
        _upper = upper;
    }

    /// <summary>An upper-bound quota.</summary>
    /// <param name="upperBound">The bound.</param>
    /// <returns>A new quota.</returns>
    public static Quota UpperBound(double upperBound) => new Quota(upperBound, true);

    /// <summary>A lower-bound quota.</summary>
    /// <param name="lowerBound">The bound.</param>
    /// <returns>A new quota.</returns>
    public static Quota LowerBound(double lowerBound) => new Quota(lowerBound, false);

    /// <summary>Whether this quota is an upper bound.</summary>
    /// <returns><see langword="true"/> for an upper bound, <see langword="false"/> for a lower one.</returns>
    public bool IsUpperBound() => _upper;

    /// <summary>The bound.</summary>
    /// <returns>The bound.</returns>
    public double Bound() => _bound;

    /// <summary>Whether <paramref name="value"/> is within this quota.</summary>
    /// <param name="value">The value.</param>
    /// <returns>
    /// <see langword="true"/> when an upper bound is at least <paramref name="value"/>, or a lower bound at most
    /// <paramref name="value"/>.
    /// </returns>
    public bool Acceptable(double value) => (_upper && value <= _bound) || (!_upper && value >= _bound);

    /// <summary>Determines whether two quotas are equal.</summary>
    /// <param name="left">The first quota.</param>
    /// <param name="right">The second quota.</param>
    /// <returns>Whether they are equal, or both <see langword="null"/>.</returns>
    public static bool operator ==(Quota? left, Quota? right) => left is null ? right is null : left.Equals(right);

    /// <summary>Determines whether two quotas differ.</summary>
    /// <param name="left">The first quota.</param>
    /// <param name="right">The second quota.</param>
    /// <returns>Whether they differ.</returns>
    public static bool operator !=(Quota? left, Quota? right) => !(left == right);

    /// <inheritdoc/>
    public bool Equals(Quota? other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        // Java compares the bounds with ==, so -0.0 equals 0.0 and NaN equals nothing but the same instance.
        return other is not null && other._bound == _bound && other._upper == _upper;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Quota other && Equals(other);

    /// <summary>Java's <c>hashCode()</c>, over the bound truncated to an <see cref="int"/> and the kind.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            const int prime = 31;
            int result = 1;
            result = (prime * result) + JavaIntOf(_bound);
            result = (prime * result) + (_upper ? 1 : 0);
            return result;
        }
    }

    /// <summary>Java's <c>toString()</c>: <c>upper=</c> or <c>lower=</c> and the bound, as Java prints a double.</summary>
    /// <returns>The text, such as <c>upper=10.5</c>.</returns>
    public override string ToString() => (_upper ? "upper=" : "lower=") + JavaText.DoubleToString(_bound);

    // Java's (int) cast of a double: NaN is 0, and a value out of range is the nearest int, on every runtime.
    private static int JavaIntOf(double value)
    {
        if (double.IsNaN(value))
        {
            return 0;
        }

        if (value >= int.MaxValue)
        {
            return int.MaxValue;
        }

        return value <= int.MinValue ? int.MinValue : (int)value;
    }
}
