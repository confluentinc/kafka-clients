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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// A regular expression compatible with Google RE2/J, used to subscribe to topics, Java's
/// <c>org.apache.kafka.clients.consumer.SubscriptionPattern</c>. It keeps only the pattern's text; the broker
/// validates it.
/// </summary>
public sealed class SubscriptionPattern
{
    private readonly string _pattern;

    /// <summary>
    /// Creates a subscription pattern, storing the argument as given, <see langword="null"/> included.
    /// </summary>
    /// <param name="pattern">The regular expression pattern compatible with RE2/J.</param>
    public SubscriptionPattern(string pattern)
    {
        _pattern = pattern;
    }

    /// <summary>The regular expression pattern compatible with RE2/J.</summary>
    /// <returns>The pattern, as given to the constructor.</returns>
    public string Pattern() => _pattern;

    /// <summary>
    /// Whether <paramref name="obj"/> is a subscription pattern with the same text, compared ordinally.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two patterns are equal; two <see langword="null"/> patterns are.</returns>
    public override bool Equals(object? obj) =>
        obj is SubscriptionPattern other && string.Equals(_pattern, other._pattern, StringComparison.Ordinal);

    /// <summary>The ordinal hash of the pattern.</summary>
    /// <returns>The hash code.</returns>
    /// <exception cref="ArgumentNullException">The pattern is <see langword="null"/>.</exception>
    public override int GetHashCode() =>
        _pattern is null ? throw new ArgumentNullException("pattern") : StringComparer.Ordinal.GetHashCode(_pattern);

    /// <summary>The pattern, or <c>"null"</c> for a <see langword="null"/> pattern.</summary>
    /// <returns>The pattern's text.</returns>
    public override string ToString() => _pattern ?? "null";
}
