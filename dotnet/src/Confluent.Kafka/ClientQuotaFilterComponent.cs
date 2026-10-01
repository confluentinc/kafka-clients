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

namespace Confluent.Kafka;

/// <summary>
/// A component of a client quota filter — the .NET realization of Java's
/// <c>org.apache.kafka.common.quota.ClientQuotaFilterComponent</c> (<c>:26</c>).
/// </summary>
public sealed class ClientQuotaFilterComponent
{
    // Recorded deviation (PLAN D37, definition-of-done.md §7): Java's nullable Optional<String>
    // match() (:88) has three states; a C# string? has two and would collapse Default into
    // Specified. The C ABI's explicit discriminant is exposed instead. Java's three factories
    // remain the only construction path, so the shape a caller writes is unchanged.
    // Private, like Java's ctor (:39) — it makes the illegal fourth state (Exact with no name)
    // unrepresentable.
    private ClientQuotaFilterComponent(string entityType, ClientQuotaMatchType matchType, string? matchName)
    {
        EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
        MatchType = matchType;
        MatchName = matchName;
    }

    /// <summary>The entity type the component applies to — Java's <c>entityType()</c> (<c>:78</c>).</summary>
    public string EntityType { get; }

    /// <summary>Which of the three matches this component performs.</summary>
    public ClientQuotaMatchType MatchType { get; }

    /// <summary>
    /// The exactly-matched entity name; non-null if and only if
    /// <see cref="MatchType"/> is <see cref="ClientQuotaMatchType.Exact"/>.
    /// </summary>
    public string? MatchName { get; }

    /// <summary>
    /// A component matching <paramref name="entityName"/> exactly — Java's <c>ofEntity</c>
    /// (<c>:51</c>).
    /// </summary>
    /// <param name="entityType">The entity type, e.g. <see cref="ClientQuotaEntity.User"/>.</param>
    /// <param name="entityName">The entity name matched exactly.</param>
    /// <returns>The component.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="entityType"/> or <paramref name="entityName"/> is null (Java's
    /// <c>Objects.requireNonNull</c> on both).
    /// </exception>
    public static ClientQuotaFilterComponent OfEntity(string entityType, string entityName)
    {
        if (entityName is null)
        {
            throw new ArgumentNullException(nameof(entityName));
        }

        return new ClientQuotaFilterComponent(entityType, ClientQuotaMatchType.Exact, entityName);
    }

    /// <summary>
    /// A component matching the built-in <em>default</em> entity for the type — Java's
    /// <c>ofDefaultEntity</c> (<c>:61</c>). Distinct from <see cref="OfEntityType"/>.
    /// </summary>
    /// <param name="entityType">The entity type.</param>
    /// <returns>The component.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="entityType"/> is null.</exception>
    public static ClientQuotaFilterComponent OfDefaultEntity(string entityType) =>
        new ClientQuotaFilterComponent(entityType, ClientQuotaMatchType.Default, null);

    /// <summary>
    /// A component matching any <em>named</em> entity of the type — Java's <c>ofEntityType</c>
    /// (<c>:71</c>). Distinct from <see cref="OfDefaultEntity"/>.
    /// </summary>
    /// <param name="entityType">The entity type.</param>
    /// <returns>The component.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="entityType"/> is null.</exception>
    public static ClientQuotaFilterComponent OfEntityType(string entityType) =>
        new ClientQuotaFilterComponent(entityType, ClientQuotaMatchType.Specified, null);

    /// <summary>
    /// Value equality over the type and the match — Java's <c>equals</c> (<c>:93</c>), where
    /// <see cref="MatchType"/> stands in for Java's nullable <c>Optional</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same component.</returns>
    public override bool Equals(object? obj) =>
        obj is ClientQuotaFilterComponent other
        && string.Equals(EntityType, other.EntityType, StringComparison.Ordinal)
        && MatchType == other.MatchType
        && string.Equals(MatchName, other.MatchName, StringComparison.Ordinal);

    /// <summary>
    /// The hash of the type and the match — Java's <c>hashCode</c> (<c>:101</c>), same
    /// <c>31 *</c> fold as <c>Objects.hash</c>. <see cref="MatchType"/> is folded in, so
    /// <see cref="ClientQuotaMatchType.Default"/> and <see cref="ClientQuotaMatchType.Specified"/>
    /// do not collide although both carry a null name.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(EntityType);
            hash = (hash * 31) + ((int)MatchType).GetHashCode();
            hash = (hash * 31) + (MatchName is null ? 0 : StringComparer.Ordinal.GetHashCode(MatchName));
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:106</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Concat(
            "ClientQuotaFilterComponent(entityType=",
            EntityType,
            ", matchType=",
            MatchType.ToString(),
            ", matchName=",
            MatchName ?? "null",
            ")");
}
