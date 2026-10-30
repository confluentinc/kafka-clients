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
using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// A client quota entity, a mapping of entity types to their names — Java's
/// <c>org.apache.kafka.common.quota.ClientQuotaEntity</c>.
/// </summary>
public sealed class ClientQuotaEntity
{
    private readonly Dictionary<string, string?> _entries;

    /// <summary>
    /// Creates a quota entity — Java's constructor. A <c>null</c> entry value names the built-in
    /// default entity for its type, which is neither omitting the type nor the name <c>""</c>.
    /// </summary>
    /// <param name="entries">
    /// Entity type to entity name; a null name means the default entity. The map is copied.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is null.</exception>
    public ClientQuotaEntity(IReadOnlyDictionary<string, string?> entries)
    {
        if (entries is null)
        {
            throw new ArgumentNullException(nameof(entries));
        }

        _entries = new Dictionary<string, string?>(entries.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string?> entry in entries)
        {
            _entries[entry.Key] = entry.Value;
        }
    }

    /// <summary>The <c>user</c> entity type — Java's <c>USER</c>.</summary>
    public const string User = "user";

    /// <summary>The <c>client-id</c> entity type — Java's <c>CLIENT_ID</c>.</summary>
    public const string ClientId = "client-id";

    /// <summary>The <c>ip</c> entity type — Java's <c>IP</c>.</summary>
    public const string Ip = "ip";

    /// <summary>
    /// Entity type to entity name — Java's <c>entries()</c>. A null value names the built-in
    /// default entity for that type.
    /// </summary>
    public IReadOnlyDictionary<string, string?> Entries => _entries;

    /// <summary>
    /// Whether <paramref name="entityType"/> is one of the three known types — Java's
    /// <c>isValidEntityType</c>; null is not valid.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>Whether the type is known.</returns>
    public static bool IsValidEntityType(string? entityType) =>
        string.Equals(entityType, User, StringComparison.Ordinal)
        || string.Equals(entityType, ClientId, StringComparison.Ordinal)
        || string.Equals(entityType, Ip, StringComparison.Ordinal);

    /// <summary>
    /// Value equality over the entries, entry-wise and order-independent — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same entity.</returns>
    public override bool Equals(object? obj)
    {
        if (obj is not ClientQuotaEntity other || _entries.Count != other._entries.Count)
        {
            return false;
        }

        foreach (KeyValuePair<string, string?> entry in _entries)
        {
            if (!other._entries.TryGetValue(entry.Key, out string? otherValue)
                || !string.Equals(entry.Value, otherValue, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The order-independent hash of the entries — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int entriesHash = 0;
            foreach (KeyValuePair<string, string?> entry in _entries)
            {
                entriesHash += StringComparer.Ordinal.GetHashCode(entry.Key)
                    ^ (entry.Value is null ? 0 : StringComparer.Ordinal.GetHashCode(entry.Value));
            }

            return (1 * 31) + entriesHash;
        }
    }

    /// <summary>A diagnostic rendering — Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        var parts = new List<string>(_entries.Count);
        foreach (KeyValuePair<string, string?> entry in _entries)
        {
            parts.Add(string.Concat(entry.Key, "=", entry.Value ?? "null"));
        }

        return string.Concat("ClientQuotaEntity(entries={", string.Join(", ", parts), "})");
    }
}
