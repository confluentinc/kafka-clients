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
using System.Collections.ObjectModel;
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A resource's configuration — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.Config</c>: a collection of
/// <see cref="ConfigEntry"/> plus lookup by name.
/// </summary>
/// <remarks>
/// <para>
/// Java's <c>Config</c> is a <c>HashMap</c> keyed by entry name (<c>Config.java:31-40</c>),
/// so this type has map semantics too: <b>one entry per name</b>, the <b>last</b>
/// occurrence winning, and value equality over the name → entry mapping regardless of
/// order.
/// </para>
/// <para>
/// Unlike Java's <c>HashMap</c>, whose iteration order is unspecified,
/// <see cref="Entries"/> keeps a deterministic order: the order the entries were supplied
/// in, a replaced duplicate keeping its <b>first</b> occurrence's position. So the ABI's
/// sorted-by-name order survives unchanged whenever there are no duplicates.
/// </para>
/// <para>
/// Value members are overrides only — there is no <c>IEquatable&lt;Config&gt;</c>, the
/// same shape as <see cref="NewTopic"/>.
/// </para>
/// </remarks>
public sealed class Config
{
    private readonly Dictionary<string, ConfigEntry> _byName;
    private readonly ReadOnlyCollection<ConfigEntry> _entries;

    /// <summary>
    /// Initializes a configuration from its entries (Java's
    /// <c>Config(Collection&lt;ConfigEntry&gt;)</c>). The entries are copied, so the
    /// instance is immutable regardless of what the caller does next.
    /// </summary>
    /// <param name="entries">
    /// The configuration entries. Two entries with the same name are one entry: the last
    /// wins, as Java's <c>entries.put(entry.name(), entry)</c> does
    /// (<c>Config.java:36-40</c>), and it takes the first one's position in
    /// <see cref="Entries"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="entries"/> is null, or contains a null element.
    /// </exception>
    public Config(IEnumerable<ConfigEntry> entries)
    {
        if (entries is null)
        {
            throw new ArgumentNullException(nameof(entries));
        }

        List<ConfigEntry> ordered = new List<ConfigEntry>();
        Dictionary<string, int> positions = new Dictionary<string, int>(StringComparer.Ordinal);
        _byName = new Dictionary<string, ConfigEntry>(StringComparer.Ordinal);
        foreach (ConfigEntry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentNullException(nameof(entries), "Config entries must not contain a null element.");
            }

            if (positions.TryGetValue(entry.Name, out int position))
            {
                ordered[position] = entry;
            }
            else
            {
                positions.Add(entry.Name, ordered.Count);
                ordered.Add(entry);
            }

            _byName[entry.Name] = entry;
        }

        _entries = new ReadOnlyCollection<ConfigEntry>(ordered);
    }

    /// <summary>
    /// The configuration entries (Java's <c>entries()</c>), one per name. As the ABI
    /// reports them: sorted by name. Read-only, as Java's
    /// <c>Collections.unmodifiableCollection(entries.values())</c> is
    /// (<c>Config.java:45-47</c>) — the view cannot be cast back to a mutable list.
    /// </summary>
    public IReadOnlyCollection<ConfigEntry> Entries => _entries;

    /// <summary>
    /// The entry with the given name, or <see langword="null"/> if the configuration
    /// has none — Java's <c>get(String)</c>, which likewise returns null rather than
    /// throwing.
    /// </summary>
    /// <param name="name">
    /// The configuration key. A <see langword="null"/> key returns <see langword="null"/>,
    /// as Java's <c>HashMap.get(null)</c> does (<c>Config.java:52-54</c>): no entry has a
    /// null name.
    /// </param>
    /// <returns>The matching entry, or <see langword="null"/>.</returns>
    public ConfigEntry? Get(string? name)
    {
        if (name is null)
        {
            return null;
        }

        return _byName.TryGetValue(name, out ConfigEntry? entry) ? entry : null;
    }

    /// <summary>
    /// Value equality — Java's <c>equals</c> (<c>Config.java:56-66</c>), which is map
    /// equality: the same set of names, each mapping to an equal
    /// <see cref="ConfigEntry"/>. Order does not matter.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two configurations hold the same entries.</returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not Config other || _byName.Count != other._byName.Count)
        {
            return false;
        }

        foreach (KeyValuePair<string, ConfigEntry> pair in _byName)
        {
            if (!other._byName.TryGetValue(pair.Key, out ConfigEntry? otherEntry) || !pair.Value.Equals(otherEntry))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// An order-insensitive hash — Java's <c>hashCode</c> (<c>Config.java:68-71</c>) is the
    /// map's, which <c>AbstractMap</c> defines as the sum over the entries of
    /// <c>key.hashCode() ^ value.hashCode()</c>; this sums the same combination with the
    /// .NET hashes.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        int hash = 0;
        foreach (KeyValuePair<string, ConfigEntry> pair in _byName)
        {
            unchecked
            {
                hash += StringComparer.Ordinal.GetHashCode(pair.Key) ^ pair.Value.GetHashCode();
            }
        }

        return hash;
    }

    /// <summary>
    /// A diagnostic rendering in Java's layout — <c>"Config(entries=" + entries.values() + ")"</c>
    /// (<c>Config.java:73-76</c>): <c>Config(entries=[e1, e2])</c>, each entry rendered by
    /// <see cref="ConfigEntry.ToString"/>, in <see cref="Entries"/> order.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        StringBuilder builder = new StringBuilder("Config(entries=[");
        for (int i = 0; i < _entries.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(_entries[i].ToString());
        }

        return builder.Append("])").ToString();
    }
}
