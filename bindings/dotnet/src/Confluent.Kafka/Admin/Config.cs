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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A resource's configuration — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.Config</c>: a collection of
/// <see cref="ConfigEntry"/> plus lookup by name.
/// </summary>
public sealed class Config
{
    private readonly Dictionary<string, ConfigEntry> _byName;
    private readonly List<ConfigEntry> _entries;

    /// <summary>
    /// Initializes a configuration from its entries (Java's
    /// <c>Config(Collection&lt;ConfigEntry&gt;)</c>). The entries are copied, so the
    /// instance is immutable regardless of what the caller does next.
    /// </summary>
    /// <param name="entries">The configuration entries.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="entries"/> is null, or contains a null element.
    /// </exception>
    public Config(IEnumerable<ConfigEntry> entries)
    {
        if (entries is null)
        {
            throw new ArgumentNullException(nameof(entries));
        }

        _entries = new List<ConfigEntry>();
        _byName = new Dictionary<string, ConfigEntry>(StringComparer.Ordinal);
        foreach (ConfigEntry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentNullException(nameof(entries), "Config entries must not contain a null element.");
            }

            _entries.Add(entry);
            _byName[entry.Name] = entry;
        }
    }

    /// <summary>
    /// The configuration entries (Java's <c>entries()</c>). As the ABI reports them:
    /// sorted by name.
    /// </summary>
    public IReadOnlyCollection<ConfigEntry> Entries => _entries;

    /// <summary>
    /// The entry with the given name, or <see langword="null"/> if the configuration
    /// has none — Java's <c>get(String)</c>, which likewise returns null rather than
    /// throwing.
    /// </summary>
    /// <param name="name">The configuration key.</param>
    /// <returns>The matching entry, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public ConfigEntry? Get(string name)
    {
        if (name is null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        return _byName.TryGetValue(name, out ConfigEntry? entry) ? entry : null;
    }
}
