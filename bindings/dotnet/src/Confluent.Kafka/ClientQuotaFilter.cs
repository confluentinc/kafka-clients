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
/// A client quota entity filter — the .NET realization of Java's
/// <c>org.apache.kafka.common.quota.ClientQuotaFilter</c> (<c>:27</c>).
/// </summary>
public sealed class ClientQuotaFilter
{
    private readonly ClientQuotaFilterComponent[] _components;

    // Private, like Java's ctor (:38) — the three factories are the only construction path.
    private ClientQuotaFilter(ClientQuotaFilterComponent[] components, bool strict)
    {
        _components = components;
        Strict = strict;
    }

    /// <summary>The filter's components — Java's <c>components()</c> (<c>:73</c>).</summary>
    public IReadOnlyCollection<ClientQuotaFilterComponent> Components => _components;

    /// <summary>
    /// Whether the filter includes only the specified component types — Java's <c>strict()</c>
    /// (<c>:80</c>).
    /// </summary>
    public bool Strict { get; }

    /// <summary>
    /// A filter matching all the given components, <em>also</em> including entities with entity
    /// types no component specifies — Java's <c>contains</c> (<c>:49</c>).
    /// </summary>
    /// <param name="components">The components to filter on.</param>
    /// <returns>The filter.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="components"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="components"/> contains a null.</exception>
    public static ClientQuotaFilter Contains(IReadOnlyCollection<ClientQuotaFilterComponent> components) =>
        new ClientQuotaFilter(Copy(components), false);

    /// <summary>
    /// A filter matching all the given components and <em>excluding</em> entities with entity
    /// types no component specifies — Java's <c>containsOnly</c> (<c>:59</c>).
    /// </summary>
    /// <param name="components">The components to filter on.</param>
    /// <returns>The filter.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="components"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="components"/> contains a null.</exception>
    public static ClientQuotaFilter ContainsOnly(IReadOnlyCollection<ClientQuotaFilterComponent> components) =>
        new ClientQuotaFilter(Copy(components), true);

    /// <summary>
    /// A filter matching every configured entity — Java's <c>all()</c> (<c>:66</c>): no
    /// components, not strict.
    /// </summary>
    /// <returns>The filter.</returns>
    public static ClientQuotaFilter All() =>
        new ClientQuotaFilter(Array.Empty<ClientQuotaFilterComponent>(), false);

    /// <summary>
    /// Value equality over the components and strictness — Java's <c>equals</c> (<c>:85</c>);
    /// components compare in order, as Java's <c>List.equals</c> does.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same filter.</returns>
    public override bool Equals(object? obj)
    {
        if (obj is not ClientQuotaFilter other
            || Strict != other.Strict
            || _components.Length != other._components.Length)
        {
            return false;
        }

        for (int i = 0; i < _components.Length; i++)
        {
            if (!_components[i].Equals(other._components[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The hash of the components and strictness — Java's <c>hashCode</c> (<c>:93</c>), same
    /// <c>31 *</c> fold as <c>Objects.hash</c> over <c>List.hashCode</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int componentsHash = 1;
            foreach (ClientQuotaFilterComponent component in _components)
            {
                componentsHash = (componentsHash * 31) + component.GetHashCode();
            }

            int hash = 1;
            hash = (hash * 31) + componentsHash;
            hash = (hash * 31) + Strict.GetHashCode();
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:98</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        var parts = new List<string>(_components.Length);
        foreach (ClientQuotaFilterComponent component in _components)
        {
            parts.Add(component.ToString());
        }

        return string.Concat(
            "ClientQuotaFilter(components=[",
            string.Join(", ", parts),
            "], strict=",
            Strict ? "true" : "false",
            ")");
    }

    // Copied so the filter is immutable, and null-checked here rather than at the FFI boundary
    // (ffi-marshalling.md §A5: preconditions are validated before the native call).
    private static ClientQuotaFilterComponent[] Copy(
        IReadOnlyCollection<ClientQuotaFilterComponent> components)
    {
        if (components is null)
        {
            throw new ArgumentNullException(nameof(components));
        }

        var copy = new ClientQuotaFilterComponent[components.Count];
        int i = 0;
        foreach (ClientQuotaFilterComponent component in components)
        {
            copy[i++] = component
                ?? throw new ArgumentException("components must not contain null", nameof(components));
        }

        return copy;
    }
}
