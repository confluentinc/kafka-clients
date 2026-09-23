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
using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// A configuration alteration to be made to a client quota entity — the .NET realization of
/// Java's <c>org.apache.kafka.common.quota.ClientQuotaAlteration</c> (<c>:26</c>).
/// </summary>
public sealed class ClientQuotaAlteration
{
    private readonly Op[] _ops;

    /// <summary>
    /// Creates an alteration — Java's <c>ClientQuotaAlteration(ClientQuotaEntity, Collection)</c>
    /// (<c>:83</c>).
    /// </summary>
    /// <param name="entity">The entity whose configuration will be modified.</param>
    /// <param name="ops">The alterations to perform.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="entity"/> or <paramref name="ops"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="ops"/> contains a null.</exception>
    public ClientQuotaAlteration(ClientQuotaEntity entity, IReadOnlyCollection<Op> ops)
    {
        Entity = entity ?? throw new ArgumentNullException(nameof(entity));

        if (ops is null)
        {
            throw new ArgumentNullException(nameof(ops));
        }

        // Copied so the alteration is immutable; null elements are rejected here rather than at
        // the FFI boundary (ffi-marshalling.md §A5).
        _ops = new Op[ops.Count];
        int i = 0;
        foreach (Op op in ops)
        {
            _ops[i++] = op ?? throw new ArgumentException("ops must not contain null", nameof(ops));
        }
    }

    /// <summary>The entity whose configuration will be modified — Java's <c>entity()</c> (<c>:91</c>).</summary>
    public ClientQuotaEntity Entity { get; }

    /// <summary>The alterations to perform — Java's <c>ops()</c> (<c>:98</c>).</summary>
    public IReadOnlyCollection<Op> Ops => _ops;

    // No Equals / GetHashCode override: Java declares neither on ClientQuotaAlteration (only on
    // Op, :58/:66), so reference equality is the faithful surface. Unlike ClientQuotaEntity this
    // type is never a result key (PLAN D39 names it not).

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:103</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString()
    {
        var parts = new List<string>(_ops.Length);
        foreach (Op op in _ops)
        {
            parts.Add(op.ToString());
        }

        return string.Concat(
            "ClientQuotaAlteration(entity=",
            Entity.ToString(),
            ", ops=[",
            string.Join(", ", parts),
            "])");
    }

    /// <summary>
    /// A single quota alteration — Java's nested <c>ClientQuotaAlteration.Op</c> (<c>:28</c>).
    /// </summary>
    public sealed class Op
    {
        /// <summary>
        /// Creates an alteration op — Java's <c>Op(String, Double)</c> (<c>:37</c>). A null
        /// <paramref name="value"/> <em>clears</em> the quota; <c>0.0</c> is a legal quota value,
        /// so the two are distinct.
        /// </summary>
        /// <param name="key">The quota type to alter.</param>
        /// <param name="value">The new value, or null to clear the quota.</param>
        /// <exception cref="ArgumentNullException"><paramref name="key"/> is null.</exception>
        public Op(string key, double? value)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Value = value;
        }

        /// <summary>The quota type to alter — Java's <c>key()</c> (<c>:45</c>).</summary>
        public string Key { get; }

        /// <summary>
        /// The new value, or null to clear the quota — Java's boxed <c>Double value()</c>
        /// (<c>:53</c>).
        /// </summary>
        public double? Value { get; }

        /// <summary>
        /// Value equality over the key and the value — Java's <c>equals</c> (<c>:58</c>); a null
        /// value is not equal to <c>0.0</c>.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>Whether the two describe the same op.</returns>
        public override bool Equals(object? obj) =>
            obj is Op other
            && string.Equals(Key, other.Key, StringComparison.Ordinal)
            && Value.Equals(other.Value);

        /// <summary>
        /// The hash of the key and the value — Java's <c>hashCode</c> (<c>:66</c>), same
        /// <c>31 *</c> fold as <c>Objects.hash</c>. Like Java, a null value folds in as 0 and so
        /// shares a bucket with <c>0.0</c>; <see cref="Equals(object?)"/> still separates them.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 1;
                hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Key);
                hash = (hash * 31) + (Value.HasValue ? Value.Value.GetHashCode() : 0);
                return hash;
            }
        }

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:71</c>).</summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            string.Concat(
                "ClientQuotaAlteration.Op(key=",
                Key,
                ", value=",
                Value.HasValue
                    ? Value.Value.ToString("R", CultureInfo.InvariantCulture)
                    : "null",
                ")");
    }
}
