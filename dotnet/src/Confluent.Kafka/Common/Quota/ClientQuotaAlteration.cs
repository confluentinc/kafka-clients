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

namespace Confluent.Kafka.Common.Quota;

/// <summary>
/// A configuration alteration to be made to a client quota entity — Java's
/// <c>org.apache.kafka.common.quota.ClientQuotaAlteration</c>.
/// </summary>
public sealed class ClientQuotaAlteration
{
    private readonly Op[] _ops;

    /// <summary>Creates an alteration — Java's constructor.</summary>
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

        _ops = new Op[ops.Count];
        int i = 0;
        foreach (Op op in ops)
        {
            _ops[i++] = op ?? throw new ArgumentException("ops must not contain null", nameof(ops));
        }
    }

    /// <summary>The entity whose configuration will be modified — Java's <c>entity()</c>.</summary>
    public ClientQuotaEntity Entity { get; }

    /// <summary>The alterations to perform — Java's <c>ops()</c>.</summary>
    public IReadOnlyCollection<Op> Ops => _ops;


    /// <summary>
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>, each op as
    /// <see cref="Op.ToString"/> renders it.
    /// </summary>
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

    /// <summary>A single quota alteration — Java's nested <c>ClientQuotaAlteration.Op</c>.</summary>
    public sealed class Op
    {
        /// <summary>
        /// Creates an alteration op — Java's constructor. A null <paramref name="value"/> clears
        /// the quota; <c>0.0</c> is a legal quota value, so the two are distinct.
        /// </summary>
        /// <param name="key">The quota type to alter.</param>
        /// <param name="value">The new value, or null to clear the quota.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="key"/> is null: stricter than Java, whose constructor accepts it and
        /// whose <c>alterClientQuotas</c> call then fails, a recorded deviation.
        /// </exception>
        public Op(string key, double? value)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Value = value;
        }

        /// <summary>The quota type to alter — Java's <c>key()</c>.</summary>
        public string Key { get; }

        /// <summary>
        /// The new value, or null to clear the quota — Java's boxed <c>Double value()</c>.
        /// </summary>
        public double? Value { get; }

        /// <summary>
        /// Value equality over the key and the value — Java's <c>equals</c>; a null value is not
        /// equal to <c>0.0</c>.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>Whether the two describe the same op.</returns>
        public override bool Equals(object? obj) =>
            obj is Op other
            && string.Equals(Key, other.Key, StringComparison.Ordinal)
            && Value.Equals(other.Value);

        /// <summary>
        /// The hash of the key and the value — Java's <c>hashCode</c>. A null value hashes as 0,
        /// as in Java; <see cref="Equals(object?)"/> still separates it from <c>0.0</c>.
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

        /// <summary>
        /// A diagnostic rendering in the shape of Java's <c>toString()</c>. A whole-number value
        /// prints as <c>1</c>, where Java's <c>Double</c> prints <c>1.0</c>.
        /// </summary>
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
