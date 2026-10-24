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

namespace Confluent.Kafka.Admin;

/// <summary>
/// One incremental change to a resource's configuration — Java's
/// <c>org.apache.kafka.clients.admin.AlterConfigOp</c>.
/// </summary>
/// <remarks>
/// <para>
/// When altering broker logger levels under <see cref="ConfigResourceType.BrokerLogger"/>, prefer
/// Kafka's log-level constants over string literals, so the value passes the broker's log-level
/// validation.
/// </para>
/// <para>
/// A <see cref="AlterConfigOpType.Delete"/> whose entry value is <see langword="null"/> sends a null
/// value, as in Java.
/// </para>
/// </remarks>
public sealed class AlterConfigOp
{
    /// <summary>
    /// Initializes an operation — Java's
    /// <c>AlterConfigOp(ConfigEntry configEntry, OpType operationType)</c>.
    /// </summary>
    /// <param name="configEntry">The configuration entry the operation applies to.</param>
    /// <param name="opType">What to do with it.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configEntry"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="opType"/> is not a defined <see cref="AlterConfigOpType"/> member.
    /// </exception>
    /// <remarks>
    /// The null check is stricter than Java, which stores the reference unchecked. An undefined
    /// <paramref name="opType"/> is refused, because Java's closed <c>OpType</c> enum cannot hold one
    /// and has no <c>UNKNOWN</c> member to map it to, while a C# enum accepts any integral value by
    /// cast.
    /// </remarks>
    public AlterConfigOp(ConfigEntry configEntry, AlterConfigOpType opType)
    {
        ConfigEntry = configEntry ?? throw new ArgumentNullException(nameof(configEntry));
        if (!Enum.IsDefined(typeof(AlterConfigOpType), opType))
        {
            throw new ArgumentOutOfRangeException(
                nameof(opType), opType, "opType must be a defined AlterConfigOpType member.");
        }

        OpType = opType;
    }

    /// <summary>The entry this operation applies to — Java's <c>configEntry()</c>.</summary>
    public ConfigEntry ConfigEntry { get; }

    /// <summary>What the operation does — Java's <c>opType()</c>.</summary>
    public AlterConfigOpType OpType { get; }

    /// <summary>
    /// Value equality over the entry and the operation type — Java's <c>equals</c>, which compares
    /// the entry with <c>Objects.equals</c> and so relies on <see cref="ConfigEntry"/>'s own value
    /// equality.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two operations are the same.</returns>
    public override bool Equals(object? obj) =>
        obj is AlterConfigOp other && OpType == other.OpType && ConfigEntry.Equals(other.ConfigEntry);

    /// <summary>The hash of the two fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (OpType.GetHashCode() * 31) + ConfigEntry.GetHashCode();
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c>, with the operation type printed as
    /// Java's constant name, as in <c>AlterConfigOp{opType=SET, configEntry=ConfigEntry(...)}</c>, and
    /// the entry rendered by <see cref="ConfigEntry.ToString"/>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture, "AlterConfigOp{{opType={0}, configEntry={1}}}", JavaName(OpType), ConfigEntry);

    private static string JavaName(AlterConfigOpType opType) => opType switch
    {
        AlterConfigOpType.Set => "SET",
        AlterConfigOpType.Delete => "DELETE",
        AlterConfigOpType.Append => "APPEND",
        AlterConfigOpType.Subtract => "SUBTRACT",
        _ => ((int)opType).ToString(CultureInfo.InvariantCulture),
    };
}
