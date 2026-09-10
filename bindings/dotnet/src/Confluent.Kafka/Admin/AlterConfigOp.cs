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
/// One incremental change to a resource's configuration — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.AlterConfigOp</c> (<c>AlterConfigOp.java:44</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Note for broker logger configuration</b> (Java's own, <c>:29-42</c>): when altering
/// broker logger levels under <see cref="ConfigResourceType.BrokerLogger"/>, prefer Kafka's
/// log-level constants over string literals, so the value passes the broker's log-level
/// validation.
/// </para>
/// <para>
/// ⚠ <b>A <see cref="AlterConfigOpType.Delete"/> whose entry value is
/// <see langword="null"/> is a real request, not a mistake.</b> The null reaches the ABI as
/// a null pointer — the header calls it out: "a NULL <c>config_values</c> entry is the null
/// value DELETE uses". Nothing on the submit path substitutes an empty string for it.
/// </para>
/// <para>
/// Both accessors are properties under M15/P3 decision D18, where Java's are methods —
/// each is a pure managed field read. The nested Java enum could not follow: see
/// <see cref="AlterConfigOpType"/> for why <c>CS0102</c> forces it out of the type.
/// </para>
/// </remarks>
public sealed class AlterConfigOp
{
    /// <summary>
    /// Initializes an operation — Java's
    /// <c>AlterConfigOp(ConfigEntry configEntry, OpType operationType)</c> (<c>:91</c>).
    /// </summary>
    /// <param name="configEntry">The configuration entry the operation applies to.</param>
    /// <param name="opType">What to do with it.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configEntry"/> is null.</exception>
    /// <remarks>
    /// ⚠ The null check is <b>stricter than Java</b>, which stores the reference without
    /// checking. <see cref="ConfigEntry"/> is a non-nullable reference here, and the submit
    /// path dereferences it to build the row — the same call
    /// <see cref="TopicListing"/> and <see cref="ConfigResource"/> already make.
    /// </remarks>
    public AlterConfigOp(ConfigEntry configEntry, AlterConfigOpType opType)
    {
        ConfigEntry = configEntry ?? throw new ArgumentNullException(nameof(configEntry));
        OpType = opType;
    }

    /// <summary>The entry this operation applies to — Java's <c>configEntry()</c> (<c>:96</c>).</summary>
    public ConfigEntry ConfigEntry { get; }

    /// <summary>What the operation does — Java's <c>opType()</c> (<c>:100</c>).</summary>
    public AlterConfigOpType OpType { get; }

    /// <summary>
    /// Value equality over the entry and the operation type — Java's <c>equals</c>
    /// (<c>:104</c>), which compares the entry with <c>Objects.equals</c> and so relies on
    /// <see cref="ConfigEntry"/>'s own value equality.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two operations are the same.</returns>
    public override bool Equals(object? obj) =>
        obj is AlterConfigOp other && OpType == other.OpType && ConfigEntry.Equals(other.ConfigEntry);

    /// <summary>The hash of the two fields — Java's <c>hashCode</c> (<c>:113</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (OpType.GetHashCode() * 31) + ConfigEntry.GetHashCode();
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:118</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture, "AlterConfigOp{{opType={0}, configEntry={1}}}", OpType, ConfigEntry);
}
