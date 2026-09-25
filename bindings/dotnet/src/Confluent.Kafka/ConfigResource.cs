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

namespace Confluent.Kafka;

/// <summary>
/// A resource that has configuration — the .NET realization of Java's
/// <c>org.apache.kafka.common.config.ConfigResource</c>
/// (<c>ConfigResource.java:30, :71, :81, :88, :96, :101, :113, :120</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Value equality is load-bearing, not decorative.</b> This type is the dictionary
/// <b>key</b> of the config RPCs' results, so a wrong <see cref="Equals(object?)"/> /
/// <see cref="GetHashCode"/> produces a result map whose keys the caller cannot look up —
/// a silent failure, not an exception. Both mirror Java exactly:
/// <c>type == that.type &amp;&amp; name.equals(that.name)</c> (<c>:109</c>) and
/// <c>31 * type.hashCode() + name.hashCode()</c> (<c>:113-117</c>). The name is compared
/// and hashed <b>ordinally</b>, which is what Java's <c>String.equals</c> does and what
/// the rest of this binding keys strings by.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common.config</c>, so this sits
/// at the root <c>Confluent.Kafka</c> namespace rather than under <c>Admin/</c> — see
/// <see cref="ConfigResourceType"/>, which carries the full rationale and the recorded
/// divergence from <c>confluent-kafka-dotnet</c>.
/// </para>
/// <para>
/// <b>Accessors are properties, not Java's methods.</b> Java has no properties, so
/// <c>type()</c> / <c>name()</c> / <c>isDefault()</c> are necessarily methods there. Here
/// each is a pure managed field read that does no P/Invoke and cannot throw, which is
/// exactly the case CLAUDE.md §3's "non-blocking getter → sync property" row is written
/// for — the same reading that made <see cref="Admin.TopicListing.Name"/>,
/// <see cref="Admin.TopicDescription.Name"/> and
/// <see cref="Admin.DeletedRecords.LowWatermark"/> properties in M15/P1 and P2b. (The
/// consumer's <c>Assignment()</c> family are methods for the opposite reason — each
/// marshals a fresh snapshot across the ABI.)
/// </para>
/// <para>
/// ⚠ <b>Recorded deviation from the M15/P3 plan sketch</b>
/// (<c>definition-of-done.md</c> §7). The plan's §4.3 sketch spelled <c>IsDefault()</c> as
/// a method "as <c>DeletedRecords.LowWatermark()</c> was mirrored in P2b". That citation
/// does not hold: <see cref="Admin.DeletedRecords.LowWatermark"/> shipped in P2b as a
/// <b>property</b>, with the rationale recorded on it. Following the shipped convention
/// keeps this binding internally consistent; following the sketch would have made this the
/// only pure field read on an admin value type spelled as a method, and the sketch was
/// already inconsistent with itself (it spelled <c>Type</c> and <c>Name</c> as properties
/// while all three are methods in Java).
/// </para>
/// </remarks>
public sealed class ConfigResource
{
    /// <summary>
    /// Creates a config resource — Java's
    /// <c>ConfigResource(Type type, String name)</c> (<c>:71</c>).
    /// </summary>
    /// <param name="type">The resource type.</param>
    /// <param name="name">
    /// The resource name; empty means "the default resource of this type"
    /// (see <see cref="IsDefault"/>).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="name"/> is null — Java's
    /// <c>Objects.requireNonNull(name, "name should not be null")</c> (<c>:73</c>).
    /// </exception>
    public ConfigResource(ConfigResourceType type, string name)
    {
        // Java also requireNonNull's the type; a C# enum has no null to reject.
        Type = type;
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }

    /// <summary>The resource type — Java's <c>type()</c> (<c>:81</c>).</summary>
    public ConfigResourceType Type { get; }

    /// <summary>The resource name — Java's <c>name()</c> (<c>:88</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// Whether this is the <b>default</b> resource of its type, i.e. whether
    /// <see cref="Name"/> is empty — Java's <c>isDefault()</c> (<c>:96</c>).
    /// </summary>
    public bool IsDefault => Name.Length == 0;

    /// <summary>
    /// Value equality over <see cref="Type"/> and <see cref="Name"/> — Java's
    /// <c>equals</c> (<c>:101</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same resource.</returns>
    public override bool Equals(object? obj) =>
        obj is ConfigResource other
        && Type == other.Type
        && string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <summary>
    /// The hash of <see cref="Type"/> combined with <see cref="Name"/> — Java's
    /// <c>hashCode</c> (<c>:113</c>), same fields and same <c>31 *</c> combination.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)Type).GetHashCode() * 31) + StringComparer.Ordinal.GetHashCode(Name);
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:120</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "ConfigResource(type={0}, name='{1}')", Type, Name);
}
