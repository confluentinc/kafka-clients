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

namespace Confluent.Kafka;

/// <summary>
/// The kind of leader election to conduct — the .NET realization of Java's
/// <c>org.apache.kafka.common.ElectionType</c>, passed to
/// <see cref="Admin.IAdmin.ElectLeaders"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Each member's numeric value is Java's <c>ElectionType.value</c> byte</b> —
/// <c>PREFERRED((byte) 0)</c>, <c>UNCLEAN((byte) 1)</c> (<c>ElectionType.java:27</c>) —
/// <b>not</b> a C#-assigned ordinal. The C ABI transports the code as a bare
/// <c>int32_t</c> (<c>kafka_admin_AdminClient_elect_leaders</c>'s <c>election_type</c>:
/// "Java's <c>ElectionType</c> byte value — <c>0</c> = <c>PREFERRED</c>, <c>1</c> =
/// <c>UNCLEAN</c>"), so an auto-assigned value would silently request the wrong
/// election. The codes are asserted member-by-member in the unit tests.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common</c> — not
/// <c>clients.admin</c> — so this lives at the root <c>Confluent.Kafka</c> namespace
/// (decision D13), beside <see cref="AclOperation"/>, <see cref="Node"/> and
/// <see cref="TopicCollection"/>.
/// </para>
/// <para>
/// Java's <c>value</c> field and <c>valueOf(byte)</c> factory have no counterpart here: a
/// C# enum already exposes its underlying value by a cast, and the wire-code mapping the
/// factory performs is done by the ABI. Java throws <c>IllegalArgumentException</c> from
/// <c>valueOf</c> for any other byte; the .NET analogue is
/// <see cref="System.ArgumentOutOfRangeException"/> raised by
/// <see cref="Admin.IAdmin.ElectLeaders"/> for a value cast into this enum from outside
/// its two members.
/// </para>
/// </remarks>
public enum ElectionType
{
    /// <summary>
    /// Elect the preferred replica — the first replica in the partition's assignment —
    /// as leader. Java's <c>PREFERRED</c> (value 0).
    /// </summary>
    Preferred = 0,

    /// <summary>
    /// Elect any in-sync-or-not replica as leader, accepting possible data loss. Java's
    /// <c>UNCLEAN</c> (value 1).
    /// </summary>
    Unclean = 1,
}
