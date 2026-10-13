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
/// The read isolation a query observes — the .NET realization of Java's
/// <c>org.apache.kafka.common.IsolationLevel</c>, used by
/// <see cref="Admin.ListOffsetsOptions.IsolationLevel"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Each member's numeric value is Java's <c>IsolationLevel.id()</c> byte</b> —
/// <c>READ_UNCOMMITTED((byte) 0)</c>, <c>READ_COMMITTED((byte) 1)</c>
/// (<c>IsolationLevel.java:22</c>) — <b>not</b> a C#-assigned ordinal. The C ABI
/// transports the id as a bare <c>int32_t</c> and <b>rejects</b> anything else, so an
/// auto-assigned value would fail the call rather than degrade.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common</c> — not
/// <c>clients.admin</c> — so this lives at the root <c>Confluent.Kafka</c> namespace
/// (decision D13), beside <see cref="ElectionType"/>, <see cref="AclOperation"/> and
/// <see cref="Node"/>.
/// </para>
/// <para>
/// Java's <c>id()</c> accessor and <c>forId(byte)</c> factory have no counterpart here: a
/// C# enum already exposes its underlying value by a cast, and the id mapping the factory
/// performs is done by the ABI. Java's <c>forId</c> throws
/// <c>IllegalArgumentException("Unknown isolation level " + id)</c> for any other byte
/// (<c>:41</c>); the .NET analogue is <see cref="System.ArgumentOutOfRangeException"/>
/// raised by <see cref="Admin.IAdmin.ListOffsets"/> for a value cast into this enum from
/// outside its two members.
/// </para>
/// </remarks>
public enum IsolationLevel
{
    /// <summary>
    /// Read all records, including those in open transactions — Java's
    /// <c>READ_UNCOMMITTED</c> (id 0), and Java's <c>ListOffsetsOptions</c> default.
    /// </summary>
    ReadUncommitted = 0,

    /// <summary>
    /// Read only records from committed transactions — Java's <c>READ_COMMITTED</c>
    /// (id 1).
    /// </summary>
    ReadCommitted = 1,
}
