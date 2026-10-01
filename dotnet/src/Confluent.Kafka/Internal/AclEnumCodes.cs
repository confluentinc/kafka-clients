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

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>fromCode</c> fallback for the four ACL enums, applied to a C# enum value: a
/// defined member is kept, anything else becomes that enum's <c>Unknown</c> (code 0).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Why this exists at all</b> (M15/P13.3 F8; <c>definition-of-done.md</c> §7 — Java has
/// no such type). A Java <c>AclOperation</c> / <c>AclPermissionType</c> / <c>ResourceType</c>
/// / <c>PatternType</c> cannot hold an undefined code: every one is built through
/// <c>fromCode</c>, which answers <c>UNKNOWN</c> for a code it has no member for
/// (<c>AclOperation.java:151</c>, <c>AclPermissionType.java:74</c>,
/// <c>ResourceType.java:94</c>, <c>PatternType.java:111</c>). A C# enum can hold any
/// <c>int</c>, through an unchecked cast. The ABI reads each code the same way Java does —
/// anything that is not a defined code, including a value outside <c>int8_t</c>, becomes
/// <c>UNKNOWN</c> — and it keys each <c>deleteAcls</c> callback by that normalized value, as
/// it has each <c>createAcls</c> callback since PR #201 round 70. The four ACL value-type
/// constructors call this so that the binding's key equals the core's: a raw code would key
/// a <see cref="System.Threading.Tasks.Task"/> the answer can never name, and two inputs that
/// differ only in undefined codes would be two keys where the core answers one.
/// </para>
/// <para>
/// <see cref="ConfigResource"/> does the same for <see cref="ConfigResourceType"/> in its own
/// constructor (M15/P13.2 G2-1). These four enums are shared by four constructors, so the
/// check is written here once rather than in each of them.
/// </para>
/// </remarks>
internal static class AclEnumCodes
{
    /// <summary>Java's <c>AclOperation.fromCode</c> fallback (<c>AclOperation.java:151</c>).</summary>
    /// <param name="value">The value the caller passed.</param>
    /// <returns><paramref name="value"/> if it is a defined member, otherwise <see cref="AclOperation.Unknown"/>.</returns>
    internal static AclOperation DefinedOrUnknown(AclOperation value) =>
        Enum.IsDefined(typeof(AclOperation), value) ? value : AclOperation.Unknown;

    /// <summary>Java's <c>AclPermissionType.fromCode</c> fallback (<c>AclPermissionType.java:74</c>).</summary>
    /// <param name="value">The value the caller passed.</param>
    /// <returns><paramref name="value"/> if it is a defined member, otherwise <see cref="AclPermissionType.Unknown"/>.</returns>
    internal static AclPermissionType DefinedOrUnknown(AclPermissionType value) =>
        Enum.IsDefined(typeof(AclPermissionType), value) ? value : AclPermissionType.Unknown;

    /// <summary>Java's <c>ResourceType.fromCode</c> fallback (<c>ResourceType.java:94</c>).</summary>
    /// <param name="value">The value the caller passed.</param>
    /// <returns><paramref name="value"/> if it is a defined member, otherwise <see cref="ResourceType.Unknown"/>.</returns>
    internal static ResourceType DefinedOrUnknown(ResourceType value) =>
        Enum.IsDefined(typeof(ResourceType), value) ? value : ResourceType.Unknown;

    /// <summary>Java's <c>PatternType.fromCode</c> fallback (<c>PatternType.java:111</c>).</summary>
    /// <param name="value">The value the caller passed.</param>
    /// <returns><paramref name="value"/> if it is a defined member, otherwise <see cref="PatternType.Unknown"/>.</returns>
    internal static PatternType DefinedOrUnknown(PatternType value) =>
        Enum.IsDefined(typeof(PatternType), value) ? value : PatternType.Unknown;
}
