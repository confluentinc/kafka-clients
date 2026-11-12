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

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>fromCode</c> fallback for the four ACL enums: a defined member is kept, anything else
/// becomes that enum's <c>UNKNOWN</c>, so the binding keys an ACL answer by the value the core keys
/// it by. A value is read through the enum's own <c>FromCode</c>; one outside a <see langword="byte"/> is
/// <c>UNKNOWN</c> too, never the member its low byte would name.
/// </summary>
internal static class AclEnumCodes
{
    internal static AclOperation DefinedOrUnknown(AclOperation value) =>
        (uint)value <= byte.MaxValue ? AclOperation.FromCode((byte)value) : AclOperation.UNKNOWN;

    internal static AclPermissionType DefinedOrUnknown(AclPermissionType value) =>
        (uint)value <= byte.MaxValue ? AclPermissionType.FromCode((byte)value) : AclPermissionType.UNKNOWN;

    internal static ResourceType DefinedOrUnknown(ResourceType value) =>
        (uint)value <= byte.MaxValue ? ResourceType.FromCode((byte)value) : ResourceType.UNKNOWN;

    internal static PatternType DefinedOrUnknown(PatternType value) =>
        (uint)value <= byte.MaxValue ? PatternType.FromCode((byte)value) : PatternType.UNKNOWN;
}
