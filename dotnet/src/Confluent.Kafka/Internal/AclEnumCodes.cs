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
/// Java's <c>fromCode</c> fallback for the four ACL enums: a defined member is kept, anything else
/// becomes that enum's <c>Unknown</c>, so the binding keys an ACL answer by the value the core keys
/// it by.
/// </summary>
internal static class AclEnumCodes
{
    internal static AclOperation DefinedOrUnknown(AclOperation value) =>
        Enum.IsDefined(typeof(AclOperation), value) ? value : AclOperation.Unknown;

    internal static AclPermissionType DefinedOrUnknown(AclPermissionType value) =>
        Enum.IsDefined(typeof(AclPermissionType), value) ? value : AclPermissionType.Unknown;

    internal static ResourceType DefinedOrUnknown(ResourceType value) =>
        Enum.IsDefined(typeof(ResourceType), value) ? value : ResourceType.Unknown;

    internal static PatternType DefinedOrUnknown(PatternType value) =>
        Enum.IsDefined(typeof(PatternType), value) ? value : PatternType.Unknown;
}
