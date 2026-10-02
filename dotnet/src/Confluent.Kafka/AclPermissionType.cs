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
/// Whether an ACL grants or denies — the .NET realization of Java's
/// <c>org.apache.kafka.common.acl.AclPermissionType</c>. Each member's value is the Kafka
/// <b>wire</b> code (Java's <c>code()</c>), not a C#-assigned ordinal.
/// </summary>
public enum AclPermissionType
{
    /// <summary>A permission type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    Unknown = 0,

    /// <summary>
    /// In a filter, matches any permission type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="AccessControlEntry"/>'s constructor; legal on <see cref="AccessControlEntryFilter"/>.
    /// </summary>
    Any = 1,

    /// <summary>Disallows the operation — Java's <c>DENY</c> (code 2).</summary>
    Deny = 2,

    /// <summary>Allows the operation — Java's <c>ALLOW</c> (code 3).</summary>
    Allow = 3,
}
