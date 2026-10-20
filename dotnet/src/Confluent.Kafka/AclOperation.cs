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
/// An operation that can be authorized on a Kafka resource — the .NET realization of
/// Java's <c>org.apache.kafka.common.acl.AclOperation</c>. Reached today through
/// <see cref="Admin.TopicDescription.AuthorizedOperations"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every member's numeric value is the Kafka <em>wire</em> code</b>
/// (Java's <c>AclOperation.code()</c>, <c>AclOperation.java:45-120</c>), <b>not</b> a
/// C#-assigned ordinal. The C ABI transports these codes as bare <c>int32_t</c>
/// (<c>kafka_admin_TopicDescription_authorized_operation</c> returns "the
/// <c>AclOperation</c> wire code"), so an auto-assigned value would silently mislabel
/// every operation the broker reports. The codes are asserted member-by-member in the
/// unit tests.
/// </para>
/// <para>
/// <b>Namespace.</b> Java's package is <c>org.apache.kafka.common.acl</c> — under
/// <c>common</c>, not <c>clients.admin</c> — so it lives at the root
/// <c>Confluent.Kafka</c> namespace, following the same rule that puts
/// <see cref="Uuid"/>, <see cref="Node"/> and <see cref="TopicCollection"/> there.
/// </para>
/// <para>
/// <b>Only the enum lands here.</b> The rest of the ACL family (<c>AclBinding</c>,
/// <c>AccessControlEntry</c>, <c>ResourcePattern</c>, …) arrives with the ACL RPCs; this
/// type is present because <c>TopicDescription.authorizedOperations()</c> needs it.
/// </para>
/// </remarks>
public enum AclOperation
{
    /// <summary>
    /// Represents any operation the client does not recognize — Java's <c>UNKNOWN</c>
    /// (code 0). A code the broker sends that this client has no member for is mapped
    /// here, mirroring Java's <c>fromCode</c>.
    /// </summary>
    Unknown = 0,

    /// <summary>In a filter, matches any operation — Java's <c>ANY</c> (code 1).</summary>
    Any = 1,

    /// <summary>All operations — Java's <c>ALL</c> (code 2).</summary>
    All = 2,

    /// <summary>READ — Java's <c>READ</c> (code 3).</summary>
    Read = 3,

    /// <summary>WRITE — Java's <c>WRITE</c> (code 4).</summary>
    Write = 4,

    /// <summary>CREATE — Java's <c>CREATE</c> (code 5).</summary>
    Create = 5,

    /// <summary>DELETE — Java's <c>DELETE</c> (code 6).</summary>
    Delete = 6,

    /// <summary>ALTER — Java's <c>ALTER</c> (code 7).</summary>
    Alter = 7,

    /// <summary>DESCRIBE — Java's <c>DESCRIBE</c> (code 8).</summary>
    Describe = 8,

    /// <summary>CLUSTER_ACTION — Java's <c>CLUSTER_ACTION</c> (code 9).</summary>
    ClusterAction = 9,

    /// <summary>DESCRIBE_CONFIGS — Java's <c>DESCRIBE_CONFIGS</c> (code 10).</summary>
    DescribeConfigs = 10,

    /// <summary>ALTER_CONFIGS — Java's <c>ALTER_CONFIGS</c> (code 11).</summary>
    AlterConfigs = 11,

    /// <summary>IDEMPOTENT_WRITE — Java's <c>IDEMPOTENT_WRITE</c> (code 12).</summary>
    IdempotentWrite = 12,

    /// <summary>CREATE_TOKENS — Java's <c>CREATE_TOKENS</c> (code 13).</summary>
    CreateTokens = 13,

    /// <summary>DESCRIBE_TOKENS — Java's <c>DESCRIBE_TOKENS</c> (code 14).</summary>
    DescribeTokens = 14,

    /// <summary>TWO_PHASE_COMMIT — Java's <c>TWO_PHASE_COMMIT</c> (code 15).</summary>
    TwoPhaseCommit = 15,
}
