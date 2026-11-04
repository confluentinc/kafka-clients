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

namespace Confluent.Kafka.Common.Resource;

/// <summary>
/// The type of resource an ACL applies to — Java's
/// <c>org.apache.kafka.common.resource.ResourceType</c>. Each member's value is the Kafka wire
/// code (Java's <c>code()</c>), which crosses the C ABI as an <c>int32_t</c>.
/// </summary>
public enum ResourceType
{
    /// <summary>A resource type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    UNKNOWN = 0,

    /// <summary>
    /// In a filter, matches any resource type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="ResourcePattern"/>'s constructor; legal on <see cref="ResourcePatternFilter"/>.
    /// </summary>
    ANY = 1,

    /// <summary>A topic — Java's <c>TOPIC</c> (code 2).</summary>
    TOPIC = 2,

    /// <summary>A consumer group — Java's <c>GROUP</c> (code 3).</summary>
    GROUP = 3,

    /// <summary>The cluster — Java's <c>CLUSTER</c> (code 4).</summary>
    CLUSTER = 4,

    /// <summary>A transactional id — Java's <c>TRANSACTIONAL_ID</c> (code 5).</summary>
    TRANSACTIONAL_ID = 5,

    /// <summary>A delegation token — Java's <c>DELEGATION_TOKEN</c> (code 6).</summary>
    DELEGATION_TOKEN = 6,

    /// <summary>A user — Java's <c>USER</c> (code 7).</summary>
    USER = 7,
}
