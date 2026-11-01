// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The group membership operation a consumer applies when it closes — Java's
/// <c>CloseOptions.GroupMembershipOperation</c>.
/// </summary>
public enum GroupMembershipOperation
{
    /// <summary>The consumer leaves the group, which triggers a rebalance.</summary>
    LeaveGroup,

    /// <summary>The consumer remains in the group.</summary>
    RemainInGroup,

    /// <summary>A static member remains in the group and a dynamic member leaves it.</summary>
    Default,
}
