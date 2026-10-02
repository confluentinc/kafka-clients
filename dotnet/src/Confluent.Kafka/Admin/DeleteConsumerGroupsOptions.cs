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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DeleteConsumerGroups"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions</c>
/// (<c>DeleteConsumerGroupsOptions.java:22</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java's class declares no members of its own — everything comes from
/// <c>AbstractOptions&lt;DeleteConsumerGroupsOptions&gt;</c>, which contributes only
/// the timeout. So <see cref="TimeoutMs"/> is the only member here, the same call every
/// sibling flat-POCO options type in this binding already makes.
/// </para>
/// </remarks>
public sealed class DeleteConsumerGroupsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }
}
