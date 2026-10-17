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
/// Options for <see cref="IAdmin.ElectLeaders"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ElectLeadersOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java's <c>ElectLeadersOptions</c> declares <b>no members of its own</b> — it is an
/// empty <c>final</c> subclass of <c>AbstractOptions</c> (<c>ElectLeadersOptions.java:29-30</c>),
/// so <see cref="TimeoutMs"/> is the whole surface. The ABI agrees:
/// <c>elect_leaders_async</c> takes no option argument beyond the timeout, and its header
/// says so — "<c>ElectLeadersOptions</c> has no other field in Java". The type still
/// exists because Java's does, and because a later Kafka release adding a field must not
/// become a signature change here.
/// </para>
/// </remarks>
public sealed class ElectLeadersOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }
}
