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
/// Options for describing consumer groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions</c>
/// (<c>DescribeConsumerGroupsOptions.java:26</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// <b>Java declares exactly one option of its own</b> — the fluent setter
/// <c>includeAuthorizedOperations(boolean)</c> (<c>:29</c>) and its getter
/// <c>includeAuthorizedOperations()</c> (<c>:34</c>) — which collapse into the single
/// property <see cref="IncludeAuthorizedOperations"/>, the pairing
/// <see cref="CreateTopicsOptions"/> rules for every options type in this binding.
/// Everything else on the Java class is inherited from
/// <c>AbstractOptions&lt;DescribeConsumerGroupsOptions&gt;</c>, which contributes only the
/// timeout; C# has no <c>extends</c> to mirror here because the options types in this
/// binding are flat POCOs, so <see cref="TimeoutMs"/> is declared directly — the same call
/// every sibling options type already makes.
/// </para>
/// <para>
/// ⚠ <b>Java's class is <em>not</em> deprecated</b>, unlike
/// <see cref="ListConsumerGroupsOptions"/>: <c>describeConsumerGroups</c> has no
/// generation-newer replacement, so no <see cref="System.ObsoleteAttribute"/> appears here.
/// The RPC's <em>result</em> element, <see cref="ConsumerGroupDescription"/>, does carry one
/// deprecated member (<c>state()</c>), but that is a property of the description, not of
/// these options.
/// </para>
/// </remarks>
public sealed class DescribeConsumerGroupsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Ask the broker to report each group's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c> (<c>:34</c>). Defaults to
    /// <see langword="false"/>, as Java's uninitialized <c>boolean</c> field does
    /// (<c>:27</c>).
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/> the broker reports nothing, which is exactly
    /// the case <see cref="ConsumerGroupDescription.AuthorizedOperations"/> renders as
    /// <see langword="null"/> rather than as an empty collection — the same distinction
    /// <see cref="DescribeTopicsOptions.IncludeAuthorizedOperations"/> draws for topics.
    /// </remarks>
    public bool IncludeAuthorizedOperations { get; set; }
}
