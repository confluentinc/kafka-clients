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
/// Options for listing a consumer group's committed offsets — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions</c>
/// (<c>ListConsumerGroupOffsetsOptions.java:24</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// <b>Java declares exactly one option of its own</b> — the fluent setter
/// <c>requireStable(boolean)</c> (<c>:31</c>) and its getter <c>requireStable()</c>
/// (<c>:36</c>) — which collapse into the single property <see cref="RequireStable"/>, the
/// pairing <see cref="CreateTopicsOptions"/> rules for every options type in this binding.
/// Everything else on the Java class is inherited from
/// <c>AbstractOptions&lt;ListConsumerGroupOffsetsOptions&gt;</c>, which contributes only the
/// timeout; C# has no <c>extends</c> to mirror here because the options types in this
/// binding are flat POCOs, so <see cref="TimeoutMs"/> is declared directly — the same call
/// every sibling options type already makes.
/// </para>
/// <para>
/// ⚠ <b>This type gates <em>how</em> the offsets are read, not <em>which</em> ones.</b>
/// The selection lives in <see cref="ListConsumerGroupOffsetsSpec"/>, one per group, and
/// the two are handed to the call together — so an unset
/// <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> is what asks for every
/// partition, never anything here.
/// </para>
/// <para>
/// ⚠ <b>Java's class is <em>not</em> deprecated</b>, unlike
/// <see cref="ListConsumerGroupsOptions"/>: <c>listConsumerGroupOffsets</c> has no
/// generation-newer replacement — the newer group protocol reads its committed offsets
/// through this same call. So no <see cref="System.ObsoleteAttribute"/> appears here, and a
/// test asserts that absence rather than leaving it implicit.
/// </para>
/// </remarks>
public sealed class ListConsumerGroupOffsetsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Wait for any in-progress transaction on a partition to resolve before reporting its
    /// committed offset — Java's <c>requireStable()</c> (<c>:36</c>). Defaults to
    /// <see langword="false"/>, as Java's explicitly initialized field does (<c>:26</c>).
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/> the broker answers immediately with the last
    /// committed offset, which for a partition with an open transaction may be superseded
    /// moments later. Setting it to <see langword="true"/> trades that promptness for a
    /// read that no in-flight transaction can still change.
    /// </remarks>
    public bool RequireStable { get; set; }
}
