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
using System.Collections.Generic;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The target replica set for one partition in a call to
/// <see cref="IAdmin.AlterPartitionReassignments"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.NewPartitionReassignment</c>
/// (<c>NewPartitionReassignment.java:32, :38</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>An empty replica list is rejected, exactly as Java rejects it</b>
/// (<c>:33-35</c>: "Cannot create a new partition reassignment without any replicas").
/// That is what keeps <em>cancelling</em> a reassignment distinct from asking for a
/// present-but-empty one: <see cref="IAdmin.AlterPartitionReassignments"/> spells
/// cancellation as a <see langword="null"/> entry — Java's <c>Optional.empty()</c> — and
/// the ABI carries it in a dedicated <c>cancel</c> flag array rather than as an empty
/// replica list. The ABI header states the intent verbatim: "A separate flag rather than
/// a NULL replica pointer, so cancelling stays distinct from 'present but empty', which
/// Java rejects."
/// </para>
/// <para>
/// <b>The replica list is copied on construction</b>, as Java's <c>List.copyOf</c> does,
/// so a caller mutating the list afterwards cannot change a submitted request.
/// </para>
/// <para>
/// <b>Recorded sub-divergence (<c>definition-of-done.md</c> §7): the null and the empty
/// list raise different .NET exception types where Java raises one.</b> Java throws
/// <c>IllegalArgumentException</c> for both (<c>:33-35</c> tests
/// <c>targetReplicas == null || targetReplicas.isEmpty()</c>). CLAUDE.md §3's idiom map
/// sends <c>IllegalArgumentException</c> to "the <see cref="ArgumentException"/>
/// family", and <see cref="ArgumentNullException"/> is the member of that family .NET
/// callers expect for a null argument — so a <c>catch (ArgumentException)</c> still
/// catches both, matching Java's single catch clause. Java's message is carried
/// unchanged on both paths.
/// </para>
/// <para>
/// Java declares no <c>toString</c>, <c>equals</c> or <c>hashCode</c> on this class, so
/// none is added here (<c>definition-of-done.md</c> §7).
/// </para>
/// </remarks>
public sealed class NewPartitionReassignment
{
    /// <summary>Java's own message, carried unchanged (<c>:34</c>).</summary>
    private const string NoReplicasMessage = "Cannot create a new partition reassignment without any replicas";

    private readonly List<int> _targetReplicas;

    /// <summary>
    /// Creates a reassignment onto <paramref name="targetReplicas"/> — Java's
    /// <c>NewPartitionReassignment(List&lt;Integer&gt;)</c> (<c>:32</c>).
    /// </summary>
    /// <param name="targetReplicas">
    /// The broker ids the partition's replicas are to move to, preferred leader first.
    /// Must not be empty — see the type remarks for why an empty list is not a way to
    /// cancel.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="targetReplicas"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="targetReplicas"/> is empty.</exception>
    public NewPartitionReassignment(IReadOnlyList<int> targetReplicas)
    {
        if (targetReplicas is null)
        {
            throw new ArgumentNullException(nameof(targetReplicas), NoReplicasMessage);
        }

        if (targetReplicas.Count == 0)
        {
            throw new ArgumentException(NoReplicasMessage, nameof(targetReplicas));
        }

        _targetReplicas = new List<int>(targetReplicas);
    }

    /// <summary>
    /// The broker ids the partition's replicas are to move to — Java's
    /// <c>targetReplicas()</c> (<c>:38</c>). Never empty.
    /// </summary>
    /// <remarks>
    /// A property rather than a method (decision D18): it is a pure managed field read
    /// that does no P/Invoke and cannot throw, and no same-named static factory forces
    /// the method form here.
    /// </remarks>
    public IReadOnlyList<int> TargetReplicas => _targetReplicas;
}
