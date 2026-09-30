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
/// Options for <see cref="IAdmin.RemoveMembersFromConsumerGroup"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions</c>
/// (<c>RemoveMembersFromConsumerGroupOptions.java:28</c>). It carries the members to be
/// removed from the consumer group.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java has <b>two</b> constructors, both translated: the members-taking one
/// (<c>:33-38</c>), which rejects an empty collection, and the no-arg one (<c>:40-42</c>),
/// which is "remove every member of the group" — <see cref="RemoveAll"/> is
/// <see langword="true"/> exactly when <see cref="Members"/> is empty, mirroring Java's
/// <c>removeAll()</c> (<c>:59-61</c>), which is likewise derived from
/// <c>members.isEmpty()</c> rather than stored separately.
/// </para>
/// </remarks>
public sealed class RemoveMembersFromConsumerGroupOptions
{
    private readonly HashSet<MemberToRemove> _members;

    /// <summary>
    /// Removes the given members — Java's
    /// <c>RemoveMembersFromConsumerGroupOptions(Collection&lt;MemberToRemove&gt; members)</c>
    /// (<c>:33-38</c>). Duplicates (by <see cref="MemberToRemove.GroupInstanceId"/>) collapse,
    /// mirroring Java's <c>Set</c> collapse (<c>:37</c>).
    /// </summary>
    /// <param name="members">The members to remove. Must not be empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="members"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="members"/> is empty — Java's exact message (<c>:35</c>).
    /// </exception>
    public RemoveMembersFromConsumerGroupOptions(IReadOnlyCollection<MemberToRemove> members)
    {
        if (members is null)
        {
            throw new ArgumentNullException(nameof(members));
        }

        if (members.Count == 0)
        {
            throw new ArgumentException("Invalid empty members has been provided", nameof(members));
        }

        _members = new HashSet<MemberToRemove>(members);
    }

    /// <summary>
    /// Removes every member of the group — Java's no-argument
    /// <c>RemoveMembersFromConsumerGroupOptions()</c> (<c>:40-42</c>).
    /// </summary>
    public RemoveMembersFromConsumerGroupOptions()
    {
        _members = new HashSet<MemberToRemove>();
    }

    /// <summary>
    /// An optional reason recorded with the removal — Java's <c>reason(String)</c> /
    /// <c>reason()</c> (<c>:44-49</c>, <c>:55-57</c>), collapsed into one property per this
    /// binding's idiom.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// The members to remove — Java's <c>members()</c> (<c>:51-53</c>).
    /// </summary>
    public IReadOnlyCollection<MemberToRemove> Members => _members;

    /// <summary>
    /// <see langword="true"/> when every member of the group should be removed — Java's
    /// <c>removeAll()</c> (<c>:59-61</c>), derived from <see cref="Members"/> being empty
    /// rather than stored as its own field.
    /// </summary>
    public bool RemoveAll => _members.Count == 0;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }
}
