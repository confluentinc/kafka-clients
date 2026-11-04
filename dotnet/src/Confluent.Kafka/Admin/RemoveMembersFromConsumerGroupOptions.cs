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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.RemoveMembersFromConsumerGroup"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions</c>,
/// carrying the members to remove from the consumer group.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class RemoveMembersFromConsumerGroupOptions
{
    private readonly HashSet<MemberToRemove> _members;
    private string? _reason;
    private int? _timeoutMs;

    /// <summary>
    /// Removes the given members — Java's
    /// <c>RemoveMembersFromConsumerGroupOptions(Collection&lt;MemberToRemove&gt; members)</c>.
    /// Duplicates (by <see cref="MemberToRemove.GroupInstanceId"/>) collapse, as in Java's
    /// <c>Set</c>.
    /// </summary>
    /// <param name="members">The members to remove. Must not be empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="members"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="members"/> is empty, with Java's message.
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
    /// <c>RemoveMembersFromConsumerGroupOptions()</c>.
    /// </summary>
    public RemoveMembersFromConsumerGroupOptions()
    {
        _members = new HashSet<MemberToRemove>();
    }

    /// <summary>
    /// An optional reason recorded with the removal — Java's <c>reason(String)</c> /
    /// <c>reason()</c>.
    /// </summary>
    public string? Reason
    {
        get => _reason;
        set
        {
            _reason = value;
            Setters.Ran(nameof(Reason));
        }
    }

    /// <summary>
    /// The members to remove — Java's <c>members()</c>.
    /// </summary>
    public IReadOnlyCollection<MemberToRemove> Members => _members;

    /// <summary>
    /// <see langword="true"/> when every member of the group should be removed, that is when
    /// <see cref="Members"/> is empty — Java's <c>removeAll()</c>.
    /// </summary>
    public bool RemoveAll => _members.Count == 0;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs
    {
        get => _timeoutMs;
        set
        {
            _timeoutMs = value;
            Setters.Ran(nameof(TimeoutMs));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
