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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.RemoveMembersFromConsumerGroup"/> — Java's
/// <c>org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult</c>: the outcome per member and for the whole request.
/// </summary>
/// <remarks>
/// Each call of an accessor returns a new future, as Java's do.
/// </remarks>
/// <seealso cref="AsyncRemoveMembersFromConsumerGroupResult"/>
public sealed class RemoveMembersFromConsumerGroupResult
{
    private readonly SafeAdminResultHandle _native;

    private RemoveMembersFromConsumerGroupResult(SafeAdminResultHandle native)
    {
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Completes when every member was removed, and fails otherwise — Java's <c>all()</c>.
    /// </summary>
    /// <returns>
    /// A new future over the whole request. When every member was to be removed, a member's failure is a
    /// <see cref="KafkaException"/> naming the member, its <see cref="Exception.InnerException"/> the member's own error.
    /// </returns>
    public KafkaFuture All() => new KafkaFuture(NativeAdminClient.RemoveMembersFromConsumerGroupAll(_native));

    /// <summary>
    /// The outcome of removing one member — Java's <c>memberResult(MemberToRemove)</c>.
    /// </summary>
    /// <param name="member">A member from the request this result came from.</param>
    /// <returns>
    /// A new future that completes when the member was removed, and fails with the member's error, or with the call's
    /// error when the whole request failed.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="member"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// The request removed every member, or <paramref name="member"/> was not part of it, as Java throws at the call;
    /// or its group instance id has a NUL character or an unpaired surrogate.
    /// </exception>
    public KafkaFuture MemberResult(MemberToRemove member)
    {
        if (member is null)
        {
            throw new ArgumentNullException(nameof(member));
        }

        return new KafkaFuture(NativeAdminClient.RemoveMembersFromConsumerGroupMemberResult(_native, member));
    }

    internal static RemoveMembersFromConsumerGroupResult OfClient(SafeAdminResultHandle native) => new RemoveMembersFromConsumerGroupResult(native);
}
