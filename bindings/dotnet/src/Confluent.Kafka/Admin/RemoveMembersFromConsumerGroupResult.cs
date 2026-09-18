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
using System.Globalization;
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.RemoveMembersFromConsumerGroup"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult</c>
/// (<c>RemoveMembersFromConsumerGroupResult.java:33-42</c>): one awaitable over a map whose
/// values are the per-member outcomes (Java's <c>Map&lt;MemberIdentity, Errors&gt;</c>, keyed
/// here by <c>group.instance.id</c>), **plus** the original request's member collection — the
/// second stored field Java carries (<c>memberInfos</c>, <c>:36</c>), the same two-field
/// pattern as <see cref="DeleteConsumerGroupOffsetsResult"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-member failure is a map VALUE here, not a faulted awaitable</b> — the same
/// shape as <see cref="DeleteConsumerGroupOffsetsResult"/> / <see cref="AlterConsumerGroupOffsetsResult"/>.
/// </para>
/// <para>
/// ⚠⚠ <b><see cref="RemoveAll"/> forces a real ABI deviation, recorded here rather than
/// hidden.</b> Java's <c>removeAll()</c> (<c>:113-115</c>) is <c>memberInfos.isEmpty()</c>,
/// and when it is true <see cref="All"/> iterates the <em>resolved</em> map instead of the
/// original request set — because in real Java the broker still reports one outcome per
/// member it actually removed. The Rust core has no per-member outcome model for "remove
/// everyone": in removeAll mode the ABI result handle always carries <b>zero rows</b>, and
/// any failure is delivered only as a call-level fault (see
/// <c>kafka_admin_AdminClient_remove_members_from_consumer_group</c>'s doc comment). So
/// <see cref="All"/> below is written in Java's exact shape — it still iterates the resolved
/// map in removeAll mode — but that iteration is over a map the core guarantees is empty,
/// so it can only ever find success there; the only way a removeAll request's own failure
/// reaches the caller is the <see langword="await"/> propagating a faulted <see cref="Task"/>.
/// </para>
/// </remarks>
public sealed class RemoveMembersFromConsumerGroupResult
{
    private readonly Task<IReadOnlyDictionary<string, KafkaException?>> _future;
    private readonly IReadOnlyCollection<MemberToRemove> _memberInfos;

    /// <summary>
    /// Wraps the single awaitable and the original request's member collection — Java's
    /// package-private
    /// <c>RemoveMembersFromConsumerGroupResult(KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;,
    /// Set&lt;MemberToRemove&gt;)</c> (<c>:38-42</c>).
    /// </summary>
    internal RemoveMembersFromConsumerGroupResult(
        Task<IReadOnlyDictionary<string, KafkaException?>> future,
        IReadOnlyCollection<MemberToRemove> memberInfos)
    {
        _future = future;
        _memberInfos = memberInfos;
    }

    /// <summary>
    /// <see langword="true"/> when this result came from a removeAll request — Java's private
    /// <c>removeAll()</c> (<c>:113-115</c>), derived from the original member collection being
    /// empty rather than stored as its own field, mirroring
    /// <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/>.
    /// </summary>
    public bool RemoveAll => _memberInfos.Count == 0;

    /// <summary>
    /// Returns a task which indicates whether the request was 100% success, i.e. no either
    /// top level or member level error — Java's <c>all()</c> (<c>:49-76</c>). If not, the
    /// first member error is thrown.
    /// </summary>
    /// <returns>A task representing the whole request.</returns>
    /// <remarks>See the type remarks for the removeAll-mode deviation this forces.</remarks>
    public async Task All()
    {
        IReadOnlyDictionary<string, KafkaException?> results = await _future.ConfigureAwait(false);

        if (RemoveAll)
        {
            // Java iterates `memberErrors.entrySet()` here (:56-64). The core guarantees this
            // map is empty in removeAll mode (see the type remarks), so this can only find
            // success — a removeAll failure reaches the caller only via the await above.
            foreach (KeyValuePair<string, KafkaException?> entry in results)
            {
                if (entry.Value is not null)
                {
                    throw new KafkaException(
                        entry.Value.Code,
                        string.Format(
                            CultureInfo.InvariantCulture,
                            "Encounter exception when trying to remove: {0}",
                            entry.Key),
                        entry.Value.IsRetriable);
                }
            }
        }
        else
        {
            foreach (MemberToRemove member in _memberInfos)
            {
                ThrowIfSubLevelError(results, member.GroupInstanceId);
            }
        }
    }

    /// <summary>
    /// Returns the selected member's task — Java's <c>memberResult(MemberToRemove)</c>
    /// (<c>:81-98</c>).
    /// </summary>
    /// <param name="member">A member from the request this result came from.</param>
    /// <returns>
    /// A task that completes when that member was removed successfully, and faults otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// This result came from a removeAll request, or <paramref name="member"/> was not part of
    /// this request — both thrown <b>synchronously</b>, out of this call, mirroring Java's own
    /// synchronous precondition checks (<c>:82-87</c>) which run before the future is even
    /// consulted.
    /// </exception>
    /// <remarks>
    /// This method itself is deliberately <b>not</b> <see langword="async"/>, for the same
    /// reason as <see cref="DeleteConsumerGroupOffsetsResult.PartitionResult"/>: an
    /// <see langword="async"/> method would defer even a pre-<see langword="await"/> throw into
    /// the returned <see cref="Task"/>, turning Java's synchronous precondition checks into an
    /// asynchronous fault.
    /// </remarks>
    public Task MemberResult(MemberToRemove member)
    {
        if (member is null)
        {
            throw new ArgumentNullException(nameof(member));
        }

        if (RemoveAll)
        {
            throw new ArgumentException("The method: memberResult is not applicable in 'removeAll' mode");
        }

        if (!Contains(_memberInfos, member))
        {
            // Java's `IllegalArgumentException` (:86) interpolates `member.toString()`, which
            // is unoverridden identity-hash text (see MemberToRemove's remarks) — this
            // substitutes GroupInstanceId, a strictly more useful message.
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Member {0} was not included in the original request",
                    member.GroupInstanceId));
        }

        return MemberResultAsync(member);
    }

    private async Task MemberResultAsync(MemberToRemove member)
    {
        IReadOnlyDictionary<string, KafkaException?> results = await _future.ConfigureAwait(false);

        ThrowIfSubLevelError(results, member.GroupInstanceId);
    }

    /// <summary>
    /// Java's <c>maybeCompleteExceptionally</c> / <c>KafkaAdminClient.getSubLevelError</c>
    /// (<c>:100-111</c>): a member missing from the resolved map faults with an
    /// <see cref="ArgumentException"/> distinct from <see cref="MemberResult"/>'s synchronous
    /// "not in the original request" one; a present entry with a non-null value is that
    /// member's own error.
    /// </summary>
    private static void ThrowIfSubLevelError(
        IReadOnlyDictionary<string, KafkaException?> results, string groupInstanceId)
    {
        if (!results.TryGetValue(groupInstanceId, out KafkaException? error))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Member \"{0}\" was not included in the removal response",
                    groupInstanceId));
        }

        if (error is not null)
        {
            throw error;
        }
    }

    private static bool Contains(IReadOnlyCollection<MemberToRemove> members, MemberToRemove member)
    {
        foreach (MemberToRemove candidate in members)
        {
            if (candidate.Equals(member))
            {
                return true;
            }
        }

        return false;
    }
}
