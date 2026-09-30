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
/// A whole-request failure faults the awaitable itself, and both accessors rethrow it
/// unchanged, as Java's continuations test the <c>throwable</c> first.
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> rethrows the core's own <c>all()</c> outcome</b>, read off the
/// native result together with the map, rather than re-deriving it here. Outside
/// <see cref="RemoveAll"/> mode that is the outcome <see cref="MemberResult"/> reports for the
/// first failing requested member; which member is first is the core's choice
/// (group-instance-id order — Java iterates its set in unspecified order).
/// </para>
/// <para>
/// ⚠⚠ <b>In <see cref="RemoveAll"/> mode a member failure has a different error code than
/// in Java, recorded here rather than hidden.</b> Java's <c>all()</c> (<c>:56-64</c>) throws
/// <c>new KafkaException("Encounter exception when trying to remove: " + identity,
/// memberException)</c>. The core builds the same error, with "exception" spelled "error",
/// but the C API cannot read an error's cause, so it arrives as a bare
/// <see cref="KafkaException"/> with <see cref="KafkaException.Code"/> -1
/// (<c>UNKNOWN_SERVER_ERROR</c>), not retriable, with no
/// <see cref="Exception.InnerException"/>: the message still names the member, but the
/// member's own error code is not observable. There is no per-member outcome in this mode
/// at all, as in Java.
/// </para>
/// </remarks>
public sealed class RemoveMembersFromConsumerGroupResult
{
    private readonly Task<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> _future;
    private readonly IReadOnlyCollection<MemberToRemove> _memberInfos;

    /// <summary>
    /// Wraps the single awaitable and the original request's member collection — Java's
    /// package-private
    /// <c>RemoveMembersFromConsumerGroupResult(KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;,
    /// Set&lt;MemberToRemove&gt;)</c> (<c>:38-42</c>).
    /// </summary>
    /// <param name="future">
    /// The single awaitable: <c>PerKey</c> is the per-member map keyed by
    /// <c>group.instance.id</c> (a <see langword="null"/> value is Java's <c>Errors.NONE</c>;
    /// empty in removeAll mode), <c>All</c> the fault Java's <c>all()</c> reports, or
    /// <see langword="null"/> when it succeeds.
    /// </param>
    /// <param name="memberInfos">The original request's members; empty in removeAll mode.</param>
    internal RemoveMembersFromConsumerGroupResult(
        Task<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> future,
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
    /// <remarks>
    /// The fault is the core's <c>kafka_admin_RemoveMembersFromConsumerGroupResult_all</c>,
    /// rethrown unchanged — see the type remarks for what it carries in
    /// <see cref="RemoveAll"/> mode. A whole-request failure faults the awaitable instead and
    /// propagates unchanged through the <see langword="await"/> (<c>:52-53</c>). A fresh task
    /// per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per <c>all()</c> call
    /// (<c>:50</c>).
    /// </remarks>
    public async Task All()
    {
        // A call-level failure propagates by the await itself.
        KafkaException? all = (await _future.ConfigureAwait(false)).All;

        if (all is not null)
        {
            throw all;
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
        // A call-level failure propagates by the await itself — Java's `throwable != null`
        // branch (:91-92).
        IReadOnlyDictionary<string, KafkaException?> perKey = (await _future.ConfigureAwait(false)).PerKey;

        // The indexer, not TryGetValue: `member` passed the request-set check above, and the
        // core reports one row per requested member, so a miss is a core contract violation —
        // faulting loudly beats reporting a success nobody observed. A value is that member's
        // own outcome, including the core's "not included in the removal response" error for a
        // requested member the broker did not answer (Java's `getSubLevelError`, :100-111).
        KafkaException? error = perKey[member.GroupInstanceId];
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
