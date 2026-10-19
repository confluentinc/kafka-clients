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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.RemoveMembersFromConsumerGroup"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult</c>
/// (<c>RemoveMembersFromConsumerGroupResult.java:33-42</c>): one completion over a map whose
/// values are the per-member outcomes (Java's <c>Map&lt;MemberIdentity, Errors&gt;</c>, keyed
/// here by <c>group.instance.id</c>), **plus** the original request's member collection — the
/// second stored field Java carries (<c>memberInfos</c>, <c>:36</c>), the same two-field
/// pattern as <see cref="DeleteConsumerGroupOffsetsResult"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-member failure is a map VALUE here, not a failed completion</b> — the same
/// shape as <see cref="DeleteConsumerGroupOffsetsResult"/> / <see cref="AlterConsumerGroupOffsetsResult"/>.
/// A whole-request failure fails the completion itself, and both accessors rethrow it
/// unchanged, as Java's continuations test the <c>throwable</c> first.
/// </para>
/// <para>
/// ⚠ <b><see cref="All"/> rethrows the core's own <c>all()</c> outcome</b>, read off the
/// native result together with the map, rather than re-deriving it here. Outside
/// <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/> mode that is the outcome
/// <see cref="MemberResult"/> reports for the first failing requested member; which member
/// is first is the core's choice (group-instance-id order — Java iterates its set in
/// unspecified order).
/// </para>
/// <para>
/// ⚠⚠ <b>In <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/> mode a member
/// failure is a wrapper whose cause is the member's own error, as in Java.</b> Java's
/// <c>all()</c> (<c>:56-64</c>) throws
/// <c>new KafkaException("Encounter exception when trying to remove: " + identity,
/// memberException)</c>. The core builds the same error, with "exception" spelled "error":
/// a bare <see cref="KafkaException"/> with <see cref="KafkaException.Code"/> -1
/// (<c>UNKNOWN_SERVER_ERROR</c>, the code a Java <c>KafkaException</c> without one flattens
/// to), not retriable, whose message names the member. The member's own error (for example
/// <c>UNKNOWN_MEMBER_ID</c>, code 25) is its <see cref="Exception.InnerException"/> — Java's
/// <c>getCause()</c>, read through <c>kafka_common_Error_cause</c> since M15/P13.3 (D11); it
/// was not observable before. There is no per-member outcome in this mode at all, as in
/// Java.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the request has completed, and needs
/// no thread-pool thread: the futures here are projections of the one latch, composed on the waiting thread, never
/// over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncRemoveMembersFromConsumerGroupResult"/>
public sealed class RemoveMembersFromConsumerGroupResult
{
    private readonly KafkaFuture<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> _future;
    private readonly IReadOnlyCollection<MemberToRemove> _memberInfos;

    /// <summary>
    /// Wraps the single completion and the original request's member collection — Java's
    /// package-private
    /// <c>RemoveMembersFromConsumerGroupResult(KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;,
    /// Set&lt;MemberToRemove&gt;)</c> (<c>:38-42</c>).
    /// </summary>
    /// <param name="future">
    /// The single completion: <c>PerKey</c> is the per-member map keyed by
    /// <c>group.instance.id</c> (a <see langword="null"/> value is Java's <c>Errors.NONE</c>;
    /// empty in removeAll mode), <c>All</c> the fault Java's <c>all()</c> reports, or
    /// <see langword="null"/> when it succeeds.
    /// </param>
    /// <param name="memberInfos">The original request's members; empty in removeAll mode.</param>
    internal RemoveMembersFromConsumerGroupResult(
        KafkaFuture<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> future,
        IReadOnlyCollection<MemberToRemove> memberInfos)
    {
        _future = future;
        _memberInfos = memberInfos;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static RemoveMembersFromConsumerGroupResult From(
        (
            SingleAdminOperation<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> Operation,
            IReadOnlyCollection<MemberToRemove> Members) submitted) =>
        new RemoveMembersFromConsumerGroupResult(
            new KafkaFuture<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)>(
                submitted.Operation.SyncSource),
            submitted.Members);

    /// <summary>
    /// <see langword="true"/> when this result came from a removeAll request — Java's private
    /// <c>removeAll()</c> (<c>:113-115</c>), derived from the original member collection being
    /// empty rather than stored as its own field, mirroring
    /// <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/>.
    /// </summary>
    /// <remarks>
    /// <see langword="internal"/>, as Java's is <c>private</c>: a caller already knows which
    /// mode it asked for, from the options it passed
    /// (<see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/>).
    /// </remarks>
    internal bool RemoveAll => _memberInfos.Count == 0;

    /// <summary>
    /// Returns a future which indicates whether the request was 100% success, i.e. no either
    /// top level or member level error — Java's <c>all()</c> (<c>:49-76</c>). If not, the
    /// first member error is thrown.
    /// </summary>
    /// <returns>A future representing the whole request. Its <c>Get()</c> waits for the request.</returns>
    /// <remarks>
    /// The fault is the core's <c>kafka_admin_RemoveMembersFromConsumerGroupResult_all</c>,
    /// rethrown unchanged — see the type remarks for what it carries in
    /// <see cref="RemoveMembersFromConsumerGroupOptions.RemoveAll"/> mode. A whole-request
    /// failure fails the completion instead and propagates unchanged through the
    /// projection (<c>:52-53</c>). A fresh future per call, as Java allocates a
    /// fresh <c>KafkaFutureImpl</c> per <c>all()</c> call (<c>:50</c>).
    /// </remarks>
    public KafkaFuture All() =>
        SyncAdminFutures.Project(_future, static outcome =>
        {
            // A call-level failure propagates through the projection's source.
            if (outcome.All is not null)
            {
                throw outcome.All;
            }

            return true;
        }).Erase();

    /// <summary>
    /// Returns the selected member's future — Java's <c>memberResult(MemberToRemove)</c>
    /// (<c>:81-98</c>).
    /// </summary>
    /// <param name="member">A member from the request this result came from.</param>
    /// <returns>
    /// A future that completes when that member was removed successfully, and fails otherwise.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// This result came from a removeAll request, or <paramref name="member"/> was not part of
    /// this request — both thrown <b>synchronously</b>, out of this call, mirroring Java's own
    /// synchronous precondition checks (<c>:82-87</c>) which run before the future is even
    /// consulted.
    /// </exception>
    /// <remarks>
    /// The checks run in this method, before the future is built, as in
    /// <see cref="DeleteConsumerGroupOffsetsResult.PartitionResult"/>: Java's synchronous precondition checks stay
    /// synchronous, and only the rest is the future's projection. A fresh future per call.
    /// </remarks>
    public KafkaFuture MemberResult(MemberToRemove member)
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

        string groupInstanceId = member.GroupInstanceId;
        return SyncAdminFutures.Project(_future, outcome =>
        {
            // A call-level failure propagates through the projection's source — Java's `throwable != null`
            // branch (:91-92).
            //
            // The indexer, not TryGetValue: `member` passed the request-set check above, and the
            // core reports one row per requested member, so a miss is a core contract violation —
            // failing loudly beats reporting a success nobody observed. A value is that member's
            // own outcome, including the core's "not included in the removal response" error for a
            // requested member the broker did not answer (Java's `getSubLevelError`, :100-111).
            KafkaException? error = outcome.PerKey[groupInstanceId];
            if (error is not null)
            {
                throw error;
            }

            return true;
        }).Erase();
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
