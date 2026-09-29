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

namespace Confluent.Kafka;

/// <summary>
/// A snapshot of a consumer's group membership — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerGroupMetadata</c>.
/// <see cref="IConsumerCommon.GroupMetadata"/> returns one whose fields are owned copies
/// read out of the owned (Category-3) ABI handle, which is then destroyed
/// (ffi-marshalling.md §B2/§B3). The public constructors build one from values the
/// caller supplies; like Java's, they are deprecated.
/// </summary>
/// <remarks>
/// Broker-free / pre-join, the fields carry defaults: on a real
/// <c>AsyncKafkaConsumer</c> before it has joined a group, <see cref="GroupId"/> is the
/// configured <c>group.id</c>, <see cref="GenerationId"/> is <c>-1</c> and
/// <see cref="MemberId"/> is empty, as in Java, and <see cref="GroupInstanceId"/> is
/// <see langword="null"/> even when <c>group.instance.id</c> is configured, where Java's
/// consumer reports the configured id; an <c>AsyncMockConsumer</c> returns Java's mock
/// sentinels. The values become meaningful membership state once the consumer joins a
/// group against a broker.
/// </remarks>
public sealed class ConsumerGroupMetadata
{
    // M17/P1 D2's text (Q7), adapted from the @deprecated javadoc on both Java constructors
    // (ConsumerGroupMetadata.java:35, :49): "Since 4.2, please use
    // KafkaConsumer#groupMetadata() instead. This class will be an interface in Kafka 5.0."
    // Both are annotated @Deprecated(since = "4.2", forRemoval = true) (:37, :51). Warning level.
    private const string ObsoleteMessage =
        "Deprecated since Kafka 4.2: use IConsumerCommon.GroupMetadata() instead. "
        + "ConsumerGroupMetadata becomes an interface in Kafka 5.0.";

    /// <summary>
    /// Initializes a new instance with all four fields — Java's
    /// <c>ConsumerGroupMetadata(String, int, String, Optional&lt;String&gt;)</c>
    /// (<c>ConsumerGroupMetadata.java:37-46</c>).
    /// </summary>
    /// <param name="groupId">The consumer group id. Must not be <see langword="null"/>.</param>
    /// <param name="generationId">
    /// The group generation id. Any value is accepted; <c>-1</c> means unknown.
    /// </param>
    /// <param name="memberId">
    /// The member id. Must not be <see langword="null"/>; the empty string means unknown.
    /// </param>
    /// <param name="groupInstanceId">
    /// The static group instance id, or <see langword="null"/> for none (Java's
    /// <c>Optional.empty()</c>).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> is <see langword="null"/> ("group.id can't be null"), or
    /// <paramref name="memberId"/> is <see langword="null"/> ("member.id can't be null").
    /// The group id is checked first, as in Java.
    /// </exception>
    /// <remarks>
    /// Deprecated as in Java, where it is <c>@Deprecated(since = "4.2", forRemoval =
    /// true)</c>: a live consumer's metadata comes from
    /// <see cref="IConsumerCommon.GroupMetadata"/>. Java's third check, that the
    /// <c>Optional</c> reference itself is not null, has no .NET analogue:
    /// <see langword="null"/> is the empty case here.
    /// </remarks>
    [Obsolete(ObsoleteMessage)]
    public ConsumerGroupMetadata(string groupId, int generationId, string memberId, string? groupInstanceId)
        : this(
            groupId ?? throw new ArgumentNullException(nameof(groupId), "group.id can't be null"),
            memberId ?? throw new ArgumentNullException(nameof(memberId), "member.id can't be null"),
            generationId,
            groupInstanceId)
    {
    }

    /// <summary>
    /// Initializes a new instance for <paramref name="groupId"/> with no membership —
    /// Java's <c>ConsumerGroupMetadata(String)</c> (<c>ConsumerGroupMetadata.java:51-57</c>):
    /// <see cref="GenerationId"/> is <c>-1</c>, <see cref="MemberId"/> is empty and
    /// <see cref="GroupInstanceId"/> is <see langword="null"/> (Java's
    /// <c>JoinGroupRequest.UNKNOWN_GENERATION_ID</c>, <c>UNKNOWN_MEMBER_ID</c> and
    /// <c>Optional.empty()</c>).
    /// </summary>
    /// <param name="groupId">The consumer group id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> is <see langword="null"/> ("group.id can't be null").
    /// </exception>
    /// <remarks>
    /// Deprecated as in Java; see
    /// <see cref="ConsumerGroupMetadata(string, int, string, string)"/>.
    /// </remarks>
    [Obsolete(ObsoleteMessage)]
    public ConsumerGroupMetadata(string groupId)
        : this(groupId, -1, string.Empty, null)
    {
    }

    // Assigns without checking. Its parameter order differs from the public four-argument
    // constructor's only so that the two signatures are distinct.
    private ConsumerGroupMetadata(string groupId, string memberId, int generationId, string? groupInstanceId)
    {
        GroupId = groupId;
        GenerationId = generationId;
        MemberId = memberId;
        GroupInstanceId = groupInstanceId;
    }

    /// <summary>
    /// The consumer group id; for an instance a real consumer returns, its configured
    /// <c>group.id</c>.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// The group generation id, or <c>-1</c> when unknown, as a real consumer reports it
    /// before it has joined a group.
    /// </summary>
    public int GenerationId { get; }

    /// <summary>
    /// The member id assigned by the coordinator, or the empty string when unknown, as a
    /// real consumer reports it before it has joined a group.
    /// </summary>
    public string MemberId { get; }

    /// <summary>
    /// The static group instance id, or <see langword="null"/> when there is none (Java's
    /// <c>Optional.empty()</c>).
    /// </summary>
    public string? GroupInstanceId { get; }

    /// <summary>
    /// Creates an instance from values the ABI marshal has copied out of the handle, where
    /// a null group or member id has already become the empty string. It checks nothing:
    /// it is the non-obsolete construction path, so the binding's own code needs no
    /// <c>CS0618</c> suppression (M17/P1 D2).
    /// </summary>
    internal static ConsumerGroupMetadata FromCopiedValues(
        string groupId, int generationId, string memberId, string? groupInstanceId)
        => new(groupId, memberId, generationId, groupInstanceId);
}
