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
using System.Linq;
using System.Text;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M17/P1 S7 — the internal plumbing of the three transaction mock helpers (D8):
/// <c>SetCommitTransactionError</c> / <c>ClearCommitTransactionError</c>, <c>SentOffsets</c> and
/// <c>CommittedOffset</c>, including <c>CommittedOffset</c>'s grow rule.
/// </summary>
/// <remarks>
/// <para>
/// <b>The grow rule.</b> The ABI copies the committed metadata into a caller buffer, truncated at a
/// UTF-8 character boundary to fit <c>cap - 1</c> bytes plus a NUL, so a truncated result is between
/// <c>cap - 4</c> and <c>cap - 1</c> bytes long. <c>ReadCommittedOffset</c> therefore accepts a result
/// only when it is shorter than <c>cap - 4</c> and otherwise retries with four times the capacity, up
/// to 1,048,581 bytes (five attempts). It is tested twice: with a counting fake that applies the
/// ABI's truncation rule to the buffer it is given, and end to end through the mock core.
/// </para>
/// <para>
/// <b>Mutations.</b> The runs are recorded in
/// <c>design/history/M17/P1-producer-transactions/gate/CP4.txt</c>.
/// </para>
/// </remarks>
public sealed class MockProducerTransactionHelperTests
{
    private const string Topic = "txn-helper-topic";

    private const string GroupId = "txn-helper-group";

    private const string ZeroCodeMessage = "The commit-transaction error code must be non-zero; 0 is Errors.NONE.";

    private const string RangeMessage = "The commit-transaction error code must fit in a 16-bit signed integer.";

    private const string TooLargeMessage =
        "The committed offset metadata is larger than 1048576 bytes and cannot be returned untruncated.";

    /// <summary>The grow-rule cases: the metadata, and the attempts the rule takes to return it whole.</summary>
    public static IEnumerable<object[]> GrowCases() => new[]
    {
        // Fits the first buffer with room to spare.
        new object[] { "4096 ASCII bytes", new string('a', 4096), 1 },

        // Truncated by the first buffer.
        new object[] { "5000 ASCII bytes", new string('b', 5000), 2 },

        // Not truncated, but inside the false-positive window [cap - 4, cap - 1]: one harmless retry.
        new object[] { "4097 ASCII bytes", new string('c', 4097), 2 },

        // A 3-byte character straddling the first buffer's 4100-byte room: cut to 4098 bytes.
        new object[] { "a 3-byte character straddling the first cap", new string('d', 4098) + "€", 2 },

        // A 4-byte character straddling it: cut to 4097 bytes, the window's bottom edge.
        new object[] { "a 4-byte character straddling the first cap", new string('e', 4097) + "\U0001F600", 2 },

        // The largest value returned whole, on the last attempt.
        new object[] { "1048576 ASCII bytes", new string('f', 1_048_576), 5 },
    };

    /// <summary>
    /// <c>SetCommitTransactionError</c> rejects code 0 and codes outside a 16-bit signed integer with
    /// D8's messages, and accepts both ends of that range.
    /// </summary>
    [Theory]
    [InlineData(0, ZeroCodeMessage)]
    [InlineData(32768, RangeMessage)]
    [InlineData(-32769, RangeMessage)]
    public void SetCommitTransactionError_RejectsCodesOutsideItsRange(int code, string message)
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        ArgumentOutOfRangeException failure =
            Assert.Throws<ArgumentOutOfRangeException>(() => producer.MockSetCommitTransactionError(code, "x"));

        Assert.Equal("code", failure.ParamName);
        Assert.Equal(code, failure.ActualValue);
        Assert.Equal(new ArgumentOutOfRangeException("code", code, message).Message, failure.Message);
    }

    /// <summary><c>short.MinValue</c> and <c>short.MaxValue</c> are accepted.</summary>
    [Theory]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void SetCommitTransactionError_AcceptsBothEndsOfTheRange(int code)
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        producer.MockSetCommitTransactionError(code, "edge");
        producer.MockClearCommitTransactionError();
    }

    /// <summary>
    /// The installed error is sticky: commits in two successive transactions both fail with it, until
    /// <c>ClearCommitTransactionError()</c>, after which the commit succeeds. It is the core's typed
    /// error, so code 120 classifies as transaction-abortable.
    /// </summary>
    [Fact]
    public void SetCommitTransactionError_IsStickyUntilCleared()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.InitTransactions();
        producer.MockSetCommitTransactionError(120, "commit failed abortably");

        producer.BeginTransaction();
        KafkaException first = Assert.Throws<KafkaException>(() => producer.CommitTransaction());
        producer.AbortTransaction();

        producer.BeginTransaction();
        KafkaException second = Assert.Throws<KafkaException>(() => producer.CommitTransaction());

        foreach (KafkaException failure in new[] { first, second })
        {
            Assert.Equal(120, failure.Code);
            Assert.Equal("commit failed abortably", failure.Message);
            Assert.True(failure.IsTransactionAbortableError);
        }

        producer.MockClearCommitTransactionError();
        producer.CommitTransaction();
    }

    /// <summary>A null message installs the code's default message.</summary>
    [Fact]
    public void SetCommitTransactionError_WithANullMessage_UsesTheCodesDefaultMessage()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.MockSetCommitTransactionError(120, null);

        KafkaException failure = Assert.Throws<KafkaException>(() => producer.CommitTransaction());

        Assert.Equal(120, failure.Code);
        // Errors.TRANSACTION_ABORTABLE's default message (measured).
        Assert.Equal(
            "The server encountered an error with the transaction. The client can abort the transaction to " +
            "continue using this transactional ID.",
            failure.Message);
        Assert.True(failure.IsTransactionAbortableError);
    }

    /// <summary>
    /// <c>SentOffsets()</c>: false after begin, true after a non-empty send-offsets, still true after
    /// commit, false after the next begin.
    /// </summary>
    [Fact]
    public void SentOffsets_FollowsTheTransactionLifecycle()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.InitTransactions();

        producer.BeginTransaction();
        Assert.False(producer.MockSentOffsets());

        producer.SendOffsetsToTransaction(Offsets(new OffsetAndMetadata(4, "m")), GroupMetadata());
        Assert.True(producer.MockSentOffsets());

        producer.CommitTransaction();
        Assert.True(producer.MockSentOffsets());

        producer.BeginTransaction();
        Assert.False(producer.MockSentOffsets());
    }

    /// <summary>
    /// <c>CommittedOffset</c>'s preconditions: a null group id and a default partition (null topic)
    /// are rejected before any native call.
    /// </summary>
    [Fact]
    public void CommittedOffset_RejectsANullGroupIdAndANullTopic()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        ArgumentNullException nullGroup = Assert.Throws<ArgumentNullException>(
            () => producer.MockCommittedOffset(null!, new TopicPartition(Topic, 0)));
        Assert.Equal("groupId", nullGroup.ParamName);

        ArgumentException nullTopic = Assert.Throws<ArgumentException>(
            () => producer.MockCommittedOffset(GroupId, default));
        Assert.Equal("partition", nullTopic.ParamName);
        Assert.Equal(new ArgumentException("Topic names must not be null.", "partition").Message, nullTopic.Message);
    }

    /// <summary>
    /// <c>CommittedOffset</c> end to end: not found is null; a committed offset round-trips, with an
    /// absent leader epoch as null and a present one as its value.
    /// </summary>
    [Fact]
    public void CommittedOffset_ReturnsWhatTheTransactionCommitted()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.SendOffsetsToTransaction(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(17, "no-epoch"),
                [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(23, "with-epoch", 6),
            },
            GroupMetadata());
        producer.CommitTransaction();

        Assert.Null(producer.MockCommittedOffset("another-group", new TopicPartition(Topic, 0)));
        Assert.Null(producer.MockCommittedOffset(GroupId, new TopicPartition(Topic, 2)));

        OffsetAndMetadata? noEpoch = producer.MockCommittedOffset(GroupId, new TopicPartition(Topic, 0));
        Assert.NotNull(noEpoch);
        Assert.Equal(17, noEpoch!.Offset);
        Assert.Equal("no-epoch", noEpoch.Metadata);
        Assert.Null(noEpoch.LeaderEpoch);

        OffsetAndMetadata? withEpoch = producer.MockCommittedOffset(GroupId, new TopicPartition(Topic, 1));
        Assert.NotNull(withEpoch);
        Assert.Equal(23, withEpoch!.Offset);
        Assert.Equal("with-epoch", withEpoch.Metadata);
        Assert.Equal(6, withEpoch.LeaderEpoch);
    }

    /// <summary>
    /// The grow rule against the counting fake: each case returns the metadata whole, in the stated
    /// number of attempts, over the capacities 4101, 16404, 65616, 262464, 1048581.
    /// </summary>
    /// <remarks>Mutation: the naive condition <c>length &gt;= capacity - 1</c> — the straddle cases return truncated.</remarks>
    [Theory]
    [MemberData(nameof(GrowCases))]
    public void ReadCommittedOffset_ReturnsTheMetadataWhole(string description, string metadata, int attempts)
    {
        TruncatingLookup lookup = new TruncatingLookup(metadata, offset: 42, leaderEpoch: 9);

        OffsetAndMetadata? result = NativeProducer.ReadCommittedOffset(lookup.Lookup);

        Assert.NotNull(result);
        Assert.True(metadata == result!.Metadata, $"{description}: the metadata came back altered");
        Assert.Equal(42, result.Offset);
        Assert.Equal(9, result.LeaderEpoch);
        Assert.Equal(attempts, lookup.Attempts);
        Assert.Equal(new[] { 4101, 16404, 65616, 262464, 1048581 }.Take(attempts), lookup.Capacities);
    }

    /// <summary>
    /// Metadata longer than 1,048,576 bytes throws <see cref="InvalidOperationException"/> with D8's
    /// message after the fifth attempt.
    /// </summary>
    [Fact]
    public void ReadCommittedOffset_WhenTheMetadataIsTooLarge_ThrowsAfterFiveAttempts()
    {
        TruncatingLookup lookup = new TruncatingLookup(new string('g', 1_048_577), offset: 1, leaderEpoch: -1);

        InvalidOperationException failure =
            Assert.Throws<InvalidOperationException>(() => NativeProducer.ReadCommittedOffset(lookup.Lookup));

        Assert.Equal(TooLargeMessage, failure.Message);
        Assert.Equal(5, lookup.Attempts);
    }

    /// <summary>A lookup that finds nothing returns null after one attempt, and -1 maps to no epoch.</summary>
    [Fact]
    public void ReadCommittedOffset_NotFoundIsNull_AndMinusOneIsNoEpoch()
    {
        int attempts = 0;
        Assert.Null(NativeProducer.ReadCommittedOffset((byte[] buffer, out long offset, out int leaderEpoch) =>
        {
            attempts++;
            offset = 0;
            leaderEpoch = 0;
            return false;
        }));
        Assert.Equal(1, attempts);

        OffsetAndMetadata? noEpoch = NativeProducer.ReadCommittedOffset(new TruncatingLookup("m", 3, -1).Lookup);
        Assert.Null(noEpoch!.LeaderEpoch);
    }

    /// <summary>The grow cases end to end: committed through the mock core, read back whole.</summary>
    [Theory]
    [MemberData(nameof(GrowCases))]
    public void CommittedOffset_ReturnsLargeMetadataWhole_EndToEnd(string description, string metadata, int attempts)
    {
        _ = attempts;
        using NativeProducer producer = CommitMetadata(metadata);

        OffsetAndMetadata? result = producer.MockCommittedOffset(GroupId, new TopicPartition(Topic, 0));

        Assert.NotNull(result);
        Assert.True(metadata == result!.Metadata, $"{description}: the metadata came back altered");
        Assert.Equal(31, result.Offset);
        Assert.Equal(2, result.LeaderEpoch);
    }

    /// <summary>The too-large case end to end.</summary>
    [Fact]
    public void CommittedOffset_WhenTheMetadataIsTooLarge_Throws_EndToEnd()
    {
        using NativeProducer producer = CommitMetadata(new string('g', 1_048_577));

        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(
            () => producer.MockCommittedOffset(GroupId, new TopicPartition(Topic, 0)));

        Assert.Equal(TooLargeMessage, failure.Message);
    }

    private static NativeProducer CommitMetadata(string metadata)
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.SendOffsetsToTransaction(Offsets(new OffsetAndMetadata(31, metadata, 2)), GroupMetadata());
        producer.CommitTransaction();
        return producer;
    }

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets(OffsetAndMetadata value) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition(Topic, 0)] = value };

    private static ConsumerGroupMetadata GroupMetadata() =>
        ConsumerGroupMetadata.FromCopiedValues(GroupId, -1, string.Empty, null);

    /// <summary>
    /// A committed-offset lookup that applies the ABI's copy rule to the buffer it is given (the
    /// header's <c>MockProducer_committed_offset</c> contract): the metadata is truncated at the last
    /// UTF-8 character boundary that leaves room for the NUL, then NUL-terminated. It records each
    /// buffer's length.
    /// </summary>
    private sealed class TruncatingLookup
    {
        private readonly byte[] _metadata;
        private readonly long _offset;
        private readonly int _leaderEpoch;

        internal TruncatingLookup(string metadata, long offset, int leaderEpoch)
        {
            _metadata = Encoding.UTF8.GetBytes(metadata);
            _offset = offset;
            _leaderEpoch = leaderEpoch;
        }

        internal List<int> Capacities { get; } = new List<int>();

        internal int Attempts => Capacities.Count;

        internal bool Lookup(byte[] buffer, out long offset, out int leaderEpoch)
        {
            Capacities.Add(buffer.Length);
            int room = buffer.Length - 1;
            int length = _metadata.Length;
            if (length > room)
            {
                // Back up to the start of the character that would cross the room.
                length = room;
                while (length > 0 && (_metadata[length] & 0xC0) == 0x80)
                {
                    length--;
                }
            }

            Array.Copy(_metadata, buffer, length);
            buffer[length] = 0;
            offset = _offset;
            leaderEpoch = _leaderEpoch;
            return true;
        }
    }
}
