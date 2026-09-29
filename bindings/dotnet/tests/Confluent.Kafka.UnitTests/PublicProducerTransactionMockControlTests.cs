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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S8, the mock helpers (D8): one forwarding smoke per public helper on both mocks, and
/// Java's <c>MockProducerTest</c> offset tests (397-583) projected through
/// <c>SentOffsets()</c> / <c>CommittedOffset</c> (B6). Every test runs on
/// <see cref="MockProducer{TKey, TValue}"/> (<c>"sync"</c>) and
/// <see cref="AsyncMockProducer{TKey, TValue}"/> (<c>"async"</c>). The helpers' edge cases are in
/// <c>Interop.MockProducerTransactionHelperTests</c> (S7).
/// </summary>
/// <remarks>
/// Java compares <c>consumerGroupOffsetsHistory()</c> to a whole list; the ABI exports only the
/// newest committed value per group and partition (B6), so each row asserts that projection for
/// every pair Java's expected map names, and <see langword="null"/> for the pairs it omits.
/// Python 1031 / 1078 / 1098 / 1125 / 1162 / 1202 are these rows (§5.4).
/// </remarks>
public sealed class PublicProducerTransactionMockControlTests
{
    private const string Topic = "txn-offsets-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- the helpers forward (one smoke each) ----

    /// <summary>
    /// <c>SetCommitTransactionError(code, message)</c> reaches the core with both arguments: the
    /// next commit fails with exactly that code and message.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SetCommitTransactionError_ForwardsTheCodeAndTheMessage(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        mock.SetCommitTransactionError(48, "forwarded commit error");

        KafkaException failure = await AssertFails(mock.CommitTransaction());
        Assert.Equal(48, failure.Code);
        Assert.Equal("forwarded commit error", failure.Message);
    }

    /// <summary>
    /// <c>SetCommitTransactionError(code)</c> with the message defaulted forwards a null message,
    /// so the core uses the code's default text (measured at CP4, <c>errors.rs:544</c>).
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SetCommitTransactionError_WithTheMessageDefaulted_UsesTheCodesDefaultText(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        mock.SetCommitTransactionError(120);

        KafkaException failure = await AssertFails(mock.CommitTransaction());
        Assert.Equal(120, failure.Code);
        Assert.Equal(
            "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.",
            failure.Message);
    }

    /// <summary><c>ClearCommitTransactionError()</c> reaches the core: a later commit succeeds.</summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task ClearCommitTransactionError_LetsTheNextCommitSucceed(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();
        mock.SetCommitTransactionError(120, "commit failed abortably");

        mock.ClearCommitTransactionError();

        await Run(mock.CommitTransaction());
    }

    /// <summary>Each of the four helpers throws <see cref="ObjectDisposedException"/> once the mock is closed.</summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public void Helpers_AfterDispose_ThrowObjectDisposed(string flavour)
    {
        ITransactionalMock mock = Create(flavour);
        mock.Dispose();

        Assert.Throws<ObjectDisposedException>(() => mock.SetCommitTransactionError(120, "x"));
        Assert.Throws<ObjectDisposedException>(mock.ClearCommitTransactionError);
        Assert.Throws<ObjectDisposedException>(() => mock.SentOffsets());
        Assert.Throws<ObjectDisposedException>(() => mock.CommittedOffset("g", new TopicPartition(Topic, 0)));
    }

    // ---- MockProducerTest, offsets ----

    /// <summary>
    /// MPT 397 <c>shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled</c>,
    /// projected (B6): nothing is committed before the commit; after it, each group's two offsets.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CommitTransaction_PublishesTheOffsetsOfEachGroup(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L), (1, 73L)), Group("g1")));
        await Run(mock.SendOffsetsToTransaction(Offsets((0, 101L), (1, 21L)), Group("g2")));
        AssertCommitted(mock, "g1", (0, null), (1, null));
        AssertCommitted(mock, "g2", (0, null), (1, null));

        await Run(mock.CommitTransaction());

        AssertCommitted(mock, "g1", (0, 42L), (1, 73L));
        AssertCommitted(mock, "g2", (0, 101L), (1, 21L));
    }

    /// <summary>MPT 438 <c>shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata</c>.</summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SendOffsetsToTransaction_WithAnEmptyMap_SendsNothing(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        await Run(mock.SendOffsetsToTransaction(new Dictionary<TopicPartition, OffsetAndMetadata>(), Group("groupId")));

        Assert.False(mock.SentOffsets());
    }

    /// <summary>MPT 447 <c>shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata</c>.</summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SendOffsetsToTransaction_WithOffsets_SetsSentOffsets(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();
        Assert.False(mock.SentOffsets());

        await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L)), Group("groupId")));

        Assert.True(mock.SentOffsets());
    }

    /// <summary>MPT 464 <c>shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction</c>.</summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SentOffsets_IsResetOnlyByBeginTransaction(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();
        Assert.False(mock.SentOffsets());

        for (int round = 0; round < 2; round++)
        {
            await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L)), Group("groupId")));
            await Run(mock.CommitTransaction());
            Assert.True(mock.SentOffsets(), $"round {round}: the commit reset the flag");

            mock.BeginTransaction();
            Assert.False(mock.SentOffsets());
        }
    }

    /// <summary>
    /// MPT 492 <c>shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled</c>,
    /// projected (B6): two sends for one group accumulate, and the later value wins for p1.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CommitTransaction_PublishesTheLatestOffsetPerPartition(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L), (1, 73L)), Group("g")));
        await Run(mock.SendOffsetsToTransaction(Offsets((1, 101L), (2, 21L)), Group("g")));
        AssertCommitted(mock, "g", (0, null), (1, null), (2, null));

        await Run(mock.CommitTransaction());

        AssertCommitted(mock, "g", (0, 42L), (1, 101L), (2, 21L));
    }

    /// <summary>
    /// MPT 529 <c>shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled</c>, projected (B6):
    /// nothing is committed after each abort and the empty commit that follows it.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task AbortTransaction_DropsTheTransactionsOffsets(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());

        for (int round = 0; round < 2; round++)
        {
            mock.BeginTransaction();
            await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L), (1, 73L)), Group("g")));
            await Run(mock.AbortTransaction());

            mock.BeginTransaction();
            await Run(mock.CommitTransaction());
            AssertCommitted(mock, "g", (0, null), (1, null));
        }
    }

    /// <summary>
    /// MPT 558 <c>shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled</c>,
    /// projected (B6): a later empty abort keeps the committed offsets.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task AbortTransaction_KeepsEarlierCommittedOffsets(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();
        await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L), (1, 73L)), Group("g")));
        await Run(mock.CommitTransaction());

        mock.BeginTransaction();
        await Run(mock.AbortTransaction());

        AssertCommitted(mock, "g", (0, 42L), (1, 73L));
    }

    /// <summary>
    /// MPT 583 <c>shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled</c>,
    /// projected (B6): the first group's commit survives; the second group's aborted offsets were
    /// never committed.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task AbortTransaction_KeepsTheCommittedGroup_AndDropsTheAbortedOne(string flavour)
    {
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();
        await Run(mock.SendOffsetsToTransaction(Offsets((0, 42L), (1, 73L)), Group("g")));
        await Run(mock.CommitTransaction());

        mock.BeginTransaction();
        await Run(mock.SendOffsetsToTransaction(Offsets((2, 53L), (3, 84L)), Group("g2")));
        await Run(mock.AbortTransaction());

        AssertCommitted(mock, "g", (0, 42L), (1, 73L));
        AssertCommitted(mock, "g2", (2, null), (3, null));
    }

    /// <summary>
    /// Python 1452 <c>test_async_txn_send_offsets_to_transaction</c> (and its sync sibling 1031's
    /// metadata source): the group metadata a <see cref="MockConsumer{TKey, TValue}"/> reports is
    /// accepted, and the offset is committed under its group id.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SendOffsetsToTransaction_WithAConsumersGroupMetadata_CommitsUnderItsGroupId(string flavour)
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        ConsumerGroupMetadata metadata = consumer.GroupMetadata();
        using ITransactionalMock mock = Create(flavour);
        await Run(mock.InitTransactions());
        mock.BeginTransaction();

        await Run(mock.SendOffsetsToTransaction(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("t", 1)] = new OffsetAndMetadata(9) },
            metadata));
        Assert.True(mock.SentOffsets());
        await Run(mock.CommitTransaction());

        OffsetAndMetadata? committed = mock.CommittedOffset(metadata.GroupId, new TopicPartition("t", 1));
        Assert.NotNull(committed);
        Assert.Equal(9L, committed!.Offset);
    }

    // ---- helpers ----

    private static ITransactionalMock Create(string flavour) => flavour switch
    {
        "sync" => new SyncMock(),
        "async" => new AsyncMock(),
        _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
    };

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(params (int Partition, long Offset)[] entries)
    {
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
        foreach ((int partition, long offset) in entries)
        {
            offsets[new TopicPartition(Topic, partition)] = new OffsetAndMetadata(offset);
        }

        return offsets;
    }

#pragma warning disable CS0618 // Java's tests use the obsolete group-id constructor too
    private static ConsumerGroupMetadata Group(string groupId) => new ConsumerGroupMetadata(groupId);
#pragma warning restore CS0618

    private static void AssertCommitted(ITransactionalMock mock, string groupId, params (int Partition, long? Offset)[] expected)
    {
        foreach ((int partition, long? offset) in expected)
        {
            OffsetAndMetadata? committed = mock.CommittedOffset(groupId, new TopicPartition(Topic, partition));
            Assert.Equal(offset, committed?.Offset);
        }
    }

    private static Task Run(Task task) => TestTimeout.Run(() => task, s_deadline);

    private static Task<KafkaException> AssertFails(Task task) => Assert.ThrowsAsync<KafkaException>(() => Run(task));

    /// <summary>
    /// The two mocks behind one shape. The <see cref="Task"/> members of the sync adapter run the
    /// sync member inline and report its throw through the returned <see cref="Task"/>, so a test
    /// body is the same for both.
    /// </summary>
    private interface ITransactionalMock : IDisposable
    {
        Task InitTransactions();

        void BeginTransaction();

        Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata);

        Task CommitTransaction();

        Task AbortTransaction();

        void SetCommitTransactionError(int code, string? message = null);

        void ClearCommitTransactionError();

        bool SentOffsets();

        OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition);
    }

    private sealed class SyncMock : ITransactionalMock
    {
        private readonly MockProducer<byte[], byte[]> _producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        public Task InitTransactions() => Inline(_producer.InitTransactions);

        public void BeginTransaction() => _producer.BeginTransaction();

        public Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) =>
            Inline(() => _producer.SendOffsetsToTransaction(offsets, groupMetadata));

        public Task CommitTransaction() => Inline(_producer.CommitTransaction);

        public Task AbortTransaction() => Inline(_producer.AbortTransaction);

        public void SetCommitTransactionError(int code, string? message = null)
        {
            if (message is null)
            {
                _producer.SetCommitTransactionError(code);
            }
            else
            {
                _producer.SetCommitTransactionError(code, message);
            }
        }

        public void ClearCommitTransactionError() => _producer.ClearCommitTransactionError();

        public bool SentOffsets() => _producer.SentOffsets();

        public OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition) => _producer.CommittedOffset(groupId, partition);

        public void Dispose() => _producer.Dispose();

        private static Task Inline(Action action)
        {
            try
            {
                action();
                return Task.CompletedTask;
            }
            catch (Exception failure)
            {
                return Task.FromException(failure);
            }
        }
    }

    private sealed class AsyncMock : ITransactionalMock
    {
        private readonly AsyncMockProducer<byte[], byte[]> _producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        public Task InitTransactions() => _producer.InitTransactions();

        public void BeginTransaction() => _producer.BeginTransaction();

        public Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) =>
            _producer.SendOffsetsToTransaction(offsets, groupMetadata);

        public Task CommitTransaction() => _producer.CommitTransaction();

        public Task AbortTransaction() => _producer.AbortTransaction();

        public void SetCommitTransactionError(int code, string? message = null)
        {
            if (message is null)
            {
                _producer.SetCommitTransactionError(code);
            }
            else
            {
                _producer.SetCommitTransactionError(code, message);
            }
        }

        public void ClearCommitTransactionError() => _producer.ClearCommitTransactionError();

        public bool SentOffsets() => _producer.SentOffsets();

        public OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition) => _producer.CommittedOffset(groupId, partition);

        public void Dispose() => _producer.Dispose();
    }
}
