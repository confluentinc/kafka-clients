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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S8, the sync half: Java's <c>MockProducerTest</c> transaction tests on
/// <see cref="MockProducer{TKey, TValue}"/>, and <c>KafkaProducerTest</c> 2163 on
/// <see cref="KafkaProducer{TKey, TValue}"/>. The D1 shape pin for both interfaces is in
/// <see cref="PublicProducerTransactionTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every error assertion is the core mock's <see cref="KafkaException.Code"/> and exact message,
/// measured at CP5 (<c>gate/CP5.txt</c>). Proxied (B2) and projected (B5) rows as in the async file.
/// </para>
/// <para>
/// Not on this surface: MPT 330 and 364 (a manual sync <c>Send</c> blocks its caller and exposes
/// no non-destructive "pending" signal, so "not done before the commit" cannot be established
/// without a race — residual R-c), and D7's failed-commit row, for the same reason. Both are
/// asserted on the async mock. Not translated for want of an ABI symbol: <c>fenceProducer()</c>
/// (255-302, 666, 680; B1) and <c>commitCount()</c> (207, 218; B3).
/// </para>
/// </remarks>
public sealed class PublicSyncProducerTransactionTests
{
    private const string Topic = "txn-sync-topic";

    private const string GroupId = "txn-sync-group";

    private const string NotInitialized = "MockProducer hasn't been initialized for transactions.";

    private const string AlreadyInitialized = "MockProducer has already been initialized for transactions.";

    private const string NoOpenTransaction = "There is no open transaction.";

    private const string AlreadyStarted = "Transaction already started";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // ---- MockProducerTest, the state machine (MockProducer) ----

    /// <summary>MPT 134 <c>shouldInitTransactions</c>, proxied (B2), as in the async file.</summary>
    [Fact]
    public void InitTransactions_InitializesTheProducer()
    {
        using MockProducer<byte[], byte[]> producer = Mock();

        producer.InitTransactions();

        producer.BeginTransaction();
        AssertCoreError(producer.InitTransactions, -4, AlreadyInitialized);
    }

    /// <summary>MPT 141 <c>shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions</c>.</summary>
    [Fact]
    public void InitTransactions_Twice_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();

        AssertCoreError(producer.InitTransactions, -4, AlreadyInitialized);
    }

    /// <summary>MPT 148 <c>shouldThrowOnBeginTransactionIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public void BeginTransaction_BeforeInit_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();

        AssertCoreError(producer.BeginTransaction, -4, NotInitialized);
    }

    /// <summary>MPT 154 <c>shouldBeginTransactions</c>, proxied (B2), as in the async file.</summary>
    [Fact]
    public void BeginTransaction_OpensATransaction()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();

        producer.BeginTransaction();

        AssertCoreError(producer.BeginTransaction, -4, AlreadyStarted);
        producer.CommitTransaction();
    }

    /// <summary>MPT 162 <c>shouldThrowOnBeginTransactionsIfTransactionInflight</c>.</summary>
    [Fact]
    public void BeginTransaction_WhileOneIsOpen_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();

        AssertCoreError(producer.BeginTransaction, -4, AlreadyStarted);
    }

    /// <summary>MPT 170, with an empty map where Java passes <see langword="null"/> (Q24).</summary>
    [Fact]
    public void SendOffsetsToTransaction_BeforeInit_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();

        AssertCoreError(() => producer.SendOffsetsToTransaction(Empty(), Group()), -4, NotInitialized);
    }

    /// <summary>MPT 176, with an empty map where Java passes <see langword="null"/> (Q24).</summary>
    [Fact]
    public void SendOffsetsToTransaction_WithNoOpenTransaction_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();

        AssertCoreError(() => producer.SendOffsetsToTransaction(Empty(), Group()), -4, NoOpenTransaction);
    }

    /// <summary>MPT 183 <c>shouldThrowOnCommitIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public void CommitTransaction_BeforeInit_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();

        AssertCoreError(producer.CommitTransaction, -4, NotInitialized);
    }

    /// <summary>MPT 189 <c>shouldThrowOnCommitTransactionIfNoTransactionGotStarted</c>.</summary>
    [Fact]
    public void CommitTransaction_WithNoOpenTransaction_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();

        AssertCoreError(producer.CommitTransaction, -4, NoOpenTransaction);
    }

    /// <summary>
    /// MPT 196 <c>shouldCommitEmptyTransaction</c>, proxied (B2); also Python 934
    /// (<c>test_txn_commit_empty</c>).
    /// </summary>
    [Fact]
    public void CommitTransaction_OfAnEmptyTransaction_ClosesIt()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();

        producer.CommitTransaction();

        AssertCoreError(producer.CommitTransaction, -4, NoOpenTransaction);
        producer.BeginTransaction();
    }

    /// <summary>MPT 231 <c>shouldThrowOnAbortIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public void AbortTransaction_BeforeInit_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();

        AssertCoreError(producer.AbortTransaction, -4, NotInitialized);
    }

    /// <summary>MPT 237 <c>shouldThrowOnAbortTransactionIfNoTransactionGotStarted</c>.</summary>
    [Fact]
    public void AbortTransaction_WithNoOpenTransaction_Fails()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();

        AssertCoreError(producer.AbortTransaction, -4, NoOpenTransaction);
    }

    /// <summary>
    /// MPT 244 <c>shouldAbortEmptyTransaction</c>, proxied (B2); also Python 944
    /// (<c>test_txn_abort_empty</c>).
    /// </summary>
    [Fact]
    public void AbortTransaction_OfAnEmptyTransaction_ClosesIt()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();

        producer.AbortTransaction();

        AssertCoreError(producer.AbortTransaction, -4, NoOpenTransaction);
        producer.BeginTransaction();
    }

    // ---- MockProducerTest, records (projected through HistoryCount, B5) ----

    /// <summary>
    /// MPT 310, projected (B5): both sends have returned and are still not in the history; the
    /// commit publishes both. Also Python 922 / 952.
    /// </summary>
    [Fact]
    public void CommitTransaction_PublishesTheTransactionsRecords()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();

        RecordMetadata first = producer.Send(Record(0x01));
        producer.Send(Record(0x02));
        Assert.Equal(0L, first.Offset);
        Assert.Equal(0, producer.HistoryCount());

        producer.CommitTransaction();

        Assert.Equal(2, producer.HistoryCount());
    }

    /// <summary>
    /// MPT 348, projected (B5): the abort discards both records, and a later empty commit publishes
    /// nothing. Also Python 965.
    /// </summary>
    [Fact]
    public void AbortTransaction_DiscardsTheTransactionsRecords()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.Send(Record(0x01));
        producer.Send(Record(0x02));

        producer.AbortTransaction();
        Assert.Equal(0, producer.HistoryCount());

        producer.BeginTransaction();
        producer.CommitTransaction();
        Assert.Equal(0, producer.HistoryCount());
    }

    /// <summary>MPT 377, projected (B5): a later abort keeps the committed records. Also Python 982.</summary>
    [Fact]
    public void AbortTransaction_KeepsEarlierCommittedRecords()
    {
        using MockProducer<byte[], byte[]> producer = Mock();
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.Send(Record(0x01));
        producer.Send(Record(0x02));
        producer.CommitTransaction();

        producer.BeginTransaction();
        producer.AbortTransaction();

        Assert.Equal(2, producer.HistoryCount());
    }

    // ---- Closed producer (MPT 617-659, KPT 2163) ----

    /// <summary>
    /// MPT 617 / 631 / 638 / 645 / 652 / 659 on the sync mock: every transaction member throws
    /// <see cref="ObjectDisposedException"/> after close (Java: <c>IllegalStateException</c>). Also
    /// Python 1348-1380.
    /// </summary>
    [Fact]
    public void TransactionMembers_OnAClosedMock_ThrowObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = Mock();
        producer.Dispose();

        AssertAllThrowObjectDisposed(producer);
    }

    /// <summary>KPT 2163 <c>testTransactionalMethodThrowsWhenSenderClosed</c>, on the real sync producer.</summary>
    [Fact]
    public void TransactionMembers_OnAClosedRealProducer_ThrowObjectDisposed()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
                ["transactional.id"] = "this-is-a-transactional-id",
            },
            Serdes.ByteArray,
            Serdes.ByteArray);
        TestTimeout.Run(producer.Dispose, s_deadline);

        AssertAllThrowObjectDisposed(producer);
    }

    private static void AssertAllThrowObjectDisposed(IProducer<byte[], byte[]> producer)
    {
        Assert.Throws<ObjectDisposedException>(producer.InitTransactions);
        Assert.Throws<ObjectDisposedException>(producer.BeginTransaction);
        Assert.Throws<ObjectDisposedException>(() => producer.SendOffsetsToTransaction(Empty(), Group()));
        Assert.Throws<ObjectDisposedException>(producer.CommitTransaction);
        Assert.Throws<ObjectDisposedException>(producer.AbortTransaction);
    }

    // ---- the real producer's state machine, broker-free ----

    /// <summary>
    /// Before <c>InitTransactions</c>, the real sync producer's transaction manager refuses each
    /// state member with its own transition — so each member reaches its own worker — while an empty
    /// <c>SendOffsetsToTransaction</c> returns before any state check (D13). Measured at CP5.
    /// </summary>
    [Fact]
    public void ControlMembers_OnARealProducerBeforeInit_AreRefusedByTheStateMachine()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
                ["transactional.id"] = "state-machine-txn",
                ["max.block.ms"] = "2000",
            },
            Serdes.ByteArray,
            Serdes.ByteArray);
        try
        {
            AssertCoreError(producer.BeginTransaction, -4, Transition("IN_TRANSACTION"));
            AssertCoreError(producer.CommitTransaction, -4, Transition("COMMITTING_TRANSACTION"));
            AssertCoreError(producer.AbortTransaction, -4, Transition("ABORTING_TRANSACTION"));
            AssertCoreError(
                () => producer.SendOffsetsToTransaction(
                    new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42) },
                    Group()),
                -4,
                "Cannot send offsets if a transaction is not in progress (currentState= UNINITIALIZED)");
            producer.SendOffsetsToTransaction(Empty(), Group());
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- helpers ----

    private static string Transition(string target) =>
        $"TransactionalId state-machine-txn: Invalid transition attempted from state UNINITIALIZED to state {target}";

    private static MockProducer<byte[], byte[]> Mock() =>
        new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static ProducerRecord<byte[], byte[]> Record(byte tag) =>
        new ProducerRecord<byte[], byte[]>(Topic, new[] { tag }, partition: 0);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() => new Dictionary<TopicPartition, OffsetAndMetadata>();

#pragma warning disable CS0618 // the obsolete public constructor is the one under test here
    private static ConsumerGroupMetadata Group() => new ConsumerGroupMetadata(GroupId);
#pragma warning restore CS0618

    private static void AssertCoreError(Action action, int code, string message)
    {
        KafkaException failure = Assert.Throws<KafkaException>(action);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }
}
