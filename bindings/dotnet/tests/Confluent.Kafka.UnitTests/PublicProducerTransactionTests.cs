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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S8, the async half: the public transaction surface of <see cref="IAsyncProducer{TKey, TValue}"/>,
/// Java's <c>MockProducerTest</c> transaction tests on <see cref="AsyncMockProducer{TKey, TValue}"/>, the
/// per-record outcomes of D7, and <c>KafkaProducerTest</c> 2163 on <see cref="AsyncKafkaProducer{TKey, TValue}"/>.
/// It also holds the D1 shape pin for both interfaces.
/// </summary>
/// <remarks>
/// <para>
/// Each <c>MockProducerTest</c> row (<c>MockProducerTest.java</c> line in the test name's summary)
/// asserts the core mock's <see cref="KafkaException.Code"/> and exact message, measured at CP5
/// (<c>gate/CP5.txt</c>). Where Java reads a getter the ABI does not export
/// (<c>transactionInitialized()</c> and its siblings, B2) the test asserts that getter's observable
/// consequence instead, and says so; where Java reads <c>history()</c> the test reads
/// <see cref="AsyncMockProducer{TKey, TValue}.HistoryCount()"/> (B5).
/// </para>
/// <para>
/// Not translated, for want of an ABI symbol: <c>fenceProducer()</c> and every test that needs it
/// (255-302, 666, 680; B1), and <c>commitCount()</c> (207, 218; B3).
/// </para>
/// </remarks>
public sealed class PublicProducerTransactionTests
{
    private const string Topic = "txn-async-topic";

    private const string GroupId = "txn-async-group";

    private const string NotInitialized = "MockProducer hasn't been initialized for transactions.";

    private const string AlreadyInitialized = "MockProducer has already been initialized for transactions.";

    private const string NoOpenTransaction = "There is no open transaction.";

    private const string AlreadyStarted = "Transaction already started";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_transactionMembers =
    {
        "InitTransactions", "BeginTransaction", "SendOffsetsToTransaction", "CommitTransaction", "AbortTransaction",
    };

    // ---- D1: the shape of both interfaces ----

    /// <summary>
    /// D1: each interface declares exactly the five transaction members with the plan's signatures —
    /// names, parameter names and types, the token's default, and return types.
    /// <see cref="IAsyncProducer{TKey, TValue}.BeginTransaction"/> returns <see langword="void"/>.
    /// </summary>
    [Fact]
    public void Interfaces_DeclareTheFiveTransactionMembers_WithThePlannedSignatures()
    {
        Type sync = typeof(IProducer<,>);
        Type async = typeof(IAsyncProducer<,>);
        Type offsets = typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>);

        Assert.Equal(s_transactionMembers.OrderBy(n => n), TransactionMembers(sync).Select(m => m.Name).OrderBy(n => n));
        Assert.Equal(s_transactionMembers.OrderBy(n => n), TransactionMembers(async).Select(m => m.Name).OrderBy(n => n));

        AssertSignature(sync, "InitTransactions", typeof(void));
        AssertSignature(sync, "BeginTransaction", typeof(void));
        AssertSignature(sync, "SendOffsetsToTransaction", typeof(void), (offsets, "offsets"), (typeof(ConsumerGroupMetadata), "groupMetadata"));
        AssertSignature(sync, "CommitTransaction", typeof(void));
        AssertSignature(sync, "AbortTransaction", typeof(void));

        AssertSignature(async, "InitTransactions", typeof(Task), (typeof(CancellationToken), "cancellationToken"));
        AssertSignature(async, "BeginTransaction", typeof(void));
        AssertSignature(
            async,
            "SendOffsetsToTransaction",
            typeof(Task),
            (offsets, "offsets"),
            (typeof(ConsumerGroupMetadata), "groupMetadata"),
            (typeof(CancellationToken), "cancellationToken"));
        AssertSignature(async, "CommitTransaction", typeof(Task), (typeof(CancellationToken), "cancellationToken"));
        AssertSignature(async, "AbortTransaction", typeof(Task), (typeof(CancellationToken), "cancellationToken"));

        foreach (MethodInfo method in TransactionMembers(async))
        {
            foreach (ParameterInfo parameter in method.GetParameters().Where(p => p.ParameterType == typeof(CancellationToken)))
            {
                Assert.True(parameter.HasDefaultValue, $"{method.Name}'s token has no default");
            }
        }
    }

    /// <summary>
    /// D1: the four implementers implement the five members as public instance methods (not
    /// explicit interface implementations), each mapped from its interface's member.
    /// </summary>
    [Theory]
    [InlineData(typeof(KafkaProducer<byte[], byte[]>), typeof(IProducer<byte[], byte[]>))]
    [InlineData(typeof(MockProducer<byte[], byte[]>), typeof(IProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncKafkaProducer<byte[], byte[]>), typeof(IAsyncProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncMockProducer<byte[], byte[]>), typeof(IAsyncProducer<byte[], byte[]>))]
    public void Implementers_ImplementTheFiveMembers_AsPublicMethods(Type implementer, Type contract)
    {
        InterfaceMapping map = implementer.GetInterfaceMap(contract);
        foreach (string name in s_transactionMembers)
        {
            int index = Array.FindIndex(map.InterfaceMethods, m => m.Name == name);
            Assert.True(index >= 0, $"{contract.Name} has no {name}");
            MethodInfo target = map.TargetMethods[index];
            Assert.Equal(name, target.Name);
            Assert.True(target.IsPublic, $"{implementer.Name}.{name} is not public");
        }
    }

    // ---- MockProducerTest, the state machine (AsyncMockProducer) ----

    /// <summary>
    /// MPT 134 <c>shouldInitTransactions</c>, proxied (B2): Java reads
    /// <c>transactionInitialized()</c>; here its consequences — <c>BeginTransaction</c> is now
    /// accepted, and a second <c>InitTransactions</c> is not.
    /// </summary>
    [Fact]
    public async Task InitTransactions_InitializesTheProducer()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();

        await Run(producer.InitTransactions());

        producer.BeginTransaction();
        await AssertCoreErrorAsync(() => producer.InitTransactions(), -4, AlreadyInitialized);
    }

    /// <summary>MPT 141 <c>shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions</c>.</summary>
    [Fact]
    public async Task InitTransactions_Twice_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());

        await AssertCoreErrorAsync(() => producer.InitTransactions(), -4, AlreadyInitialized);
    }

    /// <summary>MPT 148 <c>shouldThrowOnBeginTransactionIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public void BeginTransaction_BeforeInit_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();

        AssertCoreError(producer.BeginTransaction, -4, NotInitialized);
    }

    /// <summary>
    /// MPT 154 <c>shouldBeginTransactions</c>, proxied (B2): Java reads
    /// <c>transactionInFlight()</c>; here a second <c>BeginTransaction</c> is refused and a commit is
    /// accepted.
    /// </summary>
    [Fact]
    public async Task BeginTransaction_OpensATransaction()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());

        producer.BeginTransaction();

        AssertCoreError(producer.BeginTransaction, -4, AlreadyStarted);
        await Run(producer.CommitTransaction());
    }

    /// <summary>MPT 162 <c>shouldThrowOnBeginTransactionsIfTransactionInflight</c>.</summary>
    [Fact]
    public async Task BeginTransaction_WhileOneIsOpen_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();

        AssertCoreError(producer.BeginTransaction, -4, AlreadyStarted);
    }

    /// <summary>
    /// MPT 170 <c>shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized</c>, with an
    /// empty map where Java passes <see langword="null"/>: the binding rejects a null map before the
    /// core's state check (Q24).
    /// </summary>
    [Fact]
    public async Task SendOffsetsToTransaction_BeforeInit_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();

        await AssertCoreErrorAsync(() => producer.SendOffsetsToTransaction(Empty(), Group()), -4, NotInitialized);
    }

    /// <summary>
    /// MPT 176 <c>shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted</c>, with
    /// an empty map where Java passes <see langword="null"/> (Q24).
    /// </summary>
    [Fact]
    public async Task SendOffsetsToTransaction_WithNoOpenTransaction_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());

        await AssertCoreErrorAsync(() => producer.SendOffsetsToTransaction(Empty(), Group()), -4, NoOpenTransaction);
    }

    /// <summary>MPT 183 <c>shouldThrowOnCommitIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public async Task CommitTransaction_BeforeInit_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();

        await AssertCoreErrorAsync(() => producer.CommitTransaction(), -4, NotInitialized);
    }

    /// <summary>MPT 189 <c>shouldThrowOnCommitTransactionIfNoTransactionGotStarted</c>.</summary>
    [Fact]
    public async Task CommitTransaction_WithNoOpenTransaction_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());

        await AssertCoreErrorAsync(() => producer.CommitTransaction(), -4, NoOpenTransaction);
    }

    /// <summary>
    /// MPT 196 <c>shouldCommitEmptyTransaction</c>, proxied (B2): Java reads
    /// <c>transactionInFlight()</c> / <c>transactionCommitted()</c>; here the commit closes the
    /// transaction (a second commit finds none) and a new one can begin. Committed-versus-aborted is
    /// not observable for an empty transaction.
    /// </summary>
    [Fact]
    public async Task CommitTransaction_OfAnEmptyTransaction_ClosesIt()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();

        await Run(producer.CommitTransaction());

        await AssertCoreErrorAsync(() => producer.CommitTransaction(), -4, NoOpenTransaction);
        producer.BeginTransaction();
    }

    /// <summary>MPT 231 <c>shouldThrowOnAbortIfTransactionsNotInitialized</c>.</summary>
    [Fact]
    public async Task AbortTransaction_BeforeInit_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();

        await AssertCoreErrorAsync(() => producer.AbortTransaction(), -4, NotInitialized);
    }

    /// <summary>MPT 237 <c>shouldThrowOnAbortTransactionIfNoTransactionGotStarted</c>.</summary>
    [Fact]
    public async Task AbortTransaction_WithNoOpenTransaction_Fails()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());

        await AssertCoreErrorAsync(() => producer.AbortTransaction(), -4, NoOpenTransaction);
    }

    /// <summary>
    /// MPT 244 <c>shouldAbortEmptyTransaction</c>, proxied (B2): the mirror of 196 — the abort closes
    /// the transaction and a new one can begin.
    /// </summary>
    [Fact]
    public async Task AbortTransaction_OfAnEmptyTransaction_ClosesIt()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();

        await Run(producer.AbortTransaction());

        await AssertCoreErrorAsync(() => producer.AbortTransaction(), -4, NoOpenTransaction);
        producer.BeginTransaction();
    }

    // ---- MockProducerTest, records (projected through HistoryCount, B5) ----

    /// <summary>
    /// MPT 310 <c>shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled</c>, projected (B5):
    /// both sends have completed and are still not in the history; the commit publishes both. Also
    /// Python 922 / 952 (<c>test_txn_init_begin_send_commit</c>, <c>..._committed_records_in_history</c>).
    /// </summary>
    [Fact]
    public async Task CommitTransaction_PublishesTheTransactionsRecords()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();

        RecordMetadata first = await Run(producer.Send(Record(0x01)));
        await Run(producer.Send(Record(0x02)));
        Assert.Equal(0L, first.Offset);
        Assert.Equal(0, producer.HistoryCount());

        await Run(producer.CommitTransaction());

        Assert.Equal(2, producer.HistoryCount());
    }

    /// <summary>
    /// MPT 330 <c>shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled</c> (D4, D7): on a
    /// manual mock both sends are pending in the core before the commit, and both send
    /// <see cref="Task"/>s have completed with metadata the moment the commit's <see cref="Task"/>
    /// completes.
    /// </summary>
    /// <remarks>
    /// The sends are handed to the core first (<c>WaitForSendsToReachCore</c>), so "pending" is the
    /// core's, not the binding's accumulator.
    /// </remarks>
    [Fact]
    public async Task CommitTransaction_OnAManualMock_CompletesThePendingSends()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock(autoComplete: false);
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        Task<RecordMetadata> first = producer.Send(Record(0x01));
        Task<RecordMetadata> second = producer.Send(Record(0x02));
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        await Run(producer.CommitTransaction());

        Assert.Equal(TaskStatus.RanToCompletion, first.Status);
        Assert.Equal(TaskStatus.RanToCompletion, second.Status);
        Assert.Equal(Topic, (await first).Topic);
        Assert.Equal(2, producer.HistoryCount());
    }

    /// <summary>
    /// MPT 348 <c>shouldDropMessagesOnAbortIfTransactionsAreEnabled</c>, projected (B5): the abort
    /// discards both records, and a later empty commit publishes nothing. Also Python 965
    /// (<c>test_txn_aborted_records_discarded</c>).
    /// </summary>
    [Fact]
    public async Task AbortTransaction_DiscardsTheTransactionsRecords()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        await Run(producer.Send(Record(0x01)));
        await Run(producer.Send(Record(0x02)));

        await Run(producer.AbortTransaction());
        Assert.Equal(0, producer.HistoryCount());

        producer.BeginTransaction();
        await Run(producer.CommitTransaction());
        Assert.Equal(0, producer.HistoryCount());
    }

    /// <summary>
    /// MPT 364 <c>shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled</c> (D4, D7): the
    /// mock's abort flushes, so a send pending in the core completes with metadata by the time the
    /// abort's <see cref="Task"/> completes — and its record is discarded with the transaction.
    /// </summary>
    [Fact]
    public async Task AbortTransaction_OnAManualMock_CompletesThePendingSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock(autoComplete: false);
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        Task<RecordMetadata> send = producer.Send(Record(0x01));
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.False(send.IsCompleted);

        await Run(producer.AbortTransaction());

        Assert.Equal(TaskStatus.RanToCompletion, send.Status);
        Assert.Equal(0, producer.HistoryCount());
    }

    /// <summary>
    /// MPT 377 <c>shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled</c>, projected (B5):
    /// a later abort leaves the committed records in the history. Also Python 982
    /// (<c>test_txn_abort_then_reuse</c>) and 1417-1430 (the async <c>init_begin_send_commit</c> and
    /// <c>abort_then_reuse</c>).
    /// </summary>
    [Fact]
    public async Task AbortTransaction_KeepsEarlierCommittedRecords()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock();
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        await Run(producer.Send(Record(0x01)));
        await Run(producer.Send(Record(0x02)));
        await Run(producer.CommitTransaction());

        producer.BeginTransaction();
        await Run(producer.AbortTransaction());

        Assert.Equal(2, producer.HistoryCount());
    }

    // ---- D7: a failed commit leaves pending sends pending ----

    /// <summary>
    /// D7: a <b>failed</b> commit returns before the mock's flush, so a manual send pending in the
    /// core stays pending, and the binding does not complete it. Witnessed at the core: after the
    /// failed commit <c>CompleteNext()</c> still finds the completion, and only then does the send's
    /// <see cref="Task"/> complete.
    /// </summary>
    [Fact]
    public async Task CommitTransaction_WhenItFails_LeavesThePendingSendsPending()
    {
        using AsyncMockProducer<byte[], byte[]> producer = Mock(autoComplete: false);
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        Task<RecordMetadata> send = producer.Send(Record(0x01));
        producer.WaitForSendsToReachCore(s_deadline);
        producer.SetCommitTransactionError(120, "commit failed abortably");

        await AssertCoreErrorAsync(() => producer.CommitTransaction(), 120, "commit failed abortably");

        Assert.False(send.IsCompleted);
        Assert.True(producer.CompleteNext(), "the failed commit completed the pending send in the core");
        RecordMetadata metadata = await Run(send);
        Assert.Equal(Topic, metadata.Topic);
    }

    // ---- Closed producer (MPT 617-659, KPT 2163) ----

    /// <summary>
    /// MPT 617 / 631 / 638 / 645 / 652 / 659: every transaction member of a closed mock throws
    /// <see cref="ObjectDisposedException"/> synchronously (Java: <c>IllegalStateException</c>; the ffi
    /// §A5 closed-producer idiom). 638 / 645 pass an empty map where Java passes <see langword="null"/>
    /// (Q24).
    /// </summary>
    [Fact]
    public void TransactionMembers_OnAClosedMock_ThrowObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = Mock();
        producer.Dispose();

        AssertAllThrowObjectDisposed(producer);
    }

    /// <summary>
    /// KPT 2163 <c>testTransactionalMethodThrowsWhenSenderClosed</c>, on the real async producer:
    /// after close every transaction member throws <see cref="ObjectDisposedException"/>
    /// synchronously, broker-free.
    /// </summary>
    [Fact]
    public void TransactionMembers_OnAClosedRealProducer_ThrowObjectDisposed()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
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

    private static void AssertAllThrowObjectDisposed(IAsyncProducer<byte[], byte[]> producer)
    {
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.InitTransactions(); });
        Assert.Throws<ObjectDisposedException>(producer.BeginTransaction);
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.SendOffsetsToTransaction(Empty(), Group()); });
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.CommitTransaction(); });
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.AbortTransaction(); });
    }

    // ---- the real producer's state machine, broker-free ----

    /// <summary>
    /// Before <c>InitTransactions</c>, the real async producer's transaction manager refuses each
    /// state member with its own transition (Java's <c>TransactionManager.transitionTo</c> text) —
    /// and so each member reaches its own worker — while an empty <c>SendOffsetsToTransaction</c>
    /// returns before any state check (D13; <c>KafkaProducer.java:738</c>). Measured at CP5.
    /// </summary>
    [Fact]
    public async Task ControlMembers_OnARealProducerBeforeInit_AreRefusedByTheStateMachine()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = Real();
        try
        {
            AssertCoreError(producer.BeginTransaction, -4, Transition("IN_TRANSACTION"));
            await AssertCoreErrorAsync(() => producer.CommitTransaction(), -4, Transition("COMMITTING_TRANSACTION"));
            await AssertCoreErrorAsync(() => producer.AbortTransaction(), -4, Transition("ABORTING_TRANSACTION"));
            await AssertCoreErrorAsync(
                () => producer.SendOffsetsToTransaction(OneOffset(), Group()),
                -4,
                "Cannot send offsets if a transaction is not in progress (currentState= UNINITIALIZED)");
            await Run(producer.SendOffsetsToTransaction(Empty(), Group()));
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    /// <summary>
    /// D5 row 1 through the public members: an already-canceled token throws
    /// <see cref="OperationCanceledException"/> synchronously from each <see cref="Task"/> member,
    /// on both async producers — so each forwards the caller's token.
    /// </summary>
    [Theory]
    [InlineData("mock")]
    [InlineData("real")]
    public void TaskMembers_WithAnAlreadyCanceledToken_ThrowSynchronously(string flavour)
    {
        IAsyncProducer<byte[], byte[]> producer = flavour == "mock" ? Mock() : Real();
        try
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            Assert.ThrowsAny<OperationCanceledException>(() => { _ = producer.InitTransactions(cts.Token); });
            Assert.ThrowsAny<OperationCanceledException>(() => { _ = producer.SendOffsetsToTransaction(OneOffset(), Group(), cts.Token); });
            Assert.ThrowsAny<OperationCanceledException>(() => { _ = producer.CommitTransaction(cts.Token); });
            Assert.ThrowsAny<OperationCanceledException>(() => { _ = producer.AbortTransaction(cts.Token); });
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // ---- helpers ----

    private static AsyncKafkaProducer<byte[], byte[]> Real() => new AsyncKafkaProducer<byte[], byte[]>(
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["transactional.id"] = "state-machine-txn",
            ["max.block.ms"] = "2000",
        },
        Serdes.ByteArray,
        Serdes.ByteArray);

    private static string Transition(string target) =>
        $"TransactionalId state-machine-txn: Invalid transition attempted from state UNINITIALIZED to state {target}";

    private static Dictionary<TopicPartition, OffsetAndMetadata> OneOffset() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42) };

    private static MethodInfo[] TransactionMembers(Type contract) =>
        contract.GetMethods().Where(m => m.Name.Contains("Transaction")).ToArray();

    private static void AssertSignature(Type contract, string name, Type returnType, params (Type Type, string Name)[] parameters)
    {
        MethodInfo method = Assert.Single(contract.GetMethods(), m => m.Name == name);
        Assert.Equal(returnType, method.ReturnType);
        Assert.Equal(parameters.Select(p => p.Type), method.GetParameters().Select(p => p.ParameterType));
        Assert.Equal(parameters.Select(p => p.Name), method.GetParameters().Select(p => p.Name));
    }

    private static AsyncMockProducer<byte[], byte[]> Mock(bool autoComplete = true) =>
        new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete);

    private static ProducerRecord<byte[], byte[]> Record(byte tag) =>
        new ProducerRecord<byte[], byte[]>(Topic, new[] { tag }, partition: 0);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() => new Dictionary<TopicPartition, OffsetAndMetadata>();

#pragma warning disable CS0618 // the obsolete public constructor is the one under test here
    private static ConsumerGroupMetadata Group() => new ConsumerGroupMetadata(GroupId);
#pragma warning restore CS0618

    private static Task Run(Task task) => TestTimeout.Run(() => task, s_deadline);

    private static Task<T> Run<T>(Task<T> task) => TestTimeout.Run(() => task, s_deadline);

    private static void AssertCoreError(Action action, int code, string message)
    {
        KafkaException failure = Assert.Throws<KafkaException>(action);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }

    // The Task members report a core error through the returned Task, never synchronously.
    private static async Task AssertCoreErrorAsync(Func<Task> operation, int code, string message)
    {
        Task task = operation();
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Run(task));
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }
}
