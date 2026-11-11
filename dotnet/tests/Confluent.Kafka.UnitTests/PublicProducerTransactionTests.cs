// Copyright 2026 Confluent Inc.
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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The five transaction operations on the four producer classes: the state and argument checks, overlapping calls,
/// cancellation and retries, the mocks' transactions, and the waits called from inside a delivery callback.
/// </summary>
public sealed class PublicProducerTransactionTests
{
    private const int MaxBlockMs = 500;

    // The max.block.ms of a call expected to complete at once: long enough that a slow runner does not reach it.
    private const int AtOnceMaxBlockMs = 5000;

    private const string NonTransactional = "Transactional method invoked on a non-transactional producer.";

    private const string InitTimeout =
        "InitTransactions timed out - did not complete coordinator discovery or receive the InitProducerId response " +
        "within max.block.ms.";

    private const string NullGroupMetadata = "Consumer group metadata could not be null";

    private const string NullOffsets = "offsets must not be null";

    private const string ProducerClosed = "Cannot perform operation after producer has been closed";

    private const string Overlapping = "Transactional methods of KafkaProducer are not safe for concurrent access.";

    private const string MockClosed = "MockProducer is already closed.";

    private const string MockNotInitialized = "MockProducer hasn't been initialized for transactions.";

    private const string MockAlreadyInitialized = "MockProducer has already been initialized for transactions.";

    private const string MockNoTransaction = "There is no open transaction.";

    private const string MockTransactionStarted = "Transaction already started";

    private const string NoTransactionManager =
        "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration " +
        "property";

    private const string Init = "InitTransactions";
    private const string Begin = "BeginTransaction";
    private const string SendOffsets = "SendOffsetsToTransaction";
    private const string Commit = "CommitTransaction";
    private const string Abort = "AbortTransaction";

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_noOffsets =
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    [Theory]
    [InlineData(Init)]
    [InlineData(Begin)]
    [InlineData(Commit)]
    [InlineData(Abort)]
    public void KafkaProducer_NonTransactional_ThrowsJavasIllegalState(string operation)
    {
        using KafkaProducer<byte[], byte[]> producer = Sync(MaxBlockMs);
        using IDisposable teardown = TestTimeout.Disposal(producer);

        Exception? error = Catch(() => Call(producer, operation));

        Assert.Equal(NonTransactional, Assert.IsType<IllegalStateException>(error).Message);
    }

    [Theory]
    [InlineData(Init)]
    [InlineData(Begin)]
    [InlineData(Commit)]
    [InlineData(Abort)]
    public async Task AsyncKafkaProducer_NonTransactional_ThrowsJavasIllegalState(string operation)
    {
        AsyncKafkaProducer<byte[], byte[]> producer = Async(MaxBlockMs);
        await using IAsyncDisposable teardown = TestTimeout.AsyncDisposal(producer);

        Exception? error = await CatchAsync(() => Call(producer, operation, CancellationToken.None));

        Assert.Equal(NonTransactional, Assert.IsType<IllegalStateException>(error).Message);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task InitTransactions_UnreachableBroker_TimesOutAfterMaxBlockMs(string flavor)
    {
        object producer = Transactional(flavor, MaxBlockMs);
        try
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            Exception? error = await CatchAsync(() => Call(producer, Init, CancellationToken.None));
            long elapsedMs = elapsed.ElapsedMilliseconds;

            KafkaTimeoutException timeout = Assert.IsType<KafkaTimeoutException>(error);
            Assert.Equal(7, timeout.Code);
            Assert.Equal($"Timeout expired after {MaxBlockMs}ms while awaiting InitProducerId. {InitTimeout}", timeout.Message);
            Assert.InRange(elapsedMs, MaxBlockMs - 50, (long)Deadline.TotalMilliseconds);

            Exception? begin = Catch(() => Call(producer, Begin));
            Assert.Equal(
                "Cannot attempt operation `beginTransaction` because the previous call to `initTransactions` timed " +
                "out and must be retried",
                Assert.IsType<IllegalStateException>(begin).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task BeginTransaction_BeforeInitTransactions_ThrowsTheInvalidTransition(string flavor)
    {
        object producer = Transactional(flavor, MaxBlockMs, "tx-begin-before-init");
        try
        {
            Exception? error = Catch(() => Call(producer, Begin));

            Assert.Equal(
                "TransactionalId tx-begin-before-init: Invalid transition attempted from state UNINITIALIZED to " +
                "state IN_TRANSACTION",
                Assert.IsType<IllegalStateException>(error).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Fact]
    public async Task KafkaProducer_OverlappingTransactionCall_ThrowsConcurrentModification()
    {
        KafkaProducer<byte[], byte[]> producer = (KafkaProducer<byte[], byte[]>)Transactional(SyncFlavor, 3000);
        try
        {
            using ManualResetEventSlim started = new ManualResetEventSlim();
            Task init = Task.Run(() =>
            {
                started.Set();
                producer.InitTransactions();
            });
            Assert.True(started.Wait(Deadline), "InitTransactions never started");
            await Task.Delay(300);
            Assert.False(init.IsCompleted, "InitTransactions ended before the overlapping call");

            Exception? overlapping = Catch(producer.InitTransactions);

            Assert.Equal(Overlapping, Assert.IsType<ConcurrentModificationException>(overlapping).Message);
            _ = await CatchAsync(() => init);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Fact]
    public async Task AsyncKafkaProducer_OverlappingTransactionCall_ThrowsConcurrentModification()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = (AsyncKafkaProducer<byte[], byte[]>)Transactional(AsyncFlavor, 3000);
        try
        {
            Task init = producer.InitTransactions();
            await Task.Delay(300);
            Assert.False(init.IsCompleted, "InitTransactions ended before the overlapping call");

            Exception? overlappingAsync = await CatchAsync(() => producer.InitTransactions());
            Exception? overlappingBegin = await CatchAsync(() => producer.BeginTransaction());

            Assert.Equal(Overlapping, Assert.IsType<ConcurrentModificationException>(overlappingAsync).Message);
            Assert.Equal(Overlapping, Assert.IsType<ConcurrentModificationException>(overlappingBegin).Message);
            _ = await CatchAsync(() => init);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(Init)]
    [InlineData(Begin)]
    [InlineData(SendOffsets)]
    [InlineData(Commit)]
    [InlineData(Abort)]
    public async Task AsyncKafkaProducer_AlreadyCanceledToken_ThrowsAtTheCall(string operation)
    {
        AsyncKafkaProducer<byte[], byte[]> producer = (AsyncKafkaProducer<byte[], byte[]>)Transactional(AsyncFlavor, MaxBlockMs);
        await using IAsyncDisposable teardown = TestTimeout.AsyncDisposal(producer);
        using CancellationTokenSource canceled = new CancellationTokenSource();
        canceled.Cancel();

        Exception? error = operation == SendOffsets
            ? Catch(() => _ = producer.SendOffsetsToTransaction(Offsets(), GroupMetadata(), canceled.Token))
            : Catch(() => _ = Call(producer, operation, canceled.Token));

        Assert.IsAssignableFrom<OperationCanceledException>(error);
    }

    [Fact]
    public async Task AsyncKafkaProducer_CanceledInitTransactions_RetryWhileItRuns_ThrowsConcurrentModification()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = (AsyncKafkaProducer<byte[], byte[]>)Transactional(AsyncFlavor, 1500);
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task init = producer.InitTransactions(cancel.Token);
            await Task.Delay(100);
            cancel.Cancel();

            Exception? canceled = await CatchAsync(() => init);
            Exception? retry = await CatchAsync(() => producer.InitTransactions());
            Exception? begin = await CatchAsync(() => producer.BeginTransaction());

            Assert.IsAssignableFrom<OperationCanceledException>(canceled);
            Assert.Equal(Overlapping, Assert.IsType<ConcurrentModificationException>(retry).Message);
            Assert.Equal(Overlapping, Assert.IsType<ConcurrentModificationException>(begin).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Fact]
    public async Task AsyncKafkaProducer_CanceledInitTransactions_RetryAfterItTimedOut_GetsTheTimeout()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = (AsyncKafkaProducer<byte[], byte[]>)Transactional(AsyncFlavor, 1000);
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task init = producer.InitTransactions(cancel.Token);
            await Task.Delay(100);
            cancel.Cancel();
            Assert.IsAssignableFrom<OperationCanceledException>(await CatchAsync(() => init));

            await Task.Delay(1500);
            Exception? retry = await CatchAsync(() => producer.InitTransactions());
            Exception? begin = await CatchAsync(() => producer.BeginTransaction());

            Assert.Contains(InitTimeout, Assert.IsType<KafkaTimeoutException>(retry).Message);
            Assert.Equal(
                "Cannot attempt operation `beginTransaction` because the previous call to `initTransactions` timed " +
                "out and must be retried",
                Assert.IsType<IllegalStateException>(begin).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Fact]
    public async Task AsyncMockProducer_CanceledInitTransactions_ThatTookEffect_RetryThrowsTheStateError()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        await using IAsyncDisposable teardown = TestTimeout.AsyncDisposal(producer);
        HoldingCallback holding = new HoldingCallback();
        _ = await TestTimeout.Run(() => producer.Send(MockRecord(), holding).AsTask(), Deadline);
        Assert.True(holding.WaitEntered(Deadline), "the delivery callback never ran");

        using CancellationTokenSource cancel = new CancellationTokenSource();
        Task init = producer.InitTransactions(cancel.Token);
        await Task.Delay(100);
        Assert.False(init.IsCompleted, "InitTransactions ended while its completion was queued");
        cancel.Cancel();
        Exception? canceled = await CatchAsync(() => init);
        holding.Release();
        await Task.Delay(Settle);

        Exception? retry = await CatchAsync(() => producer.InitTransactions());

        Assert.IsAssignableFrom<OperationCanceledException>(canceled);
        Assert.Equal(MockAlreadyInitialized, Assert.IsType<IllegalStateException>(retry).Message);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task SendOffsetsToTransaction_NullGroupMetadata_ThrowsJavasIllegalArgument_BeforeTheOffsets(string flavor)
    {
        object producer = Transactional(flavor, MaxBlockMs);
        try
        {
            Exception? withOffsets = await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, null));
            Exception? withoutOffsets = await CatchAsync(() => SendOffsetsTo(producer, null, null));

            Assert.Equal(NullGroupMetadata, Assert.IsType<IllegalArgumentException>(withOffsets).Message);
            Assert.Equal(NullGroupMetadata, Assert.IsType<IllegalArgumentException>(withoutOffsets).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task SendOffsetsToTransaction_NullOffsets_ThrowsArgumentNull(string flavor)
    {
        object producer = Transactional(flavor, MaxBlockMs);
        try
        {
            Exception? error = await CatchAsync(() => SendOffsetsTo(producer, null, GroupMetadata()));

            ArgumentNullException nullOffsets = Assert.IsType<ArgumentNullException>(error);
            Assert.Equal(NullOffsets, nullOffsets.Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor, Init)]
    [InlineData(SyncFlavor, Begin)]
    [InlineData(SyncFlavor, SendOffsets)]
    [InlineData(SyncFlavor, Commit)]
    [InlineData(SyncFlavor, Abort)]
    [InlineData(AsyncFlavor, Init)]
    [InlineData(AsyncFlavor, Begin)]
    [InlineData(AsyncFlavor, SendOffsets)]
    [InlineData(AsyncFlavor, Commit)]
    [InlineData(AsyncFlavor, Abort)]
    public async Task TransactionCall_OnAClosedProducer_ThrowsTheClosedProducersIllegalState(string flavor, string operation)
    {
        object producer = Transactional(flavor, MaxBlockMs);
        await CloseNow(producer);

        // The closed check comes first, before the arguments' (a recorded order deviation).
        Exception? error = operation == SendOffsets
            ? Catch(() => _ = SendOffsetsTo(producer, null, null))
            : Catch(() => _ = Call(producer, operation, CancellationToken.None));

        Assert.Equal(ProducerClosed, Assert.IsType<IllegalStateException>(error).Message);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task MockProducer_Transaction_CommitsTheSends(string flavor)
    {
        object producer = Mock(flavor, autoComplete: true);
        try
        {
            await Call(producer, Init, CancellationToken.None);
            Call(producer, Begin);
            await MockSend(producer, MockRecord(), callback: null);
            await SendOffsetsTo(producer, Offsets(), GroupMetadata());
            await Call(producer, Commit, CancellationToken.None);

            Assert.Equal(1, MockHistoryCount(producer));
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task MockProducer_Transaction_AbortDropsTheSends(string flavor)
    {
        object producer = Mock(flavor, autoComplete: true);
        try
        {
            await Call(producer, Init, CancellationToken.None);
            Call(producer, Begin);
            await MockSend(producer, MockRecord(), callback: null);
            await Call(producer, Abort, CancellationToken.None);

            Assert.Equal(0, MockHistoryCount(producer));
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor, Commit)]
    [InlineData(SyncFlavor, Abort)]
    [InlineData(AsyncFlavor, Commit)]
    [InlineData(AsyncFlavor, Abort)]
    public async Task MockProducer_CommitOrAbort_CompletesThePendingSends_OnTheCompletionThread(string flavor, string operation)
    {
        object producer = Mock(flavor, autoComplete: false);
        try
        {
            await Call(producer, Init, CancellationToken.None);
            Call(producer, Begin);
            ProbeCallback callback = new ProbeCallback((_, _) => { });
            Func<bool> isDone = await MockSend(producer, MockRecord(), callback);
            Assert.False(isDone(), "the send completed before the transaction ended");

            await Call(producer, operation, CancellationToken.None);

            Assert.True(isDone(), "the transaction ended with a send still pending");
            Assert.Equal(1, callback.Calls);
            Assert.Null(callback.Delivered);
            Assert.Equal(
                flavor == SyncFlavor ? NativeProducer.CompletionThreadName : NativeProducer.PumpThreadName,
                callback.ThreadName);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task MockProducer_StateChecks_ThrowJavasMockMessages(string flavor)
    {
        object producer = Mock(flavor, autoComplete: true);
        try
        {
            Assert.Equal(MockNotInitialized, Assert.IsType<IllegalStateException>(Catch(() => Call(producer, Begin))).Message);
            Assert.Equal(
                MockNotInitialized,
                Assert.IsType<IllegalStateException>(await CatchAsync(() => Call(producer, Commit, CancellationToken.None))).Message);
            Assert.Equal(
                MockNotInitialized,
                Assert.IsType<IllegalStateException>(await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, GroupMetadata()))).Message);

            await Call(producer, Init, CancellationToken.None);
            Assert.Equal(
                MockAlreadyInitialized,
                Assert.IsType<IllegalStateException>(await CatchAsync(() => Call(producer, Init, CancellationToken.None))).Message);
            Assert.Equal(
                MockNoTransaction,
                Assert.IsType<IllegalStateException>(await CatchAsync(() => Call(producer, Commit, CancellationToken.None))).Message);

            Call(producer, Begin);
            Assert.Equal(MockTransactionStarted, Assert.IsType<IllegalStateException>(Catch(() => Call(producer, Begin))).Message);
            Assert.Equal(
                NullGroupMetadata,
                Assert.IsType<IllegalArgumentException>(await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, null))).Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task MockProducer_TransactionCallAfterClose_ThrowsIllegalState(string flavor)
    {
        object producer = Mock(flavor, autoComplete: true);
        await CloseNow(producer);

        Exception? init = Catch(() => _ = Call(producer, Init, CancellationToken.None));
        Exception? sendOffsets = Catch(() => _ = SendOffsetsTo(producer, null, null));

        Assert.Equal(MockClosed, Assert.IsType<IllegalStateException>(init).Message);
        Assert.Equal(MockClosed, Assert.IsType<IllegalStateException>(sendOffsets).Message);
    }

    [Fact]
    public void GroupMetadata_OutlivesTheConsumerThatGaveIt()
    {
        ConsumerGroupMetadata metadata;
        using (MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray()))
        {
            metadata = consumer.GroupMetadata();
        }

        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        using IDisposable teardown = TestTimeout.Disposal(producer);
        producer.InitTransactions();
        producer.BeginTransaction();
        producer.SendOffsetsToTransaction(Offsets(), metadata);
        producer.SendOffsetsToTransaction(Offsets(), metadata);
        producer.CommitTransaction();
    }

    [Theory]
    [InlineData(SyncFlavor, Init, "InitProducerId")]
    [InlineData(SyncFlavor, Begin, "beginTransaction")]
    [InlineData(SyncFlavor, SendOffsets, "AddOffsetsToTxn")]
    [InlineData(SyncFlavor, Commit, "EndTxn(true)")]
    [InlineData(SyncFlavor, Abort, "EndTxn(false)")]
    [InlineData(AsyncFlavor, Init, "InitProducerId")]
    [InlineData(AsyncFlavor, Begin, "beginTransaction")]
    [InlineData(AsyncFlavor, SendOffsets, "AddOffsetsToTxn")]
    [InlineData(AsyncFlavor, Commit, "EndTxn(true)")]
    [InlineData(AsyncFlavor, Abort, "EndTxn(false)")]
    public async Task TransactionCall_FromASyncDeliveryCallback_EndsAfterMaxBlockMs(
        string flavor,
        string operation,
        string operationName)
    {
        ConsumerGroupMetadata metadata = GroupMetadata();
        Exception? timedOut = null;
        long elapsedMs = 0;
        object producer = flavor == SyncFlavor ? Sync(MaxBlockMs) : Async(MaxBlockMs);
        ProbeCallback callback = new ProbeCallback((_, _) =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            timedOut = Catch(() =>
            {
                if (operation == SendOffsets)
                {
                    SendOffsetsTo(producer, s_noOffsets, metadata).GetAwaiter().GetResult();
                }
                else
                {
                    Call(producer, operation, CancellationToken.None).GetAwaiter().GetResult();
                }
            });
            elapsedMs = elapsed.ElapsedMilliseconds;
        });
        try
        {
            ProducerRecord<byte[], byte[]> record = UnreachableProducers.Record(Topic("in-callback-" + operation));
            if (producer is KafkaProducer<byte[], byte[]> sync)
            {
                _ = sync.Send(record, callback);
            }
            else
            {
                _ = await TestTimeout.Run(
                    () => ((AsyncKafkaProducer<byte[], byte[]>)producer).Send(record, callback).AsTask(),
                    Deadline);
            }

            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Null(callback.BodyFailure);
        }
        finally
        {
            await Teardown(producer);
        }

        KafkaTimeoutException error = Assert.IsType<KafkaTimeoutException>(timedOut);
        Assert.Equal(7, error.Code);
        Assert.Equal(
            $"Timeout expired after {MaxBlockMs}ms while awaiting {operationName} from a send() callback, which " +
            "blocks the producer's completion thread",
            error.Message);
        Assert.InRange(elapsedMs, MaxBlockMs - 50, (long)Deadline.TotalMilliseconds);
    }

    [Theory]
    [InlineData(Commit, "EndTxn(true)")]
    [InlineData(Abort, "EndTxn(false)")]
    public async Task AsyncMockProducer_CommitOrAbort_FromAnAsyncCallbackAfterAnAwait_EndsAfterMaxBlockMs(
        string operation,
        string operationName)
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        await using IAsyncDisposable teardown = TestTimeout.AsyncDisposal(producer);
        SetMaxBlockMs(producer, MaxBlockMs);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        Exception? timedOut = null;
        long elapsedMs = 0;
        AfterAwait callback = new AfterAwait(async () =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            timedOut = await CatchAsync(() => Call(producer, operation, CancellationToken.None));
            elapsedMs = elapsed.ElapsedMilliseconds;
        });

        _ = await TestTimeout.Run(() => producer.Send(MockRecord(), callback).AsTask(), Deadline);

        Assert.True(callback.WaitDone(Deadline), "the delivery callback never ended");
        KafkaTimeoutException error = Assert.IsType<KafkaTimeoutException>(timedOut);
        Assert.Equal(7, error.Code);
        Assert.Equal(
            $"Timeout expired after {MaxBlockMs}ms while awaiting {operationName} from an async send() callback, " +
            "whose own report it waits for",
            error.Message);
        Assert.InRange(elapsedMs, MaxBlockMs - 50, (long)Deadline.TotalMilliseconds);
    }

    [Theory]
    [InlineData(Commit)]
    [InlineData(Abort)]
    public async Task MockProducer_CommitOrAbort_FromASyncCallbackTheDrainRuns_Succeeds(string operation)
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        using IDisposable teardown = TestTimeout.Disposal(producer);
        producer.InitTransactions();
        producer.BeginTransaction();
        Exception? failure = null;
        long elapsedMs = 0;
        ProbeCallback callback = new ProbeCallback((_, _) =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            failure = Catch(() => Call(producer, operation));
            elapsedMs = elapsed.ElapsedMilliseconds;
        });

        _ = producer.Send(MockRecord(), callback);

        Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
        Assert.Null(callback.BodyFailure);
        Assert.Null(failure);
        Assert.Equal(NativeProducer.CompletionThreadName, callback.ThreadName);
        Assert.True(elapsedMs < 5000, $"{operation} waited {elapsedMs} ms");
        Assert.Equal(operation == Commit ? 1 : 0, producer.HistoryCount());
        await Task.CompletedTask;
    }

    // Java's KafkaProducer returns at once for an empty map, after its group-metadata, transaction-manager and closed
    // checks, so the transaction state is not checked.
    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task SendOffsetsToTransaction_EmptyOffsets_OnAnIdempotentProducer_Completes(string flavor)
    {
        object producer = flavor == SyncFlavor ? Sync(MaxBlockMs) : Async(MaxBlockMs);
        try
        {
            Assert.Null(await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, GroupMetadata())));
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task SendOffsetsToTransaction_EmptyOffsets_BeforeInitTransactions_Completes(string flavor)
    {
        object producer = Transactional(flavor, MaxBlockMs);
        try
        {
            Assert.Null(await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, GroupMetadata())));
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task SendOffsetsToTransaction_EmptyOffsets_WithoutIdempotence_ThrowsJavasIllegalState(string flavor)
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = MaxBlockMs,
            ["enable.idempotence"] = false,
        };
        object producer = flavor == SyncFlavor
            ? new KafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray(), Serdes.ByteArray())
            : new AsyncKafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray(), Serdes.ByteArray());
        try
        {
            IllegalStateException error = Assert.IsType<IllegalStateException>(
                await CatchAsync(() => SendOffsetsTo(producer, s_noOffsets, GroupMetadata())));
            Assert.Equal(NoTransactionManager, error.Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    // The offsets' contents are checked as the consumer's commit checks them, before anything reaches the producer.
    [Theory]
    [InlineData(SyncFlavor, false)]
    [InlineData(AsyncFlavor, true)]
    public async Task SendOffsetsToTransaction_NullOffsetValue_ThrowsArgumentException(string flavor, bool mock)
    {
        object producer = mock ? Mock(flavor, autoComplete: true) : Transactional(flavor, MaxBlockMs);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("txn-input", 0)] = null! };
        try
        {
            ArgumentException error = Assert.IsType<ArgumentException>(
                await CatchAsync(() => SendOffsetsTo(producer, offsets, GroupMetadata())));
            Assert.Equal("offsets", error.ParamName);
            Assert.Equal(new ArgumentException("Offset value must not be null.", "offsets").Message, error.Message);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    // Each operation from a mock's sync callback that the drain runs completes, as Java's mock completes it; on the
    // async mock the callback blocks on the operation's task.
    [Theory]
    [InlineData(SyncFlavor, Init)]
    [InlineData(SyncFlavor, Begin)]
    [InlineData(SyncFlavor, SendOffsets)]
    [InlineData(SyncFlavor, Commit)]
    [InlineData(SyncFlavor, Abort)]
    [InlineData(AsyncFlavor, Init)]
    [InlineData(AsyncFlavor, Begin)]
    [InlineData(AsyncFlavor, SendOffsets)]
    [InlineData(AsyncFlavor, Commit)]
    [InlineData(AsyncFlavor, Abort)]
    public async Task MockProducer_TransactionCall_FromASyncCallbackTheDrainRuns_Completes(string flavor, string operation)
    {
        object producer = Mock(flavor, autoComplete: true);
        try
        {
            await PrepareFor(producer, operation);
            Exception? failure = null;
            long elapsedMs = 0;
            ProbeCallback callback = new ProbeCallback((_, _) =>
            {
                Stopwatch elapsed = Stopwatch.StartNew();
                failure = Catch(() => CallBlocking(producer, operation));
                elapsedMs = elapsed.ElapsedMilliseconds;
            });

            _ = await MockSend(producer, MockRecord(), callback);

            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Null(callback.BodyFailure);
            Assert.Null(failure);
            Assert.Equal(
                flavor == SyncFlavor ? NativeProducer.CompletionThreadName : NativeProducer.PumpThreadName,
                callback.ThreadName);
            Assert.True(elapsedMs < 5000, $"{operation} waited {elapsedMs} ms");
            await AssertStateAfter(producer, operation);
        }
        finally
        {
            await CloseNow(producer);
        }
    }

    // A sync callback queued behind an async one runs on the completion thread outside the drain, where the core cannot
    // see it. The three operations that wait for no report complete at once, as Java's mock does; a commit or an abort
    // waits for that callback's own report, so it ends after max.block.ms.
    [Theory]
    [InlineData(Init, null)]
    [InlineData(Begin, null)]
    [InlineData(SendOffsets, null)]
    [InlineData(Commit, "EndTxn(true)")]
    [InlineData(Abort, "EndTxn(false)")]
    public async Task AsyncMockProducer_TransactionCall_FromASyncCallbackQueuedBehindAnAsyncOne(
        string operation,
        string? timedOutOperation)
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray(), autoComplete: false);
        await using IAsyncDisposable teardown = TestTimeout.AsyncDisposal(producer);
        SetMaxBlockMs(producer, timedOutOperation is null ? AtOnceMaxBlockMs : MaxBlockMs);
        TaskCompletionSource<bool> gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            await PrepareFor(producer, operation);
            Exception? failure = null;
            long elapsedMs = 0;
            ProbeCallback callback = new ProbeCallback((_, _) =>
            {
                Stopwatch elapsed = Stopwatch.StartNew();
                failure = Catch(() => CallBlocking(producer, operation));
                elapsedMs = elapsed.ElapsedMilliseconds;
            });
            HeldAsyncCallback held = new HeldAsyncCallback(gate.Task);
            _ = await TestTimeout.Run(() => producer.Send(MockRecord(), held).AsTask(), Deadline);
            _ = await TestTimeout.Run(() => producer.Send(MockRecord(), callback).AsTask(), Deadline);

            Assert.True(producer.CompleteNext());
            await TestTimeout.Run(() => held.WhenStarted, Deadline);
            Assert.True(producer.CompleteNext());
            WaitUntilQueuedBehind(producer, 1);
            gate.SetResult(true);

            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Null(callback.BodyFailure);
            Assert.Equal(NativeProducer.PumpThreadName, callback.ThreadName);
            if (timedOutOperation is null)
            {
                Assert.Null(failure);
                Assert.True(elapsedMs < AtOnceMaxBlockMs, $"{operation} waited {elapsedMs} ms");
                await AssertStateAfter(producer, operation);
            }
            else
            {
                KafkaTimeoutException error = Assert.IsType<KafkaTimeoutException>(failure);
                Assert.Equal(7, error.Code);
                Assert.Equal(
                    $"Timeout expired after {MaxBlockMs}ms while awaiting {timedOutOperation} from a send() callback, " +
                    "which blocks the producer's completion thread",
                    error.Message);
                Assert.InRange(elapsedMs, MaxBlockMs - 50, (long)Deadline.TotalMilliseconds);
            }
        }
        finally
        {
            gate.TrySetResult(true);
        }
    }

    // A Dispose during the operation returns, and the call ends once, with what the core delivers: a graceful close
    // does not fail a pending InitTransactions, which times out after max.block.ms, as Java's does.
    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task InitTransactions_DisposeDuringTheWait_EndsTheCallOnce_AndDisposeReturns(string flavor)
    {
        const int LongMaxBlockMs = 1000;
        object producer = Transactional(flavor, LongMaxBlockMs);
        Task call = flavor == SyncFlavor
            ? Task.Run(() => ((KafkaProducer<byte[], byte[]>)producer).InitTransactions())
            : ((AsyncKafkaProducer<byte[], byte[]>)producer).InitTransactions();
        await Task.Delay(100);
        Assert.False(call.IsCompleted, "InitTransactions ended before the producer was disposed");

        await Teardown(producer);
        KafkaTimeoutException error = Assert.IsType<KafkaTimeoutException>(await CatchAsync(() => call));

        Assert.Equal(7, error.Code);
        Assert.Equal($"Timeout expired after {LongMaxBlockMs}ms while awaiting InitProducerId. {InitTimeout}", error.Message);
    }

    private static Exception? Catch(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static async Task<Exception?> CatchAsync(Func<Task> action)
    {
        try
        {
            await TestTimeout.Run(action, Deadline);
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static object Transactional(string flavor, int maxBlockMs, string transactionalId = "tx-tests")
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = maxBlockMs,
            ["transactional.id"] = transactionalId,
        };
        return flavor == SyncFlavor
            ? new KafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray(), Serdes.ByteArray())
            : new AsyncKafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray(), Serdes.ByteArray());
    }

    private static object Mock(string flavor, bool autoComplete) => flavor == SyncFlavor
        ? new MockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray(), autoComplete)
        : new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray(), autoComplete);

    private static ConsumerGroupMetadata GroupMetadata()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        return consumer.GroupMetadata();
    }

    private static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("txn-input", 0)] = new OffsetAndMetadata(42) };

    private static ProducerRecord<byte[], byte[]> MockRecord() =>
        new ProducerRecord<byte[], byte[]>("txn-topic", new byte[] { 1 }, new byte[] { 2 });

    // Runs a sync operation on either flavor; the async flavor's Task is the outcome.
    private static void Call(object producer, string operation) =>
        Call(producer, operation, CancellationToken.None).GetAwaiter().GetResult();

    private static Task Call(object producer, string operation, CancellationToken cancellationToken)
    {
        switch (producer)
        {
            case IProducer<byte[], byte[]> sync:
                return Run(() =>
                {
                    switch (operation)
                    {
                        case Init: sync.InitTransactions(); break;
                        case Begin: sync.BeginTransaction(); break;
                        case Commit: sync.CommitTransaction(); break;
                        case Abort: sync.AbortTransaction(); break;
                        default: throw new ArgumentException(operation);
                    }
                });
            case IAsyncProducer<byte[], byte[]> async:
                switch (operation)
                {
                    case Init: return async.InitTransactions(cancellationToken);
                    case Begin: return async.BeginTransaction(cancellationToken);
                    case Commit: return async.CommitTransaction(cancellationToken);
                    case Abort: return async.AbortTransaction(cancellationToken);
                    default: throw new ArgumentException(operation);
                }

            default:
                throw new ArgumentException(producer.GetType().Name);
        }
    }

    private static Task SendOffsetsTo(
        object producer,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets,
        ConsumerGroupMetadata? metadata) => producer switch
        {
            IProducer<byte[], byte[]> sync => Run(() => sync.SendOffsetsToTransaction(offsets!, metadata!)),
            IAsyncProducer<byte[], byte[]> async => async.SendOffsetsToTransaction(offsets!, metadata!),
            _ => throw new ArgumentException(producer.GetType().Name),
        };

    // The sync call runs here, so it throws at the call.
    private static Task Run(Action action)
    {
        action();
        return Task.CompletedTask;
    }

    private static async Task<Func<bool>> MockSend(
        object producer,
        ProducerRecord<byte[], byte[]> record,
        ICallback? callback)
    {
        if (producer is MockProducer<byte[], byte[]> sync)
        {
            KafkaFuture<RecordMetadata> future = callback is null ? sync.Send(record) : sync.Send(record, callback);
            return () => future.IsDone();
        }

        AsyncMockProducer<byte[], byte[]> async = (AsyncMockProducer<byte[], byte[]>)producer;
        AsyncKafkaFuture<RecordMetadata> pending = callback is null
            ? await TestTimeout.Run(() => async.Send(record).AsTask(), Deadline)
            : await TestTimeout.Run(() => async.Send(record, callback).AsTask(), Deadline);
        return () => pending.IsDone();
    }

    private static int MockHistoryCount(object producer) => producer is MockProducer<byte[], byte[]> sync
        ? sync.HistoryCount()
        : ((AsyncMockProducer<byte[], byte[]>)producer).HistoryCount();

    private static async Task CloseNow(object producer)
    {
        switch (producer)
        {
            case IProducer<byte[], byte[]> sync:
                TestTimeout.Run(() => sync.Close(TimeSpan.Zero), Deadline);
                TestTimeout.Run(sync.Dispose, Deadline);
                break;
            case IAsyncProducer<byte[], byte[]> async:
                await TestTimeout.Run(() => async.Close(TimeSpan.Zero), Deadline);
                await TestTimeout.Run(() => async.DisposeAsync().AsTask(), Deadline);
                break;
        }
    }

    private static async Task Teardown(object producer)
    {
        if (producer is IProducer<byte[], byte[]> sync)
        {
            TestTimeout.Run(sync.Dispose, Deadline);
        }
        else
        {
            await TestTimeout.Run(() => ((IAsyncProducer<byte[], byte[]>)producer).DisposeAsync().AsTask(), Deadline);
        }
    }

    // Brings a mock to the state the operation needs: initialized for all but InitTransactions, in a transaction for
    // SendOffsetsToTransaction, CommitTransaction and AbortTransaction.
    private static async Task PrepareFor(object producer, string operation)
    {
        if (operation == Init)
        {
            return;
        }

        await Call(producer, Init, CancellationToken.None);
        if (operation != Begin)
        {
            Call(producer, Begin);
        }
    }

    // Runs the operation to its end on the calling thread, blocking on the async flavor's task.
    private static void CallBlocking(object producer, string operation)
    {
        if (operation == SendOffsets)
        {
            SendOffsetsTo(producer, Offsets(), GroupMetadata()).GetAwaiter().GetResult();
        }
        else
        {
            Call(producer, operation, CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    // The mock state each operation leaves, seen through the mock's own checks.
    private static async Task AssertStateAfter(object producer, string operation)
    {
        switch (operation)
        {
            case Init:
                Assert.Equal(
                    MockAlreadyInitialized,
                    Assert.IsType<IllegalStateException>(await CatchAsync(() => Call(producer, Init, CancellationToken.None))).Message);
                Call(producer, Begin);
                break;
            case Begin:
            case SendOffsets:
                Assert.Equal(MockTransactionStarted, Assert.IsType<IllegalStateException>(Catch(() => Call(producer, Begin))).Message);
                await Call(producer, Commit, CancellationToken.None);
                break;
            case Commit:
            case Abort:
                Assert.Equal(
                    MockNoTransaction,
                    Assert.IsType<IllegalStateException>(await CatchAsync(() => Call(producer, Commit, CancellationToken.None))).Message);
                break;
        }
    }

    private static void WaitUntilQueuedBehind(AsyncMockProducer<byte[], byte[]> producer, int count)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeProducer native = (NativeProducer)producer.GetType().GetField("_native", Private)!.GetValue(producer)!;
        DeliveryReporter reporter = (DeliveryReporter)typeof(NativeProducer).GetField("_reporter", Private)!.GetValue(native)!;
        object chainGate = typeof(DeliveryReporter).GetField("_chainGate", Private)!.GetValue(reporter)!;
        Queue<DeliveryState> waiting =
            (Queue<DeliveryState>)typeof(DeliveryReporter).GetField("_waiting", Private)!.GetValue(reporter)!;
        Stopwatch waited = Stopwatch.StartNew();
        while (true)
        {
            lock (chainGate)
            {
                if (waiting.Count == count)
                {
                    return;
                }
            }

            Assert.True(waited.Elapsed < Deadline, "the callback never queued behind the async callback");
            Thread.Sleep(1);
        }
    }

    // A mock reads no config, so its max.block.ms is set on the native producer to keep the test short.
    private static void SetMaxBlockMs(object producer, long maxBlockMs)
    {
        object native = producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        typeof(NativeProducer)
            .GetField("_maxBlockMs", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(native, maxBlockMs);
    }

    /// <summary>An async delivery callback whose task runs until its gate opens.</summary>
    private sealed class HeldAsyncCallback : IAsyncCallback
    {
        private readonly Task _gate;
        private readonly TaskCompletionSource<bool> _started =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal HeldAsyncCallback(Task gate) => _gate = gate;

        internal Task WhenStarted => _started.Task;

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _started.TrySetResult(true);
            return _gate;
        }
    }

    /// <summary>An async delivery callback that runs its body after an <c>await</c>.</summary>
    private sealed class AfterAwait : IAsyncCallback
    {
        private readonly Func<Task> _body;
        private readonly ManualResetEventSlim _done = new ManualResetEventSlim();

        internal AfterAwait(Func<Task> body) => _body = body;

        public async Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            try
            {
                await Task.Yield();
                await _body();
            }
            finally
            {
                _done.Set();
            }
        }

        internal bool WaitDone(TimeSpan timeout) => _done.Wait(timeout);
    }
}
