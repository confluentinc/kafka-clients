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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The mock producers' controls that Java's <c>MockProducerTest</c> does not exercise: the nine injected exceptions,
/// <c>Closed</c>, the uncommitted offsets, the <see cref="AsyncMockProducer{TKey, TValue}"/> forms, and the calls after
/// close. Each test runs on both mocks.
/// </summary>
public sealed class PublicMockProducerTransactionControlTests
{
    private const string MockClosed = "MockProducer is already closed.";

    private const string Topic = "topic";

    private static readonly TopicPartition s_partition = new TopicPartition("txn-input", 0);

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_offsets =
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_partition] = new OffsetAndMetadata(42L) };

    public static TheoryData<bool> Flavors => new TheoryData<bool> { false, true };

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task InitTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        IllegalStateException injected = new IllegalStateException("init");
        producer.InitTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.InitTransactions));
        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.InitTransactions));
        Assert.False(producer.TransactionInitialized());

        producer.InitTransactionException(null);
        await producer.InitTransactions();
        Assert.True(producer.TransactionInitialized());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task BeginTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        KafkaException injected = new KafkaException("begin");
        producer.BeginTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.BeginTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.BeginTransaction));
        Assert.False(producer.TransactionInFlight());

        producer.BeginTransactionException(null);
        await producer.BeginTransaction();
        Assert.True(producer.TransactionInFlight());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task BeginTransactionException_IsThrownBeforeTheOpenTransactionCheck(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        KafkaException injected = new KafkaException("begin");
        producer.BeginTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.BeginTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.BeginTransaction));
        Assert.True(producer.TransactionInFlight());

        producer.BeginTransactionException(null);
        Assert.Equal(
            "Transaction already started",
            (await Assert.ThrowsAsync<IllegalStateException>(producer.BeginTransaction)).Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SendOffsetsToTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        ProducerFencedException injected = new ProducerFencedException("offsets");
        producer.SendOffsetsToTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<ProducerFencedException>(producer.SendOffsets));
        Assert.Same(injected, await Assert.ThrowsAsync<ProducerFencedException>(producer.SendOffsets));
        Assert.False(producer.SentOffsets());
        Assert.Empty(producer.UncommittedOffsets());

        producer.SendOffsetsToTransactionException(null);
        await producer.SendOffsets();
        Assert.True(producer.SentOffsets());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task CommitTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        InvalidOperationException injected = new InvalidOperationException("commit");
        producer.CommitTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<InvalidOperationException>(producer.CommitTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<InvalidOperationException>(producer.CommitTransaction));
        Assert.True(producer.TransactionInFlight());
        Assert.Equal(0L, producer.CommitCount());

        producer.CommitTransactionException(null);
        await producer.CommitTransaction();
        Assert.True(producer.TransactionCommitted());
        Assert.Equal(1L, producer.CommitCount());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task AbortTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        KafkaException injected = new KafkaException("abort");
        producer.AbortTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.True(producer.TransactionInFlight());

        producer.AbortTransactionException(null);
        await producer.AbortTransaction();
        Assert.True(producer.TransactionAborted());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task UncommittedOffsets_HoldTheTransactionsOffsetsUntilItsCommit(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        Assert.Empty(producer.UncommittedOffsets());

        await producer.SendOffsets();
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets =
            Assert.Single(producer.UncommittedOffsets()).Value;
        Assert.Equal(new OffsetAndMetadata(42L), offsets[s_partition]);
        Assert.Empty(producer.ConsumerGroupOffsetsHistory());

        await producer.CommitTransaction();
        Assert.Empty(producer.UncommittedOffsets());
        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> commit =
            Assert.Single(producer.ConsumerGroupOffsetsHistory());
        Assert.Equal(new OffsetAndMetadata(42L), Assert.Single(commit).Value[s_partition]);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task UncommittedOffsets_AreDroppedOnAbort(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        await producer.SendOffsets();
        Assert.Single(producer.UncommittedOffsets());

        await producer.AbortTransaction();
        Assert.Empty(producer.UncommittedOffsets());
        Assert.Empty(producer.ConsumerGroupOffsetsHistory());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task FenceProducer_FailsTheTransactionCallsWithProducerFenced(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.FenceProducer();

        ProducerFencedException e = await Assert.ThrowsAsync<ProducerFencedException>(producer.BeginTransaction);
        Assert.Equal("MockProducer is fenced.", e.Message);
        e = await Assert.ThrowsAsync<ProducerFencedException>(producer.CommitTransaction);
        Assert.Equal("MockProducer is fenced.", e.Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SendException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("send");
        producer.SendException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.Send));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.Send));
        Assert.Equal(0, producer.HistoryCount());

        producer.SendException(null);
        await producer.Send();
        Assert.Equal(1, producer.HistoryCount());
    }

    // Java's MockProducer.send checks the send exception and the fenced state before it serializes, so a throwing
    // serializer is not reached. The async mock fails the first stage.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Send_WithASendExceptionOrFenced_ThrowsBeforeSerializing(bool async)
    {
        ThrowingSerializer<string> serializer = new ThrowingSerializer<string>();
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, value: "v");
        KafkaException injected = new KafkaException("send");
        KafkaException fenced;
        if (async)
        {
            using AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(serializer.AsAsync(), serializer.AsAsync());
            producer.SendException(injected);
            ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(record);
            Assert.True(stage.IsFaulted);
            Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => stage.AsTask()));
            producer.SendException(null);
            await producer.InitTransactions();
            producer.FenceProducer();
            stage = producer.Send(record);
            Assert.True(stage.IsFaulted);
            fenced = await Assert.ThrowsAsync<KafkaException>(() => stage.AsTask());
        }
        else
        {
            using MockProducer<string, string> producer = new MockProducer<string, string>(serializer, serializer);
            producer.SendException(injected);
            Assert.Same(injected, Assert.Throws<KafkaException>(() => producer.Send(record)));
            producer.SendException(null);
            producer.InitTransactions();
            producer.FenceProducer();
            fenced = Assert.Throws<KafkaException>(() => producer.Send(record));
        }

        Assert.Equal("MockProducer is fenced.", fenced.Message);
        Assert.Equal("Fenced", Assert.IsType<ProducerFencedException>(fenced.InnerException).Message);
        Assert.Equal(0, serializer.InvocationCount);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task FlushException_ThrowsTheSameInstanceAndCompletesNoSend(bool async)
    {
        using Mock producer = new Mock(async, autoComplete: false);
        await producer.Send();
        IllegalStateException injected = new IllegalStateException("flush");
        producer.FlushException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.Flush));
        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.Flush));
        Assert.False(producer.Flushed());

        producer.FlushException(null);
        await producer.Flush();
        Assert.True(producer.Flushed());
    }

    // Java's MockProducer.commitTransaction() and abortTransaction() call flush() after their own checks.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task FlushException_FailsCommitAndAbort(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        await producer.BeginTransaction();
        KafkaException injected = new KafkaException("flush");
        producer.FlushException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.CommitTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.True(producer.TransactionInFlight());
        Assert.Equal(0L, producer.CommitCount());

        producer.FlushException(null);
        await producer.CommitTransaction();
        Assert.True(producer.TransactionCommitted());
        Assert.Equal(1L, producer.CommitCount());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task PartitionsForException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("partitionsFor");
        producer.PartitionsForException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.PartitionsFor));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.PartitionsFor));

        producer.PartitionsForException(null);
        Assert.Empty(await producer.PartitionsFor());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task CloseException_ThrowsTheSameInstanceAndLeavesTheProducerOpen(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("close");
        producer.CloseException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => producer.CloseAsTask()));
        Assert.Same(
            injected,
            await Assert.ThrowsAsync<KafkaException>(() => producer.CloseAsTask(TimeSpan.FromSeconds(1))));
        Assert.False(producer.Closed());
        await producer.Send();
        Assert.Equal(1, producer.HistoryCount());

        producer.CloseException(null);
        await producer.CloseAsTask();
        Assert.True(producer.Closed());
        await producer.CloseAsTask();
    }

    // Java's MockProducer.close() is not synchronized and throws its closeException before it sets closed, as Python's
    // mock raises its close_exception: with a delivery callback still running, closes made together each throw the
    // same instance without waiting for it, and the producer stays open, so a send meanwhile is accepted.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task CloseException_ClosesWhileACallbackRuns_ThrowAtOnceAndLeaveTheProducerOpen(bool async)
    {
        using Mock producer = new Mock(async, autoComplete: false);
        KafkaException injected = new KafkaException("close");
        producer.CloseException(injected);
        using Blocking blocking = new Blocking();
        await producer.Send(blocking);
        Assert.True(producer.CompleteNext());
        Assert.True(blocking.Entered.Wait(BlockingCall.Deadline), "the callback did not start");

        Task first = Task.Run(() => producer.CloseAsTask());
        Task second = Task.Run(() => producer.CloseAsTask(TimeSpan.Zero));

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => Within(first)));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => Within(second)));
        Assert.False(blocking.Returned, "a close waited for the running callback");
        Assert.False(producer.Closed());
        await producer.Send();
        Assert.Equal(2, producer.HistoryCount());
    }

    // Java's and Python's mocks throw closeException from a close made inside a delivery callback too.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task CloseException_CloseFromADeliveryCallback_ThrowsTheSameInstance(bool async)
    {
        using Mock producer = new Mock(async, autoComplete: false);
        KafkaException injected = new KafkaException("close");
        producer.CloseException(injected);
        ClosingInCallback closing = new ClosingInCallback(producer);
        await producer.Send(closing);

        Assert.True(producer.CompleteNext());

        Assert.Same(injected, await Within(closing.Thrown));
        Assert.False(producer.Closed());
    }

    // Java's MockProducer.flush() throws its flushException first, also from a callback the core cannot see.
    [Fact]
    public async Task FlushException_FlushFromAnAsyncCallbackAfterAnAwait_ThrowsTheSameInstance()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(autoComplete: false, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(producer);
        IllegalStateException injected = new IllegalStateException("flush");
        producer.FlushException(injected);
        FlushingAfterAnAwait flushing = new FlushingAfterAnAwait(producer);
        _ = await producer.Send(Record(), flushing);
        _ = await producer.Send(Record());

        Assert.True(producer.CompleteNext());
        Exception? thrown = await Within(flushing.Thrown);

        Assert.Same(injected, thrown);
        Assert.False(producer.Flushed());
        producer.FlushException(null);
    }

    // FlushException sets the core's flush error, which Flush throws, and the binding's copy, which a flush from a
    // callback the core cannot see throws. A second call waits for the first, so the two copies end equal: here both
    // clear, as the clear ran last.
    [Fact]
    public async Task FlushException_ConcurrentCalls_LeaveBothFlushPathsEqual()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(autoComplete: false, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(producer);
        using PausingException pausing = new PausingException();

        Task set = Task.Run(() => producer.FlushException(pausing));
        Assert.True(pausing.Entered.Wait(BlockingCall.Deadline), "the first call did not start");
        Task clear = Task.Run(() => producer.FlushException(null));
        Assert.NotSame(clear, await Task.WhenAny(clear, Task.Delay(TimeSpan.FromMilliseconds(200))));
        pausing.Release.Set();
        await Within(set);
        await Within(clear);

        await producer.Flush();
        FlushingAfterAnAwait flushing = new FlushingAfterAnAwait(producer);
        _ = await producer.Send(Record(), flushing);
        Assert.True(producer.CompleteNext());
        Assert.Null(await Within(flushing.Thrown));
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void Closed_IsFalseUntilClose(bool async)
    {
        using Mock producer = new Mock(async);
        Assert.False(producer.Closed());

        producer.Close();
        Assert.True(producer.Closed());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void Dispose_WithACloseException_ReleasesTheProducer(bool async)
    {
        Mock producer = new Mock(async);
        producer.CloseException(new KafkaException("close"));

        producer.Dispose();

        Assert.True(producer.Closed());
        Assert.Equal(MockClosed, Assert.Throws<IllegalStateException>(() => producer.HistoryCount()).Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void Controls_AfterClose_ThrowIllegalState(bool async)
    {
        using Mock producer = new Mock(async);
        producer.Close();

        foreach (Action call in new Action[]
        {
            () => producer.InitTransactionException(null),
            () => producer.BeginTransactionException(null),
            () => producer.SendOffsetsToTransactionException(null),
            () => producer.CommitTransactionException(null),
            () => producer.AbortTransactionException(null),
            () => producer.SendException(null),
            () => producer.FlushException(null),
            () => producer.PartitionsForException(null),
            producer.FenceProducer,
            () => _ = producer.TransactionInitialized(),
            () => _ = producer.TransactionInFlight(),
            () => _ = producer.TransactionCommitted(),
            () => _ = producer.TransactionAborted(),
            () => _ = producer.Flushed(),
            () => _ = producer.SentOffsets(),
            () => _ = producer.CommitCount(),
            () => _ = producer.ConsumerGroupOffsetsHistory(),
            () => _ = producer.UncommittedOffsets(),
        })
        {
            IllegalStateException e = Assert.Throws<IllegalStateException>(call);
            Assert.Equal(MockClosed, e.Message);
        }
    }

    // Java's closeException is a public field, so it can be set after close, and close throws it then too.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task CloseException_AfterClose_CloseThrowsIt(bool async)
    {
        using Mock producer = new Mock(async);
        producer.Close();
        KafkaException injected = new KafkaException("close");

        producer.CloseException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => producer.CloseAsTask()));
        producer.CloseException(null);
        await producer.CloseAsTask();
    }

    [Fact]
    public void SetException_AfterClose_DoesNotKeepTheException()
    {
        WeakReference reference = CloseThenSet();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(reference.IsAlive);
    }

    // Not inlined, so no local of the caller's frame keeps the exception reachable.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CloseThenSet()
    {
        using Mock producer = new Mock(async: false);
        producer.Close();
        KafkaException injected = new KafkaException("closed");
        Assert.Throws<IllegalStateException>(() => producer.CommitTransactionException(injected));
        return new WeakReference(injected);
    }

    private static IConsumerGroupMetadata GroupMetadata()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("latest");
        return consumer.GroupMetadata();
    }

    // One of the two mocks, behind the sync shape; an async operation's Task is awaited.
    private static Task Within(Task task) => TestTimeout.Run(() => task, BlockingCall.Deadline);

    private static Task<T> Within<T>(Task<T> task) => TestTimeout.Run(() => task, BlockingCall.Deadline);

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, value: Encoding.UTF8.GetBytes("v"));

    /// <summary>A delivery callback that blocks the producer's completion thread until the test releases it.</summary>
    private sealed class Blocking : ICallback, IDisposable
    {
        private int _returned;

        internal ManualResetEventSlim Entered { get; } = new ManualResetEventSlim();

        internal ManualResetEventSlim Release { get; } = new ManualResetEventSlim();

        internal bool Returned => Volatile.Read(ref _returned) != 0;

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Entered.Set();
            _ = Release.Wait(BlockingCall.Deadline);
            Volatile.Write(ref _returned, 1);
        }

        public void Dispose()
        {
            Release.Set();
            Entered.Dispose();
            Release.Dispose();
        }
    }

    /// <summary>
    /// An exception whose message, read once while the binding hands it to the core, waits until the test releases it.
    /// </summary>
    private sealed class PausingException : Exception, IDisposable
    {
        private int _read;

        internal ManualResetEventSlim Entered { get; } = new ManualResetEventSlim();

        internal ManualResetEventSlim Release { get; } = new ManualResetEventSlim();

        public override string Message
        {
            get
            {
                if (Interlocked.Exchange(ref _read, 1) == 0)
                {
                    Entered.Set();
                    _ = Release.Wait(BlockingCall.Deadline);
                }

                return "flush";
            }
        }

        public void Dispose()
        {
            Release.Set();
            Entered.Dispose();
            Release.Dispose();
        }
    }

    /// <summary>A delivery callback that closes its producer and records what the close threw.</summary>
    private sealed class ClosingInCallback : ICallback
    {
        private readonly Mock _producer;
        private readonly TaskCompletionSource<Exception?> _thrown =
            new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ClosingInCallback(Mock producer) => _producer = producer;

        internal Task<Exception?> Thrown => _thrown.Task;

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Task close = _producer.CloseAsTask();
            _thrown.SetResult(close.IsFaulted ? close.Exception!.InnerException : null);
        }
    }

    /// <summary>An async delivery callback that flushes its producer after an await and records what it threw.</summary>
    private sealed class FlushingAfterAnAwait : IAsyncCallback
    {
        private readonly AsyncMockProducer<byte[], byte[]> _producer;
        private readonly TaskCompletionSource<Exception?> _thrown =
            new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal FlushingAfterAnAwait(AsyncMockProducer<byte[], byte[]> producer) => _producer = producer;

        internal Task<Exception?> Thrown => _thrown.Task;

        public async Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            await Task.Yield();
            try
            {
                await _producer.Flush().ConfigureAwait(false);
                _thrown.SetResult(null);
            }
            catch (Exception failure)
            {
                _thrown.SetResult(failure);
            }
        }
    }

    private sealed class Mock : IDisposable
    {
        private readonly MockProducer<byte[], byte[]>? _sync;

        private readonly AsyncMockProducer<byte[], byte[]>? _async;

        internal Mock(bool async, bool autoComplete = true)
        {
            if (async)
            {
                _async = new AsyncMockProducer<byte[], byte[]>(autoComplete, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
            }
            else
            {
                _sync = new MockProducer<byte[], byte[]>(autoComplete, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
            }
        }

        internal bool Closed() => _sync?.Closed() ?? _async!.Closed();

        internal int HistoryCount() => _sync?.HistoryCount() ?? _async!.HistoryCount();

        // The first stage only: a send's delivery is not awaited.
        internal Task Send() =>
            Run(
                () => _sync!.Send(Record()),
                async () => await _async!.Send(Record()).ConfigureAwait(false));

        internal Task Send(ICallback callback) =>
            Run(
                () => _sync!.Send(Record(), callback),
                async () => await _async!.Send(Record(), callback).ConfigureAwait(false));

        internal bool CompleteNext() => _sync?.CompleteNext() ?? _async!.CompleteNext();

        internal Task Flush() => Run(() => _sync!.Flush(), () => _async!.Flush());

        internal async Task<IReadOnlyList<PartitionInfo>> PartitionsFor()
        {
            if (_sync is not null)
            {
                return _sync.PartitionsFor(Topic);
            }

            return await _async!.PartitionsFor(Topic).ConfigureAwait(false);
        }

        internal Task CloseAsTask() => Run(() => _sync!.Close(), () => _async!.Close());

        internal Task CloseAsTask(TimeSpan timeout) => Run(() => _sync!.Close(timeout), () => _async!.Close(timeout));

        internal void SendException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SendException(exception);
            }
            else
            {
                _async!.SendException(exception);
            }
        }

        internal void FlushException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.FlushException(exception);
            }
            else
            {
                _async!.FlushException(exception);
            }
        }

        internal void PartitionsForException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.PartitionsForException(exception);
            }
            else
            {
                _async!.PartitionsForException(exception);
            }
        }

        internal void CloseException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.CloseException(exception);
            }
            else
            {
                _async!.CloseException(exception);
            }
        }

        internal bool TransactionInitialized() => _sync?.TransactionInitialized() ?? _async!.TransactionInitialized();

        internal bool TransactionInFlight() => _sync?.TransactionInFlight() ?? _async!.TransactionInFlight();

        internal bool TransactionCommitted() => _sync?.TransactionCommitted() ?? _async!.TransactionCommitted();

        internal bool TransactionAborted() => _sync?.TransactionAborted() ?? _async!.TransactionAborted();

        internal bool Flushed() => _sync?.Flushed() ?? _async!.Flushed();

        internal bool SentOffsets() => _sync?.SentOffsets() ?? _async!.SentOffsets();

        internal long CommitCount() => _sync?.CommitCount() ?? _async!.CommitCount();

        internal Task InitTransactions() => Run(() => _sync!.InitTransactions(), () => _async!.InitTransactions());

        internal Task BeginTransaction() => Run(() => _sync!.BeginTransaction(), () => _async!.BeginTransaction());

        internal Task SendOffsets() =>
            Run(
                () => _sync!.SendOffsetsToTransaction(s_offsets, GroupMetadata()),
                () => _async!.SendOffsetsToTransaction(s_offsets, GroupMetadata()));

        internal Task CommitTransaction() => Run(() => _sync!.CommitTransaction(), () => _async!.CommitTransaction());

        internal Task AbortTransaction() => Run(() => _sync!.AbortTransaction(), () => _async!.AbortTransaction());

        internal void Close()
        {
            if (_sync is not null)
            {
                _sync.Close();
            }
            else
            {
                _async!.Close().GetAwaiter().GetResult();
            }
        }

        internal void InitTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.InitTransactionException(exception);
            }
            else
            {
                _async!.InitTransactionException(exception);
            }
        }

        internal void BeginTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.BeginTransactionException(exception);
            }
            else
            {
                _async!.BeginTransactionException(exception);
            }
        }

        internal void SendOffsetsToTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SendOffsetsToTransactionException(exception);
            }
            else
            {
                _async!.SendOffsetsToTransactionException(exception);
            }
        }

        internal void CommitTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.CommitTransactionException(exception);
            }
            else
            {
                _async!.CommitTransactionException(exception);
            }
        }

        internal void AbortTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.AbortTransactionException(exception);
            }
            else
            {
                _async!.AbortTransactionException(exception);
            }
        }

        internal void FenceProducer()
        {
            if (_sync is not null)
            {
                _sync.FenceProducer();
            }
            else
            {
                _async!.FenceProducer();
            }
        }

        internal IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>
            ConsumerGroupOffsetsHistory() =>
            _sync?.ConsumerGroupOffsetsHistory() ?? _async!.ConsumerGroupOffsetsHistory();

        internal IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> UncommittedOffsets() =>
            _sync?.UncommittedOffsets() ?? _async!.UncommittedOffsets();

        public void Dispose()
        {
            TestTimeout.Dispose(_sync);
            TestTimeout.Dispose(_async);
        }

        private static ProducerRecord<byte[], byte[]> Record() =>
            new ProducerRecord<byte[], byte[]>(Topic, value: Encoding.UTF8.GetBytes("v"));

        private Task Run(Action sync, Func<Task> async)
        {
            if (_sync is null)
            {
                return async();
            }

            try
            {
                sync();
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                return Task.FromException(e);
            }
        }
    }
}
