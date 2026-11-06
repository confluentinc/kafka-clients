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
    public async Task SetInitTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        IllegalStateException injected = new IllegalStateException("init");
        producer.SetInitTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.InitTransactions));
        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.InitTransactions));
        Assert.False(producer.TransactionInitialized);

        producer.SetInitTransactionException(null);
        await producer.InitTransactions();
        Assert.True(producer.TransactionInitialized);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetBeginTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        KafkaException injected = new KafkaException("begin");
        producer.SetBeginTransactionException(injected);

        Assert.Same(injected, Assert.Throws<KafkaException>(producer.BeginTransaction));
        Assert.Same(injected, Assert.Throws<KafkaException>(producer.BeginTransaction));
        Assert.False(producer.TransactionInFlight);

        producer.SetBeginTransactionException(null);
        producer.BeginTransaction();
        Assert.True(producer.TransactionInFlight);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetBeginTransactionException_IsThrownBeforeTheOpenTransactionCheck(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
        KafkaException injected = new KafkaException("begin");
        producer.SetBeginTransactionException(injected);

        Assert.Same(injected, Assert.Throws<KafkaException>(producer.BeginTransaction));
        Assert.Same(injected, Assert.Throws<KafkaException>(producer.BeginTransaction));
        Assert.True(producer.TransactionInFlight);

        producer.SetBeginTransactionException(null);
        Assert.Equal(
            "Transaction already started",
            Assert.Throws<IllegalStateException>(producer.BeginTransaction).Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetSendOffsetsToTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
        ProducerFencedException injected = new ProducerFencedException("offsets");
        producer.SetSendOffsetsToTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<ProducerFencedException>(producer.SendOffsets));
        Assert.Same(injected, await Assert.ThrowsAsync<ProducerFencedException>(producer.SendOffsets));
        Assert.False(producer.SentOffsets);
        Assert.Empty(producer.UncommittedOffsets());

        producer.SetSendOffsetsToTransactionException(null);
        await producer.SendOffsets();
        Assert.True(producer.SentOffsets);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetCommitTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
        InvalidOperationException injected = new InvalidOperationException("commit");
        producer.SetCommitTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<InvalidOperationException>(producer.CommitTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<InvalidOperationException>(producer.CommitTransaction));
        Assert.True(producer.TransactionInFlight);
        Assert.Equal(0L, producer.CommitCount);

        producer.SetCommitTransactionException(null);
        await producer.CommitTransaction();
        Assert.True(producer.TransactionCommitted);
        Assert.Equal(1L, producer.CommitCount);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetAbortTransactionException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
        KafkaException injected = new KafkaException("abort");
        producer.SetAbortTransactionException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.True(producer.TransactionInFlight);

        producer.SetAbortTransactionException(null);
        await producer.AbortTransaction();
        Assert.True(producer.TransactionAborted);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task UncommittedOffsets_HoldTheTransactionsOffsetsUntilItsCommit(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
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
        producer.BeginTransaction();
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

        ProducerFencedException e = Assert.Throws<ProducerFencedException>(producer.BeginTransaction);
        Assert.Equal("MockProducer is fenced.", e.Message);
        e = await Assert.ThrowsAsync<ProducerFencedException>(producer.CommitTransaction);
        Assert.Equal("MockProducer is fenced.", e.Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetSendException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("send");
        producer.SetSendException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.Send));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.Send));
        Assert.Equal(0, producer.HistoryCount());

        producer.SetSendException(null);
        await producer.Send();
        Assert.Equal(1, producer.HistoryCount());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetFlushException_ThrowsTheSameInstanceAndCompletesNoSend(bool async)
    {
        using Mock producer = new Mock(async, autoComplete: false);
        await producer.Send();
        IllegalStateException injected = new IllegalStateException("flush");
        producer.SetFlushException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.Flush));
        Assert.Same(injected, await Assert.ThrowsAsync<IllegalStateException>(producer.Flush));
        Assert.False(producer.Flushed);

        producer.SetFlushException(null);
        await producer.Flush();
        Assert.True(producer.Flushed);
    }

    // Java's MockProducer.commitTransaction() and abortTransaction() call flush() after their own checks.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetFlushException_FailsCommitAndAbort(bool async)
    {
        using Mock producer = new Mock(async);
        await producer.InitTransactions();
        producer.BeginTransaction();
        KafkaException injected = new KafkaException("flush");
        producer.SetFlushException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.CommitTransaction));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.AbortTransaction));
        Assert.True(producer.TransactionInFlight);
        Assert.Equal(0L, producer.CommitCount);

        producer.SetFlushException(null);
        await producer.CommitTransaction();
        Assert.True(producer.TransactionCommitted);
        Assert.Equal(1L, producer.CommitCount);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetPartitionsForException_ThrowsTheSameInstanceUntilCleared(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("partitionsFor");
        producer.SetPartitionsForException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.PartitionsFor));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(producer.PartitionsFor));

        producer.SetPartitionsForException(null);
        Assert.Empty(await producer.PartitionsFor());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task SetCloseException_ThrowsTheSameInstanceAndLeavesTheProducerOpen(bool async)
    {
        using Mock producer = new Mock(async);
        KafkaException injected = new KafkaException("close");
        producer.SetCloseException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => producer.CloseAsTask()));
        Assert.Same(
            injected,
            await Assert.ThrowsAsync<KafkaException>(() => producer.CloseAsTask(TimeSpan.FromSeconds(1))));
        Assert.False(producer.Closed);
        await producer.Send();
        Assert.Equal(1, producer.HistoryCount());

        producer.SetCloseException(null);
        await producer.CloseAsTask();
        Assert.True(producer.Closed);
        await producer.CloseAsTask();
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void Closed_IsFalseUntilClose(bool async)
    {
        using Mock producer = new Mock(async);
        Assert.False(producer.Closed);

        producer.Close();
        Assert.True(producer.Closed);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void Dispose_WithACloseException_ReleasesTheProducer(bool async)
    {
        Mock producer = new Mock(async);
        producer.SetCloseException(new KafkaException("close"));

        producer.Dispose();

        Assert.True(producer.Closed);
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
            () => producer.SetInitTransactionException(null),
            () => producer.SetBeginTransactionException(null),
            () => producer.SetSendOffsetsToTransactionException(null),
            () => producer.SetCommitTransactionException(null),
            () => producer.SetAbortTransactionException(null),
            () => producer.SetSendException(null),
            () => producer.SetFlushException(null),
            () => producer.SetPartitionsForException(null),
            () => producer.SetCloseException(null),
            producer.FenceProducer,
            () => _ = producer.TransactionInitialized,
            () => _ = producer.TransactionInFlight,
            () => _ = producer.TransactionCommitted,
            () => _ = producer.TransactionAborted,
            () => _ = producer.Flushed,
            () => _ = producer.SentOffsets,
            () => _ = producer.CommitCount,
            () => _ = producer.ConsumerGroupOffsetsHistory(),
            () => _ = producer.UncommittedOffsets(),
        })
        {
            IllegalStateException e = Assert.Throws<IllegalStateException>(call);
            Assert.Equal(MockClosed, e.Message);
        }
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
        Assert.Throws<IllegalStateException>(() => producer.SetCommitTransactionException(injected));
        return new WeakReference(injected);
    }

    private static ConsumerGroupMetadata GroupMetadata()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        return consumer.GroupMetadata();
    }

    // One of the two mocks, behind the sync shape; an async operation's Task is awaited.
    private sealed class Mock : IDisposable
    {
        private readonly MockProducer<byte[], byte[]>? _sync;

        private readonly AsyncMockProducer<byte[], byte[]>? _async;

        internal Mock(bool async, bool autoComplete = true)
        {
            if (async)
            {
                _async = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete);
            }
            else
            {
                _sync = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete);
            }
        }

        internal bool Closed => _sync?.Closed ?? _async!.Closed;

        internal int HistoryCount() => _sync?.HistoryCount() ?? _async!.HistoryCount();

        // The first stage only: a send's delivery is not awaited.
        internal Task Send() =>
            Run(
                () => _sync!.Send(Record()),
                async () => await _async!.Send(Record()).ConfigureAwait(false));

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

        internal void SetSendException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetSendException(exception);
            }
            else
            {
                _async!.SetSendException(exception);
            }
        }

        internal void SetFlushException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetFlushException(exception);
            }
            else
            {
                _async!.SetFlushException(exception);
            }
        }

        internal void SetPartitionsForException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetPartitionsForException(exception);
            }
            else
            {
                _async!.SetPartitionsForException(exception);
            }
        }

        internal void SetCloseException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetCloseException(exception);
            }
            else
            {
                _async!.SetCloseException(exception);
            }
        }

        internal bool TransactionInitialized => _sync?.TransactionInitialized ?? _async!.TransactionInitialized;

        internal bool TransactionInFlight => _sync?.TransactionInFlight ?? _async!.TransactionInFlight;

        internal bool TransactionCommitted => _sync?.TransactionCommitted ?? _async!.TransactionCommitted;

        internal bool TransactionAborted => _sync?.TransactionAborted ?? _async!.TransactionAborted;

        internal bool Flushed => _sync?.Flushed ?? _async!.Flushed;

        internal bool SentOffsets => _sync?.SentOffsets ?? _async!.SentOffsets;

        internal long CommitCount => _sync?.CommitCount ?? _async!.CommitCount;

        internal Task InitTransactions() => Run(() => _sync!.InitTransactions(), () => _async!.InitTransactions());

        internal void BeginTransaction()
        {
            if (_sync is not null)
            {
                _sync.BeginTransaction();
            }
            else
            {
                _async!.BeginTransaction();
            }
        }

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

        internal void SetInitTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetInitTransactionException(exception);
            }
            else
            {
                _async!.SetInitTransactionException(exception);
            }
        }

        internal void SetBeginTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetBeginTransactionException(exception);
            }
            else
            {
                _async!.SetBeginTransactionException(exception);
            }
        }

        internal void SetSendOffsetsToTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetSendOffsetsToTransactionException(exception);
            }
            else
            {
                _async!.SetSendOffsetsToTransactionException(exception);
            }
        }

        internal void SetCommitTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetCommitTransactionException(exception);
            }
            else
            {
                _async!.SetCommitTransactionException(exception);
            }
        }

        internal void SetAbortTransactionException(Exception? exception)
        {
            if (_sync is not null)
            {
                _sync.SetAbortTransactionException(exception);
            }
            else
            {
                _async!.SetAbortTransactionException(exception);
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
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"));

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
