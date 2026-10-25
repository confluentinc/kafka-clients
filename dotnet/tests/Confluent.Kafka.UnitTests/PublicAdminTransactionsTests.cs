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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// DescribeProducers, DescribeTransactions, AbortTransaction, FenceProducers and ForceTerminateTransaction on both
/// flavours: the mocks' "Not implemented yet" answer, the real clients' timeout, which futures are stored and which
/// are composed per call, and a call after <c>Dispose</c>.
/// </summary>
public sealed class PublicAdminTransactionsTests
{
    private const int RequestTimedOutCode = 7;

    private const string NotImplementedMessage = "Not implemented yet";

    private const string ProducersCall = "describeProducers(api=METADATA)";

    private const string TransactionsCall = "describeTransactions(api=FIND_COORDINATOR)";

    private const string AbortCall = "abortTransaction(api=METADATA)";

    private const string FenceCall = "fenceProducer(api=FIND_COORDINATOR)";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition[] s_partitions =
    {
        new TopicPartition("alpha", 0),
        new TopicPartition("alpha", 4),
        new TopicPartition("beta", 2),
    };

    private static readonly string[] s_transactionalIds = { "txn-b", "txn-a" };

    private static readonly AbortTransactionSpec s_spec =
        new AbortTransactionSpec(new TopicPartition("txn-topic", 3), 91_234L, 7, 42);

    [Fact]
    public void MockAdminClient_DescribeProducers_FailsEveryPartitionAndAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DescribeProducersResult result = admin.DescribeProducers(s_partitions);

        foreach (TopicPartition partition in s_partitions)
        {
            AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.PartitionResult(partition).Get(s_deadline)));
        }

        AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_DescribeTransactions_FailsEveryIdAndAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DescribeTransactionsResult result = admin.DescribeTransactions(s_transactionalIds);

        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.Description(id).Get(s_deadline)));
        }

        AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_AbortTransaction_FailsAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AbortTransactionResult result = admin.AbortTransaction(s_spec, new AbortTransactionOptions { TimeoutMs = 1000 });

        AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_FenceProducers_FailsEveryViewAndAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        FenceProducersResult result = admin.FenceProducers(s_transactionalIds, new FenceProducersOptions { TimeoutMs = 1000 });

        Assert.Equal(s_transactionalIds.Length, result.FencedProducers.Count);
        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.FencedProducers[id].Get(s_deadline)));
            AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.ProducerId(id).Get(s_deadline)));
            AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.EpochId(id).Get(s_deadline)));
        }

        AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_ForceTerminateTransaction_FailsResult()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        TerminateTransactionResult result =
            admin.ForceTerminateTransaction("txn-a", new TerminateTransactionOptions { TimeoutMs = 1000 });

        AssertNotImplemented(Assert.Throws<NotSupportedException>(() => result.Result().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncMockAdminClient_FailsEveryFutureWithTheMocksRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeProducersResult producers = admin.DescribeProducers(
            s_partitions, new DescribeProducersOptions { BrokerId = 1 });
        AsyncDescribeTransactionsResult transactions = admin.DescribeTransactions(s_transactionalIds);
        AsyncAbortTransactionResult abort = admin.AbortTransaction(s_spec);

        foreach (TopicPartition partition in s_partitions)
        {
            AssertNotImplemented(await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => producers.PartitionResult(partition).Get()), s_deadline));
        }

        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => transactions.Description(id).Get()), s_deadline));
        }

        AssertNotImplemented(await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => producers.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => transactions.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => abort.All().Get()), s_deadline));
    }

    [Fact]
    public async Task AsyncMockAdminClient_FenceAndTerminate_FailEveryFutureWithTheMocksRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncFenceProducersResult fence = admin.FenceProducers(s_transactionalIds);
        AsyncTerminateTransactionResult terminate = admin.ForceTerminateTransaction("txn-a");

        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => fence.FencedProducers[id].Get()), s_deadline));
            AssertNotImplemented(await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => fence.ProducerId(id).Get()), s_deadline));
            AssertNotImplemented(await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => fence.EpochId(id).Get()), s_deadline));
        }

        AssertNotImplemented(await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => fence.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => terminate.Result().Get()), s_deadline));
    }

    [Fact]
    public async Task PerKeyFuturesAreStored_AndAllIsComposedPerCall_OnBothFlavours()
    {
        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        DescribeProducersResult producers = sync.DescribeProducers(s_partitions);
        DescribeTransactionsResult transactions = sync.DescribeTransactions(s_transactionalIds);
        AbortTransactionResult abort = sync.AbortTransaction(s_spec);
        AsyncDescribeProducersResult asyncProducers = async.DescribeProducers(s_partitions);
        AsyncDescribeTransactionsResult asyncTransactions = async.DescribeTransactions(s_transactionalIds);
        AsyncAbortTransactionResult asyncAbort = async.AbortTransaction(s_spec);

        for (int round = 0; round < 2; round++)
        {
            Assert.True(producers.PartitionResult(s_partitions[0]) == producers.PartitionResult(s_partitions[0]));
            Assert.True(transactions.Description("txn-a") == transactions.Description("txn-a"));
            Assert.True(
                asyncProducers.PartitionResult(s_partitions[0]) == asyncProducers.PartitionResult(s_partitions[0]));
            Assert.True(asyncTransactions.Description("txn-a") == asyncTransactions.Description("txn-a"));

            Assert.False(producers.All() == producers.All());
            Assert.False(transactions.All() == transactions.All());
            Assert.False(abort.All() == abort.All());
            Assert.False(asyncProducers.All() == asyncProducers.All());
            Assert.False(asyncTransactions.All() == asyncTransactions.All());
            Assert.False(asyncAbort.All() == asyncAbort.All());

            if (round == 0)
            {
                AssertNotImplemented(Assert.Throws<NotSupportedException>(() => producers.All().Get(s_deadline)));
                AssertNotImplemented(Assert.Throws<NotSupportedException>(() => transactions.All().Get(s_deadline)));
                AssertNotImplemented(Assert.Throws<NotSupportedException>(() => abort.All().Get(s_deadline)));
                AssertNotImplemented(await TestTimeout.Run(
                    () => Assert.ThrowsAsync<NotSupportedException>(() => asyncProducers.All().Get()), s_deadline));
                AssertNotImplemented(await TestTimeout.Run(
                    () => Assert.ThrowsAsync<NotSupportedException>(() => asyncTransactions.All().Get()), s_deadline));
                AssertNotImplemented(await TestTimeout.Run(
                    () => Assert.ThrowsAsync<NotSupportedException>(() => asyncAbort.All().Get()), s_deadline));
            }
        }

        Assert.True(producers.All().IsDone);
        Assert.True(transactions.All().IsDone);
        Assert.True(abort.All().IsDone);
        Assert.True(asyncProducers.All().IsDone);
        Assert.True(asyncTransactions.All().IsDone);
        Assert.True(asyncAbort.All().IsDone);
    }

    [Fact]
    public void FenceAndTerminate_ResultIsStored_AndTheFenceViewsAreComposedPerCall_OnBothFlavours()
    {
        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        FenceProducersResult fence = sync.FenceProducers(s_transactionalIds);
        TerminateTransactionResult terminate = sync.ForceTerminateTransaction("txn-a");
        AsyncFenceProducersResult asyncFence = async.FenceProducers(s_transactionalIds);
        AsyncTerminateTransactionResult asyncTerminate = async.ForceTerminateTransaction("txn-a");

        Assert.True(terminate.Result() == terminate.Result());
        Assert.True(asyncTerminate.Result() == asyncTerminate.Result());

        Assert.NotSame(fence.FencedProducers, fence.FencedProducers);
        Assert.NotSame(asyncFence.FencedProducers, asyncFence.FencedProducers);
        Assert.False(fence.FencedProducers["txn-a"] == fence.FencedProducers["txn-a"]);
        Assert.False(asyncFence.FencedProducers["txn-a"] == asyncFence.FencedProducers["txn-a"]);
        Assert.False(fence.ProducerId("txn-a") == fence.ProducerId("txn-a"));
        Assert.False(fence.EpochId("txn-a") == fence.EpochId("txn-a"));
        Assert.False(asyncFence.ProducerId("txn-a") == asyncFence.ProducerId("txn-a"));
        Assert.False(asyncFence.EpochId("txn-a") == asyncFence.EpochId("txn-a"));
        Assert.False(fence.All() == fence.All());
        Assert.False(asyncFence.All() == asyncFence.All());
    }

    [Fact]
    public async Task RealClient_ATimeout_FailsEveryFutureWithTheTimeoutCode_OnBothFlavours()
    {
        KafkaAdminClient sync = new KafkaAdminClient(Unreachable());
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncKafkaAdminClient async = new AsyncKafkaAdminClient(Unreachable());
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        DescribeProducersResult producers =
            sync.DescribeProducers(s_partitions, new DescribeProducersOptions { TimeoutMs = 10 });
        DescribeTransactionsResult transactions =
            sync.DescribeTransactions(s_transactionalIds, new DescribeTransactionsOptions { TimeoutMs = 10 });
        AbortTransactionResult abort = sync.AbortTransaction(s_spec, new AbortTransactionOptions { TimeoutMs = 10 });
        AsyncDescribeProducersResult asyncProducers =
            async.DescribeProducers(s_partitions, new DescribeProducersOptions { TimeoutMs = 10 });
        AsyncDescribeTransactionsResult asyncTransactions =
            async.DescribeTransactions(s_transactionalIds, new DescribeTransactionsOptions { TimeoutMs = 10 });
        AsyncAbortTransactionResult asyncAbort =
            async.AbortTransaction(s_spec, new AbortTransactionOptions { TimeoutMs = 10 });

        AssertTimedOut(ProducersCall, Assert.Throws<KafkaException>(() => producers.All().Get(s_deadline)));
        AssertTimedOut(TransactionsCall, Assert.Throws<KafkaException>(() => transactions.All().Get(s_deadline)));
        AssertTimedOut(AbortCall, Assert.Throws<KafkaException>(() => abort.All().Get(s_deadline)));
        AssertTimedOut(ProducersCall, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncProducers.All().Get()), s_deadline));
        AssertTimedOut(TransactionsCall, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncTransactions.All().Get()), s_deadline));
        AssertTimedOut(AbortCall, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncAbort.All().Get()), s_deadline));
    }

    [Fact]
    public async Task RealClient_FenceAndTerminate_ATimeout_FailsEveryFutureWithTheTimeoutCode_OnBothFlavours()
    {
        KafkaAdminClient sync = new KafkaAdminClient(Unreachable());
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncKafkaAdminClient async = new AsyncKafkaAdminClient(Unreachable());
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        FenceProducersResult fence = sync.FenceProducers(s_transactionalIds, new FenceProducersOptions { TimeoutMs = 10 });
        TerminateTransactionResult terminate =
            sync.ForceTerminateTransaction("txn-a", new TerminateTransactionOptions { TimeoutMs = 10 });
        AsyncFenceProducersResult asyncFence =
            async.FenceProducers(s_transactionalIds, new FenceProducersOptions { TimeoutMs = 10 });
        AsyncTerminateTransactionResult asyncTerminate =
            async.ForceTerminateTransaction("txn-a", new TerminateTransactionOptions { TimeoutMs = 10 });

        foreach (string id in s_transactionalIds)
        {
            AssertTimedOut(FenceCall, Assert.Throws<KafkaException>(() => fence.FencedProducers[id].Get(s_deadline)));
            AssertTimedOut(FenceCall, Assert.Throws<KafkaException>(() => fence.ProducerId(id).Get(s_deadline)));
            AssertTimedOut(FenceCall, Assert.Throws<KafkaException>(() => fence.EpochId(id).Get(s_deadline)));
            AssertTimedOut(FenceCall, await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => asyncFence.FencedProducers[id].Get()), s_deadline));
        }

        AssertTimedOut(FenceCall, Assert.Throws<KafkaException>(() => fence.All().Get(s_deadline)));
        AssertTimedOut(FenceCall, Assert.Throws<KafkaException>(() => terminate.Result().Get(s_deadline)));
        AssertTimedOut(FenceCall, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncFence.All().Get()), s_deadline));
        AssertTimedOut(FenceCall, await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncTerminate.Result().Get()), s_deadline));
    }

    [Fact]
    public void AfterDispose_EveryCallThrowsObjectDisposed()
    {
        MockAdminClient mock = MockAdminClient.Create().Build();
        AsyncMockAdminClient asyncMock = AsyncMockAdminClient.Create().Build();
        KafkaAdminClient real = new KafkaAdminClient(Unreachable());
        AsyncKafkaAdminClient asyncReal = new AsyncKafkaAdminClient(Unreachable());
        TestTimeout.Run(mock.Dispose, s_deadline);
        TestTimeout.Run(asyncMock.Dispose, s_deadline);
        TestTimeout.Run(real.Dispose, s_deadline);
        TestTimeout.Run(asyncReal.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => mock.DescribeProducers(s_partitions));
        Assert.Throws<ObjectDisposedException>(() => mock.DescribeTransactions(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => mock.AbortTransaction(s_spec));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.DescribeProducers(s_partitions));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.DescribeTransactions(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.AbortTransaction(s_spec));
        Assert.Throws<ObjectDisposedException>(() => real.DescribeProducers(s_partitions));
        Assert.Throws<ObjectDisposedException>(() => real.DescribeTransactions(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => real.AbortTransaction(s_spec));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.DescribeProducers(s_partitions));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.DescribeTransactions(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.AbortTransaction(s_spec));
        Assert.Throws<ObjectDisposedException>(() => mock.FenceProducers(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => mock.ForceTerminateTransaction("txn-a"));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.FenceProducers(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.ForceTerminateTransaction("txn-a"));
        Assert.Throws<ObjectDisposedException>(() => real.FenceProducers(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => real.ForceTerminateTransaction("txn-a"));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.FenceProducers(s_transactionalIds));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.ForceTerminateTransaction("txn-a"));
    }

    private static void AssertNotImplemented(NotSupportedException exception)
    {
        Assert.Equal(NotImplementedMessage, exception.Message);
    }

    private static void AssertTimedOut(string call, KafkaException exception)
    {
        Assert.Equal(RequestTimedOutCode, exception.Code);
        Assert.True(exception.IsRetriable);
        Assert.Equal("Timed out waiting for a node assignment. Call: " + call, exception.Message);
    }

    private static Dictionary<string, string> Unreachable() =>
        new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" };
}
