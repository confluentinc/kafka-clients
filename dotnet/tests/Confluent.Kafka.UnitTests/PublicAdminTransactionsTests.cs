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

using Confluent.Kafka.Common;
using Xunit;

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

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
            AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.PartitionResult(partition).Get(s_deadline)));
        }

        AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_DescribeTransactions_FailsEveryIdAndAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DescribeTransactionsResult result = admin.DescribeTransactions(s_transactionalIds);

        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.Description(id).Get(s_deadline)));
        }

        AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_AbortTransaction_FailsAll()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AbortTransactionResult result = admin.AbortTransaction(s_spec, new AbortTransactionOptions { TimeoutMs = 1000 });

        AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.All().Get(s_deadline)));
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
            AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.FencedProducers[id].Get(s_deadline)));
            AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.ProducerId(id).Get(s_deadline)));
            AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.EpochId(id).Get(s_deadline)));
        }

        AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void MockAdminClient_ForceTerminateTransaction_FailsResult()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        TerminateTransactionResult result =
            admin.ForceTerminateTransaction("txn-a", new TerminateTransactionOptions { TimeoutMs = 1000 });

        AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => result.Result().Get(s_deadline)));
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
                () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => producers.PartitionResult(partition).Get()), s_deadline));
        }

        foreach (string id in s_transactionalIds)
        {
            AssertNotImplemented(await TestTimeout.Run(
                () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => transactions.Description(id).Get()), s_deadline));
        }

        AssertNotImplemented(await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => producers.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => transactions.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => abort.All().Get()), s_deadline));
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
                () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => fence.FencedProducers[id].Get()), s_deadline));
            AssertNotImplemented(await TestTimeout.Run(
                () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => fence.ProducerId(id).Get()), s_deadline));
            AssertNotImplemented(await TestTimeout.Run(
                () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => fence.EpochId(id).Get()), s_deadline));
        }

        AssertNotImplemented(await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => fence.All().Get()), s_deadline));
        AssertNotImplemented(await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => terminate.Result().Get()), s_deadline));
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
                AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => producers.All().Get(s_deadline)));
                AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => transactions.All().Get(s_deadline)));
                AssertNotImplemented(FutureAssert.Throws<UnsupportedOperationException>(() => abort.All().Get(s_deadline)));
                AssertNotImplemented(await TestTimeout.Run(
                    () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => asyncProducers.All().Get()), s_deadline));
                AssertNotImplemented(await TestTimeout.Run(
                    () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => asyncTransactions.All().Get()), s_deadline));
                AssertNotImplemented(await TestTimeout.Run(
                    () => FutureAssert.ThrowsAsync<UnsupportedOperationException>(() => asyncAbort.All().Get()), s_deadline));
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

        AssertTimedOut(ProducersCall, FutureAssert.Throws<KafkaTimeoutException>(() => producers.All().Get(s_deadline)));
        AssertTimedOut(TransactionsCall, FutureAssert.Throws<KafkaTimeoutException>(() => transactions.All().Get(s_deadline)));
        AssertTimedOut(AbortCall, FutureAssert.Throws<KafkaTimeoutException>(() => abort.All().Get(s_deadline)));
        AssertTimedOut(ProducersCall, await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncProducers.All().Get()), s_deadline));
        AssertTimedOut(TransactionsCall, await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncTransactions.All().Get()), s_deadline));
        AssertTimedOut(AbortCall, await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncAbort.All().Get()), s_deadline));
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
            AssertTimedOut(FenceCall, FutureAssert.Throws<KafkaTimeoutException>(() => fence.FencedProducers[id].Get(s_deadline)));
            AssertTimedOut(FenceCall, FutureAssert.Throws<KafkaTimeoutException>(() => fence.ProducerId(id).Get(s_deadline)));
            AssertTimedOut(FenceCall, FutureAssert.Throws<KafkaTimeoutException>(() => fence.EpochId(id).Get(s_deadline)));
            AssertTimedOut(FenceCall, await TestTimeout.Run(
                () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncFence.FencedProducers[id].Get()), s_deadline));
        }

        AssertTimedOut(FenceCall, FutureAssert.Throws<KafkaTimeoutException>(() => fence.All().Get(s_deadline)));
        AssertTimedOut(FenceCall, FutureAssert.Throws<KafkaTimeoutException>(() => terminate.Result().Get(s_deadline)));
        AssertTimedOut(FenceCall, await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncFence.All().Get()), s_deadline));
        AssertTimedOut(FenceCall, await TestTimeout.Run(
            () => FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => asyncTerminate.Result().Get()), s_deadline));
    }

    [Fact]
    public void AfterDispose_EveryCallThrowsIllegalState()
    {
        MockAdminClient mock = MockAdminClient.Create().Build();
        AsyncMockAdminClient asyncMock = AsyncMockAdminClient.Create().Build();
        KafkaAdminClient real = new KafkaAdminClient(Unreachable());
        AsyncKafkaAdminClient asyncReal = new AsyncKafkaAdminClient(Unreachable());
        TestTimeout.Run(mock.Dispose, s_deadline);
        TestTimeout.Run(asyncMock.Dispose, s_deadline);
        TestTimeout.Run(real.Dispose, s_deadline);
        TestTimeout.Run(asyncReal.Dispose, s_deadline);

        Assert.Throws<IllegalStateException>(() => mock.DescribeProducers(s_partitions));
        Assert.Throws<IllegalStateException>(() => mock.DescribeTransactions(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => mock.AbortTransaction(s_spec));
        Assert.Throws<IllegalStateException>(() => asyncMock.DescribeProducers(s_partitions));
        Assert.Throws<IllegalStateException>(() => asyncMock.DescribeTransactions(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => asyncMock.AbortTransaction(s_spec));
        Assert.Throws<IllegalStateException>(() => real.DescribeProducers(s_partitions));
        Assert.Throws<IllegalStateException>(() => real.DescribeTransactions(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => real.AbortTransaction(s_spec));
        Assert.Throws<IllegalStateException>(() => asyncReal.DescribeProducers(s_partitions));
        Assert.Throws<IllegalStateException>(() => asyncReal.DescribeTransactions(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => asyncReal.AbortTransaction(s_spec));
        Assert.Throws<IllegalStateException>(() => mock.FenceProducers(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => mock.ForceTerminateTransaction("txn-a"));
        Assert.Throws<IllegalStateException>(() => asyncMock.FenceProducers(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => asyncMock.ForceTerminateTransaction("txn-a"));
        Assert.Throws<IllegalStateException>(() => real.FenceProducers(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => real.ForceTerminateTransaction("txn-a"));
        Assert.Throws<IllegalStateException>(() => asyncReal.FenceProducers(s_transactionalIds));
        Assert.Throws<IllegalStateException>(() => asyncReal.ForceTerminateTransaction("txn-a"));
    }

    private static void AssertNotImplemented(UnsupportedOperationException exception)
    {
        Assert.Equal(NotImplementedMessage, exception.Message);
    }

    private static void AssertTimedOut(string call, KafkaException exception)
    {
        Assert.Equal(RequestTimedOutCode, exception.Code);
        Assert.IsAssignableFrom<Common.Errors.RetriableException>(exception);
        Assert.Equal("Timed out waiting for a node assignment. Call: " + call, exception.Message);
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };
}
