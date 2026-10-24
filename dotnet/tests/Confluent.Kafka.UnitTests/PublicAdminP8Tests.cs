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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The six transaction and producer RPCs against the mocks, which refuse every non-empty request with
/// "Not implemented yet", as Java's mock does: the refusal, the preconditions, a negative timeout and a call after a
/// close. The C options are read back in <c>AdminTransactionsSubmitArgumentTests</c>.
/// </summary>
public sealed class PublicAdminP8Tests
{
    [Fact]
    public async Task AbortTransaction_FaultsWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("txn-topic", 3), 91_234L, 7, 42)).All().Get());

        Assert.Equal("Not implemented yet", error.Message);
    }

    [Fact]
    public void AbortTransaction_RejectsANullSpec()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.AbortTransaction(null!));
    }

    [Fact]
    public void AbortTransaction_RejectsADefaultTopicPartition()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => admin.AbortTransaction(new AbortTransactionSpec(default, 1L, 1, 1)));

        Assert.Equal("spec", error.ParamName);
    }

    [Fact]
    public void AbortTransaction_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1),
                new AbortTransactionOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void AbortTransaction_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1)));
    }

    [Fact]
    public async Task ForceTerminateTransaction_FaultsWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.ForceTerminateTransaction("txn-a").Result().Get());

        Assert.Equal("Not implemented yet", error.Message);
    }

    [Fact]
    public void ForceTerminateTransaction_RejectsANullId()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => admin.ForceTerminateTransaction(null!));

        Assert.Equal("transactionalId", error.ParamName);
    }

    [Fact]
    public void ForceTerminateTransaction_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.ForceTerminateTransaction(
                "txn-a", new TerminateTransactionOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void ForceTerminateTransaction_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.ForceTerminateTransaction("txn-a"));
    }

    [Fact]
    public async Task FenceProducers_FaultsEveryIdWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncFenceProducersResult result = admin.FenceProducers(new[] { "txn-y", "txn-x" });

        Assert.Equal(2, result.FencedProducers.Count);
        foreach (AsyncKafkaFuture fenced in result.FencedProducers.Values)
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(() => fenced.Get());
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All().Get())).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.ProducerId("txn-x").Get())).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.EpochId("txn-y").Get())).Message);
    }

    [Fact]
    public async Task FenceProducers_EmptyBatch_IsAnEmptySuccess()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncFenceProducersResult result = admin.FenceProducers(Array.Empty<string>());

        Assert.Empty(result.FencedProducers);
        await result.All().Get();
    }

    [Fact]
    public async Task FenceProducers_RepeatedId_CollapsesToOneEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncFenceProducersResult result = admin.FenceProducers(new[] { "txn-a", "txn-a" });

        Assert.Equal(new[] { "txn-a" }, result.FencedProducers.Keys);
        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
    }

    [Fact]
    public async Task FenceProducers_UnrequestedId_ThrowsJavasMessageSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncFenceProducersResult result = admin.FenceProducers(new[] { "txn-a" });

        // A statement lambda, so xUnit binds the Action overload, not the Task-returning one.
        ArgumentException producerId = Assert.Throws<ArgumentException>(
            () => { _ = result.ProducerId("txn-missing"); });
        ArgumentException epochId = Assert.Throws<ArgumentException>(
            () => { _ = result.EpochId("txn-missing"); });

        Assert.StartsWith(
            "TransactionalId `txn-missing` was not included in the request",
            producerId.Message,
            StringComparison.Ordinal);
        Assert.StartsWith(
            "TransactionalId `txn-missing` was not included in the request",
            epochId.Message,
            StringComparison.Ordinal);
        Assert.Equal("transactionalId", producerId.ParamName);

        Assert.Throws<ArgumentNullException>(() => { _ = result.ProducerId(null!); });
        Assert.Throws<ArgumentNullException>(() => { _ = result.EpochId(null!); });

        // Observe the requested id's own fault, so nothing goes unobserved.
        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
    }

    [Fact]
    public void FenceProducers_RejectsANullCollectionOrNullElement()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.FenceProducers(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.FenceProducers(new[] { "txn-a", null! }));
        Assert.Equal("transactionalIds", element.ParamName);
    }

    [Fact]
    public void FenceProducers_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.FenceProducers(
                new[] { "txn-a" }, new FenceProducersOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void FenceProducers_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.FenceProducers(new[] { "txn-a" }));
    }

    [Fact]
    public async Task DescribeTransactions_FaultsEveryIdWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeTransactionsResult result = admin.DescribeTransactions(new[] { "txn-b", "txn-a" });

        foreach (string id in new[] { "txn-a", "txn-b" })
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(
                () => result.Description(id).Get());
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All().Get())).Message);
    }

    [Fact]
    public async Task DescribeTransactions_EmptyBatch_IsAnEmptySuccess()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Empty(await admin.DescribeTransactions(Array.Empty<string>()).All().Get());
    }

    [Fact]
    public async Task DescribeTransactions_UnrequestedId_ThrowsJavasMessageSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeTransactionsResult result = admin.DescribeTransactions(new[] { "txn-a" });

        // A statement lambda, so xUnit binds the Action overload, not the Task-returning one.
        ArgumentException missing = Assert.Throws<ArgumentException>(
            () => { _ = result.Description("txn-missing"); });

        Assert.StartsWith(
            "TransactionalId `txn-missing` was not included in the request",
            missing.Message,
            StringComparison.Ordinal);
        Assert.Equal("transactionalId", missing.ParamName);

        Assert.Throws<ArgumentNullException>(() => { _ = result.Description(null!); });

        // Observe the requested id's own fault, so nothing goes unobserved.
        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
    }

    [Fact]
    public void DescribeTransactions_RejectsANullCollectionOrNullElement()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.DescribeTransactions(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.DescribeTransactions(new[] { "txn-a", null! }));
        Assert.Equal("transactionalIds", element.ParamName);
    }

    [Fact]
    public void DescribeTransactions_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.DescribeTransactions(
                new[] { "txn-a" }, new DescribeTransactionsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void DescribeTransactions_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeTransactions(new[] { "txn-a" }));
    }

    [Fact]
    public async Task DescribeProducers_FaultsEveryPartitionWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        TopicPartition[] requested =
        {
            new TopicPartition("alpha", 0),
            new TopicPartition("alpha", 4),
            new TopicPartition("beta", 2),
        };
        AsyncDescribeProducersResult result = admin.DescribeProducers(requested);

        foreach (TopicPartition partition in requested)
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(
                () => result.PartitionResult(partition).Get());
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All().Get())).Message);
    }

    [Fact]
    public async Task DescribeProducers_EmptyBatch_IsAnEmptySuccess()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Empty(await admin.DescribeProducers(Array.Empty<TopicPartition>()).All().Get());
    }

    [Fact]
    public async Task DescribeProducers_RepeatedPartition_CollapsesToOneEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeProducersResult result = admin.DescribeProducers(
            new[] { new TopicPartition("alpha", 0), new TopicPartition("alpha", 0) });

        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());

        await Assert.ThrowsAsync<KafkaException>(
            () => result.PartitionResult(new TopicPartition("alpha", 0)).Get());
        Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionResult(new TopicPartition("alpha", 1)); });
    }

    [Fact]
    public async Task DescribeProducers_UnrequestedPartition_ThrowsJavasMessageSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeProducersResult result = admin.DescribeProducers(
            new[] { new TopicPartition("alpha", 0) });

        ArgumentException missing = Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionResult(new TopicPartition("beta", 0)); });

        Assert.StartsWith(
            "Topic partition ",
            missing.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "was not included in the request",
            missing.Message,
            StringComparison.Ordinal);
        Assert.Equal("partition", missing.ParamName);

        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
    }

    [Fact]
    public void DescribeProducers_RejectsANullCollectionOrANullTopic()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.DescribeProducers(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.DescribeProducers(new[] { new TopicPartition("alpha", 0), default }));
        Assert.Equal("partitions", element.ParamName);
    }

    [Fact]
    public void DescribeProducers_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.DescribeProducers(
                new[] { new TopicPartition("alpha", 0) },
                new DescribeProducersOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public async Task DescribeProducers_NegativeBrokerId_IsAccepted()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeProducersResult result = admin.DescribeProducers(
            new[] { new TopicPartition("alpha", 0) },
            new DescribeProducersOptions { BrokerId = -1 });

        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
    }

    [Fact]
    public void DescribeProducers_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeProducers(new[] { new TopicPartition("alpha", 0) }));
    }

    [Fact]
    public async Task ListTransactions_FailsEveryViewWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncListTransactionsResult result = admin.ListTransactions();

        AssertNotImplemented(await Assert.ThrowsAsync<KafkaException>(() => result.All().Get()));
        AssertNotImplemented(await Assert.ThrowsAsync<KafkaException>(() => result.ByBrokerId().Get()));
        AssertNotImplemented(await Assert.ThrowsAsync<KafkaException>(() => result.AllByBrokerId().Get()));
    }

    [Fact]
    public async Task ListTransactions_WithEveryFilter_FailsTheWholeCall()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncListTransactionsResult result = admin.ListTransactions(
            new ListTransactionsOptions
            {
                FilteredStates = new[] { TransactionState.Ongoing, TransactionState.PrepareAbort },
                FilteredProducerIds = new[] { 11L, 22L, 33L },
                FilteredDuration = 60_000L,
                FilteredTransactionalIdPattern = "txn-.*",
            });

        AssertNotImplemented(await Assert.ThrowsAsync<KafkaException>(() => result.All().Get()));
    }

    [Fact]
    public void ListTransactions_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Null(Record.Exception(
            () => admin.ListTransactions(new ListTransactionsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public async Task ListTransactions_NegativeDuration_IsAccepted()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncListTransactionsResult result =
            admin.ListTransactions(new ListTransactionsOptions { FilteredDuration = -5L });

        AssertNotImplemented(await Assert.ThrowsAsync<KafkaException>(() => result.All().Get()));
    }

    [Fact]
    public void ListTransactions_AfterClose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.ListTransactions());
    }

    [Fact]
    public async Task ResultHandleLessRpcs_SettleBeforeDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Task abort = admin.AbortTransaction(
            new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1)).All().Get();
        Task terminate = admin.ForceTerminateTransaction("txn-a").Result().Get();

        await Assert.ThrowsAsync<KafkaException>(() => abort);
        await Assert.ThrowsAsync<KafkaException>(() => terminate);
    }

    private static void AssertNotImplemented(KafkaException exception)
    {
        Assert.Equal(35, exception.Code);
        Assert.Equal("Not implemented yet", exception.Message);
    }
}
