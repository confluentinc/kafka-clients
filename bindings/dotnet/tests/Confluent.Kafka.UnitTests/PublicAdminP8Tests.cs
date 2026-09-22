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
/// The M15/P8 RPCs over the real mock.
/// </summary>
/// <remarks>
/// ⚠⚠ <b>None of the six has a happy path here, and that is faithful.</b> Java's own
/// <c>MockAdminClient</c> throws <c>UnsupportedOperationException("Not implemented yet")</c>
/// for all of them and the core mirrors it, so the marshalling is asserted at the submit seam
/// (<c>AdminP8SubmitArgumentTests</c>) and the value readers over injected accessors
/// (<c>AdminP8ResultMarshalTests</c>). The assertions below are ported from the C suite's
/// coverage of this same RPC set (<c>bindings/c/tests/test_mock_admin.c:5858-6046</c>).
/// </remarks>
public sealed class PublicAdminP8Tests
{
    // ---- abortTransaction ---------------------------------------------------------------

    /// <summary>
    /// The abort faults with the <b>exact</b> message Java's mock throws; the code alone does
    /// not distinguish it from any other unsupported operation.
    /// </summary>
    [Fact]
    public async Task AbortTransaction_FaultsWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("txn-topic", 3), 91_234L, 7, 42)).All());

        Assert.Equal("Not implemented yet", error.Message);
    }

    [Fact]
    public void AbortTransaction_RejectsANullSpec()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentNullException>(() => admin.AbortTransaction(null!));
    }

    /// <summary>
    /// A <c>default(TopicPartition)</c> carries a null topic, which the ABI rejects with
    /// "abort transaction topic must not be null". The binding rejects it first, before any
    /// pin, as a precondition (ffi §A5).
    /// </summary>
    [Fact]
    public void AbortTransaction_RejectsADefaultTopicPartition()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => admin.AbortTransaction(new AbortTransactionSpec(default, 1L, 1, 1)));

        Assert.Equal("spec", error.ParamName);
    }

    [Fact]
    public void AbortTransaction_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1),
                new AbortTransactionOptions { TimeoutMs = -1 }));
    }

    [Fact]
    public void AbortTransaction_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => admin.AbortTransaction(
                new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1)));
    }

    // ---- forceTerminateTransaction -------------------------------------------------------

    [Fact]
    public async Task ForceTerminateTransaction_FaultsWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.ForceTerminateTransaction("txn-a").Result());

        Assert.Equal("Not implemented yet", error.Message);
    }

    /// <summary>
    /// The ABI rejects a NULL transactional id with "transactional id must not be null"; the
    /// binding rejects it first, before any pin.
    /// </summary>
    [Fact]
    public void ForceTerminateTransaction_RejectsANullId()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => admin.ForceTerminateTransaction(null!));

        Assert.Equal("transactionalId", error.ParamName);
    }

    [Fact]
    public void ForceTerminateTransaction_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ForceTerminateTransaction(
                "txn-a", new TerminateTransactionOptions { TimeoutMs = -1 }));
    }

    [Fact]
    public void ForceTerminateTransaction_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ForceTerminateTransaction("txn-a"));
    }

    // ---- fenceProducers -------------------------------------------------------------------

    /// <summary>
    /// Every requested id gets its own awaiter, and each faults with the exact message Java's
    /// mock throws. Ported from <c>test_mock_admin.c:5951-5969</c>.
    /// </summary>
    [Fact]
    public async Task FenceProducers_FaultsEveryIdWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        FenceProducersResult result = admin.FenceProducers(new[] { "txn-y", "txn-x" });

        Assert.Equal(2, result.FencedProducers.Count);
        foreach (Task fenced in result.FencedProducers.Values)
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(() => fenced);
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All())).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.ProducerId("txn-x"))).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.EpochId("txn-y"))).Message);
    }

    /// <summary>
    /// ⚠ An empty batch has nothing to join, so it is an empty <b>success</b>, not an error —
    /// Java has no empty guard (<c>KafkaAdminClient.java:4889-4895</c>) and neither does this
    /// (<c>test_mock_admin.c:5972-5977</c>).
    /// </summary>
    [Fact]
    public async Task FenceProducers_EmptyBatch_IsAnEmptySuccess()
    {
        using MockAdminClient admin = new MockAdminClient();

        FenceProducersResult result = admin.FenceProducers(Array.Empty<string>());

        Assert.Empty(result.FencedProducers);
        await result.All();
    }

    /// <summary>A repeated id collapses to one awaiter, as Java's map-keyed result does.</summary>
    [Fact]
    public async Task FenceProducers_RepeatedId_CollapsesToOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient();

        FenceProducersResult result = admin.FenceProducers(new[] { "txn-a", "txn-a" });

        Assert.Equal(new[] { "txn-a" }, result.FencedProducers.Keys);
        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    /// <summary>
    /// An id that was not requested throws <b>synchronously</b>, with Java's own wording
    /// (<c>FenceProducersResult.java:74-77</c>) — a usage error, not a task outcome.
    /// </summary>
    [Fact]
    public async Task FenceProducers_UnrequestedId_ThrowsJavasMessageSynchronously()
    {
        using MockAdminClient admin = new MockAdminClient();

        FenceProducersResult result = admin.FenceProducers(new[] { "txn-a" });

        // A statement lambda, so xUnit binds the Action overload rather than the
        // Task-returning one these methods' return type would otherwise select.
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

        // Observe the requested id's own fault, so nothing is left unobserved.
        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void FenceProducers_RejectsANullCollectionOrNullElement()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentNullException>(() => admin.FenceProducers(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.FenceProducers(new[] { "txn-a", null! }));
        Assert.Equal("transactionalIds", element.ParamName);
    }

    [Fact]
    public void FenceProducers_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.FenceProducers(
                new[] { "txn-a" }, new FenceProducersOptions { TimeoutMs = -1 }));
    }

    [Fact]
    public void FenceProducers_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.FenceProducers(new[] { "txn-a" }));
    }

    // ---- describeTransactions -------------------------------------------------------------

    /// <summary>
    /// Every requested id gets its own awaiter, each faulting with the exact message Java's
    /// mock throws. Ported from <c>test_mock_admin.c:5913-5947</c>.
    /// </summary>
    [Fact]
    public async Task DescribeTransactions_FaultsEveryIdWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeTransactionsResult result = admin.DescribeTransactions(new[] { "txn-b", "txn-a" });

        foreach (string id in new[] { "txn-a", "txn-b" })
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(
                () => result.Description(id));
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All())).Message);
    }

    /// <summary>An empty batch has nothing to join, so it is an empty success.</summary>
    [Fact]
    public async Task DescribeTransactions_EmptyBatch_IsAnEmptySuccess()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Empty(await admin.DescribeTransactions(Array.Empty<string>()).All());
    }

    /// <summary>
    /// An id that was not requested throws <b>synchronously</b>, with Java's own wording
    /// (<c>DescribeTransactionsResult.java:47-48</c>).
    /// </summary>
    [Fact]
    public async Task DescribeTransactions_UnrequestedId_ThrowsJavasMessageSynchronously()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeTransactionsResult result = admin.DescribeTransactions(new[] { "txn-a" });

        // A statement lambda, so xUnit binds the Action overload rather than the
        // Task-returning one this method's return type would otherwise select.
        ArgumentException missing = Assert.Throws<ArgumentException>(
            () => { _ = result.Description("txn-missing"); });

        Assert.StartsWith(
            "TransactionalId `txn-missing` was not included in the request",
            missing.Message,
            StringComparison.Ordinal);
        Assert.Equal("transactionalId", missing.ParamName);

        Assert.Throws<ArgumentNullException>(() => { _ = result.Description(null!); });

        // Observe the requested id's own fault, so nothing is left unobserved.
        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void DescribeTransactions_RejectsANullCollectionOrNullElement()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentNullException>(() => admin.DescribeTransactions(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.DescribeTransactions(new[] { "txn-a", null! }));
        Assert.Equal("transactionalIds", element.ParamName);
    }

    [Fact]
    public void DescribeTransactions_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeTransactions(
                new[] { "txn-a" }, new DescribeTransactionsOptions { TimeoutMs = -1 }));
    }

    [Fact]
    public void DescribeTransactions_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeTransactions(new[] { "txn-a" }));
    }

    // ---- describeProducers -----------------------------------------------------------------

    /// <summary>
    /// Every requested partition gets its own awaiter, each faulting with the exact message
    /// Java's mock throws. Ported from <c>test_mock_admin.c:5858-5900</c>.
    /// </summary>
    [Fact]
    public async Task DescribeProducers_FaultsEveryPartitionWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        TopicPartition[] requested =
        {
            new TopicPartition("alpha", 0),
            new TopicPartition("alpha", 4),
            new TopicPartition("beta", 2),
        };
        DescribeProducersResult result = admin.DescribeProducers(requested);

        foreach (TopicPartition partition in requested)
        {
            KafkaException error = await Assert.ThrowsAsync<KafkaException>(
                () => result.PartitionResult(partition));
            Assert.Equal("Not implemented yet", error.Message);
        }

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All())).Message);
    }

    [Fact]
    public async Task DescribeProducers_EmptyBatch_IsAnEmptySuccess()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Empty(await admin.DescribeProducers(Array.Empty<TopicPartition>()).All());
    }

    /// <summary>A repeated partition collapses to one awaiter, as Java's map-keyed result does.</summary>
    [Fact]
    public async Task DescribeProducers_RepeatedPartition_CollapsesToOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeProducersResult result = admin.DescribeProducers(
            new[] { new TopicPartition("alpha", 0), new TopicPartition("alpha", 0) });

        await Assert.ThrowsAsync<KafkaException>(() => result.All());

        // The collapsed key is still addressable, and the partition that was never requested
        // is not — so the collapse did not widen the key set.
        await Assert.ThrowsAsync<KafkaException>(
            () => result.PartitionResult(new TopicPartition("alpha", 0)));
        Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionResult(new TopicPartition("alpha", 1)); });
    }

    /// <summary>
    /// A partition that was not requested throws <b>synchronously</b>, with Java's own wording
    /// (<c>DescribeProducersResult.java:39-40</c>).
    /// </summary>
    [Fact]
    public async Task DescribeProducers_UnrequestedPartition_ThrowsJavasMessageSynchronously()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeProducersResult result = admin.DescribeProducers(
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

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void DescribeProducers_RejectsANullCollectionOrANullTopic()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentNullException>(() => admin.DescribeProducers(null!));

        ArgumentException element = Assert.Throws<ArgumentException>(
            () => admin.DescribeProducers(new[] { new TopicPartition("alpha", 0), default }));
        Assert.Equal("partitions", element.ParamName);
    }

    [Fact]
    public void DescribeProducers_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeProducers(
                new[] { new TopicPartition("alpha", 0) },
                new DescribeProducersOptions { TimeoutMs = -1 }));
    }

    /// <summary>
    /// ⚠ A <b>negative broker id</b> is not rejected: Java's <c>brokerId(int)</c> accepts any
    /// <c>int</c>, so the timeout's guard deliberately does not extend to it.
    /// </summary>
    [Fact]
    public async Task DescribeProducers_NegativeBrokerId_IsAccepted()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeProducersResult result = admin.DescribeProducers(
            new[] { new TopicPartition("alpha", 0) },
            new DescribeProducersOptions { BrokerId = -1 });

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void DescribeProducers_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeProducers(new[] { new TopicPartition("alpha", 0) }));
    }

    // ---- listTransactions -------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>This is the one P8 RPC whose mock failure is the <em>call's</em>, not a row's.</b>
    /// Java's mock fails the top-level broker-discovery future, so all three views fault
    /// (<c>test_mock_admin.c:5982-6005</c>) — including <see cref="ListTransactionsResult.ByBrokerId"/>,
    /// which survives a <em>per-broker</em> failure but not this one.
    /// </summary>
    [Fact]
    public async Task ListTransactions_FailsEveryViewWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListTransactionsResult result = admin.ListTransactions();

        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.All())).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.ByBrokerId())).Message);
        Assert.Equal(
            "Not implemented yet",
            (await Assert.ThrowsAsync<KafkaException>(() => result.AllByBrokerId())).Message);
    }

    /// <summary>Every filter set at once still reaches the same top-level failure.</summary>
    [Fact]
    public async Task ListTransactions_WithEveryFilter_FailsTheWholeCall()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListTransactionsResult result = admin.ListTransactions(
            new ListTransactionsOptions
            {
                FilteredStates = new[] { TransactionState.Ongoing, TransactionState.PrepareAbort },
                FilteredProducerIds = new[] { 11L, 22L, 33L },
                FilteredDuration = 60_000L,
                FilteredTransactionalIdPattern = "txn-.*",
            });

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void ListTransactions_RejectsANegativeTimeout()
    {
        using MockAdminClient admin = new MockAdminClient();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListTransactions(new ListTransactionsOptions { TimeoutMs = -1 }));
    }

    /// <summary>
    /// ⚠ A negative <c>FilteredDuration</c> is <b>not</b> rejected — it is Java's own "no
    /// duration filter" default, unlike the timeout beside it.
    /// </summary>
    [Fact]
    public async Task ListTransactions_NegativeDuration_IsAccepted()
    {
        using MockAdminClient admin = new MockAdminClient();

        ListTransactionsResult result =
            admin.ListTransactions(new ListTransactionsOptions { FilteredDuration = -5L });

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
    }

    [Fact]
    public void ListTransactions_AfterClose_Throws()
    {
        MockAdminClient admin = new MockAdminClient();
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.ListTransactions());
    }

    /// <summary>
    /// Both RPCs settle their awaiter on the real mock, so neither leaks an operation that
    /// would defer the client's native destroy past <c>Dispose</c>.
    /// </summary>
    [Fact]
    public async Task ResultHandleLessRpcs_SettleBeforeDispose()
    {
        using MockAdminClient admin = new MockAdminClient();

        Task abort = admin.AbortTransaction(
            new AbortTransactionSpec(new TopicPartition("t", 0), 1L, 1, 1)).All();
        Task terminate = admin.ForceTerminateTransaction("txn-a").Result();

        await Assert.ThrowsAsync<KafkaException>(() => abort);
        await Assert.ThrowsAsync<KafkaException>(() => terminate);
    }
}
