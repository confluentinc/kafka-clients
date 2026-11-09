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
using System.Collections.Concurrent;
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>MockProducerTest</c>, on the sync <see cref="MockProducer{TKey, TValue}"/>, test for test under Java's
/// names. Where a port checks less than Java's test, or the test is skipped, the test says why.
/// </summary>
/// <remarks>
/// <para>
/// Java builds group metadata with <c>new ConsumerGroupMetadata(groupId)</c>, which .NET does not have. The ports take
/// it from a <see cref="KafkaConsumer{TKey, TValue}"/> that never connects, built with the Java test's group id:
/// before it joins, it reports the configured id (<see cref="GroupMetadata_FromANeverConnectedConsumer_IsTheConfiguredGroupId"/>).
/// </para>
/// <para>
/// The .NET mock exposes the count of its sent records, not the records (<see cref="MockProducer{TKey, TValue}.HistoryCount"/>):
/// the ports that compare Java's <c>history()</c> list compare the count.
/// </para>
/// <para>
/// Six Java tests pass <c>null</c> offsets only to reach a state check. The core reports a null map before its state
/// checks, so those ports pass an empty dictionary.
/// </para>
/// </remarks>
public sealed class PublicMockProducerTests : IDisposable
{
    private const string Topic = "topic";

    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(30);

    private const string GroupId = "group";

    private const string MockClosed = "MockProducer is already closed.";

    private const string MockFenced = "MockProducer is fenced.";

    private const string MockNotInitialized = "MockProducer hasn't been initialized for transactions.";

    private const string MockAlreadyInitialized = "MockProducer has already been initialized for transactions.";

    private const string MockNoTransaction = "There is no open transaction.";

    private const string MockTransactionStarted = "Transaction already started";

    private static readonly ConcurrentDictionary<string, ConsumerGroupMetadata> s_groupMetadata =
        new ConcurrentDictionary<string, ConsumerGroupMetadata>();

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_noOffsets =
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private readonly ProducerRecord<string, string> _record1 = new ProducerRecord<string, string>(Topic, "value1", "key1");

    private readonly ProducerRecord<string, string> _record2 = new ProducerRecord<string, string>(Topic, "value2", "key2");

    private MockProducer<string, string>? _producer;

    private MockProducer<string, string> Producer => _producer!;

    // Java's @AfterEach cleanup closes the producer; Dispose after a test's own Close is harmless.
    public void Dispose() => TestTimeout.Dispose(_producer);

    [Fact]
    public void GroupMetadata_FromANeverConnectedConsumer_IsTheConfiguredGroupId()
    {
        using KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(ConsumerConfig("g1"), Serdes.ByteArray(), Serdes.ByteArray());

        ConsumerGroupMetadata metadata = consumer.GroupMetadata();

        Assert.Equal("g1", metadata.GroupId());
        Assert.Equal("g2", GroupMetadata("g2").GroupId());
    }

    // Checks the history's count, not its records (the class remarks). The future completes on the producer's
    // completion thread, after Send returns, so the port waits for it before checking that it is done.
    [Fact]
    public void TestAutoCompleteMock()
    {
        BuildMockProducer(true);
        KafkaFuture<RecordMetadata> metadata = Producer.Send(_record1);
        Assert.False(IsError(metadata), "Send should be successful");
        Assert.True(metadata.IsDone(), "Send should be complete");
        Assert.Equal(0L, metadata.Get(s_bound).Offset());
        Assert.Equal(Topic, metadata.Get(s_bound).Topic());
        Assert.Equal(1, Producer.HistoryCount());
        Producer.Clear();
        Assert.Equal(0, Producer.HistoryCount());
    }

    [Fact(Skip = "The .NET mock takes no Cluster or Partitioner: the native mock is built with autoComplete only, and "
        + "Java's partitioner implementations are not translated (the config selects one by partitioner.type).")]
    public void TestPartitioner()
    {
    }

    [Fact]
    public void TestManualCompletion()
    {
        BuildMockProducer(false);
        KafkaFuture<RecordMetadata> md1 = Producer.Send(_record1);
        Assert.False(md1.IsDone(), "Send shouldn't have completed");
        KafkaFuture<RecordMetadata> md2 = Producer.Send(_record2);
        Assert.False(md2.IsDone(), "Send shouldn't have completed");
        Assert.True(Producer.CompleteNext(), "Complete the first request");
        Assert.False(IsError(md1), "Request should be successful");
        Assert.False(md2.IsDone(), "Second request still incomplete");
        IllegalArgumentException e = new IllegalArgumentException("blah");
        Assert.True(Producer.ErrorNext(e), "Complete the second request with an error");
        ExecutionException err = Assert.Throws<ExecutionException>(() => md2.Get(s_bound));
        Assert.Same(e, err.InnerException);
        Assert.False(Producer.CompleteNext(), "No more requests to complete");

        KafkaFuture<RecordMetadata> md3 = Producer.Send(_record1);
        KafkaFuture<RecordMetadata> md4 = Producer.Send(_record2);
        Assert.True(!md3.IsDone() && !md4.IsDone(), "Requests should not be completed.");
        Producer.Flush();
        Assert.True(md3.IsDone() && md4.IsDone(), "Requests should be completed.");
    }

    [Fact]
    public void ShouldInitTransactions()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Assert.True(Producer.TransactionInitialized());
    }

    [Fact]
    public void ShouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        AssertIllegalState(MockAlreadyInitialized, () => Producer.InitTransactions());
    }

    [Fact]
    public void ShouldThrowOnBeginTransactionIfTransactionsNotInitialized()
    {
        BuildMockProducer(true);
        AssertIllegalState(MockNotInitialized, () => Producer.BeginTransaction());
    }

    [Fact]
    public void ShouldBeginTransactions()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();
        Assert.True(Producer.TransactionInFlight());
    }

    [Fact]
    public void ShouldThrowOnBeginTransactionsIfTransactionInflight()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();
        AssertIllegalState(MockTransactionStarted, () => Producer.BeginTransaction());
    }

    // Java passes null offsets; an empty map reaches the same state check (the class remarks).
    [Fact]
    public void ShouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized()
    {
        BuildMockProducer(true);
        AssertIllegalState(
            MockNotInitialized,
            () => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    // Java passes null offsets; an empty map reaches the same state check (the class remarks).
    [Fact]
    public void ShouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        AssertIllegalState(
            MockNoTransaction,
            () => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    [Fact]
    public void ShouldThrowOnCommitIfTransactionsNotInitialized()
    {
        BuildMockProducer(true);
        AssertIllegalState(MockNotInitialized, () => Producer.CommitTransaction());
    }

    [Fact]
    public void ShouldThrowOnCommitTransactionIfNoTransactionGotStarted()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        AssertIllegalState(MockNoTransaction, () => Producer.CommitTransaction());
    }

    [Fact]
    public void ShouldCommitEmptyTransaction()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();
        Producer.CommitTransaction();
        Assert.False(Producer.TransactionInFlight());
        Assert.True(Producer.TransactionCommitted());
        Assert.False(Producer.TransactionAborted());
    }

    [Fact]
    public void ShouldCountCommittedTransaction()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        Assert.Equal(0L, Producer.CommitCount());
        Producer.CommitTransaction();
        Assert.Equal(1L, Producer.CommitCount());
    }

    [Fact]
    public void ShouldNotCountAbortedTransaction()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();

        Producer.BeginTransaction();
        Producer.AbortTransaction();

        Producer.BeginTransaction();
        Producer.CommitTransaction();
        Assert.Equal(1L, Producer.CommitCount());
    }

    [Fact]
    public void ShouldThrowOnAbortIfTransactionsNotInitialized()
    {
        BuildMockProducer(true);
        AssertIllegalState(MockNotInitialized, () => Producer.AbortTransaction());
    }

    [Fact]
    public void ShouldThrowOnAbortTransactionIfNoTransactionGotStarted()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        AssertIllegalState(MockNoTransaction, () => Producer.AbortTransaction());
    }

    [Fact]
    public void ShouldAbortEmptyTransaction()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();
        Producer.AbortTransaction();
        Assert.False(Producer.TransactionInFlight());
        Assert.True(Producer.TransactionAborted());
        Assert.False(Producer.TransactionCommitted());
    }

    [Fact]
    public void ShouldThrowFenceProducerIfTransactionsNotInitialized()
    {
        BuildMockProducer(true);
        AssertIllegalState(MockNotInitialized, () => Producer.FenceProducer());
    }

    [Fact]
    public void ShouldThrowOnBeginTransactionsIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        AssertFenced(() => Producer.BeginTransaction());
    }

    // Java sends null, which its mock never reads once fenced; .NET refuses a null record first, so this sends one.
    [Fact]
    public void ShouldThrowOnSendIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        KafkaException e = Assert.Throws<KafkaException>(() => Producer.Send(_record1));
        Assert.Equal(MockFenced, e.Message);
        ProducerFencedException cause = Assert.IsType<ProducerFencedException>(e.InnerException);
        Assert.Equal("Fenced", cause.Message);
    }

    // Java passes null offsets; an empty map reaches the same fenced check (the class remarks).
    [Fact]
    public void ShouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        AssertFenced(() => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    // Java passes null offsets; an empty map reaches the same fenced check (the class remarks).
    [Fact]
    public void ShouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        AssertFenced(() => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    [Fact]
    public void ShouldThrowOnCommitTransactionIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        AssertFenced(() => Producer.CommitTransaction());
    }

    [Fact]
    public void ShouldThrowOnAbortTransactionIfProducerGotFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        AssertFenced(() => Producer.AbortTransaction());
    }

    // Checks the history's count, not that it holds record1 and record2 in order (the class remarks).
    [Fact]
    public void ShouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        Producer.Send(_record1);
        Producer.Send(_record2);

        Assert.Equal(0, Producer.HistoryCount());

        Producer.CommitTransaction();

        Assert.Equal(2, Producer.HistoryCount());
    }

    [Fact]
    public void ShouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled()
    {
        BuildMockProducer(false);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        KafkaFuture<RecordMetadata> md1 = Producer.Send(_record1);
        KafkaFuture<RecordMetadata> md2 = Producer.Send(_record2);

        Assert.False(md1.IsDone());
        Assert.False(md2.IsDone());

        Producer.CommitTransaction();

        Assert.True(md1.IsDone());
        Assert.True(md2.IsDone());
    }

    // Checks the history's count (the class remarks).
    [Fact]
    public void ShouldDropMessagesOnAbortIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();

        Producer.BeginTransaction();
        Producer.Send(_record1);
        Producer.Send(_record2);
        Producer.AbortTransaction();
        Assert.Equal(0, Producer.HistoryCount());

        Producer.BeginTransaction();
        Producer.CommitTransaction();
        Assert.Equal(0, Producer.HistoryCount());
    }

    [Fact]
    public void ShouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled()
    {
        BuildMockProducer(false);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        KafkaFuture<RecordMetadata> md1 = Producer.Send(_record1);
        Assert.False(md1.IsDone());

        Producer.AbortTransaction();
        Assert.True(md1.IsDone());
    }

    // Checks the history's count, not that it holds record1 and record2 in order (the class remarks).
    [Fact]
    public void ShouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();

        Producer.BeginTransaction();
        Producer.Send(_record1);
        Producer.Send(_record2);
        Producer.CommitTransaction();

        Producer.BeginTransaction();
        Producer.AbortTransaction();

        Assert.Equal(2, Producer.HistoryCount());
    }

    [Fact]
    public void ShouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        string group1 = "g1";
        Dictionary<TopicPartition, OffsetAndMetadata> group1Commit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(73L),
        };
        string group2 = "g2";
        Dictionary<TopicPartition, OffsetAndMetadata> group2Commit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(101L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(21L),
        };
        Producer.SendOffsetsToTransaction(group1Commit, GroupMetadata(group1));
        Producer.SendOffsetsToTransaction(group2Commit, GroupMetadata(group2));

        Assert.Empty(Producer.ConsumerGroupOffsetsHistory());

        Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> expectedResult =
            new Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>
            {
                [group1] = group1Commit,
                [group2] = group2Commit,
            };

        Producer.CommitTransaction();
        AssertHistory(Producer.ConsumerGroupOffsetsHistory(), expectedResult);
    }

    [Fact(Skip = "Its NullPointerException comes from Java's deprecated ConsumerGroupMetadata(null) constructor, which "
        + ".NET does not have: group metadata comes only from a consumer, whose group id is never null. A null "
        + "groupMetadata is the core's IllegalArgumentException (PublicProducerTransactionTests).")]
    public void ShouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction()
    {
    }

    [Fact]
    public void ShouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();
        Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata("groupId"));
        Assert.False(Producer.SentOffsets());
    }

    [Fact]
    public void ShouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        Assert.False(Producer.SentOffsets());

        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
        };
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata("groupId"));
        Assert.True(Producer.SentOffsets());
    }

    [Fact]
    public void ShouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        Assert.False(Producer.SentOffsets());

        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
        };
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata("groupId"));
        Producer.CommitTransaction(); // commit should not reset "sentOffsets" flag
        Assert.True(Producer.SentOffsets());

        Producer.BeginTransaction();
        Assert.False(Producer.SentOffsets());

        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata("groupId"));
        Producer.CommitTransaction(); // commit should not reset "sentOffsets" flag
        Assert.True(Producer.SentOffsets());

        Producer.BeginTransaction();
        Assert.False(Producer.SentOffsets());
    }

    [Fact]
    public void ShouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        string group = "g";
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit1 = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(73L),
        };
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit2 = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(101L),
            [new TopicPartition(Topic, 2)] = new OffsetAndMetadata(21L),
        };
        Producer.SendOffsetsToTransaction(groupCommit1, GroupMetadata(group));
        Producer.SendOffsetsToTransaction(groupCommit2, GroupMetadata(group));

        Assert.Empty(Producer.ConsumerGroupOffsetsHistory());

        Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> expectedResult =
            new Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>
            {
                [group] = new Dictionary<TopicPartition, OffsetAndMetadata>
                {
                    [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
                    [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(101L),
                    [new TopicPartition(Topic, 2)] = new OffsetAndMetadata(21L),
                },
            };

        Producer.CommitTransaction();
        AssertHistory(Producer.ConsumerGroupOffsetsHistory(), expectedResult);
    }

    [Fact]
    public void ShouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        string group = "g";
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(73L),
        };
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata(group));
        Producer.AbortTransaction();

        Producer.BeginTransaction();
        Producer.CommitTransaction();
        Assert.Empty(Producer.ConsumerGroupOffsetsHistory());

        Producer.BeginTransaction();
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata(group));
        Producer.AbortTransaction();

        Producer.BeginTransaction();
        Producer.CommitTransaction();
        Assert.Empty(Producer.ConsumerGroupOffsetsHistory());
    }

    [Fact]
    public void ShouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        string group = "g";
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(73L),
        };
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata(group));
        Producer.CommitTransaction();

        Producer.BeginTransaction();
        Producer.AbortTransaction();

        Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> expectedResult =
            new Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> { [group] = groupCommit };

        AssertHistory(Producer.ConsumerGroupOffsetsHistory(), expectedResult);
    }

    [Fact]
    public void ShouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.BeginTransaction();

        string group = "g";
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(42L),
            [new TopicPartition(Topic, 1)] = new OffsetAndMetadata(73L),
        };
        Producer.SendOffsetsToTransaction(groupCommit, GroupMetadata(group));
        Producer.CommitTransaction();

        Producer.BeginTransaction();

        string group2 = "g2";
        Dictionary<TopicPartition, OffsetAndMetadata> groupCommit2 = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 2)] = new OffsetAndMetadata(53L),
            [new TopicPartition(Topic, 3)] = new OffsetAndMetadata(84L),
        };
        Producer.SendOffsetsToTransaction(groupCommit2, GroupMetadata(group2));
        Producer.AbortTransaction();

        Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> expectedResult =
            new Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> { [group] = groupCommit };

        AssertHistory(Producer.ConsumerGroupOffsetsHistory(), expectedResult);
    }

    [Fact]
    public void ShouldThrowOnInitTransactionIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.InitTransactions());
    }

    // Java sends null, which its mock never reads once closed; .NET refuses a null record first, so this sends one.
    [Fact]
    public void ShouldThrowOnSendIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.Send(_record1));
    }

    [Fact]
    public void ShouldThrowOnBeginTransactionIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.BeginTransaction());
    }

    // Java passes null offsets; an empty map reaches the same closed check (the class remarks).
    [Fact]
    public void ShouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    // Java passes null offsets; an empty map reaches the same closed check (the class remarks).
    [Fact]
    public void ShouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.SendOffsetsToTransaction(s_noOffsets, GroupMetadata(GroupId)));
    }

    [Fact]
    public void ShouldThrowOnCommitTransactionIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.CommitTransaction());
    }

    [Fact]
    public void ShouldThrowOnAbortTransactionIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.AbortTransaction());
    }

    [Fact]
    public void ShouldThrowOnFenceProducerIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.FenceProducer());
    }

    [Fact]
    public void ShouldThrowOnFlushProducerIfProducerIsClosed()
    {
        BuildMockProducer(true);
        Producer.Close();
        AssertIllegalState(MockClosed, () => Producer.Flush());
    }

    [Fact]
    public void ShouldNotThrowOnFlushProducerIfProducerIsFenced()
    {
        BuildMockProducer(true);
        Producer.InitTransactions();
        Producer.FenceProducer();
        Producer.Flush();
    }

    // Java's raw ProducerRecord hands a String key to an IntegerSerializer; here an object key reaches a serializer
    // that casts it to int. The mock rethrows the serializer's InvalidCastException unchanged, as Java's mock does.
    [Fact]
    public void ShouldThrowClassCastException()
    {
        using MockProducer<object, string> customProducer =
            new MockProducer<object, string>(new IntegerKeySerializer(), Serdes.String());
        Assert.Throws<InvalidCastException>(
            () => customProducer.Send(new ProducerRecord<object, string>(Topic, "value1", "key1")));
    }

    [Fact]
    public void ShouldBeFlushedIfNoBufferedRecords()
    {
        BuildMockProducer(true);
        Assert.True(Producer.Flushed());
    }

    [Fact]
    public void ShouldBeFlushedWithAutoCompleteIfBufferedRecords()
    {
        BuildMockProducer(true);
        Producer.Send(_record1);
        Assert.True(Producer.Flushed());
    }

    [Fact]
    public void ShouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords()
    {
        BuildMockProducer(false);
        Producer.Send(_record1);
        Assert.False(Producer.Flushed());
    }

    [Fact]
    public void ShouldNotBeFlushedAfterFlush()
    {
        BuildMockProducer(false);
        Producer.Send(_record1);
        Producer.Flush();
        Assert.True(Producer.Flushed());
    }

    [Fact]
    public void TestMetadataOnException()
    {
        BuildMockProducer(false);
        RecordingCallback callback = new RecordingCallback();
        KafkaFuture<RecordMetadata> metadata = Producer.Send(_record2, callback);
        IllegalArgumentException e = new IllegalArgumentException("dummy exception");
        Assert.True(Producer.ErrorNext(e), "Complete the second request with an error");
        ExecutionException err = Assert.Throws<ExecutionException>(() => metadata.Get(s_bound));
        Assert.Same(e, err.InnerException);

        // Get returns after the callback has run, so its metadata is already recorded.
        RecordMetadata md = Assert.IsType<RecordMetadata>(callback.Metadata);
        Assert.Equal(-1L, md.Offset());
        Assert.Equal(-1L, md.Timestamp());
        Assert.Equal(-1L, md.SerializedKeySize());
        Assert.Equal(-1L, md.SerializedValueSize());
        Assert.Same(e, callback.Exception);
    }

    private static Dictionary<string, object?> ConsumerConfig(string groupId) => new Dictionary<string, object?>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    // The metadata outlives the consumer that gave it, so each group id needs one consumer for the whole class.
    private static ConsumerGroupMetadata GroupMetadata(string groupId) =>
        s_groupMetadata.GetOrAdd(groupId, id =>
        {
            using KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<byte[], byte[]>(ConsumerConfig(id), Serdes.ByteArray(), Serdes.ByteArray());
            return consumer.GroupMetadata();
        });

    private static void AssertIllegalState(string message, Action action)
    {
        IllegalStateException e = Assert.Throws<IllegalStateException>(() => action());
        Assert.Equal(message, e.Message);
    }

    private static void AssertFenced(Action action)
    {
        ProducerFencedException e = Assert.Throws<ProducerFencedException>(() => action());
        Assert.Equal(MockFenced, e.Message);
    }

    private static void AssertHistory(
        IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>> actual,
        Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>> expected)
    {
        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> commit = Assert.Single(actual);
        Assert.Equal(expected.Count, commit.Count);
        foreach (KeyValuePair<string, Dictionary<TopicPartition, OffsetAndMetadata>> group in expected)
        {
            Assert.True(commit.TryGetValue(group.Key, out IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets));
            Assert.Equal(
                (IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>>)group.Value,
                offsets!,
                KeyValueComparer.Instance);
        }
    }

    private static bool IsError(KafkaFuture<RecordMetadata> future)
    {
        try
        {
            future.Get(s_bound);
            return false;
        }
        catch (ExecutionException)
        {
            return true;
        }
    }

    private void BuildMockProducer(bool autoComplete) =>
        _producer = new MockProducer<string, string>(Serdes.String(), Serdes.String(), autoComplete);

    private sealed class IntegerKeySerializer : ISerializer<object>
    {
        public byte[]? Serialize(string topic, object data) => Serdes.Integer().Serialize(topic, (int)data);
    }

    private sealed class RecordingCallback : IDeliveryCallback
    {
        internal RecordMetadata? Metadata { get; private set; }

        internal Exception? Exception { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Metadata = metadata;
            Exception = exception;
        }
    }

    // Compares two maps as sets of entries, whatever their iteration order.
    private sealed class KeyValueComparer : IEqualityComparer<IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>>>
    {
        internal static readonly KeyValueComparer Instance = new KeyValueComparer();

        public bool Equals(
            IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>>? x,
            IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>>? y)
        {
            Dictionary<TopicPartition, OffsetAndMetadata> left = ToDictionary(x!);
            Dictionary<TopicPartition, OffsetAndMetadata> right = ToDictionary(y!);
            if (left.Count != right.Count)
            {
                return false;
            }

            foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in left)
            {
                if (!right.TryGetValue(entry.Key, out OffsetAndMetadata? other) || !entry.Value.Equals(other))
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode(IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>> obj) => 0;

        private static Dictionary<TopicPartition, OffsetAndMetadata> ToDictionary(
            IEnumerable<KeyValuePair<TopicPartition, OffsetAndMetadata>> entries)
        {
            Dictionary<TopicPartition, OffsetAndMetadata> result = new Dictionary<TopicPartition, OffsetAndMetadata>();
            foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in entries)
            {
                result[entry.Key] = entry.Value;
            }

            return result;
        }
    }
}
