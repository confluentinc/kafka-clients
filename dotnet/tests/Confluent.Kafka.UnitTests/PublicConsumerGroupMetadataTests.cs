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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="IConsumerGroupMetadata"/> is Java's four getters as an interface, and a producer's
/// <c>SendOffsetsToTransaction</c> takes an implementation of the user's own: it reads each value once, on the calling
/// thread, refuses a null group id or member id, as Java's class does, and a NUL, and gives the core what it read.
/// </summary>
public sealed class PublicConsumerGroupMetadataTests
{
    private const string NulMessage =
        "Group metadata must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private static readonly Dictionary<TopicPartition, OffsetAndMetadata> s_offsets =
        new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition("topic", 0)] = new OffsetAndMetadata(42L),
        };

    [Fact]
    public void ConsumerGroupMetadata_IsAnInterfaceOfJavasFourGetters()
    {
        Assert.True(typeof(IConsumerGroupMetadata).IsInterface);
        Assert.Equal(
            new[] { "GenerationId", "GroupId", "GroupInstanceId", "MemberId" },
            typeof(IConsumerGroupMetadata).GetMethods().Select(method => method.Name).OrderBy(name => name, StringComparer.Ordinal));
    }

    [Fact]
    public void MockProducer_CommitsTheOffsetsOfAUserImplementation_UnderItsGroupId()
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);

        producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata("user-group", 3, "member-1", "instance-1"));
        producer.CommitTransaction();

        AssertCommitted(producer.ConsumerGroupOffsetsHistory());
    }

    [Fact]
    public async Task AsyncMockProducer_CommitsTheOffsetsOfAUserImplementation_UnderItsGroupId()
    {
        AsyncMockProducer<byte[], byte[]> producer = await TransactionalAsyncMockProducer();
        try
        {
            await producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata("user-group", 3, "member-1", "instance-1"));
            await producer.CommitTransaction();

            AssertCommitted(producer.ConsumerGroupOffsetsHistory());
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }
    }

    [Fact]
    public void SendOffsetsToTransaction_ReadsEachValueOnce_OnTheCallingThread()
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);
        CountingGroupMetadata metadata = new CountingGroupMetadata();

        producer.SendOffsetsToTransaction(s_offsets, metadata);
        producer.CommitTransaction();

        Assert.Equal(
            new[] { "GenerationId", "GroupId", "GroupInstanceId", "MemberId" },
            metadata.Reads.Select(read => read.Getter).OrderBy(name => name, StringComparer.Ordinal));
        Assert.All(metadata.Reads, read => Assert.Equal(Environment.CurrentManagedThreadId, read.Thread));
    }

    [Fact]
    public void SendOffsetsToTransaction_ThrowsTheExceptionOfAUserImplementation_AndSendsNothing()
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);
        InvalidOperationException failure = new InvalidOperationException("no group");

        InvalidOperationException e = Assert.Throws<InvalidOperationException>(
            () => producer.SendOffsetsToTransaction(s_offsets, new ThrowingGroupMetadata(failure)));

        Assert.Same(failure, e);
        Assert.False(producer.SentOffsets());
    }

    [Fact]
    public async Task AsyncSendOffsetsToTransaction_ThrowsTheExceptionOfAUserImplementation_AtTheCall()
    {
        AsyncMockProducer<byte[], byte[]> producer = await TransactionalAsyncMockProducer();
        try
        {
            InvalidOperationException failure = new InvalidOperationException("no group");

            InvalidOperationException e = Assert.Throws<InvalidOperationException>(
                () => { _ = producer.SendOffsetsToTransaction(s_offsets, new ThrowingGroupMetadata(failure)); });

            Assert.Same(failure, e);
            Assert.False(producer.SentOffsets());
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }
    }

    [Theory]
    [InlineData("gro\0up", "member", "instance")]
    [InlineData("group", "mem\0ber", "instance")]
    [InlineData("group", "member", "inst\0ance")]
    public void SendOffsetsToTransaction_RefusesANulInAUserImplementation(
        string groupId, string memberId, string groupInstanceId)
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);

        ArgumentException e = Assert.Throws<ArgumentException>(
            () => producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata(groupId, 1, memberId, groupInstanceId)));

        Assert.Equal("groupMetadata", e.ParamName);
        Assert.Equal(new ArgumentException(NulMessage, "groupMetadata").Message, e.Message);
        Assert.False(producer.SentOffsets());
    }

    // Java's ConsumerGroupMetadata refuses a null group id, then a null member id, with these messages.
    [Theory]
    [InlineData(null, "member", "group.id can't be null")]
    [InlineData("group", null, "member.id can't be null")]
    [InlineData(null, null, "group.id can't be null")]
    public void SendOffsetsToTransaction_RefusesANullGroupIdOrMemberIdOfAUserImplementation(
        string? groupId, string? memberId, string message)
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata(groupId!, 1, memberId!, "instance")));

        Assert.Equal("groupMetadata", e.ParamName);
        Assert.Equal(new ArgumentNullException("groupMetadata", message).Message, e.Message);
        Assert.False(producer.SentOffsets());
    }

    [Fact]
    public async Task AsyncSendOffsetsToTransaction_RefusesANullMemberIdOfAUserImplementation_AtTheCall()
    {
        AsyncMockProducer<byte[], byte[]> producer = await TransactionalAsyncMockProducer();
        try
        {
            ArgumentNullException e = Assert.Throws<ArgumentNullException>(
                () => { _ = producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata("group", 1, null!, null)); });

            Assert.Equal("groupMetadata", e.ParamName);
            Assert.Equal(new ArgumentNullException("groupMetadata", "member.id can't be null").Message, e.Message);
            Assert.False(producer.SentOffsets());
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }
    }

    // A null group instance id is Java's Optional.empty(), which the producer passes on.
    [Fact]
    public void MockProducer_CommitsTheOffsetsOfAUserImplementation_WithNoGroupInstanceId()
    {
        MockProducer<byte[], byte[]> producer = TransactionalMockProducer();
        using IDisposable disposal = TestTimeout.Disposal(producer);

        producer.SendOffsetsToTransaction(s_offsets, new TestGroupMetadata("user-group", 3, "member-1", null));
        producer.CommitTransaction();

        AssertCommitted(producer.ConsumerGroupOffsetsHistory());
    }

    // The real producers read a user's values before the core checks the transaction state, as Java's KafkaProducer
    // reads the group metadata first: here no transaction was initialized.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task KafkaProducers_RefuseANullGroupIdOfAUserImplementation_BeforeTheTransactionChecks(bool asynchronous)
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["transactional.id"] = "txn-user-group-metadata",
            ["max.block.ms"] = 1000,
        };
        TestGroupMetadata metadata = new TestGroupMetadata(null!, 1, "member", null);
        TimeSpan deadline = TimeSpan.FromSeconds(30);
        if (!asynchronous)
        {
            KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(
                configs, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
            try
            {
                ArgumentNullException e = Assert.Throws<ArgumentNullException>(
                    () => producer.SendOffsetsToTransaction(s_offsets, metadata));
                Assert.Equal(new ArgumentNullException("groupMetadata", "group.id can't be null").Message, e.Message);
            }
            finally
            {
                TestTimeout.Run(() => producer.Close(TimeSpan.Zero), deadline);
                TestTimeout.Run(producer.Dispose, deadline);
            }

            return;
        }

        AsyncKafkaProducer<byte[], byte[]> asyncProducer = await AsyncKafkaProducer<byte[], byte[]>.Create(
            configs, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        try
        {
            ArgumentNullException e = Assert.Throws<ArgumentNullException>(
                () => { _ = asyncProducer.SendOffsetsToTransaction(s_offsets, metadata); });
            Assert.Equal(new ArgumentNullException("groupMetadata", "group.id can't be null").Message, e.Message);
        }
        finally
        {
            await TestTimeout.Run(() => asyncProducer.Close(TimeSpan.Zero), deadline);
            await TestTimeout.Run(() => asyncProducer.DisposeAsync().AsTask(), deadline);
        }
    }

    private static MockProducer<byte[], byte[]> TransactionalMockProducer()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            autoComplete: true, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        producer.InitTransactions();
        producer.BeginTransaction();
        return producer;
    }

    private static async Task<AsyncMockProducer<byte[], byte[]>> TransactionalAsyncMockProducer()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(
            autoComplete: true, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        await producer.InitTransactions();
        await producer.BeginTransaction();
        return producer;
    }

    private static void AssertCommitted(
        IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>> history)
    {
        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> commit = Assert.Single(history);
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets = commit["user-group"];
        Assert.Equal(s_offsets.ToArray(), offsets.ToArray());
    }

    /// <summary>A user's metadata that records each read, with the thread that made it.</summary>
    private sealed class CountingGroupMetadata : IConsumerGroupMetadata
    {
        internal List<(string Getter, int Thread)> Reads { get; } = new List<(string Getter, int Thread)>();

        public string GroupId() => Read(nameof(GroupId), "user-group");

        public int GenerationId()
        {
            Read(nameof(GenerationId), string.Empty);
            return 3;
        }

        public string MemberId() => Read(nameof(MemberId), "member-1");

        public string? GroupInstanceId() => Read(nameof(GroupInstanceId), "instance-1");

        private string Read(string getter, string value)
        {
            Reads.Add((getter, Environment.CurrentManagedThreadId));
            return value;
        }
    }

    /// <summary>A user's metadata whose group id cannot be read.</summary>
    private sealed class ThrowingGroupMetadata : IConsumerGroupMetadata
    {
        private readonly Exception _failure;

        internal ThrowingGroupMetadata(Exception failure) => _failure = failure;

        public string GroupId() => throw _failure;

        public int GenerationId() => 1;

        public string MemberId() => "member-1";

        public string? GroupInstanceId() => null;
    }
}
