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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S3, the control-path half: <c>SendOffsetsToTransaction</c>'s preconditions and their
/// order (D2), on all four producers unless a test says otherwise; KPT 1944 / 1950 on the real
/// flavours; the snapshot semantics on both mocks; and the Python 1395 analogue.
/// </summary>
/// <remarks>
/// The preconditions are managed and asserted by type, <c>ParamName</c> and message (§5.1 item 2).
/// The snapshot's negative-partition branch is not reached through the public
/// <see cref="TopicPartition"/> constructor, which rejects the value first, so it is not asserted.
/// </remarks>
public sealed class PublicProducerSendOffsetsToTransactionTests
{
    private const string Topic = "txn-send-offsets-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_all = { "sync-mock", "async-mock", "sync-real", "async-real" };

    public static IEnumerable<object[]> AllFlavours()
    {
        foreach (string flavour in s_all)
        {
            yield return new object[] { flavour };
        }
    }

    // ---- D2: argument checks and their order ----

    /// <summary>
    /// D2 step 1 (Java's first check; KPT 1944 <c>testNullGroupMetadataInSendOffsets</c> on the real
    /// flavours): a null group metadata throws <see cref="ArgumentNullException"/> for
    /// <c>groupMetadata</c> with Java's message — also when the offsets are null too.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllFlavours))]
    public void NullGroupMetadata_ThrowsArgumentNull_BeforeTheOffsetsAreChecked(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        string expected = new ArgumentNullException("groupMetadata", "Consumer group metadata could not be null").Message;

        ArgumentNullException withOffsets = Assert.Throws<ArgumentNullException>(() => { _ = subject.Invoke(Offsets(42), null!); });
        ArgumentNullException withNeither = Assert.Throws<ArgumentNullException>(() => { _ = subject.Invoke(null!, null!); });

        Assert.Equal("groupMetadata", withOffsets.ParamName);
        Assert.Equal(expected, withOffsets.Message);
        Assert.Equal("groupMetadata", withNeither.ParamName);
        Assert.Equal(expected, withNeither.Message);
    }

    /// <summary>D2 step 2: null offsets throw <see cref="ArgumentNullException"/> for <c>offsets</c>.</summary>
    [Theory]
    [MemberData(nameof(AllFlavours))]
    public void NullOffsets_ThrowsArgumentNull(string flavour)
    {
        using Subject subject = Subject.Create(flavour);

        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(() => { _ = subject.Invoke(null!, Group()); });

        Assert.Equal("offsets", failure.ParamName);
        Assert.Equal(new ArgumentNullException("offsets").Message, failure.Message);
    }

    /// <summary>
    /// D2 step 2: a key with no topic (<c>default(TopicPartition)</c>) and a null value each throw
    /// <see cref="ArgumentException"/> for <c>offsets</c> with the consumer commit's messages.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllFlavours))]
    public void InvalidEntries_ThrowArgumentException(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        Dictionary<TopicPartition, OffsetAndMetadata> noTopic = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [default] = new OffsetAndMetadata(1),
        };
        Dictionary<TopicPartition, OffsetAndMetadata> noValue = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = null!,
        };

        ArgumentException topic = Assert.Throws<ArgumentException>(() => { _ = subject.Invoke(noTopic, Group()); });
        ArgumentException value = Assert.Throws<ArgumentException>(() => { _ = subject.Invoke(noValue, Group()); });

        Assert.Equal("offsets", topic.ParamName);
        Assert.Equal(new ArgumentException("Topic names must not be null.", "offsets").Message, topic.Message);
        Assert.Equal("offsets", value.ParamName);
        Assert.Equal(new ArgumentException("Offset value must not be null.", "offsets").Message, value.Message);
    }

    /// <summary>
    /// D2 step 3: a closed producer throws <see cref="ObjectDisposedException"/>, after the argument
    /// checks — a closed producer given null metadata or null offsets still reports the argument.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllFlavours))]
    public void ClosedProducer_ThrowsObjectDisposed_AfterTheArgumentChecks(string flavour)
    {
        Subject subject = Subject.Create(flavour);
        subject.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = subject.Invoke(Offsets(42), Group()); });
        Assert.Equal("groupMetadata", Assert.Throws<ArgumentNullException>(() => { _ = subject.Invoke(Offsets(42), null!); }).ParamName);
        Assert.Equal("offsets", Assert.Throws<ArgumentNullException>(() => { _ = subject.Invoke(null!, Group()); }).ParamName);
    }

    /// <summary>
    /// The async surface: an already-canceled token throws <see cref="OperationCanceledException"/>
    /// synchronously, on both async producers.
    /// </summary>
    [Theory]
    [InlineData("async-mock")]
    [InlineData("async-real")]
    public void AlreadyCanceledToken_ThrowsSynchronously(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() => { _ = subject.Async!.SendOffsetsToTransaction(Offsets(42), Group(), cts.Token); });
    }

    // ---- KPT 1950 and its mock counterpart ----

    /// <summary>
    /// KPT 1950 <c>testInvalidGenerationIdAndMemberIdCombinedInSendOffsets</c>, broker-free: a
    /// generation above zero with an unknown member id is rejected by the core — before its
    /// transaction checks, so no <c>InitTransactions</c> is needed — with the core's -3.
    /// </summary>
    [Theory]
    [InlineData("sync-real")]
    [InlineData("async-real")]
    public async Task GenerationWithoutAMemberId_IsRejectedByTheCore(string flavour)
    {
        using Subject subject = Subject.Create(flavour);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => Run(subject.Invoke(new Dictionary<TopicPartition, OffsetAndMetadata>(), GenerationWithoutMember())));

        Assert.Equal(-3, failure.Code);
        Assert.Equal(
            "Passed in group metadata GroupMetadata(groupId = group, generationId = 2, memberId = , groupInstanceId = ) "
            + "has generationId > 0 but the member.id is unknown",
            failure.Message);
    }

    /// <summary>
    /// The same metadata is accepted by both mocks inside a transaction: Java's
    /// <c>MockProducer.sendOffsetsToTransaction</c> has no generation check, and the core mock
    /// mirrors it.
    /// </summary>
    [Theory]
    [InlineData("sync-mock")]
    [InlineData("async-mock")]
    public async Task GenerationWithoutAMemberId_IsAcceptedByTheMocks(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        await Run(subject.InitTransactions());
        subject.BeginTransaction();

        await Run(subject.Invoke(Offsets(42), GenerationWithoutMember()));
        await Run(subject.CommitTransaction());

        Assert.Equal(42L, subject.CommittedOffset("group")?.Offset);
    }

    // ---- the snapshot ----

    /// <summary>
    /// The offsets are a snapshot taken by the time the call returns: mutating the caller's
    /// dictionary after that (on the async mock, before the returned <see cref="Task"/> is awaited)
    /// does not change what <c>CommittedOffset</c> reports after the commit.
    /// </summary>
    [Theory]
    [InlineData("sync-mock")]
    [InlineData("async-mock")]
    public async Task MutatingTheOffsetsAfterTheCallReturns_DoesNotChangeWhatIsCommitted(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        await Run(subject.InitTransactions());
        subject.BeginTransaction();
        await Run(subject.Send());
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = Offsets(42);

        Task sent = subject.Invoke(offsets, Group());
        offsets[new TopicPartition(Topic, 0)] = new OffsetAndMetadata(99);
        offsets[new TopicPartition(Topic, 1)] = new OffsetAndMetadata(7);
        await Run(sent);
        await Run(subject.CommitTransaction());

        Assert.Equal(42L, subject.CommittedOffset("group")?.Offset);
        Assert.Null(subject.CommittedOffset("group", partition: 1));
    }

    /// <summary>
    /// The snapshot is taken before the submit: a submit that mutates the caller's dictionary
    /// before handing its arrays to the real native call changes nothing (the async mock, through
    /// the send-offsets submit seam). The dictionary cannot be held inside the D3 drain itself
    /// without closing the core, which would fail the commit; this witnesses the later point.
    /// </summary>
    [Fact]
    public async Task MutatingTheOffsetsDuringTheSubmit_DoesNotChangeWhatIsCommitted()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await Run(producer.InitTransactions());
        producer.BeginTransaction();
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = Offsets(42);
        int submits = 0;

        await Run(Native(producer).SendOffsetsToTransactionWithCallback(
            offsets,
            Group(),
            CancellationToken.None,
            (handle, topics, partitions, values, epochs, metadata, count, groupMetadata, callback, userData) =>
            {
                submits++;
                offsets[new TopicPartition(Topic, 0)] = new OffsetAndMetadata(99);
                offsets[new TopicPartition(Topic, 1)] = new OffsetAndMetadata(7);
                NativeMethods.ProducerSendOffsetsToTransactionAsync(
                    handle, topics, partitions, values, epochs, metadata, count, groupMetadata, callback, userData);
            }));
        await Run(producer.CommitTransaction());

        Assert.Equal(1, submits);
        Assert.Equal(42L, producer.CommittedOffset("group", new TopicPartition(Topic, 0))?.Offset);
        Assert.Null(producer.CommittedOffset("group", new TopicPartition(Topic, 1)));
    }

    /// <summary>
    /// Python 1395 <c>test_group_metadata_handle_lifecycle</c>, the analogue (D12): 5000 calls in
    /// one transaction, each building and destroying a transient native group metadata, do not
    /// crash, and the commit records the last value sent.
    /// </summary>
    [Theory]
    [InlineData("sync-mock")]
    [InlineData("async-mock")]
    public async Task FiveThousandCalls_InOneTransaction_CommitTheLastValue(string flavour)
    {
        using Subject subject = Subject.Create(flavour);
        await Run(subject.InitTransactions());
        subject.BeginTransaction();

        for (long i = 0; i < 5000; i++)
        {
            await Run(subject.Invoke(Offsets(i), Group()));
        }

        await Run(subject.CommitTransaction());
        Assert.Equal(4999L, subject.CommittedOffset("group")?.Offset);
    }

    // ---- helpers ----

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(offset) };

#pragma warning disable CS0618 // the obsolete public constructors are Java's, and the ones under test
    private static ConsumerGroupMetadata Group() => new ConsumerGroupMetadata("group");

    private static ConsumerGroupMetadata GenerationWithoutMember() => new ConsumerGroupMetadata("group", 2, string.Empty, null);
#pragma warning restore CS0618

    private static Task Run(Task task) => TestTimeout.Run(() => task, s_deadline);

    private static NativeProducer Native(object producer) =>
        (NativeProducer)producer.GetType().GetField("_native", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(producer)!;

    /// <summary>
    /// One of the four producers. <see cref="Invoke"/> calls <c>SendOffsetsToTransaction</c> and
    /// lets a synchronous throw escape; a sync member's core error is reported through the
    /// returned <see cref="Task"/>, as the async members report theirs.
    /// </summary>
    private sealed class Subject : IDisposable
    {
        private readonly IProducer<byte[], byte[]>? _sync;

        private Subject(IProducer<byte[], byte[]>? sync, IAsyncProducer<byte[], byte[]>? async)
        {
            _sync = sync;
            Async = async;
        }

        internal IAsyncProducer<byte[], byte[]>? Async { get; }

        internal static Subject Create(string flavour)
        {
            Dictionary<string, string> config = new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
                ["transactional.id"] = "send-offsets-txn",
                ["max.block.ms"] = "2000",
            };
            return flavour switch
            {
                "sync-mock" => new Subject(new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray), null),
                "async-mock" => new Subject(null, new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)),
                "sync-real" => new Subject(new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray), null),
                "async-real" => new Subject(null, new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)),
                _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
            };
        }

        internal Task Invoke(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)
        {
            if (Async is not null)
            {
                return Async.SendOffsetsToTransaction(offsets, groupMetadata);
            }

            try
            {
                _sync!.SendOffsetsToTransaction(offsets, groupMetadata);
                return Task.CompletedTask;
            }
            catch (KafkaException failure)
            {
                return Task.FromException(failure);
            }
        }

        internal Task InitTransactions() => Async?.InitTransactions() ?? Inline(_sync!.InitTransactions);

        internal void BeginTransaction()
        {
            if (Async is not null)
            {
                Async.BeginTransaction();
            }
            else
            {
                _sync!.BeginTransaction();
            }
        }

        internal Task CommitTransaction() => Async?.CommitTransaction() ?? Inline(_sync!.CommitTransaction);

        internal Task Send()
        {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, new byte[] { 0x01 }, partition: 0);
            return Async?.Send(record) ?? Inline(() => _sync!.Send(record));
        }

        internal OffsetAndMetadata? CommittedOffset(string groupId, int partition = 0)
        {
            TopicPartition tp = new TopicPartition(Topic, partition);
            return Async is AsyncMockProducer<byte[], byte[]> asyncMock
                ? asyncMock.CommittedOffset(groupId, tp)
                : ((MockProducer<byte[], byte[]>)_sync!).CommittedOffset(groupId, tp);
        }

        public void Dispose()
        {
            if (Async is not null)
            {
                TestTimeout.Run(Async.Dispose, s_deadline);
            }
            else
            {
                TestTimeout.Run(_sync!.Dispose, s_deadline);
            }
        }

        private static Task Inline(Action action)
        {
            try
            {
                action();
                return Task.CompletedTask;
            }
            catch (Exception failure)
            {
                return Task.FromException(failure);
            }
        }
    }
}
