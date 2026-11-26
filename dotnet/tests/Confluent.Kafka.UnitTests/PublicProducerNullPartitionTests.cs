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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A <see langword="null"/> <see cref="TopicPartition"/> key in <c>SendOffsetsToTransaction</c>'s offsets, on the four
/// producers. Java's <c>KafkaProducer</c> dereferences it after its group-metadata, transaction-manager and closed
/// checks, Java's <c>MockProducer</c> stores it after its state checks and its injected exception: each check comes
/// first, then an <see cref="ArgumentNullException"/>, thrown by the sync producers and faulting the asynchronous
/// producers' <see cref="Task"/>, as those checks do. Nothing is sent. A topic or a metadata string holding a NUL
/// character, which the native client would cut, is answered the same way, with an <see cref="ArgumentException"/>.
/// </summary>
public sealed class PublicProducerNullPartitionTests
{
    private const string NoTransactionManager =
        "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration " +
        "property";

    private const string NullGroupMetadata = "Consumer group metadata could not be null";

    private const string ProducerClosed = "Cannot perform operation after producer has been closed";

    private const string MockNotInitialized = "MockProducer hasn't been initialized for transactions.";

    private const string MockNoTransaction = "There is no open transaction.";

    private const string MockFenced = "MockProducer is fenced.";

    private const string MockClosed = "MockProducer is already closed.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition("np", 0);

    private const string NulTopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private const string NulMetadataMessage =
        "Offset metadata must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulTopic =
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("n\0p", 0)] = new OffsetAndMetadata(5) };

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulMetadata =
        new Dictionary<TopicPartition, OffsetAndMetadata> { [new TopicPartition("np", 0)] = new OffsetAndMetadata(5, "m\0x") };

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nulThenNullKey =
        new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(new TopicPartition("n\0p", 0), new OffsetAndMetadata(5)),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nullKey =
        new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(s_tp0, new OffsetAndMetadata(5)),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

    public static IEnumerable<object[]> RealProducers() => new[]
    {
        new object[] { "KafkaProducer" },
        new object[] { "AsyncKafkaProducer" },
    };

    public static IEnumerable<object[]> Mocks() => new[]
    {
        new object[] { "MockProducer" },
        new object[] { "AsyncMockProducer" },
    };

    // Java's KafkaProducer has a transaction manager here, so only TopicPartition::topic finds the null key; the state
    // of the transaction is checked after it.
    [Theory]
    [MemberData(nameof(RealProducers))]
    public async Task OnTheRealProducers_ANullKey_IsArgumentNullException_AfterJavasChecks(string kind)
    {
        foreach (string? transactionalId in new[] { "null-partition-tx", null })
        {
            object producer = await Real(kind, transactionalId, idempotence: true);
            try
            {
                (Exception? error, bool atTheCall) = await Outcome(producer, s_nullKey, GroupMetadata());
                AssertMustNotContainNull(error);
                Assert.Equal(producer is IProducer<byte[], byte[]>, atTheCall);
            }
            finally
            {
                await Close(producer);
            }
        }
    }

    // Java's throwIfNoTransactionManager and its group-metadata check come before the null key is read; the closed
    // check is the binding's first (the producer's handle is gone).
    [Theory]
    [MemberData(nameof(RealProducers))]
    public async Task OnTheRealProducers_ANullKey_ComesAfterTheGroupMetadataTransactionManagerAndClosedChecks(string kind)
    {
        object noTransactions = await Real(kind, transactionalId: null, idempotence: false);
        try
        {
            (Exception? error, bool _) = await Outcome(noTransactions, s_nullKey, GroupMetadata());
            Assert.Equal(NoTransactionManager, Assert.IsType<IllegalStateException>(error).Message);

            (Exception? nullMetadata, bool _) = await Outcome(noTransactions, s_nullKey, null);
            Assert.Equal(NullGroupMetadata, Assert.IsType<IllegalArgumentException>(nullMetadata).Message);
        }
        finally
        {
            await Close(noTransactions);
        }

        object closed = await Real(kind, "null-partition-tx", idempotence: true);
        await Close(closed);
        (Exception? afterClose, bool atTheCall) = await Outcome(closed, s_nullKey, GroupMetadata());
        Assert.Equal(ProducerClosed, Assert.IsType<IllegalStateException>(afterClose).Message);
        Assert.True(atTheCall);
    }

    // Java's MockProducer stores the null key with putAll, which a native map cannot hold: refused, after the
    // checks Java's mock makes first, and nothing is recorded.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_ANullKey_IsArgumentNullException_InATransaction_AndNothingIsSent(string kind)
    {
        object producer = Mock(kind);
        try
        {
            await Run(producer, p => p.InitTransactions(), p => p.InitTransactions());
            await Run(producer, p => p.BeginTransaction(), p => p.BeginTransaction());

            (Exception? error, bool atTheCall) = await Outcome(producer, s_nullKey, GroupMetadata());
            AssertMustNotContainNull(error);
            Assert.Equal(producer is IProducer<byte[], byte[]>, atTheCall);
            Assert.False(SentOffsets(producer));

            await Run(producer, p => p.CommitTransaction(), p => p.CommitTransaction());
            Assert.Empty(History(producer));
        }
        finally
        {
            await Close(producer);
        }
    }

    // Java's MockProducer#sendOffsetsToTransaction makes its state checks and throws its injected exception before
    // it reads the map.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_ANullKey_ComesAfterJavasStateChecks_AndTheInjectedException(string kind)
    {
        object producer = Mock(kind);
        try
        {
            (Exception? notInitialized, bool _) = await Outcome(producer, s_nullKey, GroupMetadata());
            Assert.Equal(MockNotInitialized, Assert.IsType<IllegalStateException>(notInitialized).Message);

            await Run(producer, p => p.InitTransactions(), p => p.InitTransactions());
            (Exception? noTransaction, bool _) = await Outcome(producer, s_nullKey, GroupMetadata());
            Assert.Equal(MockNoTransaction, Assert.IsType<IllegalStateException>(noTransaction).Message);

            await Run(producer, p => p.BeginTransaction(), p => p.BeginTransaction());
            KafkaException injected = new KafkaException("injected");
            SetSendOffsetsException(producer, injected);
            (Exception? thrown, bool _) = await Outcome(producer, s_nullKey, GroupMetadata());
            Assert.Same(injected, thrown);

            SetSendOffsetsException(producer, null);
            Fence(producer);
            (Exception? fenced, bool _) = await Outcome(producer, s_nullKey, GroupMetadata());
            Assert.Equal(MockFenced, Assert.IsType<ProducerFencedException>(fenced).Message);
        }
        finally
        {
            await Close(producer);
        }

        (Exception? closed, bool atTheCall) = await Outcome(producer, s_nullKey, GroupMetadata());
        Assert.Equal(MockClosed, Assert.IsType<IllegalStateException>(closed).Message);
        Assert.True(atTheCall);
    }

    // The real producers send an empty map in place of offsets the native client cannot carry, so they return at once
    // without a transaction state check, then refuse the NUL; a null key anywhere in the map still wins.
    [Theory]
    [MemberData(nameof(RealProducers))]
    public async Task OnTheRealProducers_ANulTopicOrMetadata_IsArgumentException_AfterJavasChecks(string kind)
    {
        object producer = await Real(kind, "nul-string-tx", idempotence: true);
        try
        {
            foreach ((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, string message) in Nuls())
            {
                (Exception? error, bool atTheCall) = await Outcome(producer, offsets, GroupMetadata());
                AssertNul(error, message);
                Assert.Equal(producer is IProducer<byte[], byte[]>, atTheCall);
            }

            (Exception? nullKey, bool _) = await Outcome(producer, s_nulThenNullKey, GroupMetadata());
            AssertMustNotContainNull(nullKey);
        }
        finally
        {
            await Close(producer);
        }
    }

    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_ANulTopicOrMetadata_IsArgumentException_InATransaction_AndNothingIsSent(string kind)
    {
        object producer = Mock(kind);
        try
        {
            await Run(producer, p => p.InitTransactions(), p => p.InitTransactions());
            await Run(producer, p => p.BeginTransaction(), p => p.BeginTransaction());
            foreach ((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, string message) in Nuls())
            {
                (Exception? error, bool atTheCall) = await Outcome(producer, offsets, GroupMetadata());
                AssertNul(error, message);
                Assert.Equal(producer is IProducer<byte[], byte[]>, atTheCall);
            }

            Assert.False(SentOffsets(producer));
            await Run(producer, p => p.CommitTransaction(), p => p.CommitTransaction());
            Assert.Empty(History(producer));
        }
        finally
        {
            await Close(producer);
        }
    }

    // Java's MockProducer makes its state checks before it reads the map.
    [Theory]
    [MemberData(nameof(Mocks))]
    public async Task OnTheMocks_ANulTopic_ComesAfterJavasStateChecks(string kind)
    {
        object producer = Mock(kind);
        try
        {
            (Exception? error, bool _) = await Outcome(producer, s_nulTopic, GroupMetadata());
            Assert.Equal(MockNotInitialized, Assert.IsType<IllegalStateException>(error).Message);
        }
        finally
        {
            await Close(producer);
        }
    }

    private static IEnumerable<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, string Message)> Nuls() => new[]
    {
        (s_nulTopic, NulTopicMessage),
        (s_nulMetadata, NulMetadataMessage),
    };

    private static void AssertNul(Exception? error, string message)
    {
        Assert.True(error is not null && error.GetType() == typeof(ArgumentException), $"{error}");
        Assert.Equal("offsets", ((ArgumentException)error!).ParamName);
        Assert.Equal(new ArgumentException(message, "offsets").Message, error.Message);
    }

    private static void AssertMustNotContainNull(Exception? error)
    {
        ArgumentNullException refused = Assert.IsType<ArgumentNullException>(error);
        Assert.Equal("offsets", refused.ParamName);
        Assert.Equal(new ArgumentNullException("offsets", "offsets must not contain null").Message, refused.Message);
    }

    // The exception the call gives, and whether it is thrown at the call or faults the returned Task.
    private static async Task<(Exception? Error, bool AtTheCall)> Outcome(
        object producer,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IConsumerGroupMetadata? groupMetadata)
    {
        if (producer is IProducer<byte[], byte[]> sync)
        {
            return (Record.Exception(() => sync.SendOffsetsToTransaction(offsets, groupMetadata!)), true);
        }

        Task? task = null;
        Exception? thrown = Record.Exception(
            () => { task = ((IAsyncProducer<byte[], byte[]>)producer).SendOffsetsToTransaction(offsets, groupMetadata!); });
        if (thrown is not null)
        {
            return (thrown, true);
        }

        return (await Record.ExceptionAsync(() => TestTimeout.Run(() => task!, s_deadline)), false);
    }

    private static Task Run(
        object producer,
        Action<IProducer<byte[], byte[]>> sync,
        Func<IAsyncProducer<byte[], byte[]>, Task> async)
    {
        if (producer is IProducer<byte[], byte[]> syncProducer)
        {
            sync(syncProducer);
            return Task.CompletedTask;
        }

        return TestTimeout.Run(() => async((IAsyncProducer<byte[], byte[]>)producer), s_deadline);
    }

    private static async Task<object> Real(string kind, string? transactionalId, bool idempotence)
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = 5000,
            ["enable.idempotence"] = idempotence,
        };
        if (transactionalId is not null)
        {
            configs["transactional.id"] = transactionalId;
        }

        return kind == "KafkaProducer"
            ? KafkaProducer<byte[], byte[]>.Create(configs, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer())
            : await AsyncKafkaProducer<byte[], byte[]>.Create(configs, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
    }

    private static object Mock(string kind) => kind == "MockProducer"
        ? new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer(), true)
        : new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer(), true);

    private static IConsumerGroupMetadata GroupMetadata()
    {
        using MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>("latest");
        return consumer.GroupMetadata();
    }

    private static bool SentOffsets(object producer) => producer is MockProducer<byte[], byte[]> sync
        ? sync.SentOffsets()
        : ((AsyncMockProducer<byte[], byte[]>)producer).SentOffsets();

    private static IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>> History(
        object producer) => producer is MockProducer<byte[], byte[]> sync
        ? sync.ConsumerGroupOffsetsHistory()
        : ((AsyncMockProducer<byte[], byte[]>)producer).ConsumerGroupOffsetsHistory();

    private static void SetSendOffsetsException(object producer, Exception? exception)
    {
        if (producer is MockProducer<byte[], byte[]> sync)
        {
            sync.SetSendOffsetsToTransactionException(exception);
        }
        else
        {
            ((AsyncMockProducer<byte[], byte[]>)producer).SetSendOffsetsToTransactionException(exception);
        }
    }

    private static void Fence(object producer)
    {
        if (producer is MockProducer<byte[], byte[]> sync)
        {
            sync.FenceProducer();
        }
        else
        {
            ((AsyncMockProducer<byte[], byte[]>)producer).FenceProducer();
        }
    }

    private static async Task Close(object producer)
    {
        switch (producer)
        {
            case IProducer<byte[], byte[]> sync:
                TestTimeout.Run(() => sync.Close(TimeSpan.Zero), s_deadline);
                TestTimeout.Run(sync.Dispose, s_deadline);
                break;
            case IAsyncProducer<byte[], byte[]> async:
                await TestTimeout.Run(() => async.Close(TimeSpan.Zero), s_deadline);
                await TestTimeout.Run(() => async.DisposeAsync().AsTask(), s_deadline);
                break;
        }
    }
}
