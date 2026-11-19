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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The asynchronous producers' <c>Send</c> with a serializer that is awaited, on
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> over an unreachable broker and on
/// <see cref="AsyncMockProducer{TKey, TValue}"/>: the key is serialized and then the value, the first stage covers the
/// wait, a record enters the producer when its serialization completes, a serializer failure or cancellation ends the
/// first stage with nothing sent, and two built-in serializers keep the serialization inside the call.
/// </summary>
public sealed class AsyncProducerAwaitedSerializerTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "awaited-serializer-topic";

    private const string Real = "AsyncKafkaProducer";

    private const string Mock = "AsyncMockProducer";

    private const string Boom = "the serializer failed";

    private const int TimeoutCode = 7;

    [Theory]
    [InlineData(Real, true)]
    [InlineData(Real, false)]
    [InlineData(Mock, true)]
    [InlineData(Mock, false)]
    public async Task AnAwaitedSerializer_SerializesTheKeyThenTheValue_AndTheFirstStageCoversTheWait(string flavor, bool yields)
    {
        Awaiting serializer = new Awaiting(yields);
        IAsyncProducer<string, string> producer = Create(flavor, serializer, serializer);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        if (!yields)
        {
            Assert.False(firstStage.IsCompleted);
            Assert.Equal(new[] { (Topic, (string?)"key") }, serializer.Calls());
            Assert.False(record.Headers().IsReadOnly);
            serializer.Complete();
        }

        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => firstStage.AsTask(), s_deadline);

        Assert.Equal(new[] { (Topic, (string?)"key"), (Topic, (string?)"value") }, serializer.Calls());
        if (flavor == Mock)
        {
            RecordMetadata metadata = await TestTimeout.Run(() => future.Get(), s_deadline);
            Assert.Equal(0, metadata.Offset());
            Assert.Equal(1, ((AsyncMockProducer<string, string>)producer).HistoryCount());
            Assert.All(serializer.HeadersGiven(), given => Assert.NotSame(record.Headers(), given));
            Assert.False(record.Headers().IsReadOnly);
        }
        else
        {
            Assert.All(serializer.HeadersGiven(), given => Assert.Same(record.Headers(), given));
            Assert.True(record.Headers().IsReadOnly);
            await AssertMetadataTimeout(future);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SerializeAsync_GivesTheSerializersBytes_AndTheRecordsOwnHeaders(bool yields)
    {
        Awaiting serializer = new Awaiting(yields);
        ProducerRecord<string, string> record = Record("value");

        ValueTask<SerializedProducerRecord> pending = SerializedProducerRecord.SerializeAsync(
            record,
            new AsyncSerializerCall<string>(serializer),
            new AsyncSerializerCall<string>(serializer),
            mock: false,
            CancellationToken.None);
        serializer.Complete();
        SerializedProducerRecord serialized = await TestTimeout.Run(() => pending.AsTask(), s_deadline);

        Assert.Equal(Topic, serialized.Topic);
        Assert.Equal(0, serialized.Partition);
        Assert.Equal(Bytes("key"), serialized.Key!.Value.ToArray());
        Assert.Equal(Bytes("value"), serialized.Value!.Value.ToArray());
        Assert.Same(record.Headers(), serialized.Headers);
    }

    [Fact]
    public async Task SerializeAsync_ANullValue_ReachesTheSerializer_AndItsNullIsANullValue()
    {
        Awaiting serializer = new Awaiting(yields: true);
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, value: null!, key: "key");

        SerializedProducerRecord serialized = await TestTimeout.Run(
            () => SerializedProducerRecord.SerializeAsync(
                record,
                new AsyncSerializerCall<string>(serializer),
                new AsyncSerializerCall<string>(serializer),
                mock: false,
                CancellationToken.None).AsTask(),
            s_deadline);

        Assert.Equal(new[] { (Topic, (string?)"key"), (Topic, (string?)null) }, serializer.Calls());
        Assert.Equal(Bytes("key"), serialized.Key!.Value.ToArray());
        Assert.Null(serialized.Value);
    }

    [Fact]
    public async Task OnTheMock_TwoSendsWhoseSerializationsCompleteInReverse_EnterInCompletionOrder()
    {
        Gated value = new Gated();
        AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(new AsyncStringSerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> first = producer.Send(Record("first"));
        ValueTask<AsyncKafkaFuture<RecordMetadata>> second = producer.Send(Record("second"));
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);
        Assert.Equal(0, producer.HistoryCount());

        value.Release("second");
        RecordMetadata secondMetadata = await TestTimeout.Run(() => second.Delivery(), s_deadline);
        Assert.False(first.IsCompleted);
        Assert.Equal(1, producer.HistoryCount());

        value.Release("first");
        RecordMetadata firstMetadata = await TestTimeout.Run(() => first.Delivery(), s_deadline);

        Assert.Equal(0, secondMetadata.Offset());
        Assert.Equal(1, firstMetadata.Offset());
        Assert.Equal(2, producer.HistoryCount());
    }

    [Fact]
    public async Task OnTheKafkaProducer_TwoSendsWhoseSerializationsCompleteInReverse_EnterInCompletionOrder()
    {
        Gated value = new Gated();
        AsyncKafkaProducer<string, string> producer =
            new AsyncKafkaProducer<string, string>(Unreachable(), new AsyncStringSerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> firstRecord = Record("first");
        ProducerRecord<string, string> secondRecord = Record("second");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> first = producer.Send(firstRecord);
        ValueTask<AsyncKafkaFuture<RecordMetadata>> second = producer.Send(secondRecord);

        value.Release("second");
        AsyncKafkaFuture<RecordMetadata> secondFuture = await TestTimeout.Run(() => second.AsTask(), s_deadline);
        Assert.True(secondRecord.Headers().IsReadOnly);
        Assert.False(first.IsCompleted);
        Assert.False(firstRecord.Headers().IsReadOnly);

        value.Release("first");
        AsyncKafkaFuture<RecordMetadata> firstFuture = await TestTimeout.Run(() => first.AsTask(), s_deadline);
        Assert.True(firstRecord.Headers().IsReadOnly);
        await AssertMetadataTimeout(secondFuture);
        await AssertMetadataTimeout(firstFuture);
    }

    [Theory]
    [InlineData(Real, Failing.Thrown)]
    [InlineData(Real, Failing.Faulted)]
    [InlineData(Real, Failing.AfterAWait)]
    [InlineData(Mock, Failing.Thrown)]
    [InlineData(Mock, Failing.Faulted)]
    [InlineData(Mock, Failing.AfterAWait)]
    public async Task AFailingValueSerializer_EndsTheFirstStageWithItsException_AndNothingIsSent(string flavor, string how)
    {
        BoomException boom = new BoomException(Boom);
        IAsyncProducer<string, string> producer = Create(flavor, new AsyncStringSerializer(), new Failing(how, boom));
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);

        BoomException thrown = await Assert.ThrowsAsync<BoomException>(
            () => TestTimeout.Run(() => firstStage.AsTask(), s_deadline));
        Assert.Same(boom, thrown);
        Assert.Equal(Boom, thrown.Message);
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Real, Failing.Thrown)]
    [InlineData(Real, Failing.Faulted)]
    [InlineData(Real, Failing.AfterAWait)]
    [InlineData(Mock, Failing.Thrown)]
    [InlineData(Mock, Failing.Faulted)]
    [InlineData(Mock, Failing.AfterAWait)]
    public async Task AFailingKeySerializer_EndsTheFirstStage_BeforeTheValueSerializerIsCalled(string flavor, string how)
    {
        BoomException boom = new BoomException(Boom);
        Awaiting value = new Awaiting(yields: true);
        IAsyncProducer<string, string> producer = Create(flavor, new Failing(how, boom), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        BoomException thrown = await Assert.ThrowsAsync<BoomException>(
            () => TestTimeout.Run(() => producer.Send(record).AsTask(), s_deadline));

        Assert.Same(boom, thrown);
        Assert.Empty(value.Calls());
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Failing.Thrown, true)]
    [InlineData(Failing.Faulted, true)]
    [InlineData(Failing.AfterAWait, true)]
    [InlineData(Failing.Thrown, false)]
    [InlineData(Failing.Faulted, false)]
    [InlineData(Failing.AfterAWait, false)]
    public async Task OnTheKafkaProducer_AnInvalidCast_IsWrappedAsTheBuiltInPathWrapsIt(string how, bool isKey)
    {
        InvalidCastException cast = new InvalidCastException("not a string");
        Failing failing = new Failing(how, cast);
        Awaiting other = new Awaiting(yields: true);
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            Unreachable(), isKey ? failing : other, isKey ? other : failing);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");
        string field = isKey ? "key" : "value";

        SerializationException thrown = await Assert.ThrowsAsync<SerializationException>(
            () => TestTimeout.Run(() => producer.Send(record).AsTask(), s_deadline));

        Assert.Equal(
            "Can't convert " + field + " of class System.String to class " + typeof(Failing).FullName
            + " specified in " + field + ".serializer",
            thrown.Message);
        Assert.Same(cast, thrown.InnerException);
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Failing.Thrown)]
    [InlineData(Failing.Faulted)]
    [InlineData(Failing.AfterAWait)]
    public async Task OnTheMock_AnInvalidCast_EndsTheFirstStageUnchanged(string how)
    {
        InvalidCastException cast = new InvalidCastException("not a string");
        AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(new AsyncStringSerializer(), new Failing(how, cast));
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        InvalidCastException thrown = await Assert.ThrowsAsync<InvalidCastException>(
            () => TestTimeout.Run(() => producer.Send(record).AsTask(), s_deadline));

        Assert.Same(cast, thrown);
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Failing.Thrown)]
    [InlineData(Failing.Faulted)]
    [InlineData(Failing.AfterAWait)]
    public async Task OnTheKafkaProducer_AnApiException_FailsTheRecordThroughItsFuture(string how)
    {
        RecordTooLargeException tooLarge = new RecordTooLargeException("too large for the test");
        AsyncKafkaProducer<string, string> producer =
            new AsyncKafkaProducer<string, string>(Unreachable(), new AsyncStringSerializer(), new Failing(how, tooLarge));
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        AsyncKafkaFuture<RecordMetadata> future =
            await TestTimeout.Run(() => producer.Send(record).AsTask(), s_deadline);

        Assert.Same(
            tooLarge,
            await FutureAssert.ThrowsAsync<RecordTooLargeException>(() => TestTimeout.Run(() => future.Get(), s_deadline)));
        Assert.False(record.Headers().IsReadOnly);
    }

    [Theory]
    [InlineData(Failing.Thrown)]
    [InlineData(Failing.Faulted)]
    [InlineData(Failing.AfterAWait)]
    public async Task OnTheMock_AnApiException_EndsTheFirstStageUnchanged(string how)
    {
        RecordTooLargeException tooLarge = new RecordTooLargeException("too large for the test");
        AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(new AsyncStringSerializer(), new Failing(how, tooLarge));
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        RecordTooLargeException thrown = await Assert.ThrowsAsync<RecordTooLargeException>(
            () => TestTimeout.Run(() => producer.Send(record).AsTask(), s_deadline));

        Assert.Same(tooLarge, thrown);
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Real)]
    [InlineData(Mock)]
    public async Task ASerializer_IsGivenSendsToken_AndItsCancellationCancelsTheFirstStage(string flavor)
    {
        Cancellable key = new Cancellable();
        Awaiting value = new Awaiting(yields: true);
        IAsyncProducer<string, string> producer = Create(flavor, key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record, cancellation.Token);
        await TestTimeout.Run(() => key.Entered, s_deadline);
        Assert.False(firstStage.IsCompleted);
        Assert.Equal(cancellation.Token, key.Token);

        cancellation.Cancel();
        OperationCanceledException cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => firstStage.AsTask(), s_deadline));

        Assert.Equal(cancellation.Token, cancelled.CancellationToken);
        Assert.Empty(value.Calls());
        AssertNothingSent(producer, record);
    }

    [Theory]
    [InlineData(Real)]
    [InlineData(Mock)]
    public async Task ATokenCancelledWhileASerializerThatIgnoresItWaits_CancelsTheFirstStageOnceItCompletes(string flavor)
    {
        Gated value = new Gated();
        IAsyncProducer<string, string> producer = Create(flavor, new AsyncStringSerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(Record("ignored"), cancellation.Token);
        cancellation.Cancel();
        Assert.False(firstStage.IsCompleted);
        value.Release("ignored");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => firstStage.AsTask(), s_deadline));
        if (producer is AsyncMockProducer<string, string> mock)
        {
            Assert.Equal(0, mock.HistoryCount());
        }
    }

    [Fact]
    public async Task OnTheKafkaProducer_AHeaderAnAwaitedSerializerAdds_IsOnTheRecord_WhoseHeadersThenTurnReadOnly()
    {
        HeaderAdding value = new HeaderAdding();
        AsyncKafkaProducer<string, string> producer =
            new AsyncKafkaProducer<string, string>(Unreachable(), new AsyncStringSerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        Assert.False(firstStage.IsCompleted);
        Assert.False(record.Headers().IsReadOnly);
        record.Headers().Add("during", Bytes("the wait"));
        value.Complete();
        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => firstStage.AsTask(), s_deadline);

        Assert.Equal(new[] { "h", "during", "added" }, Array.ConvertAll(record.Headers().ToArray(), header => header.Key()));
        Assert.Equal(Bytes("by the serializer"), record.Headers().LastHeader("added")?.Value());
        Assert.True(record.Headers().IsReadOnly);
        IllegalStateException closed = Assert.Throws<IllegalStateException>(() => record.Headers().Add("after", Bytes("x")));
        Assert.Equal("RecordHeaders has been closed.", closed.Message);
        await AssertMetadataTimeout(future);
    }

    [Fact]
    public async Task OnTheMock_AHeaderAnAwaitedSerializerAdds_IsDropped_AndTheRecordsHeadersStayWritable()
    {
        HeaderAdding value = new HeaderAdding();
        AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(new AsyncStringSerializer(), value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("value");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        value.Complete();
        await TestTimeout.Run(() => firstStage.Delivery(), s_deadline);

        Assert.Equal(new[] { "h" }, Array.ConvertAll(record.Headers().ToArray(), header => header.Key()));
        Assert.False(record.Headers().IsReadOnly);
        record.Headers().Add("after", Bytes("x"));
    }

    [Theory]
    [InlineData(Real)]
    [InlineData(Mock)]
    public async Task WithTwoBuiltInSerializers_ASerializerThrow_LeavesSendItself(string flavor)
    {
        IAsyncProducer<Guid?, Guid?> producer = flavor == Real
            ? new AsyncKafkaProducer<Guid?, Guid?>(Unreachable(), new AsyncUUIDSerializer(), UnsupportedEncoding())
            : new AsyncMockProducer<Guid?, Guid?>(new AsyncUUIDSerializer(), UnsupportedEncoding());
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<Guid?, Guid?> record = new ProducerRecord<Guid?, Guid?>(Topic, value: Guid.NewGuid(), key: Guid.NewGuid());

        SerializationException thrown = Assert.Throws<SerializationException>(() => { _ = producer.Send(record); });

        Assert.Equal("Error when serializing UUID to byte[] due to unsupported encoding no-such-encoding", thrown.Message);
        Assert.False(record.Headers().IsReadOnly);
    }

    [Theory]
    [InlineData(Real)]
    [InlineData(Mock)]
    public async Task WithOneAwaitedSerializer_ABuiltInSerializersThrow_EndsTheFirstStage(string flavor)
    {
        PassThrough key = new PassThrough();
        IAsyncProducer<Guid?, Guid?> producer = flavor == Real
            ? new AsyncKafkaProducer<Guid?, Guid?>(Unreachable(), key, UnsupportedEncoding())
            : new AsyncMockProducer<Guid?, Guid?>(key, UnsupportedEncoding());
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<Guid?, Guid?> record = new ProducerRecord<Guid?, Guid?>(Topic, value: Guid.NewGuid(), key: Guid.NewGuid());

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        SerializationException thrown = await Assert.ThrowsAsync<SerializationException>(
            () => TestTimeout.Run(() => firstStage.AsTask(), s_deadline));

        Assert.Equal("Error when serializing UUID to byte[] due to unsupported encoding no-such-encoding", thrown.Message);
        Assert.Equal(1, key.CallCount);
        Assert.False(record.Headers().IsReadOnly);
    }

    [Theory]
    [InlineData(Real, true)]
    [InlineData(Real, false)]
    [InlineData(Mock, true)]
    [InlineData(Mock, false)]
    public async Task ABuiltInSerializerOnOneSide_AndAnAwaitedOneOnTheOther_SerializeTheKeyThenTheValue(string flavor, bool awaitedKey)
    {
        Awaiting awaited = new Awaiting(yields: false);
        IAsyncSerializer<string> builtIn = new AsyncStringSerializer();
        IAsyncProducer<string, string> producer = Create(flavor, awaitedKey ? awaited : builtIn, awaitedKey ? builtIn : awaited);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record("a longer value");

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        Assert.False(firstStage.IsCompleted);
        Assert.Equal(new[] { (Topic, (string?)(awaitedKey ? "key" : "a longer value")) }, awaited.Calls());
        awaited.Complete();
        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => firstStage.AsTask(), s_deadline);

        if (flavor == Mock)
        {
            RecordMetadata metadata = await TestTimeout.Run(() => future.Get(), s_deadline);
            Assert.Equal(0, metadata.Offset());
            Assert.Equal(1, ((AsyncMockProducer<string, string>)producer).HistoryCount());
        }
        else
        {
            Assert.True(record.Headers().IsReadOnly);
            await AssertMetadataTimeout(future);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SerializeAsync_ABuiltInSide_IsCalledThroughItsSynchronousClass(bool awaitedKey)
    {
        Awaiting awaited = new Awaiting(yields: true);
        IAsyncSerializer<string> builtIn = new AsyncStringSerializer();
        ProducerRecord<string, string> record = Record("value");

        SerializedProducerRecord serialized = await TestTimeout.Run(
            () => SerializedProducerRecord.SerializeAsync(
                record,
                new AsyncSerializerCall<string>(awaitedKey ? awaited : builtIn),
                new AsyncSerializerCall<string>(awaitedKey ? builtIn : awaited),
                mock: false,
                CancellationToken.None).AsTask(),
            s_deadline);

        Assert.Equal(new[] { (Topic, (string?)(awaitedKey ? "key" : "value")) }, awaited.Calls());
        Assert.Equal(Bytes("key"), serialized.Key!.Value.ToArray());
        Assert.Equal(Bytes("value"), serialized.Value!.Value.ToArray());
    }

    [Theory]
    [InlineData("DisposeAsync")]
    [InlineData("Dispose")]
    [InlineData("Close")]
    public async Task TheKafkaProducer_DisposesASerializerOnlyThroughDisposeAsync_EachOnce(string how)
    {
        BothDisposals key = new BothDisposals();
        BothDisposals value = new BothDisposals();
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(Unreachable(), key, value);

        switch (how)
        {
            case "DisposeAsync":
                await TestTimeout.DisposeAsync(producer);
                break;
            case "Dispose":
                TestTimeout.Dispose(producer);
                break;
            default:
                await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
                await TestTimeout.DisposeAsync(producer);
                break;
        }

        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(1, value.AsyncDisposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheKafkaProducer_RefusesASerializerThatIsOnlyIDisposable_AndDisposesNothing(bool asKey)
    {
        OnlyDisposable refused = new OnlyDisposable();
        BothDisposals other = new BothDisposals();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => new AsyncKafkaProducer<string, string>(Unreachable(), asKey ? refused : other, asKey ? other : refused));

        Assert.Equal(
            "Confluent.Kafka.UnitTests.AsyncProducerAwaitedSerializerTests+OnlyDisposable implements IDisposable but does "
                + "not implement IAsyncDisposable, so the producer would never dispose it.",
            e.Message);
        Assert.Equal(0, refused.Disposals);
        Assert.Equal(0, other.AsyncDisposals);
        Assert.Equal(0, other.Disposals);
    }

    [Fact]
    public async Task TheKafkaProducer_GivenOneSerializerForBothSides_DisposesItOncePerSide_AndNotAgainWhenDisposedAgain()
    {
        BothDisposals both = new BothDisposals();
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(Unreachable(), both, both);

        await TestTimeout.DisposeAsync(producer);
        await TestTimeout.DisposeAsync(producer);

        Assert.Equal(2, both.AsyncDisposals);
        Assert.Equal(0, both.Disposals);
    }

    [Fact]
    public async Task TheMock_DisposesNoSerializer()
    {
        BothDisposals key = new BothDisposals();
        OnlyDisposable value = new OnlyDisposable();
        AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(key, value);

        await TestTimeout.Run(() => producer.Close(), s_deadline);
        await TestTimeout.DisposeAsync(producer);

        Assert.Equal(0, key.AsyncDisposals);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    private static IAsyncProducer<string, string> Create(string flavor, IAsyncSerializer<string> key, IAsyncSerializer<string> value) =>
        flavor == Real
            ? new AsyncKafkaProducer<string, string>(Unreachable(), key, value)
            : new AsyncMockProducer<string, string>(key, value);

    // The Kafka producer makes the headers read-only just before it takes the record, so writable headers mean it
    // never took it.
    private static void AssertNothingSent<TKey, TValue>(IAsyncProducer<TKey, TValue> producer, ProducerRecord<TKey, TValue> record)
    {
        Assert.False(record.Headers().IsReadOnly);
        if (producer is AsyncMockProducer<TKey, TValue> mock)
        {
            Assert.Equal(0, mock.HistoryCount());
        }
    }

    private static async Task AssertMetadataTimeout(AsyncKafkaFuture<RecordMetadata> future)
    {
        Common.Errors.TimeoutException failure = await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
            () => TestTimeout.Run(() => future.Get(), s_deadline));
        Assert.Equal(TimeoutCode, failure.Code);
    }

    private static ProducerRecord<string, string> Record(string value) =>
        new ProducerRecord<string, string>(Topic, value: value, key: "key", headers: new[] { new Header("h", Bytes("1")) }, partition: 0);

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static AsyncUUIDSerializer UnsupportedEncoding()
    {
        AsyncUUIDSerializer serializer = new AsyncUUIDSerializer();
        Assert.True(
            serializer
                .Configure(new Dictionary<string, object?> { ["value.serializer.encoding"] = "no-such-encoding" }, isKey: false)
                .IsCompletedSuccessfully);
        return serializer;
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static ReadOnlyMemory<byte>? Encoded(string? data) => data is null ? (ReadOnlyMemory<byte>?)null : Bytes(data);

    /// <summary>
    /// A UTF-8 serializer that records each call and waits, for the thread pool or until the test completes it.
    /// </summary>
    private sealed class Awaiting : IAsyncSerializer<string>
    {
        private readonly bool _yields;
        private readonly TaskCompletionSource<bool> _later =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly List<(string Topic, string? Data)> _calls = new List<(string Topic, string? Data)>();
        private readonly List<Headers> _headers = new List<Headers>();

        internal Awaiting(bool yields) => _yields = yields;

        public async ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken)
        {
            lock (_calls)
            {
                _calls.Add((topic, data));
                _headers.Add(headers);
            }

            if (_yields)
            {
                await Task.Yield();
            }
            else
            {
                await _later.Task.ConfigureAwait(false);
            }

            return Encoded(data);
        }

        internal void Complete() => _later.TrySetResult(true);

        internal (string Topic, string? Data)[] Calls()
        {
            lock (_calls)
            {
                return _calls.ToArray();
            }
        }

        internal Headers[] HeadersGiven()
        {
            lock (_calls)
            {
                return _headers.ToArray();
            }
        }
    }

    /// <summary>A UTF-8 serializer whose call for a datum waits until the test releases that datum.</summary>
    private sealed class Gated : IAsyncSerializer<string>
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _gates =
            new ConcurrentDictionary<string, TaskCompletionSource<bool>>();

        public async ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken)
        {
            await Gate(data!).Task.ConfigureAwait(false);
            return Encoded(data);
        }

        internal void Release(string data) => Gate(data).TrySetResult(true);

        private TaskCompletionSource<bool> Gate(string data) =>
            _gates.GetOrAdd(data, _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    /// <summary>
    /// A serializer that fails with the exception it is given: thrown by the call, carried by a faulted
    /// <see cref="ValueTask{TResult}"/>, or thrown after a wait.
    /// </summary>
    private sealed class Failing : IAsyncSerializer<string>
    {
        internal const string Thrown = "thrown";

        internal const string Faulted = "faulted";

        internal const string AfterAWait = "after a wait";

        private readonly string _how;
        private readonly Exception _failure;

        internal Failing(string how, Exception failure)
        {
            _how = how;
            _failure = failure;
        }

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken) =>
            _how switch
            {
                Thrown => throw _failure,
                Faulted => new ValueTask<ReadOnlyMemory<byte>?>(Task.FromException<ReadOnlyMemory<byte>?>(_failure)),
                _ => ThrowAfterAWait(),
            };

        private async ValueTask<ReadOnlyMemory<byte>?> ThrowAfterAWait()
        {
            await Task.Yield();
            throw _failure;
        }
    }

    /// <summary>The exception the failing serializers throw, which nothing else in a send throws.</summary>
    private sealed class BoomException : Exception
    {
        internal BoomException(string message)
            : base(message)
        {
        }
    }

    /// <summary>A serializer that records the token it is given and waits until that token is cancelled.</summary>
    private sealed class Cancellable : IAsyncSerializer<string>
    {
        private readonly TaskCompletionSource<bool> _entered =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Entered => _entered.Task;

        internal CancellationToken Token { get; private set; }

        public async ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken)
        {
            Token = cancellationToken;
            _entered.TrySetResult(true);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return Encoded(data);
        }
    }

    /// <summary>A UTF-8 serializer that waits for the test and then adds a header to the headers it was given.</summary>
    private sealed class HeaderAdding : IAsyncSerializer<string>
    {
        private readonly TaskCompletionSource<bool> _later =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken)
        {
            await _later.Task.ConfigureAwait(false);
            headers.Add("added", Bytes("by the serializer"));
            return Encoded(data);
        }

        internal void Complete() => _later.TrySetResult(true);
    }

    /// <summary>A serializer of UUIDs that is not a built-in, completes at once and counts its calls.</summary>
    private sealed class PassThrough : IAsyncSerializer<Guid?>
    {
        private int _callCount;

        internal int CallCount => Volatile.Read(ref _callCount);

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, Guid? data, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _callCount);
            return new ValueTask<ReadOnlyMemory<byte>?>(data?.ToByteArray());
        }
    }

    /// <summary>A serializer with both disposal interfaces that counts the calls of each.</summary>
    private sealed class BothDisposals : IAsyncSerializer<string>, IAsyncDisposable, IDisposable
    {
        private int _asyncDisposals;
        private int _disposals;

        internal int AsyncDisposals => Volatile.Read(ref _asyncDisposals);

        internal int Disposals => Volatile.Read(ref _disposals);

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken) =>
            new ValueTask<ReadOnlyMemory<byte>?>(Encoded(data));

        public async ValueTask DisposeAsync()
        {
            await Task.Yield();
            Interlocked.Increment(ref _asyncDisposals);
        }

        public void Dispose() => Interlocked.Increment(ref _disposals);
    }

    /// <summary>A serializer with <see cref="IDisposable"/> only that counts its disposals.</summary>
    private sealed class OnlyDisposable : IAsyncSerializer<string>, IDisposable
    {
        private int _disposals;

        internal int Disposals => Volatile.Read(ref _disposals);

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken) =>
            new ValueTask<ReadOnlyMemory<byte>?>(Encoded(data));

        public void Dispose() => Interlocked.Increment(ref _disposals);
    }
}
