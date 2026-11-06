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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

using FutureTimeoutException = Confluent.Kafka.TimeoutException;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The waits the public futures make, as Java's <c>KafkaFuture.get</c> and <c>getNow</c> answer: a failure inside an
/// <see cref="ExecutionException"/> whose message is the failure's <c>toString()</c>, a cancellation as it is, a
/// timeout as <c>java.util.concurrent.TimeoutException</c> (with <c>FutureRecordMetadata</c>'s message for a send's
/// future); and the binding's own waits, which keep the failure itself.
/// </summary>
public sealed class KafkaFutureWrappedWaitTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task AFailure_IsWrappedByEveryJavaWait_AndKeptByTheBindingsOwnWaits()
    {
        var future = new KafkaFutureImpl<int>();
        var failure = new InvalidTopicException("Invalid topic: a b");
        Assert.True(future.CompleteWithError(failure));

        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetWrapped()));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetWrapped(TimeSpan.Zero)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetWrapped(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetWrapped(CancellationToken.None)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetNowWrapped(-1)));
        KafkaFutureImpl erased = future;
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => erased.WaitWrapped()));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => erased.GetNowWrapped()));

        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => future.GetWrappedAsync()));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => future.GetWrappedAsync(s_deadline)));
        Assert.Same(
            failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => future.GetWrappedAsync(CancellationToken.None)));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => erased.WaitWrappedAsync()));

        ExecutionException wrapper = Assert.Throws<ExecutionException>(() => future.GetWrapped());
        Assert.Equal("Confluent.Kafka.Common.Errors.InvalidTopicException: Invalid topic: a b", wrapper.Message);
        Assert.NotSame(wrapper, Assert.Throws<ExecutionException>(() => future.GetWrapped()));

        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => future.Get()));
        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => future.GetNow(-1)));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidTopicException>(() => future.GetAsync()));
    }

    [Fact]
    public async Task TheJavaTask_IsTheSameOnEveryCall_AndNotTheRawTask_WhileTheFailureIsPending()
    {
        var future = new KafkaFutureImpl<int>();
        Task<int> javaTask = future.GetWrappedAsync();
        Assert.Same(javaTask, future.GetWrappedAsync());
        Assert.NotSame(future.GetAsync(), javaTask);

        var failure = new InvalidOperationException("late failure");
        Assert.True(future.CompleteWithError(failure));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => javaTask));
        Assert.Same(javaTask, future.GetWrappedAsync());
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => future.GetAsync()));
    }

    [Fact]
    public async Task AFailureWithNoMessage_IsNamedByItsClassInTheMessage()
    {
        var future = new KafkaFutureImpl<int>();
        var failure = new TopicExistsException((string?)null);
        Assert.True(future.CompleteWithError(failure));

        ExecutionException wrapper = Assert.Throws<ExecutionException>(() => future.GetWrapped());
        Assert.Same(failure, wrapper.InnerException);
        Assert.Equal("Confluent.Kafka.Common.Errors.TopicExistsException", wrapper.Message);
        ExecutionException awaited = await Assert.ThrowsAsync<ExecutionException>(() => future.GetWrappedAsync());
        Assert.Equal(wrapper.Message, awaited.Message);
    }

    [Fact]
    public async Task ACancelledFuture_ThrowsTheCancellationAsItIs()
    {
        var future = new KafkaFutureImpl<int>();
        Assert.True(future.Cancel(true));

        Assert.ThrowsAny<OperationCanceledException>(() => future.GetWrapped());
        Assert.ThrowsAny<OperationCanceledException>(() => future.GetWrapped(TimeSpan.Zero));
        Assert.ThrowsAny<OperationCanceledException>(() => future.GetNowWrapped(-1));
        Task<int> javaTask = future.GetWrappedAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => javaTask);
        Assert.True(javaTask.IsCanceled);
    }

    [Fact]
    public async Task ACancellationGivenAsTheError_CancelsAnOrdinaryFuture_AndIsThrownAsItIs()
    {
        var future = new KafkaFutureImpl<int>();
        var cancellation = new OperationCanceledException("given as the error");
        Assert.True(future.CompleteWithError(cancellation));

        Assert.True(future.IsCancelled);
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => future.GetWrapped()));
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => future.GetNowWrapped(-1)));
        Task<int> javaTask = future.GetWrappedAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => javaTask);
        Assert.True(javaTask.IsCanceled);
    }

    [Fact]
    public async Task ACancellationGivenAsTheError_FailsASendsFuture_AndIsWrappedLikeAnyOtherFailure()
    {
        KafkaFutureImpl<RecordMetadata> pending = KafkaFutureImpl<RecordMetadata>.ForSend();
        Task<RecordMetadata> takenWhilePending = pending.GetWrappedAsync();
        var cancellation = new OperationCanceledException("given as the error");
        Assert.True(pending.CompleteWithError(cancellation));

        Assert.False(pending.IsCancelled);
        Assert.Same(
            cancellation, await FutureAssert.ThrowsAsync<OperationCanceledException>(() => takenWhilePending));
        Assert.True(takenWhilePending.IsFaulted);
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => pending.GetWrapped()));
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => pending.GetNowWrapped(null!)));

        KafkaFutureImpl<RecordMetadata> done = KafkaFutureImpl<RecordMetadata>.ForSend();
        Assert.True(done.CompleteWithError(cancellation));
        Task<RecordMetadata> takenAfter = done.GetWrappedAsync();
        Assert.True(takenAfter.IsFaulted);
        Assert.Same(cancellation, await FutureAssert.ThrowsAsync<OperationCanceledException>(() => takenAfter));
    }

    [Fact]
    public void MockProducerErrorNext_WithACancellation_FailsGetWithAnExecutionException()
    {
        var producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable disposal = TestTimeout.Disposal(producer);
        var cancellation = new OperationCanceledException("injected");
        var callback = new Recording();

        KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);
        Assert.True(producer.ErrorNext(cancellation));

        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => future.Get(s_deadline)));
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => future.GetNow(null!)));
        Assert.False(future.IsCancelled());
        Assert.Same(cancellation, callback.Exception);
    }

    [Fact]
    public async Task AsyncMockProducerErrorNext_WithACancellation_FaultsGetWithAnExecutionException_TakenBeforeOrAfter()
    {
        var producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable disposal = TestTimeout.Disposal(producer);
        var cancellation = new OperationCanceledException("injected");

        AsyncKafkaFuture<RecordMetadata> before = await producer.Send(Record());
        Task<RecordMetadata> takenBefore = before.Get();
        Assert.False(takenBefore.IsCompleted);
        Assert.True(producer.ErrorNext(cancellation));
        Assert.Same(
            cancellation,
            await FutureAssert.ThrowsAsync<OperationCanceledException>(() => TestTimeout.Run(() => takenBefore, s_deadline)));
        Assert.True(takenBefore.IsFaulted);

        AsyncKafkaFuture<RecordMetadata> after = await producer.Send(Record());
        Assert.True(producer.ErrorNext(cancellation));
        Assert.Same(
            cancellation,
            await Assert.ThrowsAsync<OperationCanceledException>(() => TestTimeout.Run(() => after.ToCompletionStage(), s_deadline)));
        Task<RecordMetadata> takenAfter = after.Get();
        Assert.True(takenAfter.IsFaulted);
        Assert.Same(cancellation, await FutureAssert.ThrowsAsync<OperationCanceledException>(() => takenAfter));
        Assert.False(after.IsCancelled());
    }

    [Fact]
    public async Task ADependant_IsWrappedOnItsJavaWaits_WhileTheActionSeesTheFailureItself()
    {
        var source = new KafkaFutureImpl<int>();
        var thrown = new InvalidOperationException("the function threw");
        KafkaFutureImpl<int> applied = source.ThenApply<int>(_ => throw thrown);
        Exception? seen = null;
        KafkaFutureImpl<int> completed = applied.WhenComplete((_, error) => seen = error);

        Assert.True(source.Complete(1));

        Assert.Same(thrown, FutureAssert.Throws<InvalidOperationException>(() => applied.GetWrapped(s_deadline)));
        Assert.Same(thrown, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => completed.GetWrappedAsync()));
        Assert.Same(thrown, seen);
    }

    [Fact]
    public async Task AnOrdinaryFuture_TimesOutWithNoMessage()
    {
        var future = new KafkaFutureImpl<int>();

        FutureTimeoutException timedOut = Assert.Throws<FutureTimeoutException>(() => future.GetWrapped(TimeSpan.Zero));
        Assert.Equal(string.Empty, timedOut.Message);
        Assert.IsAssignableFrom<System.TimeoutException>(timedOut);
        Assert.Equal(
            string.Empty,
            (await Assert.ThrowsAsync<FutureTimeoutException>(() => future.GetWrappedAsync(TimeSpan.FromMilliseconds(5)))).Message);
        Assert.Equal(
            string.Empty, Assert.Throws<FutureTimeoutException>(() => future.Get(TimeSpan.FromMilliseconds(5))).Message);
    }

    [Theory]
    [InlineData(0L, "0")]
    [InlineData(50_000L, "5")]
    [InlineData(15_000L, "1")]
    [InlineData(9_999L, "0")]
    public async Task ASendsFuture_TimesOutWithFutureRecordMetadatasMessage(long ticks, string milliseconds)
    {
        KafkaFutureImpl<RecordMetadata> future = KafkaFutureImpl<RecordMetadata>.ForSend();
        TimeSpan timeout = TimeSpan.FromTicks(ticks);
        string expected = "Timeout after waiting for " + milliseconds + " ms.";

        Assert.Equal(expected, Assert.Throws<FutureTimeoutException>(() => future.GetWrapped(timeout)).Message);
        Assert.Equal(expected, (await Assert.ThrowsAsync<FutureTimeoutException>(() => future.GetWrappedAsync(timeout))).Message);
        Assert.False(future.IsDone);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task ASerializersFailure_FailsTheFutureWrapped_AndTheCallbackSeesItItself(string flavour)
    {
        var failure = new InvalidTopicException("rejected by the serializer");
        var callback = new Recording();

        if (flavour == "sync")
        {
            KafkaFuture<RecordMetadata> future = FutureFailure.Send("serde-topic", 3, failure, callback);
            Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.Get()));
            Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetNow(null!)));
            Assert.Equal(
                "Timeout after waiting for 0 ms.",
                Assert.Throws<FutureTimeoutException>(
                    () => new KafkaFuture<RecordMetadata>(KafkaFutureImpl<RecordMetadata>.ForSend()).Get(TimeSpan.Zero)).Message);
        }
        else
        {
            AsyncKafkaFuture<RecordMetadata> future = await FutureFailure.SendAsync(
                "serde-topic", 3, failure, callback, asyncCallback: null, CancellationToken.None);
            Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => future.Get()));
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidTopicException>(() => future.ToCompletionStage()));
        }

        Assert.Same(failure, callback.Exception);
        Assert.Equal(3, callback.Metadata!.Partition());
    }

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>("errornext-topic", new byte[] { 1 }, partition: 0);

    private sealed class Recording : IDeliveryCallback
    {
        internal RecordMetadata? Metadata { get; private set; }

        internal Exception? Exception { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Metadata = metadata;
            Exception = exception;
        }
    }
}
