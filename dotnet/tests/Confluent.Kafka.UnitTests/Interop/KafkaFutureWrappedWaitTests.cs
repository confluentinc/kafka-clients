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
/// future); and the binding's own waits, which keep the failure itself. The sync waits run on the sync engine and the
/// async ones on the async engine.
/// </summary>
public sealed class KafkaFutureWrappedWaitTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task AFailure_IsWrappedByEveryJavaWait_AndKeptByTheBindingsOwnWaits()
    {
        var future = new KafkaFutureImpl<int>();
        var awaitable = new AsyncKafkaFutureImpl<int>();
        var failure = new InvalidTopicException("Invalid topic: a b");
        Assert.True(future.CompleteWithError(failure));
        Assert.True(awaitable.CompleteWithError(failure));

        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.Get()));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.Get(TimeSpan.Zero)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.Get(s_deadline)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.Get(CancellationToken.None)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => future.GetNow(-1)));
        KafkaFuture plain = future;
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => plain.Get()));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => plain.GetNow()));

        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => awaitable.Get()));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => awaitable.Get(s_deadline)));
        Assert.Same(
            failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => awaitable.Get(CancellationToken.None)));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => awaitable.GetNow(-1)));
        AsyncKafkaFuture awaitablePlain = awaitable;
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidTopicException>(() => awaitablePlain.Get()));
        Assert.Same(failure, FutureAssert.Throws<InvalidTopicException>(() => awaitablePlain.GetNow()));

        ExecutionException wrapper = Assert.Throws<ExecutionException>(() => future.Get());
        Assert.Equal("Confluent.Kafka.Common.Errors.InvalidTopicException: Invalid topic: a b", wrapper.Message);
        Assert.NotSame(wrapper, Assert.Throws<ExecutionException>(() => future.Get()));

        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => future.GetUnwrapped()));
        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => future.GetNowUnwrapped(-1)));
        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => awaitable.GetUnwrapped()));
        Assert.Same(failure, Assert.Throws<InvalidTopicException>(() => awaitable.GetNowUnwrapped(-1)));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidTopicException>(() => awaitable.ToCompletionStage()));
    }

    [Fact]
    public async Task TheJavaTask_IsTheSameOnEveryCall_AndNotTheRawTask_WhileTheFailureIsPending()
    {
        var future = new AsyncKafkaFutureImpl<int>();
        Task<int> javaTask = future.Get();
        Assert.Same(javaTask, future.Get());
        Assert.NotSame(future.ToCompletionStage(), javaTask);

        var failure = new InvalidOperationException("late failure");
        Assert.True(future.CompleteWithError(failure));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => javaTask));
        Assert.Same(javaTask, future.Get());
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => future.ToCompletionStage()));
    }

    [Fact]
    public async Task AFailureWithNoMessage_IsNamedByItsClassInTheMessage()
    {
        var future = new KafkaFutureImpl<int>();
        var awaitable = new AsyncKafkaFutureImpl<int>();
        var failure = new TopicExistsException((string?)null);
        Assert.True(future.CompleteWithError(failure));
        Assert.True(awaitable.CompleteWithError(failure));

        ExecutionException wrapper = Assert.Throws<ExecutionException>(() => future.Get());
        Assert.Same(failure, wrapper.InnerException);
        Assert.Equal("Confluent.Kafka.Common.Errors.TopicExistsException", wrapper.Message);
        ExecutionException awaited = await Assert.ThrowsAsync<ExecutionException>(() => awaitable.Get());
        Assert.Same(failure, awaited.InnerException);
        Assert.Equal(wrapper.Message, awaited.Message);
    }

    [Fact]
    public async Task ACancelledFuture_ThrowsTheCancellationAsItIs()
    {
        var future = new KafkaFutureImpl<int>();
        var awaitable = new AsyncKafkaFutureImpl<int>();
        Assert.True(future.Cancel(true));
        Assert.True(awaitable.Cancel(true));

        Assert.ThrowsAny<OperationCanceledException>(() => future.Get());
        Assert.ThrowsAny<OperationCanceledException>(() => future.Get(TimeSpan.Zero));
        Assert.ThrowsAny<OperationCanceledException>(() => future.GetNow(-1));
        Assert.ThrowsAny<OperationCanceledException>(() => awaitable.GetNow(-1));
        Task<int> javaTask = awaitable.Get();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => javaTask);
        Assert.True(javaTask.IsCanceled);
    }

    [Fact]
    public async Task ACancellationGivenAsTheError_CancelsAnOrdinaryFuture_AndIsThrownAsItIs()
    {
        var future = new KafkaFutureImpl<int>();
        var awaitable = new AsyncKafkaFutureImpl<int>();
        var cancellation = new OperationCanceledException("given as the error");
        Assert.True(future.CompleteWithError(cancellation));
        Assert.True(awaitable.CompleteWithError(cancellation));

        Assert.True(future.IsCancelled());
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => future.Get()));
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => future.GetNow(-1)));
        Assert.True(awaitable.IsCancelled());
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => awaitable.GetNow(-1)));
        Task<int> javaTask = awaitable.Get();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => javaTask);
        Assert.True(javaTask.IsCanceled);
    }

    [Fact]
    public async Task ACancellationGivenAsTheError_FailsASendsFuture_AndIsWrappedLikeAnyOtherFailure()
    {
        AsyncKafkaFutureImpl<RecordMetadata> pending = AsyncKafkaFutureImpl<RecordMetadata>.ForSend();
        Task<RecordMetadata> takenWhilePending = pending.Get();
        KafkaFutureImpl<RecordMetadata> blocking = KafkaFutureImpl<RecordMetadata>.ForSend();
        var cancellation = new OperationCanceledException("given as the error");
        Assert.True(pending.CompleteWithError(cancellation));
        Assert.True(blocking.CompleteWithError(cancellation));

        Assert.False(pending.IsCancelled());
        Assert.Same(
            cancellation, await FutureAssert.ThrowsAsync<OperationCanceledException>(() => takenWhilePending));
        Assert.True(takenWhilePending.IsFaulted);
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => pending.GetNow(null!)));
        Assert.False(blocking.IsCancelled());
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => blocking.Get()));
        Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => blocking.GetNow(null!)));

        AsyncKafkaFutureImpl<RecordMetadata> done = AsyncKafkaFutureImpl<RecordMetadata>.ForSend();
        Assert.True(done.CompleteWithError(cancellation));
        Task<RecordMetadata> takenAfter = done.Get();
        Assert.True(takenAfter.IsFaulted);
        Assert.Same(cancellation, await FutureAssert.ThrowsAsync<OperationCanceledException>(() => takenAfter));
    }

    [Fact]
    public void MockProducerErrorNext_WithACancellation_FailsGetWithAnExecutionException()
    {
        var producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer(), autoComplete: false);
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
        var producer = new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer(), autoComplete: false);
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
        var awaitableSource = new AsyncKafkaFutureImpl<int>();
        var thrown = new InvalidOperationException("the function threw");
        KafkaFutureImpl<int> applied = source.ThenApplyFunc<int>(_ => throw thrown);
        AsyncKafkaFutureImpl<int> awaitableApplied = awaitableSource.ThenApplyFunc<int>(_ => throw thrown);
        Exception? seen = null;
        Exception? awaitableSeen = null;
        KafkaFutureImpl<int> completed = applied.WhenCompleteAction((_, error) => seen = error);
        AsyncKafkaFutureImpl<int> awaitableCompleted =
            awaitableApplied.WhenCompleteAction((_, error) => awaitableSeen = error);

        Assert.True(source.Complete(1));
        Assert.True(awaitableSource.Complete(1));

        Assert.Same(thrown, FutureAssert.Throws<InvalidOperationException>(() => applied.Get(s_deadline)));
        Assert.Same(thrown, FutureAssert.Throws<InvalidOperationException>(() => completed.Get(s_deadline)));
        Assert.Same(thrown, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => awaitableApplied.Get()));
        Assert.Same(thrown, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => awaitableCompleted.Get()));
        Assert.Same(thrown, seen);
        Assert.Same(thrown, awaitableSeen);
    }

    [Fact]
    public async Task AnOrdinaryFuture_TimesOutWithNoMessage()
    {
        var future = new KafkaFutureImpl<int>();
        var awaitable = new AsyncKafkaFutureImpl<int>();

        FutureTimeoutException timedOut = Assert.Throws<FutureTimeoutException>(() => future.Get(TimeSpan.Zero));
        Assert.Equal(string.Empty, timedOut.Message);
        Assert.IsAssignableFrom<System.TimeoutException>(timedOut);
        Assert.Equal(
            string.Empty,
            (await Assert.ThrowsAsync<FutureTimeoutException>(() => awaitable.Get(TimeSpan.FromMilliseconds(5)))).Message);
        Assert.Equal(
            string.Empty, Assert.Throws<FutureTimeoutException>(() => future.GetUnwrapped(TimeSpan.FromMilliseconds(5))).Message);
        Assert.Equal(
            string.Empty, Assert.Throws<FutureTimeoutException>(() => awaitable.GetUnwrapped(TimeSpan.FromMilliseconds(5))).Message);
    }

    [Theory]
    [InlineData(0L, "0")]
    [InlineData(50_000L, "5")]
    [InlineData(15_000L, "1")]
    [InlineData(9_999L, "0")]
    public async Task ASendsFuture_TimesOutWithFutureRecordMetadatasMessage(long ticks, string milliseconds)
    {
        KafkaFutureImpl<RecordMetadata> future = KafkaFutureImpl<RecordMetadata>.ForSend();
        AsyncKafkaFutureImpl<RecordMetadata> awaitable = AsyncKafkaFutureImpl<RecordMetadata>.ForSend();
        TimeSpan timeout = TimeSpan.FromTicks(ticks);
        string expected = "Timeout after waiting for " + milliseconds + " ms.";

        Assert.Equal(expected, Assert.Throws<FutureTimeoutException>(() => future.Get(timeout)).Message);
        Assert.Equal(expected, (await Assert.ThrowsAsync<FutureTimeoutException>(() => awaitable.Get(timeout))).Message);
        Assert.False(future.IsDone());
        Assert.False(awaitable.IsDone());
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
                    () => KafkaFutureImpl<RecordMetadata>.ForSend().Get(TimeSpan.Zero)).Message);
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
        new ProducerRecord<byte[], byte[]>("errornext-topic", value: new byte[] { 1 }, partition: 0, key: null);

    private sealed class Recording : ICallback
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
