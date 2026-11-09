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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A serializer's throw. On <see cref="KafkaProducer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/>, as Java's <c>doSend</c> answers it: an <see cref="ApiException"/>
/// goes to the delivery callback, once, with placeholder metadata, and fails the returned future; an
/// <see cref="InvalidCastException"/> is wrapped in a <see cref="SerializationException"/> naming the classes; any
/// other throw leaves <c>Send</c> as it is. On the two mocks, as Java's <c>MockProducer</c>, every throw leaves
/// <c>Send</c> as the same instance. No case reaches the core.
/// </summary>
public sealed class PublicProducerSerializerFailureTests
{
    private const string Topic = "serializer-failure-topic";

    private const string SyncMock = "sync-mock";
    private const string SyncReal = "sync-real";
    private const string AsyncMock = "async-mock";
    private const string AsyncReal = "async-real";

    private const string ApiKind = "api";
    private const string InvalidCastKind = "invalid-cast";
    private const string OtherKind = "other";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static TheoryData<string> Flavors() => new TheoryData<string> { SyncMock, SyncReal, AsyncMock, AsyncReal };

    public static TheoryData<string> RealFlavors() => new TheoryData<string> { SyncReal, AsyncReal };

    public static TheoryData<string, string, bool> MockThrows()
    {
        TheoryData<string, string, bool> data = new TheoryData<string, string, bool>();
        foreach (string flavor in new[] { SyncMock, AsyncMock })
        {
            foreach (string kind in new[] { ApiKind, InvalidCastKind, OtherKind })
            {
                data.Add(flavor, kind, true);
                data.Add(flavor, kind, false);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public async Task ApiException_ReachesTheCallbackOnce_AndFailsTheFuture(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using Producer producer = Producer.Create(flavor, Serdes.String(), value);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> future = producer.Send(
            new ProducerRecord<string, string>(Topic, "v", key: "k", partition: 3), callback);

        // The callback ran inside the call, on the caller's thread, as Java's doSend runs it.
        (RecordMetadata metadata, Exception? exception, int threadId) = Assert.Single(callback.Completions);
        Assert.Same(value.LastThrown, exception);
        Assert.Equal(Environment.CurrentManagedThreadId, threadId);
        AssertPlaceholder(metadata, partition: 3);

        Assert.Same(value.LastThrown, await FutureAssert.ThrowsAsync<RecordTooLargeException>(() => future));
        Assert.Single(callback.Completions);
        Assert.Equal(1, value.InvocationCount);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public async Task ApiException_WithoutACallback_FailsTheFuture(string flavor)
    {
        ThrowingSerializer<string> key = new ThrowingSerializer<string>(() => new TopicAuthorizationException("denied"));
        using Producer producer = Producer.Create(flavor, key, Serdes.String());

        Task<RecordMetadata> future = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), null);

        Assert.Same(key.LastThrown, await FutureAssert.ThrowsAsync<TopicAuthorizationException>(() => future));
        Assert.Equal(0, producer.HistoryCount());
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public void ApiException_WithoutAPartition_HasThePlaceholderPartition(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using Producer producer = Producer.Create(flavor, Serdes.String(), value);
        RecordingCallback callback = new RecordingCallback();

        _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback);

        AssertPlaceholder(Assert.Single(callback.Completions).Metadata, partition: RecordMetadata.UNKNOWN_PARTITION);
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public void ACallbackThrow_LeavesTheSend(string flavor)
    {
        // Java calls the callback without a try, so its throw leaves send.
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using Producer producer = Producer.Create(flavor, Serdes.String(), value);
        InvalidOperationException thrown = new InvalidOperationException("from-callback");

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() =>
        {
            _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), new ThrowingCallback(thrown));
        });

        Assert.Same(thrown, error);
    }

    [Theory]
    [InlineData(AsyncReal)]
    public async Task ApiException_TheFirstStageCompletes_AndTheFutureFailsOnceTheAsyncCallbackEnds(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using IAsyncProducer<string, string> producer = (IAsyncProducer<string, string>)Producer.CreateRaw(
            flavor, Serdes.String(), value);
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        GatedAsyncCallback callback = new GatedAsyncCallback(release.Task);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(
            new ProducerRecord<string, string>(Topic, "v", key: "k", partition: 1), callback);

        Assert.True(firstStage.IsCompletedSuccessfully);
        AsyncKafkaFuture<RecordMetadata> future = await firstStage;
        Assert.Equal(1, callback.Calls);
        Assert.Same(value.LastThrown, callback.Exception);
        AssertPlaceholder(callback.Metadata!, partition: 1);
        Assert.False(future.IsDone());

        release.SetResult(true);

        Assert.Same(
            value.LastThrown,
            await FutureAssert.ThrowsAsync<RecordTooLargeException>(() => TestTimeout.Run(() => future.Get(), s_deadline)));
        Assert.Equal(1, callback.Calls);
    }

    [Theory]
    [InlineData(AsyncReal)]
    public async Task ApiException_ASlowAsyncCallback_HoldsFlushUntilItEnds(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using IAsyncProducer<string, string> producer = (IAsyncProducer<string, string>)Producer.CreateRaw(
            flavor, Serdes.String(), value);
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        GatedAsyncCallback callback = new GatedAsyncCallback(release.Task);

        AsyncKafkaFuture<RecordMetadata> future = await producer.Send(
            new ProducerRecord<string, string>(Topic, "v", key: "k", partition: 1), callback);

        // Java's flush() returns only after the callbacks of the records it covers.
        Task flush = producer.Flush();
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        Assert.False(flush.IsCompleted);

        release.SetResult(true);

        await TestTimeout.Run(() => flush, s_deadline);
        Assert.True(future.IsDone());
    }

    // A nested serializer-failure callback must not clear the outer callback's in-callback flag, or the outer
    // callback's Flush would wait for itself.
    [Theory]
    [InlineData(AsyncReal)]
    public async Task ApiException_ANestedFailingSendInsideAnAsyncCallback_LetsItsFlushComplete(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using IAsyncProducer<string, string> producer = (IAsyncProducer<string, string>)Producer.CreateRaw(
            flavor, Serdes.String(), value);
        NestingAsyncCallback callback = new NestingAsyncCallback(producer);

        _ = await producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback);

        await TestTimeout.Run(() => callback.Ended, s_deadline);
        await TestTimeout.Run(() => producer.Flush(), s_deadline);
    }

    [Theory]
    [InlineData(AsyncReal)]
    public async Task ApiException_AFaultedAsyncCallback_StillFailsTheFutureWithTheSerializersException(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using IAsyncProducer<string, string> producer = (IAsyncProducer<string, string>)Producer.CreateRaw(
            flavor, Serdes.String(), value);

        AsyncKafkaFuture<RecordMetadata> future = await producer.Send(
            new ProducerRecord<string, string>(Topic, "v", key: "k"),
            new GatedAsyncCallback(Task.FromException(new InvalidOperationException("callback-faulted"))));

        Assert.Same(
            value.LastThrown,
            await FutureAssert.ThrowsAsync<RecordTooLargeException>(() => TestTimeout.Run(() => future.Get(), s_deadline)));
    }

    [Theory]
    [InlineData(AsyncReal)]
    public void ApiException_ACanceledToken_ThrowsBeforeTheCallback(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
        using IAsyncProducer<string, string> producer = (IAsyncProducer<string, string>)Producer.CreateRaw(
            flavor, Serdes.String(), value);
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource canceled = new CancellationTokenSource();
        canceled.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() =>
        {
            _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback, canceled.Token);
        });
        Assert.Empty(callback.Completions);
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public void InvalidCastException_FromTheKeySerializer_IsWrapped(string flavor)
    {
        InvalidCastSerializer key = new InvalidCastSerializer();
        using Producer producer = Producer.Create(flavor, key, Serdes.String());
        RecordingCallback callback = new RecordingCallback();

        SerializationException error = Assert.Throws<SerializationException>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback); });

        Assert.Equal(
            "Can't convert key of class System.String to class " + typeof(InvalidCastSerializer).FullName
            + " specified as the key serializer",
            error.Message);
        Assert.Same(key.LastThrown, error.InnerException);
        Assert.Empty(callback.Completions);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public void InvalidCastException_FromTheValueSerializer_IsWrapped(string flavor)
    {
        InvalidCastSerializer value = new InvalidCastSerializer();
        using Producer producer = Producer.Create(flavor, Serdes.String(), value);

        SerializationException error = Assert.Throws<SerializationException>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), null); });

        Assert.Equal(
            "Can't convert value of class System.String to class " + typeof(InvalidCastSerializer).FullName
            + " specified as the value serializer",
            error.Message);
        Assert.Same(value.LastThrown, error.InnerException);
    }

    [Theory]
    [MemberData(nameof(RealFlavors))]
    public void InvalidCastException_ForANullKey_NamesNull(string flavor)
    {
        InvalidCastSerializer key = new InvalidCastSerializer();
        using Producer producer = Producer.Create(flavor, key, Serdes.String());

        SerializationException error = Assert.Throws<SerializationException>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v"), null); });

        Assert.Equal(
            "Can't convert key of class null to class " + typeof(InvalidCastSerializer).FullName
            + " specified as the key serializer",
            error.Message);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public void AnyOtherException_LeavesTheSendAsItIs(string flavor)
    {
        ThrowingSerializer<string> value = new ThrowingSerializer<string>(() => new KafkaException("not an api error"));
        using Producer producer = Producer.Create(flavor, Serdes.String(), value);
        RecordingCallback callback = new RecordingCallback();

        KafkaException error = Assert.Throws<KafkaException>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback); });

        Assert.Same(value.LastThrown, error);
        Assert.Empty(callback.Completions);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Theory]
    [MemberData(nameof(MockThrows))]
    public void OnAMock_EverySerializerThrow_LeavesTheSendAsTheSameInstance(string flavor, string kind, bool onKey)
    {
        // Java's MockProducer calls the serializers with no try: no wrap, no callback, no history entry.
        ISerializer<string> failing = Failing(kind, out Func<Exception?> lastThrown);
        using Producer producer = onKey
            ? Producer.Create(flavor, failing, Serdes.String())
            : Producer.Create(flavor, Serdes.String(), failing);
        RecordingCallback callback = new RecordingCallback();

        Exception error = Assert.ThrowsAny<Exception>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback); });

        Assert.NotNull(lastThrown());
        Assert.Same(lastThrown(), error);
        Assert.Empty(callback.Completions);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Theory]
    [InlineData(ApiKind)]
    [InlineData(InvalidCastKind)]
    [InlineData(OtherKind)]
    public void OnTheAsyncMock_ASerializerThrow_LeavesTheSend_WithoutTheAsyncCallback(string kind)
    {
        ISerializer<string> failing = Failing(kind, out Func<Exception?> lastThrown);
        using AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(Serdes.String(), failing);
        GatedAsyncCallback callback = new GatedAsyncCallback(Task.CompletedTask);

        Exception error = Assert.ThrowsAny<Exception>(
            () => { _ = producer.Send(new ProducerRecord<string, string>(Topic, "v", key: "k"), callback); });

        Assert.Same(lastThrown(), error);
        Assert.Equal(0, callback.Calls);
        Assert.Equal(0, producer.HistoryCount());
    }

    private static ISerializer<string> Failing(string kind, out Func<Exception?> lastThrown)
    {
        switch (kind)
        {
            case ApiKind:
                ThrowingSerializer<string> api =
                    new ThrowingSerializer<string>(() => new RecordTooLargeException("too large"));
                lastThrown = () => api.LastThrown;
                return api;
            case InvalidCastKind:
                InvalidCastSerializer cast = new InvalidCastSerializer();
                lastThrown = () => cast.LastThrown;
                return cast;
            default:
                ThrowingSerializer<string> other =
                    new ThrowingSerializer<string>(() => new KafkaException("not an api error"));
                lastThrown = () => other.LastThrown;
                return other;
        }
    }

    private static void AssertPlaceholder(RecordMetadata metadata, int partition)
    {
        Assert.Equal(Topic, metadata.Topic());
        Assert.Equal(partition, metadata.Partition());
        Assert.False(metadata.HasOffset());
        Assert.Equal(-1L, metadata.Offset());
        Assert.False(metadata.HasTimestamp());
        Assert.Equal(-1, metadata.SerializedKeySize());
        Assert.Equal(-1, metadata.SerializedValueSize());
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    /// <summary>One of the four producers behind one send that returns the record's future as a task.</summary>
    private sealed class Producer : IDisposable
    {
        private readonly object _producer;

        private Producer(object producer)
        {
            _producer = producer;
        }

        internal static Producer Create(string flavor, ISerializer<string> key, ISerializer<string> value) =>
            new Producer(CreateRaw(flavor, key, value));

        internal static IDisposable CreateRaw(string flavor, ISerializer<string> key, ISerializer<string> value) =>
            flavor switch
            {
                SyncMock => new MockProducer<string, string>(key, value),
                SyncReal => new KafkaProducer<string, string>(Unreachable(), key, value),
                AsyncMock => new AsyncMockProducer<string, string>(key, value),
                _ => new AsyncKafkaProducer<string, string>(Unreachable(), key, value),
            };

        internal Task<RecordMetadata> Send(ProducerRecord<string, string> record, ICallback? callback)
        {
            if (_producer is IProducer<string, string> sync)
            {
                KafkaFuture<RecordMetadata> future = callback is null ? sync.Send(record) : sync.Send(record, callback);
                return Task.Run(() => future.Get());
            }

            IAsyncProducer<string, string> async = (IAsyncProducer<string, string>)_producer;
            ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage =
                callback is null ? async.Send(record) : async.Send(record, callback);

            // The first stage completes inside the call.
            Assert.True(firstStage.IsCompletedSuccessfully);
            return firstStage.Result.Get();
        }

        internal int HistoryCount() => _producer switch
        {
            MockProducer<string, string> mock => mock.HistoryCount(),
            AsyncMockProducer<string, string> mock => mock.HistoryCount(),
            _ => 0,
        };

        public void Dispose() => TestTimeout.Dispose((IDisposable)_producer);
    }

    private sealed class RecordingCallback : ICallback
    {
        private readonly List<(RecordMetadata Metadata, Exception? Exception, int ThreadId)> _completions =
            new List<(RecordMetadata Metadata, Exception? Exception, int ThreadId)>();

        internal IReadOnlyList<(RecordMetadata Metadata, Exception? Exception, int ThreadId)> Completions
        {
            get
            {
                lock (_completions)
                {
                    return _completions.ToArray();
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            lock (_completions)
            {
                _completions.Add((metadata, exception, Environment.CurrentManagedThreadId));
            }
        }
    }

    private sealed class ThrowingCallback : ICallback
    {
        private readonly Exception _thrown;

        internal ThrowingCallback(Exception thrown)
        {
            _thrown = thrown;
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => throw _thrown;
    }

    private sealed class NestingAsyncCallback : IAsyncCallback
    {
        private readonly IAsyncProducer<string, string> _producer;
        private readonly TaskCompletionSource<bool> _ended = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal NestingAsyncCallback(IAsyncProducer<string, string> producer)
        {
            _producer = producer;
        }

        internal Task Ended => _ended.Task;

        public async Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            try
            {
                await Task.Yield();
                _ = await _producer.Send(
                    new ProducerRecord<string, string>(Topic, "v2", key: "k2"),
                    new GatedAsyncCallback(Task.CompletedTask));
                await _producer.Flush();
                _ended.TrySetResult(true);
            }
            catch (Exception e)
            {
                _ended.TrySetException(e);
            }
        }
    }

    private sealed class GatedAsyncCallback : IAsyncCallback
    {
        private readonly Task _gate;
        private int _calls;

        internal GatedAsyncCallback(Task gate)
        {
            _gate = gate;
        }

        internal int Calls => Volatile.Read(ref _calls);

        internal RecordMetadata? Metadata { get; private set; }

        internal Exception? Exception { get; private set; }

        public Task OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Metadata = metadata;
            Exception = exception;
            Interlocked.Increment(ref _calls);
            return _gate;
        }
    }

    private sealed class InvalidCastSerializer : ISerializer<string>
    {
        internal Exception? LastThrown { get; private set; }

        public byte[]? Serialize(string topic, string data)
        {
            InvalidCastException thrown = new InvalidCastException("not a string");
            LastThrown = thrown;
            throw thrown;
        }
    }
}
