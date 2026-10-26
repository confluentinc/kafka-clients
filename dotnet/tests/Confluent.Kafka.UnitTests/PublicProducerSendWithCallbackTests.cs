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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;
using Xunit.Sdk;

using RecordingCallback = Confluent.Kafka.UnitTests.PublicProducerFirstStageTests.RecordingCallback;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The per-record <c>kafka_producer_Callback_t</c> of both real producers on an unreachable broker: one registration
/// per record, its state kept until the destroy after a refused send, the user callback before the future completes,
/// a throwing callback, and sync sends from inside a delivery callback.
/// </summary>
public sealed class PublicProducerSendWithCallbackTests
{
    private const string Sync = "sync";
    private const string Async = "async";
    private const int MaxBlockMs = 300;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_settle = TimeSpan.FromMilliseconds(300);

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task EveryRecord_HasItsOwnRegistration(string flavour)
    {
        string prefix = "own-registration-" + Guid.NewGuid().ToString("N") + "-";
        string[] topics = { prefix + "a", prefix + "b", prefix + "c" };
        RecordingCallback callback = new RecordingCallback();
        Dictionary<string, Exception> failures = new Dictionary<string, Exception>();

        if (flavour == Sync)
        {
            KafkaProducer<byte[], byte[]> producer = SyncUnreachable(MaxBlockMs);
            try
            {
                foreach (string topic in topics)
                {
                    KafkaFuture<RecordMetadata> future = default;
                    TestTimeout.Run(() => future = producer.Send(Record(topic), callback), s_deadline);
                    failures[topic] = Capture(() => future.Get());
                }
            }
            finally
            {
                TestTimeout.Run(producer.Dispose, s_deadline);
            }
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = AsyncUnreachable(MaxBlockMs);
            try
            {
                ValueTask<AsyncKafkaFuture<RecordMetadata>>[] stages =
                    topics.Select(topic => producer.Send(Record(topic), callback)).ToArray();
                for (int i = 0; i < topics.Length; i++)
                {
                    AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stages[i].AsTask(), s_deadline);
                    failures[topics[i]] = await Assert.ThrowsAnyAsync<Exception>(
                        () => TestTimeout.Run(() => future.Get(), s_deadline));
                }
            }
            finally
            {
                await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
            }
        }

        List<(RecordMetadata Metadata, Exception? Exception)> calls = callback.Calls();
        Assert.Equal(topics.OrderBy(t => t, StringComparer.Ordinal), calls.Select(c => c.Metadata.Topic).OrderBy(t => t, StringComparer.Ordinal));
        foreach ((RecordMetadata metadata, Exception? exception) in calls)
        {
            KafkaException timeout = Assert.IsAssignableFrom<KafkaException>(exception);
            Assert.Same(failures[metadata.Topic], timeout);
            Assert.Equal(7, timeout.Code);
            Assert.Equal($"Topic {metadata.Topic} not present in metadata after {MaxBlockMs} ms.", timeout.Message);
        }
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void ARefusedSend_KeepsItsStateUntilTheDestroyHasReturned(string flavour)
    {
        IDisposable producer = flavour == Sync ? SyncUnreachable(60000) : AsyncUnreachable(60000);
        WeakReference callback;
        try
        {
            PublicProducerFirstStageTests.CloseTheCore(producer);

            (callback, Exception? refused) = SendRefused(producer);
            InvalidOperationException closed = Assert.IsType<InvalidOperationException>(refused);
            Assert.Equal("Cannot perform operation after producer has been closed", closed.Message);

            AssertAlive(callback, "the state of a refused send was released before the destroy");
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }

        AssertCollected(callback, "the destroy did not release the state of a refused send");
    }

    [Fact]
    public void Callback_RunsBeforeTheFutureCompletes()
    {
        KafkaProducer<byte[], byte[]> producer = SyncUnreachable(MaxBlockMs);
        OrderProbe probe = new OrderProbe();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    KafkaFuture<RecordMetadata> future = producer.Send(Record("order-" + Guid.NewGuid().ToString("N")), probe);

                    Thread getter = new Thread(() =>
                    {
                        _ = Capture(() => future.Get());
                        Volatile.Write(ref probe.GetReturned, true);
                    });
                    getter.IsBackground = true;
                    getter.Start();
                    SpinWait.SpinUntil(() => (getter.ThreadState & ThreadState.WaitSleepJoin) != 0, s_deadline);

                    probe.Publish(future);
                    Assert.True(getter.Join(s_deadline), "Get did not return after the callback");
                },
                s_deadline);

            Assert.Null(probe.Failure);
            Assert.False(probe.DoneInsideTheCallback, "the future completed before the callback ran");
            Assert.False(probe.GetReturnedInsideTheCallback, "a blocked Get returned before the callback ran");
            Assert.True(Volatile.Read(ref probe.GetReturned));
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task AThrowingCallback_IsTracedOnce_AndTheFutureKeepsTheRecordsOutcome(string flavour)
    {
        string marker = "throwing-callback-" + Guid.NewGuid().ToString("N");
        string topic = marker + "-topic";
        string other = marker + "-other";
        ThrowingCallback throwing = new ThrowingCallback(marker);
        RecordingCallback next = new RecordingCallback();
        using TraceCapture trace = TraceCapture.Start();

        Exception outcome;
        Exception otherOutcome;
        if (flavour == Sync)
        {
            KafkaProducer<byte[], byte[]> producer = SyncUnreachable(MaxBlockMs);
            try
            {
                KafkaFuture<RecordMetadata> future = default;
                KafkaFuture<RecordMetadata> otherFuture = default;
                TestTimeout.Run(
                    () =>
                    {
                        future = producer.Send(Record(topic), throwing);
                        otherFuture = producer.Send(Record(other), next);
                    },
                    s_deadline);
                outcome = Capture(() => future.Get());
                otherOutcome = Capture(() => otherFuture.Get());
            }
            finally
            {
                TestTimeout.Run(producer.Dispose, s_deadline);
            }
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = AsyncUnreachable(MaxBlockMs);
            try
            {
                ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(Record(topic), throwing);
                ValueTask<AsyncKafkaFuture<RecordMetadata>> otherStage = producer.Send(Record(other), next);
                AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stage.AsTask(), s_deadline);
                AsyncKafkaFuture<RecordMetadata> otherFuture = await TestTimeout.Run(() => otherStage.AsTask(), s_deadline);
                outcome = await Assert.ThrowsAnyAsync<Exception>(() => TestTimeout.Run(() => future.Get(), s_deadline));
                otherOutcome = await Assert.ThrowsAnyAsync<Exception>(
                    () => TestTimeout.Run(() => otherFuture.Get(), s_deadline));
            }
            finally
            {
                await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
            }
        }

        KafkaException timeout = Assert.IsAssignableFrom<KafkaException>(outcome);
        Assert.Equal(7, timeout.Code);
        Assert.Equal($"Topic {topic} not present in metadata after {MaxBlockMs} ms.", timeout.Message);
        Assert.Equal(1, throwing.Count);

        (RecordMetadata otherMetadata, Exception? otherDelivered) = Assert.Single(next.Calls());
        Assert.Equal(other, otherMetadata.Topic);
        Assert.Same(otherOutcome, otherDelivered);

        Assert.Single(trace.LinesContaining(marker));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Send_WaitingForMetadata_KeepsTheTimeoutInTheFuture(string flavour)
    {
        string topic = "waiting-for-metadata-" + Guid.NewGuid().ToString("N");
        RecordingCallback callback = new RecordingCallback();
        Exception failure;
        if (flavour == Sync)
        {
            KafkaProducer<byte[], byte[]> producer = SyncUnreachable(MaxBlockMs);
            try
            {
                KafkaFuture<RecordMetadata> future = default;
                TestTimeout.Run(() => future = producer.Send(Record(topic), callback), s_deadline);
                failure = Capture(() => future.Get());
                await callback.WaitForFirst(s_deadline);
                Assert.Equal(NativeProducer.CompletionThreadName, callback.LastThreadName);
            }
            finally
            {
                TestTimeout.Run(producer.Dispose, s_deadline);
            }
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = AsyncUnreachable(MaxBlockMs);
            try
            {
                AsyncKafkaFuture<RecordMetadata> future =
                    await TestTimeout.Run(() => producer.Send(Record(topic), callback).AsTask(), s_deadline);
                failure = await Assert.ThrowsAnyAsync<Exception>(() => TestTimeout.Run(() => future.Get(), s_deadline));
                await callback.WaitForFirst(s_deadline);
                Assert.Equal(NativeProducer.PumpThreadName, callback.LastThreadName);
            }
            finally
            {
                await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
            }
        }

        KafkaException timeout = Assert.IsAssignableFrom<KafkaException>(failure);
        Assert.Equal(7, timeout.Code);
        Assert.True(timeout.IsRetriable);
        Assert.Equal($"Topic {topic} not present in metadata after {MaxBlockMs} ms.", timeout.Message);

        await Task.Delay(s_settle);
        (RecordMetadata metadata, Exception? delivered) = Assert.Single(callback.Calls());
        Assert.Same(timeout, delivered);
        Assert.Equal(topic, metadata.Topic);
        Assert.Equal(-1, metadata.Partition);
        Assert.Equal(-1L, metadata.Offset);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SyncSend_FromADeliveryCallback_WaitsOnThePumpAndDoesNotDeadlock(bool betweenTwoProducers)
    {
        string prefix = "send-from-callback-" + Guid.NewGuid().ToString("N") + "-";
        KafkaProducer<byte[], byte[]> first = SyncUnreachable(MaxBlockMs);
        KafkaProducer<byte[], byte[]> second = betweenTwoProducers ? SyncUnreachable(MaxBlockMs) : first;
        RecordingCallback last = new RecordingCallback();
        NestedSendCallback middle = new NestedSendCallback(first, Record(prefix + "3"), last);
        NestedSendCallback outer = new NestedSendCallback(second, Record(prefix + "2"), middle);
        try
        {
            KafkaFuture<RecordMetadata> future = default;
            TestTimeout.Run(() => future = first.Send(Record(prefix + "1"), outer), s_deadline);

            Exception firstOutcome = Capture(() => future.Get());
            KafkaFuture<RecordMetadata> secondFuture = outer.Nested(s_deadline);
            KafkaFuture<RecordMetadata> thirdFuture = middle.Nested(s_deadline);
            Exception secondOutcome = Capture(() => secondFuture.Get());
            Exception thirdOutcome = Capture(() => thirdFuture.Get());

            int index = 1;
            foreach (Exception outcome in new[] { firstOutcome, secondOutcome, thirdOutcome })
            {
                KafkaException timeout = Assert.IsAssignableFrom<KafkaException>(outcome);
                Assert.Equal(7, timeout.Code);
                Assert.Equal($"Topic {prefix}{index} not present in metadata after {MaxBlockMs} ms.", timeout.Message);
                index++;
            }

            Assert.Equal(NativeProducer.CompletionThreadName, outer.ThreadName);
            Assert.Equal(NativeProducer.CompletionThreadName, middle.ThreadName);
            (RecordMetadata metadata, Exception? delivered) = Assert.Single(last.Calls());
            Assert.Equal(prefix + "3", metadata.Topic);
            Assert.Same(thirdOutcome, delivered);
        }
        finally
        {
            TestTimeout.Run(first.Dispose, s_deadline);
            if (betweenTwoProducers)
            {
                TestTimeout.Run(second.Dispose, s_deadline);
            }
        }
    }

    private static ProducerRecord<byte[], byte[]> Record(string topic) =>
        new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v"));

    private static Dictionary<string, string> Unreachable(int maxBlockMs) =>
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = maxBlockMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };

    private static KafkaProducer<byte[], byte[]> SyncUnreachable(int maxBlockMs) =>
        new KafkaProducer<byte[], byte[]>(Unreachable(maxBlockMs), Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncKafkaProducer<byte[], byte[]> AsyncUnreachable(int maxBlockMs) =>
        new AsyncKafkaProducer<byte[], byte[]>(Unreachable(maxBlockMs), Serdes.ByteArray, Serdes.ByteArray);

    private static Exception Capture(Action action)
    {
        try
        {
            action();
        }
        catch (Exception e)
        {
            return e;
        }

        throw new XunitException("the call did not throw");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Callback, Exception? Refused) SendRefused(IDisposable producer)
    {
        RecordingCallback callback = new RecordingCallback();
        WeakReference reference = new WeakReference(callback);
        if (producer is KafkaProducer<byte[], byte[]> sync)
        {
            try
            {
                _ = sync.Send(Record("refused-sync"), callback);
            }
            catch (Exception e)
            {
                return (reference, e);
            }

            return (reference, null);
        }

        Task<AsyncKafkaFuture<RecordMetadata>> stage =
            ((AsyncKafkaProducer<byte[], byte[]>)producer).Send(Record("refused-async"), callback).AsTask();
        Assert.True(stage.IsFaulted, "the refusal did not fault the first stage inside the call");
        return (reference, stage.Exception?.InnerException);
    }

    private static void AssertAlive(WeakReference reference, string message)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.True(reference.IsAlive, message);
    }

    private static void AssertCollected(WeakReference reference, string message)
    {
        for (int i = 0; i < 5 && reference.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(reference.IsAlive, message);
    }

    /// <summary>Waits for the test to publish the record's future, then reads it from inside the callback.</summary>
    private sealed class OrderProbe : IDeliveryCallback
    {
        private readonly ManualResetEventSlim _published = new ManualResetEventSlim();
        private KafkaFuture<RecordMetadata> _future;

        internal bool GetReturned;

        internal bool DoneInsideTheCallback { get; private set; }

        internal bool GetReturnedInsideTheCallback { get; private set; }

        internal string? Failure { get; private set; }

        internal void Publish(KafkaFuture<RecordMetadata> future)
        {
            _future = future;
            _published.Set();
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            if (!_published.Wait(s_deadline))
            {
                Failure = "the test never published the future";
                return;
            }

            DoneInsideTheCallback = _future.IsDone;
            GetReturnedInsideTheCallback = Volatile.Read(ref GetReturned);
        }
    }

    /// <summary>Throws from <see cref="OnCompletion"/>, with its marker in the message.</summary>
    private sealed class ThrowingCallback : IDeliveryCallback
    {
        private readonly string _marker;
        private int _count;

        internal ThrowingCallback(string marker)
        {
            _marker = marker;
        }

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _count);
            throw new InvalidOperationException(_marker);
        }
    }

    /// <summary>Sends one more record, through the sync <c>Send</c>, from inside its own delivery callback.</summary>
    private sealed class NestedSendCallback : IDeliveryCallback
    {
        private readonly KafkaProducer<byte[], byte[]> _producer;
        private readonly ProducerRecord<byte[], byte[]> _record;
        private readonly IDeliveryCallback _callback;
        private readonly TaskCompletionSource<KafkaFuture<RecordMetadata>> _nested =
            new TaskCompletionSource<KafkaFuture<RecordMetadata>>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal NestedSendCallback(KafkaProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback)
        {
            _producer = producer;
            _record = record;
            _callback = callback;
        }

        internal string? ThreadName { get; private set; }

        internal KafkaFuture<RecordMetadata> Nested(TimeSpan deadline) =>
            _nested.Task.Wait(deadline) ? _nested.Task.Result : throw new XunitException("the nested send never ran");

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            ThreadName = Thread.CurrentThread.Name;
            try
            {
                _nested.TrySetResult(_producer.Send(_record, _callback));
            }
            catch (Exception e)
            {
                _nested.TrySetException(e);
            }
        }
    }
}
