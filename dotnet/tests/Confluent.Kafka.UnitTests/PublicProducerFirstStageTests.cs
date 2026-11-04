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
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The first stage of the async <see cref="AsyncKafkaProducer{TKey, TValue}"/> send on an unreachable broker: a
/// queued record, the caller's token, a record the closed core refuses inside the call, and the pins the hand-over
/// releases.
/// </summary>
public sealed class PublicProducerFirstStageTests
{
    private const string WaitTimesOut = "the wait times out";
    private const string CloseFailsIt = "Close fails it";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_settle = TimeSpan.FromMilliseconds(300);
    private static readonly NativeMethods.ProducerVoidCallback s_coreClosed = OnCoreClosed;
    private static readonly CompletionThread.NotifyCallback s_recordingNotify = OnRecordingNotify;
    private static readonly ConcurrentQueue<int> s_notifiedThreads = new ConcurrentQueue<int>();

    [Theory]
    [InlineData(WaitTimesOut)]
    [InlineData(CloseFailsIt)]
    public async Task Send_CallerTokenFiresWhileTheRecordIsQueued_EndsTheFirstStageCanceledAfterTheHandOver(string outcome)
    {
        string topic = "first-stage-token-" + Guid.NewGuid().ToString("N");
        int maxBlockMs = outcome == WaitTimesOut ? 2000 : 60000;
        AsyncKafkaProducer<byte[], byte[]> producer = Unreachable(maxBlockMs);
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource cts = new CancellationTokenSource();
        try
        {
            (ValueTask<AsyncKafkaFuture<RecordMetadata>> stage, WeakReference value) =
                SendUnreferenced(producer, topic, callback, cts.Token);
            Assert.False(stage.IsCompleted, "the record was not queued");

            cts.Cancel();
            await Task.Delay(s_settle);
            Assert.False(stage.IsCompleted, "the token ended the first stage before the hand-over");

            await TestTimeout.Run(() => producer.Flush(), s_deadline);
            Assert.False(stage.IsCompleted, "Flush waited for the queued record");

            if (outcome == CloseFailsIt)
            {
                await TestTimeout.Run(() => producer.Close(), s_deadline);
            }

            OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => TestTimeout.Run(() => stage.AsTask(), s_deadline));
            Assert.Equal(cts.Token, canceled.CancellationToken);

            if (outcome == WaitTimesOut)
            {
                await callback.WaitForFirst(s_deadline);
            }

            await Task.Delay(s_settle);
            if (outcome == WaitTimesOut)
            {
                (RecordMetadata metadata, Exception? exception) = Assert.Single(callback.Calls());
                KafkaException timeout = Assert.IsAssignableFrom<KafkaException>(exception);
                Assert.Equal(7, timeout.Code);
                Assert.Equal($"Topic {topic} not present in metadata after 2000 ms.", timeout.Message);
                Assert.Equal(topic, metadata.Topic);
                Assert.Equal(-1L, metadata.Offset);
            }
            else
            {
                Assert.Empty(callback.Calls());
            }

            AssertCollected(value, "the hand-over did not release the value's pin");
        }
        finally
        {
            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task Send_CallerTokenFiresAfterTheHandOver_DoesNotCancelGet()
    {
        string topic = "first-stage-token-after-" + Guid.NewGuid().ToString("N");
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        RecordingCallback callback = new RecordingCallback();
        using CancellationTokenSource cts = new CancellationTokenSource();

        ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(
            new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v")), callback, cts.Token);
        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stage.AsTask(), s_deadline);
        Assert.False(future.IsDone, "the record's future was done before the token fired");

        cts.Cancel();
        await Task.Delay(s_settle);
        Assert.False(future.IsDone, "the token ended the record's future");
        Assert.Empty(callback.Calls());

        Assert.True(producer.CompleteNext());
        RecordMetadata metadata = await TestTimeout.Run(() => future.Get(), s_deadline);
        Assert.Equal(topic, metadata.Topic);
        Assert.Equal(0L, metadata.Offset);
        Assert.False(future.IsCancelled);

        await callback.WaitForFirst(s_deadline);
        await Task.Delay(s_settle);
        (RecordMetadata delivered, Exception? exception) = Assert.Single(callback.Calls());
        Assert.Null(exception);
        Assert.Equal(0L, delivered.Offset);
    }

    [Fact]
    public async Task AsyncSend_OnAClosedProducer_FaultsTheFirstStageInsideTheCall()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = Unreachable(60000);
        RecordingCallback callback = new RecordingCallback();
        try
        {
            CloseTheCore(producer);

            ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(
                new ProducerRecord<byte[], byte[]>("first-stage-closed", Encoding.UTF8.GetBytes("v")), callback);

            Assert.True(stage.IsFaulted, "the refusal did not fault the first stage inside the call");
            InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(() => stage.AsTask());
            Assert.Equal("Cannot perform operation after producer has been closed", refused.Message);

            await Task.Delay(s_settle);
            Assert.Empty(callback.Calls());
        }
        finally
        {
            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
        }

        Assert.Empty(callback.Calls());
    }

    [Fact]
    public async Task AsyncSend_UnreachableBroker_ReturnsAtOnce_AndTheFirstStageEndsWithAFailedFuture()
    {
        string topic = "first-stage-unreachable-" + Guid.NewGuid().ToString("N");
        AsyncKafkaProducer<byte[], byte[]> producer = Unreachable(1000);
        RecordingCallback callback = new RecordingCallback();
        try
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(
                new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v")), callback);
            elapsed.Stop();

            Assert.False(stage.IsCompleted, "Send waited for the hand-over");
            Assert.True(elapsed.ElapsedMilliseconds < 1000, $"Send took {elapsed.ElapsedMilliseconds} ms");

            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stage.AsTask(), s_deadline);
            KafkaException failure = await Assert.ThrowsAnyAsync<KafkaException>(
                () => TestTimeout.Run(() => future.Get(), s_deadline));
            Assert.Equal(7, failure.Code);
            Assert.Equal($"Topic {topic} not present in metadata after 1000 ms.", failure.Message);

            await Task.Delay(s_settle);
            (RecordMetadata _, Exception? delivered) = Assert.Single(callback.Calls());
            Assert.Same(failure, delivered);
            Assert.Equal(NativeProducer.PumpThreadName, callback.LastThreadName);
        }
        finally
        {
            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncSend_RecordTakenAtOnce_FirstStageIsCompleteWhenSendReturns()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        UnreachableProducers.HoldingCallback hold = new UnreachableProducers.HoldingCallback();
        try
        {
            _ = await TestTimeout.Run(() => producer.Send(MockRecord("first-stage-held"), hold).AsTask(), s_deadline);
            Assert.True(producer.CompleteNext());
            Assert.True(hold.WaitEntered(s_deadline), "the pump never ran the holding callback");

            ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(MockRecord("first-stage-at-once"));

            Assert.True(stage.IsCompletedSuccessfully, "the hand-over did not run inside the call while the pump was held");
            AsyncKafkaFuture<RecordMetadata> future = await stage;
            Assert.False(future.IsDone);
        }
        finally
        {
            hold.Release();
        }
    }

    [Fact]
    public async Task AsyncSend_NotifyHookFiredOnTheSendingThread_WakesThePump_WithoutADeadlock()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: true);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        NativeProducer native = NativeOf(producer);
        CompletionThread completion = (CompletionThread)typeof(NativeProducer)
            .GetField("_completion", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(native)!;
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ProducerSetCallbacksNotify(native.Handle, s_recordingNotify, completion.NotifyOpaque, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        RecordingCallback callback = new RecordingCallback();

        int sender = 0;
        Task<bool> send = Task.Factory.StartNew(
            () =>
            {
                sender = Environment.CurrentManagedThreadId;
                return producer.Send(MockRecord("notify-on-sender"), callback).IsCompletedSuccessfully;
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        Assert.True(await TestTimeout.Run(() => send, s_deadline), "the record was not taken at once");
        await callback.WaitForFirst(s_deadline);
        Assert.Equal(NativeProducer.PumpThreadName, callback.LastThreadName);
        Assert.Contains(sender, s_notifiedThreads);
    }

    private static AsyncKafkaProducer<byte[], byte[]> Unreachable(int maxBlockMs) =>
        new AsyncKafkaProducer<byte[], byte[]>(
            new Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:1",
                ["max.block.ms"] = maxBlockMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            },
            Serdes.ByteArray,
            Serdes.ByteArray);

    private static ProducerRecord<byte[], byte[]> MockRecord(string prefix) =>
        new ProducerRecord<byte[], byte[]>(prefix + "-" + Guid.NewGuid().ToString("N"), Encoding.UTF8.GetBytes("v"));

    private static NativeProducer NativeOf(object producer) =>
        (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;

    private static void OnRecordingNotify(IntPtr opaque)
    {
        try
        {
            s_notifiedThreads.Enqueue(Environment.CurrentManagedThreadId);
            CompletionThread.NotifyHook(opaque);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (ValueTask<AsyncKafkaFuture<RecordMetadata>> Stage, WeakReference Value) SendUnreferenced(
        AsyncKafkaProducer<byte[], byte[]> producer, string topic, IDeliveryCallback callback, CancellationToken token)
    {
        byte[] value = Encoding.UTF8.GetBytes("value-" + topic);
        return (producer.Send(new ProducerRecord<byte[], byte[]>(topic, value), callback, token), new WeakReference(value));
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

    internal static void CloseTheCore(object producer)
    {
        NativeProducer native = (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        ManualResetEventSlim closed = new ManualResetEventSlim();
        GCHandle opaque = GCHandle.Alloc(closed);
        NativeMethods.ProducerCloseCb(native.Handle, s_coreClosed, GCHandle.ToIntPtr(opaque));
        Assert.True(closed.Wait(s_deadline), "the core's close never completed");
    }

    private static void OnCoreClosed(IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(opaque);
            ManualResetEventSlim closed = (ManualResetEventSlim)handle.Target!;
            handle.Free();
            if (error != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(error);
            }

            closed.Set();
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    /// <summary>Records every delivery callback, with the thread it ran on.</summary>
    internal sealed class RecordingCallback : IDeliveryCallback
    {
        private readonly List<(RecordMetadata Metadata, Exception? Exception)> _calls =
            new List<(RecordMetadata Metadata, Exception? Exception)>();

        private readonly TaskCompletionSource<bool> _first =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal string? LastThreadName { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            lock (_calls)
            {
                _calls.Add((metadata, exception));
                LastThreadName = Thread.CurrentThread.Name;
            }

            _first.TrySetResult(true);
        }

        internal List<(RecordMetadata Metadata, Exception? Exception)> Calls()
        {
            lock (_calls)
            {
                return new List<(RecordMetadata Metadata, Exception? Exception)>(_calls);
            }
        }

        internal Task WaitForFirst(TimeSpan deadline) => TestTimeout.Run(() => _first.Task, deadline);
    }
}
