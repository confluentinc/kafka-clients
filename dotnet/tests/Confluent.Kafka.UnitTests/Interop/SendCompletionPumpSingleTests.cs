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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P4.2 slice S2 — the send-completion pump's <b>single-future path</b>
/// (<c>SendCompletionPump.EnqueueSingle</c> / <c>ProcessSingle</c>) and the D9 guard on
/// <c>SyncCompletion.Get</c>, exercised directly against the pump before <c>NativeProducer.Send</c> is
/// switched onto it (S3).
/// </summary>
/// <remarks>
/// <para>
/// <b>Futures.</b> Where the pump <em>reads</em> a future, it is a real one, from
/// <c>Producer_send</c> on a mock producer — the singular <c>get</c> is not null-safe. Where nothing
/// reads it (the gate already closed or the loop already exited), <see cref="IntPtr.Zero"/> stands in:
/// every destroy is null-safe. T5 uses a real future even though the correct gate never reads it,
/// because the mutation it guards against (the gate skipping its stopped check) <em>would</em> read it.
/// </para>
/// <para>
/// <b>Waits.</b> Every wait is bounded: a latch is polled through <c>IsDone</c> with a deadline before
/// any <c>Get</c>, so a hang fails the test instead of stalling the run, and every pump is stopped
/// through <see cref="TestTimeout"/> after the mock's pending records are resolved.
/// </para>
/// <para>
/// <b>T8 has no test here.</b> <c>RunLoopFault_FaultsASingle_WithThePumpFailureMessage</c> needs a throw
/// out of <c>ProcessSingle</c>, and the pump has no fault-injection seam for one (nothing short of a
/// failing native <c>get</c>, an allocation failure or a stale native reaches its <c>catch</c>). None is
/// invented here, so that row is a structural review check.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpSingleTests
{
    private const string Topic = "pump-single-topic";

    private const string PumpThreadName = "confluent-kafka-producer-send-pump";

    private const string TeardownMessage = "The producer was closed before the send completed.";

    private const string GetOnOwningPumpMessage =
        "KafkaFuture.Get() was called on this producer's send-completion thread — from inside a delivery callback — " +
        "for a send that has not completed; that would deadlock. Wait for it from another thread.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly byte[] s_value = { 1, 2, 3 };

    [Fact]
    public void Single_Resolved_CompletesWithTheMetadata()
    {
        // T1.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        bool doneWhenFired = true;
        RecordingCallback callback = new RecordingCallback((_, _) => doneWhenFired = completion.IsDone);
        try
        {
            pump.EnqueueSingle(Send(producer), completion, new DeliveryRegistration(callback, Topic, 0));
            WaitDone(completion);
        }
        finally
        {
            StopPump(pump, producer);
        }

        RecordMetadata metadata = completion.Get();
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);

        // The callback saw the very object the latch holds, with no exception, BEFORE the latch opened
        // — and, counted after Stop's join (nothing can fire later), exactly once.
        Assert.Equal(1, callback.Invocations);
        Assert.Same(metadata, callback.Metadata);
        Assert.Null(callback.Exception);
        Assert.False(doneWhenFired, "the latch was already open when the delivery callback ran");
    }

    [Fact]
    public void Single_Failed_FiresThePlaceholderThenFaults_InJavaOrder()
    {
        // T2. The completion arrives as a failure: the callback gets Java's -1 placeholder (non-null,
        // carrying the record's topic and explicit partition) and the very exception the latch then
        // rethrows — and runs before the latch opens (ProducerBatch.java:303-323: set → callbacks → done).
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        bool doneWhenFired = true;
        RecordingCallback callback = new RecordingCallback((_, _) => doneWhenFired = completion.IsDone);
        try
        {
            IntPtr future = Send(producer, partition: 3);
            Assert.True(producer.MockErrorNext(2, "pump-single-boom"));
            pump.EnqueueSingle(future, completion, new DeliveryRegistration(callback, Topic, 3));
            WaitDone(completion);
        }
        finally
        {
            StopPump(pump, producer);
        }

        KafkaException thrown = Assert.Throws<KafkaException>(() => completion.Get());
        Assert.Equal(2, thrown.Code);
        Assert.Contains("pump-single-boom", thrown.Message, StringComparison.Ordinal);

        Assert.Equal(1, callback.Invocations);
        Assert.Same(thrown, callback.Exception);
        RecordMetadata placeholder = callback.Metadata!;
        Assert.Equal(Topic, placeholder.Topic);
        Assert.Equal(3, placeholder.Partition);
        Assert.Equal(-1L, placeholder.Offset);
        Assert.Equal(-1L, placeholder.Timestamp);
        Assert.False(doneWhenFired, "the latch was already faulted when the delivery callback ran");
    }

    [Fact]
    public void Single_FiresOnThePumpThread()
    {
        // T3.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        string? firedOnName = null;
        int firedOnId = -1;
        RecordingCallback callback = new RecordingCallback((_, _) =>
        {
            firedOnName = Thread.CurrentThread.Name;
            firedOnId = Environment.CurrentManagedThreadId;
        });
        try
        {
            pump.EnqueueSingle(Send(producer), completion, new DeliveryRegistration(callback, Topic, 0));
            WaitDone(completion);
        }
        finally
        {
            StopPump(pump, producer);
        }

        Assert.Equal(PumpThreadName, firedOnName);
        Assert.NotEqual(Environment.CurrentManagedThreadId, firedOnId);
    }

    [Fact]
    public void Singles_CompleteInFifoOrder()
    {
        // T4 — D1 (a): one singular get per single, strictly FIFO, never coalesced. On a MANUAL mock each
        // CompleteNext resolves the oldest pending record, so it must complete exactly the next single
        // while every later one stays pending. A pump that coalesced singles into one get_all would
        // complete NONE until all had resolved (get_all returns only when every future in it has); one
        // that read them out of order would sit in an unresolved get and never complete the oldest.
        const int Singles = 3;

        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata>[] completions = new SyncCompletion<RecordMetadata>[Singles];
        ConcurrentQueue<int> fired = new ConcurrentQueue<int>();
        try
        {
            for (int i = 0; i < Singles; i++)
            {
                int index = i;
                completions[i] = new SyncCompletion<RecordMetadata>(pump);
                RecordingCallback callback = new RecordingCallback((_, _) => fired.Enqueue(index));
                pump.EnqueueSingle(Send(producer), completions[i], new DeliveryRegistration(callback, Topic, 0));
            }

            for (int i = 0; i < Singles; i++)
            {
                Assert.True(producer.MockCompleteNext(), $"the mock had no pending record left at {i}");
                WaitDone(completions[i]);

                for (int later = i + 1; later < Singles; later++)
                {
                    Assert.False(completions[later].IsDone, $"single {later} completed before its record resolved");
                }

                Assert.Equal((long)i, completions[i].Get().Offset);
            }
        }
        finally
        {
            StopPump(pump, producer);
        }

        Assert.Equal(new[] { 0, 1, 2 }, fired.ToArray());
    }

    [Fact]
    public void CloseGate_ThenEnqueueSingle_FaultsInPlaceSynchronously_WithTheTeardownMessage_AndDoesNotFire()
    {
        // T5 — the gate EnqueueSingle shares with Enqueue. Once it has closed, the single is faulted and
        // its future freed before EnqueueSingle returns, with no callback (recorded residual 1).
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        RecordingCallback callback = new RecordingCallback();
        try
        {
            pump.CloseGate();
            pump.EnqueueSingle(Send(producer), completion, new DeliveryRegistration(callback, Topic, 0));

            // Synchronously faulted: the latch is complete the moment EnqueueSingle returns (so the Get
            // below cannot block).
            Assert.True(completion.IsDone, "the gate was closed, but the single was not faulted in place");
            KafkaException thrown = Assert.Throws<KafkaException>(() => completion.Get());
            Assert.Equal(TeardownMessage, thrown.Message);
        }
        finally
        {
            StopPump(pump, producer);
        }

        Assert.Equal(0, callback.Invocations);

        // Never queued, so never taken off the queue.
        Assert.Equal(0L, pump.DrainedSendCount);
    }

    [Fact]
    public void Stop_WithAQueuedSingle_FaultsItWithTheTeardownMessage_AndDoesNotFire()
    {
        // T6 — Stop's terminal drain. The loop is made to exit before it can take the single (the
        // SendCompletionPumpDrainCapTests injection: _stopping set before the enqueue, so the loop breaks
        // at its next top), which leaves the single for DrainAndFaultRemaining on every run. Nothing
        // reads the future, so IntPtr.Zero stands in (the destroy is null-safe).
        SendCompletionPump pump = new SendCompletionPump();
        StopTheLoopWithoutDraining(pump);

        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        RecordingCallback callback = new RecordingCallback();
        pump.EnqueueSingle(IntPtr.Zero, completion, new DeliveryRegistration(callback, Topic, 0));

        TestTimeout.Run(pump.Stop, s_deadline);

        Assert.True(completion.IsDone, "Stop's terminal drain left the queued single unsettled");
        KafkaException thrown = Assert.Throws<KafkaException>(() => completion.Get());
        Assert.Equal(TeardownMessage, thrown.Message);
        Assert.Equal(0, callback.Invocations);

        // Taken off the queue once — by the drain, not by the loop.
        Assert.Equal(1L, pump.DrainedSendCount);
    }

    [Fact]
    public void WaitForQueueDrain_SeesSingles()
    {
        // T7 — the pre-stop drain. Two singles on a MANUAL mock: neither can resolve, so the pump is in
        // the first one's get (or has not taken it yet) and the second is still queued — the wait must
        // see it. Then teardown's order: close the gate, resolve both (the teardown flush's role), wait
        // for the drain, stop. Both are COMPLETED, not faulted.
        const int Singles = 2;

        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: false);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata>[] completions = new SyncCompletion<RecordMetadata>[Singles];
        RecordingCallback callback = new RecordingCallback();
        try
        {
            for (int i = 0; i < Singles; i++)
            {
                completions[i] = new SyncCompletion<RecordMetadata>(pump);
                pump.EnqueueSingle(Send(producer), completions[i], new DeliveryRegistration(callback, Topic, 0));
            }

            Assert.False(pump.WaitForQueueDrain(TimeSpan.Zero), "a queued single was not seen by the pre-stop drain");

            pump.CloseGate();
            for (int i = 0; i < Singles; i++)
            {
                Assert.True(producer.MockCompleteNext(), $"the mock had no pending record left at {i}");
            }

            Assert.True(pump.WaitForQueueDrain(s_deadline), "the queue did not drain once both records resolved");
        }
        finally
        {
            StopPump(pump, producer);
        }

        for (int i = 0; i < Singles; i++)
        {
            Assert.True(completions[i].IsDone, $"single {i} was left unsettled");
            Assert.Equal((long)i, completions[i].Get().Offset);
        }

        Assert.Equal(Singles, callback.Invocations);
        Assert.Equal((long)Singles, pump.DrainedSendCount);
    }

    [Fact]
    public void DrainedSendCount_CountsASingleAsOne()
    {
        // T9 — and the pump's entry handling whichever kinds appear: a single, a two-record group, a
        // single. DrainedSendCount counts RECORDS, so 1 + 2 + 1; ProcessedBatchCount counts get_all
        // passes, which only the group runs. The group is the DrainCap tests' stand-in (IntPtr.Zero
        // futures, which get_all settles as faulted).
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> first = new SyncCompletion<RecordMetadata>(pump);
        SyncCompletion<RecordMetadata> last = new SyncCompletion<RecordMetadata>(pump);
        TaskCompletionSource<RecordMetadata>[] group =
        {
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously),
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        try
        {
            pump.EnqueueSingle(Send(producer), first, delivery: null);
            pump.Enqueue(new IntPtr[2], group, new DeliveryRegistration?[2], count: 2);
            pump.EnqueueSingle(Send(producer), last, delivery: null);

            // FIFO: once the last single is complete, everything queued before it has been processed.
            WaitDone(last);
        }
        finally
        {
            StopPump(pump, producer);
        }

        Assert.Equal(4L, pump.DrainedSendCount);
        Assert.Equal(1L, pump.ProcessedBatchCount);
        Assert.True(first.IsDone);
        Assert.Equal(0L, first.Get().Offset);
        Assert.Equal(1L, last.Get().Offset);
        Assert.True(group[0].Task.IsCompleted && group[1].Task.IsCompleted, "the group between the singles was left unsettled");
    }

    [Fact]
    public void Get_OnTheOwningPump_NotDone_Throws_WithItsMessage()
    {
        // T10 — D9. The callback runs on the pump BEFORE the latch opens, so a Get on its own send's
        // latch from inside it is the self-wait that would deadlock: the thread that must complete the
        // latch is this one. It must throw instead, and the send itself must be unaffected.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pump);
        Exception? fromGet = null;
        bool getReturned = false;
        RecordingCallback callback = new RecordingCallback((_, _) =>
        {
            try
            {
                completion.Get();
                getReturned = true;
            }
            catch (Exception exception)
            {
                fromGet = exception;
            }
        });
        try
        {
            pump.EnqueueSingle(Send(producer), completion, new DeliveryRegistration(callback, Topic, 0));
            WaitDone(completion);
        }
        finally
        {
            // Unwedge a pump whose guard did not fire (its callback would be blocked in Get forever): a
            // no-op when the latch is already complete.
            completion.TrySetException(new InvalidOperationException("test cleanup"));
            StopPump(pump, producer);
        }

        InvalidOperationException guard = Assert.IsType<InvalidOperationException>(fromGet);
        Assert.Equal(GetOnOwningPumpMessage, guard.Message);
        Assert.False(getReturned);
        Assert.Equal(0L, completion.Get().Offset);
    }

    [Fact]
    public void Get_OnTheOwningPump_AlreadyDone_Returns()
    {
        // T11 — D9 applies only to a latch that is not yet complete. The second single's callback, on the
        // same pump, reads the first single's latch, which the pump completed before taking the second.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pump = new SendCompletionPump();
        SyncCompletion<RecordMetadata> first = new SyncCompletion<RecordMetadata>(pump);
        SyncCompletion<RecordMetadata> second = new SyncCompletion<RecordMetadata>(pump);
        RecordMetadata? fromGet = null;
        Exception? getFailure = null;
        RecordingCallback callback = new RecordingCallback((_, _) =>
        {
            try
            {
                fromGet = first.Get();
            }
            catch (Exception exception)
            {
                getFailure = exception;
            }
        });
        try
        {
            pump.EnqueueSingle(Send(producer), first, delivery: null);
            pump.EnqueueSingle(Send(producer), second, new DeliveryRegistration(callback, Topic, 0));
            WaitDone(second);
        }
        finally
        {
            StopPump(pump, producer);
        }

        Assert.Null(getFailure);
        Assert.Same(first.Get(), fromGet);
    }

    [Fact]
    public void Get_OnAnotherProducersPump_DoesNotThrow()
    {
        // T12 — D9 is keyed on the latch's OWNER, not on "any pump". A callback on pump A waits on a latch
        // owned by pump B — not yet complete — and must simply block until it is completed. The latch is
        // completed only after A's thread is seen parked inside that Get, so the guard's decision is the
        // one actually exercised (completing it first would take Get's already-done fast path instead).
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SendCompletionPump pumpA = new SendCompletionPump();
        SendCompletionPump pumpB = new SendCompletionPump();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>(pumpA);
        SyncCompletion<RecordMetadata> otherProducers = new SyncCompletion<RecordMetadata>(pumpB);
        RecordMetadata otherValue = new RecordMetadata(Topic, 5, 42L, 7L);
        using ManualResetEventSlim entered = new ManualResetEventSlim(initialState: false);
        Thread? waiter = null;
        RecordMetadata? fromGet = null;
        Exception? getFailure = null;
        RecordingCallback callback = new RecordingCallback((_, _) =>
        {
            waiter = Thread.CurrentThread;
            entered.Set();
            try
            {
                fromGet = otherProducers.Get();
            }
            catch (Exception exception)
            {
                getFailure = exception;
            }
        });
        try
        {
            pumpA.EnqueueSingle(Send(producer), completion, new DeliveryRegistration(callback, Topic, 0));

            Assert.True(entered.Wait(s_deadline), "pump A never ran the delivery callback");
            Assert.True(
                SpinWait.SpinUntil(() => (waiter!.ThreadState & ThreadState.WaitSleepJoin) != 0, s_deadline),
                "pump A's thread never blocked");
            Assert.True(otherProducers.TrySetResult(otherValue));

            WaitDone(completion);
        }
        finally
        {
            // Release a callback a broken implementation may still have parked, then stop both pumps.
            otherProducers.TrySetResult(otherValue);
            StopPump(pumpA, producer);
            TestTimeout.Run(pumpB.Stop, s_deadline);
        }

        Assert.Null(getFailure);
        Assert.Same(otherValue, fromGet);
        Assert.Equal(0L, completion.Get().Offset);
    }

    /// <summary>One <c>Producer_send</c> on the mock — a real future the pump may read.</summary>
    private static IntPtr Send(NativeProducer producer, int partition = 0) =>
        ProducerSendMarshal.Send(producer.Handle, Topic, partition, -1L, key: null, new ReadOnlyMemory<byte>(s_value));

    /// <summary>Waits — bounded — for the latch to complete, so a later <c>Get</c> cannot block.</summary>
    private static void WaitDone(SyncCompletion<RecordMetadata> completion) =>
        Assert.True(SpinWait.SpinUntil(() => completion.IsDone, s_deadline), $"the latch did not complete within {s_deadline}");

    /// <summary>
    /// Resolves whatever the mock still holds — so a pump a broken implementation left in a
    /// <c>get</c> can finish it — then stops the pump under a deadline, so a hang fails instead of
    /// stalling the run.
    /// </summary>
    private static void StopPump(SendCompletionPump pump, NativeProducer producer)
    {
        while (producer.MockCompleteNext())
        {
        }

        TestTimeout.Run(pump.Stop, s_deadline);
    }

    /// <summary>
    /// Sets the pump's <c>_stopping</c> flag without joining, so the loop exits at its next top and
    /// leaves everything subsequently enqueued for <c>Stop</c>'s terminal drain — the same injection,
    /// with the same warrant, as <c>SendCompletionPumpDrainCapTests.StopTheLoopWithoutDraining</c>.
    /// </summary>
    private static void StopTheLoopWithoutDraining(SendCompletionPump pump)
    {
        FieldInfo stopping = typeof(SendCompletionPump).GetField(
            "_stopping", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                "SendCompletionPump no longer exposes a _stopping flag — this injection needs to " +
                "be re-derived against the new shape rather than silently skipped.");

        Assert.False((bool)stopping.GetValue(pump)!, "the pump was already stopping");
        stopping.SetValue(pump, true);
    }

    /// <summary>
    /// A delivery callback that records what it was handed and how often, then runs an optional probe
    /// on the thread that invoked it.
    /// </summary>
    private sealed class RecordingCallback : IDeliveryCallback
    {
        private readonly Action<RecordMetadata, KafkaException?>? _probe;
        private int _invocations;

        internal RecordingCallback(Action<RecordMetadata, KafkaException?>? probe = null) => _probe = probe;

        internal int Invocations => Volatile.Read(ref _invocations);

        internal RecordMetadata? Metadata { get; private set; }

        internal KafkaException? Exception { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Metadata = metadata;
            Exception = exception;
            Interlocked.Increment(ref _invocations);
            _probe?.Invoke(metadata, exception);
        }
    }
}
