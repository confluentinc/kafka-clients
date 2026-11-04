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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests the producer handle's lifetime: create and dispose, the teardown order (close, join the completion thread,
/// wait for the calls in flight, destroy on the closing thread, then release the notify opaque), the close from the
/// completion thread, and the leak warning of a producer never closed.
/// </summary>
public sealed class SafeProducerHandleTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Create_ThenDispose_ReleasesTheHandle()
    {
        NativeProducer producer = NativeProducer.Create(Config());
        SafeProducerHandle handle = producer.Handle;
        Assert.IsType<SafeKafkaProducerHandle>(handle);
        Assert.False(handle.IsInvalid);

        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.True(handle.IsClosed);
    }

    [Fact]
    public void Dispose_CalledTwice_IsSafe()
    {
        NativeProducer producer = NativeProducer.Create(Config());

        TestTimeout.Run(producer.Dispose, s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_ThenDispose_IsSafe()
    {
        NativeProducer producer = NativeProducer.Create(Config());

        await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }

    [Fact]
    public void Handle_AfterDispose_ThrowsObjectDisposedException()
    {
        NativeProducer producer = NativeProducer.Create(Config());

        TestTimeout.Run(producer.Dispose, s_deadline);

        ObjectDisposedException failure = Assert.Throws<ObjectDisposedException>(() => producer.Handle);
        Assert.Equal(nameof(NativeProducer), failure.ObjectName);
    }

    [Fact]
    public void Flush_AfterDispose_ThrowsObjectDisposedException()
    {
        NativeProducer producer = NativeProducer.Create(Config());

        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public void CreateAndDisposeMany_NoLeakOrCrash()
    {
        for (int i = 0; i < 20; i++)
        {
            NativeProducer producer = NativeProducer.Create(Config());
            Assert.False(producer.Handle.IsInvalid);
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void Dispose_DestroysOnTheClosingThread_AfterTheCompletionThreadStops_ThenReleasesTheOpaque()
    {
        NativeProducer producer = NativeProducer.Create(Config());
        CompletionThread completion = CompletionOf(producer);
        Thread completionThread = ThreadOf(completion);
        DestroyProbe probe = DestroyProbe.Attach(producer, completion, completionThread);

        Thread closer = new Thread(producer.Dispose);
        closer.Start();
        Assert.True(closer.Join(s_deadline));

        Assert.True(probe.Ran);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
        Assert.False(probe.CompletionThreadAlive);
        Assert.False(probe.OpaqueReleasedAtDestroy);
        Assert.True(OpaqueReleased(completion));
    }

    [Fact]
    public void Close_OnTheCompletionThread_TearsDownOnThatThread_AfterItsDrain()
    {
        NativeProducer producer = NativeProducer.Create(Config());
        CompletionThread completion = CompletionOf(producer);
        Thread completionThread = ThreadOf(completion);
        DestroyProbe probe = DestroyProbe.Attach(producer, completion, completionThread);

        completion.Post(producer.Close);

        Assert.True(probe.Destroyed.Wait(s_deadline));
        Assert.Equal(completionThread.ManagedThreadId, probe.ThreadId);
        Assert.True(completionThread.Join(s_deadline));
        Assert.True(OpaqueReleased(completion));
        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }

    [Fact]
    public void Dispose_WaitsForACallInFlight_BeforeTheDestroy()
    {
        NativeProducer producer = NativeProducer.Create(Config());
        CompletionThread completion = CompletionOf(producer);
        Thread completionThread = ThreadOf(completion);
        DestroyProbe probe = DestroyProbe.Attach(producer, completion, completionThread);
        Invoke(producer, "EnterUse");

        Thread closer = new Thread(producer.Dispose);
        closer.Start();
        Assert.True(completionThread.Join(s_deadline));
        Assert.True(SpinWait.SpinUntil(() => (closer.ThreadState & ThreadState.WaitSleepJoin) != 0, s_deadline));

        Assert.False(probe.Destroyed.Wait(TimeSpan.FromMilliseconds(200)));
        Assert.False(probe.Ran);

        Invoke(producer, "ExitUse");

        Assert.True(closer.Join(s_deadline));
        Assert.True(probe.Ran);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
    }

    [Fact]
    public void Finalization_LeaksWithOneWarning_AndKeepsTheNotifyOpaque()
    {
        using TraceCapture trace = TraceCapture.Start();

        (WeakReference handle, CompletionThread completion, string marker) = Abandon();
        for (int attempt = 0; attempt < 10 && trace.LinesContaining(marker).Count == 0; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(handle.IsAlive);
        string line = Assert.Single(trace.LinesContaining(marker));
        Assert.Contains(
            "Confluent.Kafka: " + marker + " was released without Close or Dispose; the native client is leaked.",
            line,
            StringComparison.Ordinal);
        Assert.False(OpaqueReleased(completion));
    }

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Handle, CompletionThread Completion, string Marker) Abandon()
    {
        NativeProducer producer = NativeProducer.Create(Config());
        SafeProducerHandle handle = producer.Handle;
        bool added = false;
        IntPtr classPointer = handle.DangerousAddRefClass(ref added);
        handle.DangerousRelease();
        string marker = nameof(SafeKafkaProducerHandle) + " 0x" + classPointer.ToInt64().ToString("X");
        return (new WeakReference(handle), CompletionOf(producer), marker);
    }

    private static CompletionThread CompletionOf(NativeProducer producer) =>
        (CompletionThread)Field(typeof(NativeProducer), "_completion").GetValue(producer)!;

    private static Thread ThreadOf(CompletionThread completion) =>
        (Thread)Field(typeof(CompletionThread), "_thread").GetValue(completion)!;

    private static bool OpaqueReleased(CompletionThread completion) =>
        (int)Field(typeof(CompletionThread), "_opaqueReleased").GetValue(completion)! != 0;

    private static void Invoke(NativeProducer producer, string method) =>
        (typeof(NativeProducer).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(nameof(NativeProducer) + " has no method " + method + "."))
        .Invoke(producer, null);

    private static FieldInfo Field(Type type, string name) =>
        type.GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException(type.Name + " has no field " + name + ".");

    /// <summary>Wraps the action the handle runs after its destroy, to record where and when the destroy ran.</summary>
    private sealed class DestroyProbe
    {
        private readonly CompletionThread _completion;
        private readonly Thread _completionThread;
        private readonly Action _release;

        private DestroyProbe(CompletionThread completion, Thread completionThread, Action release)
        {
            _completion = completion;
            _completionThread = completionThread;
            _release = release;
        }

        internal ManualResetEventSlim Destroyed { get; } = new ManualResetEventSlim();

        internal bool Ran { get; private set; }

        internal int ThreadId { get; private set; }

        internal bool CompletionThreadAlive { get; private set; }

        internal bool OpaqueReleasedAtDestroy { get; private set; }

        internal static DestroyProbe Attach(NativeProducer producer, CompletionThread completion, Thread completionThread)
        {
            SafeProducerHandle handle = producer.Handle;
            Action release = (Action)Field(typeof(SafeProducerHandle), "_afterDestroy").GetValue(handle)!;
            var probe = new DestroyProbe(completion, completionThread, release);
            handle.ReleaseAfterDestroy(probe.AfterDestroy);
            return probe;
        }

        private void AfterDestroy()
        {
            Ran = true;
            ThreadId = Environment.CurrentManagedThreadId;
            CompletionThreadAlive = _completionThread.IsAlive && Thread.CurrentThread != _completionThread;
            OpaqueReleasedAtDestroy = OpaqueReleased(_completion);
            _release();
            Destroyed.Set();
        }
    }
}
