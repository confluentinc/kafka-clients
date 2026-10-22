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
/// Tests the consumer handle's lifetime: create and dispose, the teardown order (refuse new calls, wait for the calls
/// in flight, close, destroy on the closing thread), the failed construction, and the leak warning of a consumer never
/// closed.
/// </summary>
public sealed class SafeConsumerHandleTests
{
    private const int UnsupportedVersionCode = 35;

    private const int HandleRaceRounds = 200;

    private const int HandleRaceCreators = 4;

    private const string ClassicProtocolMessage =
        "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void MockConsumer_Create_ThenDispose_DestroysOnTheClosingThread()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;
        Assert.IsType<SafeMockConsumerHandle>(handle);
        Assert.False(handle.IsInvalid);
        DestroyProbe probe = DestroyProbe.Attach(handle);

        Thread closer = RunOnThread(consumer.Dispose);

        Assert.True(handle.IsClosed);
        Assert.Equal(1, probe.Count);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
    }

    [Fact]
    public void KafkaConsumer_Create_ThenDispose_DestroysOnTheClosingThread()
    {
        NativeConsumer consumer = NativeConsumer.Create(Config());
        SafeConsumerHandle handle = consumer.Handle;
        Assert.IsType<SafeKafkaConsumerHandle>(handle);
        Assert.False(handle.IsInvalid);
        DestroyProbe probe = DestroyProbe.Attach(handle);

        Thread closer = RunOnThread(consumer.Dispose);

        Assert.True(handle.IsClosed);
        Assert.Equal(1, probe.Count);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
    }

    [Fact]
    public void KafkaConsumer_Close_DestroysOnTheClosingThread_ThenDisposeIsSafe()
    {
        NativeConsumer consumer = NativeConsumer.Create(Config());
        SafeConsumerHandle handle = consumer.Handle;
        DestroyProbe probe = DestroyProbe.Attach(handle);

        Thread closer = RunOnThread(consumer.CloseSync);
        Assert.True(handle.IsClosed);
        Assert.Equal(1, probe.Count);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);

        TestTimeout.Run(consumer.Dispose, s_deadline);
        TestTimeout.Run(consumer.CloseSync, s_deadline);
        Assert.Equal(1, probe.Count);
    }

    [Fact]
    public void Dispose_CalledTwice_DestroysOnce()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        DestroyProbe probe = DestroyProbe.Attach(consumer.Handle);

        TestTimeout.Run(consumer.Dispose, s_deadline);
        TestTimeout.Run(consumer.Dispose, s_deadline);

        Assert.Equal(1, probe.Count);
    }

    [Fact]
    public async Task DisposeAsync_ThenDispose_DestroysOnce()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        DestroyProbe probe = DestroyProbe.Attach(consumer.Handle);

        await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        TestTimeout.Run(consumer.Dispose, s_deadline);

        Assert.Equal(1, probe.Count);
        Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
    }

    [Fact]
    public void Handle_AfterDispose_ThrowsObjectDisposedException()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        TestTimeout.Run(consumer.Dispose, s_deadline);

        ObjectDisposedException failure = Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
        Assert.Equal(nameof(NativeConsumer), failure.ObjectName);
    }

    [Fact]
    public void Getters_AfterDispose_ThrowObjectDisposedException_AndWakeupIsANoOp()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        TestTimeout.Run(consumer.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        Assert.Throws<ObjectDisposedException>(() => consumer.Paused());
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
        consumer.Wakeup();
    }

    [Fact]
    public void CreateAndDisposeMany_MockConsumers_NoLeakOrCrash()
    {
        for (int i = 0; i < 100; i++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock();
            Assert.False(consumer.Handle.IsInvalid);
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void Create_WithTheClassicProtocol_ThrowsTheCoreError()
    {
        Dictionary<string, string> config = Config();
        config["group.protocol"] = "classic";

        for (int i = 0; i < 50; i++)
        {
            KafkaException failure = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.False(failure.IsRetriable);
            Assert.Equal(ClassicProtocolMessage, failure.Message);
        }
    }

    [Fact]
    public void CreateMock_WithAnUnknownResetStrategy_ThrowsTheCoreError()
    {
        ArgumentException failure = Assert.Throws<ArgumentException>(() => NativeConsumer.CreateMock("nowhere"));
        Assert.Equal("Unknown auto offset reset strategy: nowhere", failure.Message);
    }

    [Fact]
    public void Dispose_WaitsForACallInFlight_ThenDestroysOnTheClosingThread()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;
        DestroyProbe probe = DestroyProbe.Attach(handle);
        int owner = EnterUse(consumer);

        Exception? closeFailure = null;
        Thread closer = new Thread(() => closeFailure = Record(consumer.Dispose));
        closer.Start();
        Assert.True(SpinWait.SpinUntil(() => (closer.ThreadState & ThreadState.WaitSleepJoin) != 0, s_deadline));

        Assert.False(closer.Join(TimeSpan.FromMilliseconds(200)));
        Assert.False(handle.IsClosed);
        Assert.Equal(0, probe.Count);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());

        ExitUse(consumer, owner);

        Assert.True(closer.Join(s_deadline));
        Assert.Null(closeFailure);
        Assert.True(handle.IsClosed);
        Assert.Equal(1, probe.Count);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
    }

    [Fact]
    public void Close_WaitsForACallInFlight_ThenDestroysOnTheClosingThread()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;
        DestroyProbe probe = DestroyProbe.Attach(handle);
        int owner = EnterUse(consumer);

        Exception? closeFailure = null;
        Thread closer = new Thread(() => closeFailure = Record(consumer.CloseSync));
        closer.Start();
        Assert.True(SpinWait.SpinUntil(() => (closer.ThreadState & ThreadState.WaitSleepJoin) != 0, s_deadline));

        Assert.False(closer.Join(TimeSpan.FromMilliseconds(200)));
        Assert.False(handle.IsClosed);
        Assert.Equal(0, probe.Count);
        Assert.Empty(consumer.Subscription());

        ExitUse(consumer, owner);

        Assert.True(closer.Join(s_deadline));
        Assert.Null(closeFailure);
        Assert.True(handle.IsClosed);
        Assert.Equal(1, probe.Count);
        Assert.Equal(closer.ManagedThreadId, probe.ThreadId);
    }

    [Fact]
    public void Dispose_WhileOtherThreadsCreateReentrancyHandles_DestroysOnTheClosingThreadBeforeItReturns()
    {
        for (int round = 0; round < HandleRaceRounds; round++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock(withPump: false);
            DestroyProbe probe = DestroyProbe.Attach(consumer.Handle);
            int started = 0;
            Exception? creatorFailure = null;
            Thread[] creators = new Thread[HandleRaceCreators];
            for (int i = 0; i < creators.Length; i++)
            {
                creators[i] = new Thread(() =>
                {
                    Interlocked.Increment(ref started);
                    try
                    {
                        while (true)
                        {
                            consumer.CreateReentrancyHandle().Dispose();
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref creatorFailure, e, null);
                    }
                });
                creators[i].IsBackground = true;
                creators[i].Start();
            }

            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref started) == creators.Length, s_deadline));
            Thread.SpinWait(500 + ((round % 7) * 300));

            consumer.Dispose();
            int destroysWhenDisposeReturned = probe.Count;
            int destroyThread = probe.ThreadId;

            foreach (Thread creator in creators)
            {
                Assert.True(creator.Join(s_deadline));
            }

            Assert.Null(creatorFailure);
            Assert.Equal(1, destroysWhenDisposeReturned);
            Assert.Equal(Environment.CurrentManagedThreadId, destroyThread);
        }
    }

    [Fact]
    public void Finalization_LeaksWithOneWarning()
    {
        using TraceCapture trace = TraceCapture.Start();

        (WeakReference handle, DestroyProbe probe, string marker) = Abandon();
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
        Assert.Equal(0, probe.Count);
    }

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
    };

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Handle, DestroyProbe Probe, string Marker) Abandon()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle handle = consumer.Handle;
        DestroyProbe probe = DestroyProbe.Attach(handle);
        bool added = false;
        IntPtr classPointer = handle.DangerousAddRefClass(ref added);
        handle.DangerousRelease();
        string marker = nameof(SafeMockConsumerHandle) + " 0x" + classPointer.ToInt64().ToString("X");
        return (new WeakReference(handle), probe, marker);
    }

    private static Thread RunOnThread(Action action)
    {
        Exception? failure = null;
        Thread thread = new Thread(() => failure = Record(action));
        thread.Start();
        Assert.True(thread.Join(s_deadline));
        Assert.Null(failure);
        return thread;
    }

    private static Exception? Record(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static int EnterUse(NativeConsumer consumer)
    {
        Type kind = typeof(NativeConsumer).GetNestedType("UseKind", BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(nameof(NativeConsumer) + " has no UseKind.");
        return (int)Method("EnterUse").Invoke(consumer, new[] { Enum.Parse(kind, "Owned"), (object)0 })!;
    }

    private static void ExitUse(NativeConsumer consumer, int owner) =>
        Method("ExitUse").Invoke(consumer, new object[] { owner });

    private static MethodInfo Method(string name) =>
        typeof(NativeConsumer).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(nameof(NativeConsumer) + " has no method " + name + ".");

    /// <summary>
    /// Records how often, and on which thread, the handle ran the action it runs after its destroy. Only the
    /// handle holds that action, so a test holding the probe does not keep the consumer alive.
    /// </summary>
    private sealed class DestroyProbe
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int ThreadId { get; private set; }

        internal static DestroyProbe Attach(SafeConsumerHandle handle)
        {
            FieldInfo field = typeof(SafeConsumerHandle).GetField("_afterDestroy", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException(nameof(SafeConsumerHandle) + " has no field _afterDestroy.");
            Action? release = (Action?)field.GetValue(handle);
            DestroyProbe probe = new DestroyProbe();
            handle.ReleaseAfterDestroy(() =>
            {
                probe.Record();
                release?.Invoke();
            });
            return probe;
        }

        private void Record()
        {
            ThreadId = Environment.CurrentManagedThreadId;
            Interlocked.Increment(ref _count);
        }
    }
}
