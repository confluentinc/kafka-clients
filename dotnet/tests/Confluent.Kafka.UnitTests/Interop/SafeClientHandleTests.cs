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
using System.Runtime.CompilerServices;
using System.Threading;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests the <see cref="SafeClientHandle"/> base's destroy, leak warning and class-call reference, over a subclass
/// whose destroy records the pointer it is given.
/// </summary>
public sealed class SafeClientHandleTests
{
    private static long s_nextPointer = 0x7A0000;

    [Fact]
    public void CloseExplicitly_DestroysWithTheClassPointer_NeverTheView()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        var destroyed = new List<IntPtr>();
        var handle = new RecordingClientHandle(view, classPointer, destroyed);
        Assert.Equal(view, handle.DangerousGetHandle());

        handle.CloseExplicitly();

        Assert.True(handle.IsClosed);
        Assert.Equal(new[] { classPointer }, destroyed);
    }

    [Fact]
    public void Dispose_WithoutTheExplicitTeardown_DoesNotDestroy_AndTracesOneWarning()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        using TraceCapture trace = TraceCapture.Start();
        var destroyed = new List<IntPtr>();
        var handle = new RecordingClientHandle(view, classPointer, destroyed);

        handle.Dispose();
        handle.Dispose();

        Assert.True(handle.IsClosed);
        Assert.Empty(destroyed);
        string line = Assert.Single(trace.LinesContaining(Marker(classPointer)));
        Assert.Contains(" Warning: ", line, StringComparison.Ordinal);
        Assert.Contains(
            "Confluent.Kafka: " + Marker(classPointer) + " was released without Close or Dispose; the native client is leaked.",
            line,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Finalization_DoesNotDestroy_AndTracesOneWarning()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        using TraceCapture trace = TraceCapture.Start();
        var destroyed = new List<IntPtr>();

        WeakReference abandoned = Abandon(view, classPointer, destroyed);
        for (int attempt = 0; attempt < 10 && trace.LinesContaining(Marker(classPointer)).Count == 0; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(abandoned.IsAlive);
        lock (destroyed)
        {
            Assert.Empty(destroyed);
        }

        string line = Assert.Single(trace.LinesContaining(Marker(classPointer)));
        Assert.Contains(" Warning: ", line, StringComparison.Ordinal);
    }

    [Fact]
    public void DangerousAddRefClass_ReturnsTheClassPointer_AndKeepsTheHandleOpenUntilReleased()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        var destroyed = new List<IntPtr>();
        var handle = new RecordingClientHandle(view, classPointer, destroyed);

        bool added = false;
        try
        {
            Assert.Equal(classPointer, handle.DangerousAddRefClass(ref added));
            Assert.True(added);

            handle.CloseExplicitly();

            Assert.False(handle.IsClosed);
            Assert.Empty(destroyed);
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }

        Assert.True(handle.IsClosed);
        Assert.Equal(new[] { classPointer }, destroyed);
    }

    [Fact]
    public void DangerousAddRefClass_OnAClosedHandle_IsRefusedAsASafeHandleParameterIs()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        var destroyed = new List<IntPtr>();
        var handle = new RecordingClientHandle(view, classPointer, destroyed);
        handle.CloseExplicitly();

        bool added = false;
        ObjectDisposedException refused = Assert.Throws<ObjectDisposedException>(() => handle.DangerousAddRefClass(ref added));

        Assert.False(added);
        bool marshallerAdded = false;
        ObjectDisposedException reference = Assert.Throws<ObjectDisposedException>(() => handle.DangerousAddRef(ref marshallerAdded));
        Assert.Equal(reference.Message, refused.Message);
        Assert.Equal(new[] { classPointer }, destroyed);
    }

    [Fact]
    public void ASecondTeardown_DestroysNothingMore()
    {
        (IntPtr view, IntPtr classPointer) = FakePointers();
        var destroyed = new List<IntPtr>();
        var handle = new RecordingClientHandle(view, classPointer, destroyed);

        handle.CloseExplicitly();
        handle.CloseExplicitly();
        handle.Dispose();

        Assert.Equal(new[] { classPointer }, destroyed);
    }

    private static (IntPtr View, IntPtr ClassPointer) FakePointers()
    {
        long view = Interlocked.Add(ref s_nextPointer, 0x100);
        return (new IntPtr(view), new IntPtr(view + 0x10));
    }

    private static string Marker(IntPtr classPointer) =>
        nameof(RecordingClientHandle) + " 0x" + classPointer.ToInt64().ToString("X");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference Abandon(IntPtr view, IntPtr classPointer, List<IntPtr> destroyed) =>
        new WeakReference(new RecordingClientHandle(view, classPointer, destroyed));

    /// <summary>A client handle over fake pointers whose destroy records the pointer it is given.</summary>
    private sealed class RecordingClientHandle : SafeClientHandle
    {
        private readonly List<IntPtr> _destroyed;

        internal RecordingClientHandle(IntPtr view, IntPtr classPointer, List<IntPtr> destroyed)
            : base(view, classPointer)
        {
            _destroyed = destroyed;
        }

        protected override void Destroy(IntPtr classPointer)
        {
            lock (_destroyed)
            {
                _destroyed.Add(classPointer);
            }
        }
    }
}
