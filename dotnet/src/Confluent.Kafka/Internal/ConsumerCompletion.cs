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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The completion of one queued (<c>_cb</c>) consumer operation: rooted by a <see cref="GCHandle"/> passed as the
/// operation's opaque, which its trampoline frees when the core delivers the outcome.
/// </summary>
internal abstract class ConsumerCompletion
{
    internal static readonly NativeMethods.ConsumerVoidCallback Void = OnVoid;

    internal static readonly NativeMethods.ConsumerScalarCallback Scalar = OnScalar;

    internal static readonly NativeMethods.ConsumerPointerCallback Pointer = OnPointer;

    internal const long NoOwnWake = -1;

    private int _arrived;

    private long _ownWakeMark = NoOwnWake;

    internal bool HasArrived => Volatile.Read(ref _arrived) != 0;

    internal int UseOwner { get; set; }

    internal bool RefusesOtherThreads { get; set; }

    internal long OwnWakeMark
    {
        get => Interlocked.Read(ref _ownWakeMark);
        set => Interlocked.Exchange(ref _ownWakeMark, value);
    }

    internal static void Abandon(IntPtr opaque) => GCHandle.FromIntPtr(opaque).Free();

    internal IntPtr Root() => GCHandle.ToIntPtr(GCHandle.Alloc(this));

    protected abstract void Complete(IntPtr value, long scalar, IntPtr error);

    private static void OnVoid(IntPtr error, IntPtr opaque) => Deliver(opaque, IntPtr.Zero, 0, error);

    private static void OnScalar(long value, IntPtr error, IntPtr opaque) => Deliver(opaque, IntPtr.Zero, value, error);

    private static void OnPointer(IntPtr value, IntPtr error, IntPtr opaque) => Deliver(opaque, value, 0, error);

    private static void Deliver(IntPtr opaque, IntPtr value, long scalar, IntPtr error)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(opaque);
            if (gcHandle.Target is not ConsumerCompletion completion)
            {
                return;
            }

            gcHandle.Free();
            Volatile.Write(ref completion._arrived, 1);
            completion.Complete(value, scalar, error);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }
}

/// <summary>
/// A consumer operation's completion that converts the delivered value, on the thread that delivers it, into the
/// result of a <see cref="Task{TResult}"/> whose continuations run asynchronously.
/// </summary>
internal sealed class ConsumerCompletion<T> : ConsumerCompletion
{
    private readonly TaskCompletionSource<T> _result =
        new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly Func<IntPtr, long, T> _convert;

    private readonly Action<bool>? _settle;

    internal ConsumerCompletion(Func<IntPtr, long, T> convert, Action<bool>? settle = null)
    {
        _convert = convert;
        _settle = settle;
    }

    internal Task<T> Result => _result.Task;

    internal Func<ConsumerCompletion<T>>? Again { get; set; }

    protected override void Complete(IntPtr value, long scalar, IntPtr error)
    {
        try
        {
            _settle?.Invoke(error != IntPtr.Zero);
        }
        finally
        {
            SetOutcome(value, scalar, error);
        }
    }

    private void SetOutcome(IntPtr value, long scalar, IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            _result.TrySetException(KafkaException.FromFfiError(error, owned: true)!);
            return;
        }

        try
        {
            _result.TrySetResult(_convert(value, scalar));
        }
        catch (Exception exception)
        {
            _result.TrySetException(exception);
        }
    }
}
