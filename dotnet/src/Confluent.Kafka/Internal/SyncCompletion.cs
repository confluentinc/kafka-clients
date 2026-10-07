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
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The managed latch behind one sync send's <see cref="KafkaFuture{T}"/> — Java's
/// <c>FutureRecordMetadata</c> latch (<c>ProduceRequestResult</c>'s <c>CountDownLatch</c>), a plain lock plus
/// <see cref="Monitor"/>. It is completed exactly once, by whichever of <see cref="TrySetResult"/> /
/// <see cref="TrySetException"/> comes first, and <see cref="Get"/> blocks until then.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>No <c>Task</c>.</b> Never a <c>Task</c>, a <c>TaskCompletionSource</c> or a per-send
/// <c>ManualResetEventSlim</c>: a blocked <see cref="Get"/> parks on <see cref="Monitor.Wait(object)"/>, so it is
/// not sync-over-async (M11/P4.2 S-2), and the latch is one object per send.
/// </description></item>
/// <item><description>
/// <b>Locks on <c>this</c>.</b> The instance is internal and never handed to user code (the public
/// <see cref="KafkaFuture{T}"/> keeps it in a private field), so nothing else can lock on it; this saves a
/// separate lock object per send.
/// </description></item>
/// <item><description>
/// <b>Publication.</b> A completer writes the value or the error, then <c>_done</c> (a volatile write), then
/// pulses, all under the lock. The fast path reads <c>_done</c> (a volatile read) before the value or the error,
/// so a reader that sees it set also sees what was written before it; the slow path reads them after leaving the
/// lock, which orders them the same way.
/// </description></item>
/// <item><description>
/// <b>Failure is rethrown, not wrapped.</b> The exception is kept as an <see cref="ExceptionDispatchInfo"/>, so
/// every <see cref="Get"/> rethrows the <b>same</b> instance — no <see cref="AggregateException"/>, no copy (D2).
/// </description></item>
/// </list>
/// </remarks>
/// <typeparam name="T">The completion's value: <see cref="RecordMetadata"/> for a producer send.</typeparam>
internal sealed class SyncCompletion<T>
{
    private T _value = default!;
    private ExceptionDispatchInfo? _error;
    private volatile bool _done;

    /// <summary>
    /// Completes the latch with <paramref name="value"/> and releases every waiter, unless it is already completed.
    /// </summary>
    /// <returns><see langword="true"/> if this call completed it; <see langword="false"/> if an earlier call had.</returns>
    internal bool TrySetResult(T value)
    {
        lock (this)
        {
            if (_done)
            {
                return false;
            }

            _value = value;
            _done = true;
            Monitor.PulseAll(this);
            return true;
        }
    }

    /// <summary>
    /// Completes the latch with <paramref name="exception"/> and releases every waiter, unless it is already
    /// completed. Every later <see cref="Get"/> rethrows this same instance.
    /// </summary>
    /// <returns><see langword="true"/> if this call completed it; <see langword="false"/> if an earlier call had.</returns>
    internal bool TrySetException(Exception exception)
    {
        lock (this)
        {
            if (_done)
            {
                return false;
            }

            _error = ExceptionDispatchInfo.Capture(exception);
            _done = true;
            Monitor.PulseAll(this);
            return true;
        }
    }

    /// <summary>
    /// Returns the value once the latch is completed, blocking the calling thread until then; if it was completed
    /// with an exception, rethrows that exception (the same instance on every call).
    /// </summary>
    internal T Get()
    {
        if (!_done)
        {
            WaitUntilDone();
        }

        _error?.Throw();
        return _value;
    }

    // The only place Get() blocks: anything that must decide whether this caller may wait at all goes here, under
    // the lock and before the first Monitor.Wait, where `_done` is stable.
    private void WaitUntilDone()
    {
        lock (this)
        {
            while (!_done)
            {
                Monitor.Wait(this);
            }
        }
    }
}
