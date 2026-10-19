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
/// The latch a sync send's or a sync admin key's future waits on: a lock plus <see cref="Monitor"/>, completed once,
/// by the first of <see cref="TrySetResult"/> and <see cref="TrySetException"/>.
/// </summary>
/// <remarks>
/// A latch with an owning pump throws <see cref="InvalidOperationException"/> instead of blocking on that pump's
/// thread while it is not completed, where Java's <c>get()</c> would deadlock.
/// </remarks>
internal sealed class SyncCompletion<T> : SyncFutureSource<T>, IAdminCompletion<T>
{
    private const string GetOnOwningPumpMessage =
        "KafkaFuture.Get() was called on this producer's send-completion thread — from inside a delivery callback — " +
        "for a send that has not completed; that would deadlock. Wait for it from another thread.";

    private readonly SendCompletionPump? _owner;
    private T _value = default!;
    private ExceptionDispatchInfo? _error;
    private volatile bool _done;

    internal SyncCompletion(SendCompletionPump? owner = null) => _owner = owner;

    internal override bool IsDone => _done;

    bool IAdminCompletion<T>.IsCompleted => _done;

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

    bool IAdminCompletion<T>.TrySetResult(T result) => TrySetResult(result);

    bool IAdminCompletion<T>.TrySetException(Exception exception) => TrySetException(exception);

    internal override T Get()
    {
        if (!_done)
        {
            WaitUntilDone();
        }

        _error?.Throw();
        return _value;
    }

    private void WaitUntilDone()
    {
        lock (this)
        {
            if (!_done && _owner is not null && _owner.IsCurrentThread)
            {
                throw new InvalidOperationException(GetOnOwningPumpMessage);
            }

            while (!_done)
            {
                Monitor.Wait(this);
            }
        }
    }
}
