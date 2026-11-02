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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Counts a client's calls in flight into its native handle, waits until none is left, blocking or awaited, and
/// marks the handle destroyed under the same lock, so that no use starts after the mark.
/// </summary>
internal sealed class HandleUses
{
    private readonly object _gate = new object();
    private readonly List<TaskCompletionSource<bool>> _waiters = new List<TaskCompletionSource<bool>>();
    private int _count;
    private bool _destroyed;

    internal void Enter() => Interlocked.Increment(ref _count);

    internal bool TryEnterUnlessDestroyed()
    {
        lock (_gate)
        {
            if (_destroyed)
            {
                return false;
            }

            Interlocked.Increment(ref _count);
            return true;
        }
    }

    internal bool Exit() => Interlocked.Decrement(ref _count) == 0;

    internal void Wake()
    {
        lock (_gate)
        {
            Monitor.PulseAll(_gate);
            if (Volatile.Read(ref _count) == 0)
            {
                foreach (TaskCompletionSource<bool> waiter in _waiters)
                {
                    waiter.TrySetResult(true);
                }

                _waiters.Clear();
            }
        }
    }

    internal Task WhenNone()
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _count) == 0)
            {
                return Task.CompletedTask;
            }

            TaskCompletionSource<bool> waiter =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add(waiter);
            return waiter.Task;
        }
    }

    internal void WaitForNone()
    {
        lock (_gate)
        {
            while (Volatile.Read(ref _count) != 0)
            {
                Monitor.Wait(_gate);
            }
        }
    }

    internal void WaitForNoneThenMarkDestroyed()
    {
        lock (_gate)
        {
            WaitForNone();
            _destroyed = true;
        }
    }
}
