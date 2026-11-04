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
using System.Diagnostics;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The completion of one queued (<c>_cb</c>) operation, whose payload <c>Wait</c> takes on the calling thread; a
/// payload no wait takes is released with the given free.
/// </summary>
internal sealed class Completion<T>
{
    private readonly Action<T> _free;

    private T _payload = default!;

    private bool _arrived;

    private bool _abandoned;

    internal Completion(Action<T> free) => _free = free;

    internal void Complete(T payload)
    {
        lock (this)
        {
            if (!_abandoned)
            {
                _payload = payload;
                _arrived = true;
                Monitor.PulseAll(this);
                return;
            }
        }

        _free(payload);
    }

    internal bool Wait(TimeSpan timeout, out T payload)
    {
        try
        {
            return AwaitPayload(timeout, out payload);
        }
        catch
        {
            if (Abandon(out T stranded))
            {
                _free(stranded);
            }

            throw;
        }
    }

    private bool AwaitPayload(TimeSpan timeout, out T payload)
    {
        Stopwatch? clock = timeout == Timeout.InfiniteTimeSpan ? null : Stopwatch.StartNew();
        lock (this)
        {
            while (!_arrived)
            {
                int wait = Timeout.Infinite;
                if (clock is not null)
                {
                    TimeSpan left = timeout <= TimeSpan.Zero ? TimeSpan.Zero : timeout - clock.Elapsed;
                    if (left <= TimeSpan.Zero)
                    {
                        _abandoned = true;
                        payload = default!;
                        return false;
                    }

                    wait = (int)Math.Min(Math.Ceiling(left.TotalMilliseconds), int.MaxValue);
                }

                Monitor.Wait(this, wait);
            }

            payload = _payload;
            _payload = default!;
            _arrived = false;
            return true;
        }
    }

    private bool Abandon(out T stranded)
    {
        lock (this)
        {
            _abandoned = true;
            stranded = _payload;
            _payload = default!;
            bool arrived = _arrived;
            _arrived = false;
            return arrived;
        }
    }
}
