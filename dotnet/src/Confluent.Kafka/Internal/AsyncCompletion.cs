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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The completion of one queued (<c>_cb</c>) operation, awaited: a timeout or the caller's token can end the wait
/// first, and a payload no wait takes is released with the given free.
/// </summary>
internal sealed class AsyncCompletion<T>
{
    private readonly object _gate = new object();

    private readonly Action<T> _free;

    private readonly TaskCompletionSource<bool> _arrived =
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private T _payload = default!;

    private bool _abandoned;

    internal AsyncCompletion(Action<T> free) => _free = free;

    internal T Payload => _payload;

    internal void Complete(T payload)
    {
        lock (_gate)
        {
            if (!_abandoned)
            {
                _payload = payload;
                _arrived.TrySetResult(true);
                return;
            }
        }

        _free(payload);
    }

    internal async Task<bool> Wait(TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task arrived = _arrived.Task;
        if (timeout != Timeout.InfiniteTimeSpan || cancellationToken.CanBeCanceled)
        {
            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Task first = await Task.WhenAny(arrived, Task.Delay(timeout, stop.Token)).ConfigureAwait(false);
            stop.Cancel();
            if (first != arrived && Abandon())
            {
                cancellationToken.ThrowIfCancellationRequested();
                return false;
            }
        }

        await arrived.ConfigureAwait(false);
        return true;
    }

    private bool Abandon()
    {
        lock (_gate)
        {
            if (_arrived.Task.IsCompleted)
            {
                return false;
            }

            _abandoned = true;
            return true;
        }
    }
}
