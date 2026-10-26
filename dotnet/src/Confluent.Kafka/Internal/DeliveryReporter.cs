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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A producer's side of its sends' delivery callbacks: one chain that handles the completions in order, each waiting
/// while an async callback's task runs; the report of each callback's end to the producer until the producer's
/// destroy begins; and the state of every send that failed, kept until that destroy has returned. It holds the
/// producer weakly, so that the kept states never keep a producer from its finalizer.
/// </summary>
internal sealed class DeliveryReporter
{
    private readonly WeakReference<SafeProducerHandle> _producer;
    private readonly CompletionThread _completion;
    private readonly HashSet<DeliveryState> _kept = new HashSet<DeliveryState>();
    private readonly object _chainGate = new object();
    private readonly Queue<DeliveryState> _waiting = new Queue<DeliveryState>();
    private readonly AsyncLocal<bool> _inAsyncCallback = new AsyncLocal<bool>();
    private bool _running;
    private volatile bool _destroying;

    internal DeliveryReporter(SafeProducerHandle producer, CompletionThread completion)
    {
        _producer = new WeakReference<SafeProducerHandle>(producer);
        _completion = completion;
    }

    internal bool InAsyncCallback => _inAsyncCallback.Value;

    internal void Deliver(DeliveryState state)
    {
        lock (_chainGate)
        {
            if (_running)
            {
                _waiting.Enqueue(state);
                return;
            }

            _running = true;
        }

        RunChain(state, queued: false);
    }

    internal void Resume()
    {
        DeliveryState? next = Next();
        if (next is not null)
        {
            RunChain(next, queued: true);
        }
    }

    internal void Post(Action action) => _completion.Post(action);

    internal Task CallAsync(IAsyncDeliveryCallback callback, RecordMetadata metadata, Exception? exception)
    {
        _inAsyncCallback.Value = true;
        try
        {
            return callback.OnCompletion(metadata, exception);
        }
        finally
        {
            _inAsyncCallback.Value = false;
        }
    }

    internal void Report(long callbackId, IntPtr error)
    {
        if (_destroying || !_producer.TryGetTarget(out SafeProducerHandle? producer))
        {
            FreeError(error);
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        try
        {
            NativeMethods.ProducerSetCallbackResult(producer, callbackId, IntPtr.Zero, error, ref outErr);
        }
        catch (Exception)
        {
            FreeError(error);
            return;
        }

        FreeError(outErr);
    }

    internal void Keep(DeliveryState state)
    {
        lock (_kept)
        {
            _kept.Add(state);
        }
    }

    internal void Forget(DeliveryState state)
    {
        lock (_kept)
        {
            _kept.Remove(state);
        }
    }

    internal void BeginDestroy() => _destroying = true;

    internal void ReleaseKept()
    {
        DeliveryState[] kept;
        lock (_kept)
        {
            kept = new DeliveryState[_kept.Count];
            _kept.CopyTo(kept);
            _kept.Clear();
        }

        foreach (DeliveryState state in kept)
        {
            state.Release();
        }
    }

    private void RunChain(DeliveryState state, bool queued)
    {
        DeliveryState? next = state;
        while (next is not null)
        {
            if (next.Run(queued))
            {
                return;
            }

            next = Next();
            queued = true;
        }
    }

    private DeliveryState? Next()
    {
        lock (_chainGate)
        {
            if (_waiting.Count == 0)
            {
                _running = false;
                return null;
            }

            return _waiting.Dequeue();
        }
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }
}
