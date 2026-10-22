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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The completion of one admin operation whose callback carries a result: a <see cref="TaskCompletionSource{TResult}"/>
/// with asynchronous continuations, the <see cref="GCHandle"/> that roots it for native, and the client handle
/// reference the submitter took, both released once by <see cref="FreeGcHandle"/>.
/// </summary>
/// <typeparam name="TResult">The managed result the callback has already copied out.</typeparam>
internal class OperationCompletionSource<TResult>
{
    private readonly TaskCompletionSource<TResult> _tcs =
        new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);

    private GCHandle _gcHandle;
    private int _gcHandleFreed;

    private SafeHandle? _handleRef;

    internal Task<TResult> Task => _tcs.Task;

    internal void SetGcHandle(GCHandle handle) => _gcHandle = handle;

    internal void SetHandleRef(SafeHandle handle) => _handleRef = handle;

    internal void CompleteWithResult(TResult result)
    {
        _tcs.TrySetResult(result);
    }

    internal void Complete(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            _tcs.TrySetException(KafkaException.FromHandle(error)!);
        }
        else
        {
            CompleteWithSuccessNoResult();
        }
    }

    private protected virtual void CompleteWithSuccessNoResult()
    {
    }

    internal void TrySetException(Exception exception) => _tcs.TrySetException(exception);

    internal void AbandonBeforeSubmit()
    {
        FreeGcHandle();
    }

    internal void FreeGcHandle()
    {
        if (Interlocked.Exchange(ref _gcHandleFreed, 1) == 0)
        {
            if (_gcHandle.IsAllocated)
            {
                _gcHandle.Free();
            }

            _handleRef?.DangerousRelease();
            _handleRef = null;
        }
    }
}

/// <summary>
/// The completion of an admin operation with no result, whose task completes on a null error.
/// </summary>
internal sealed class OperationCompletionSource : OperationCompletionSource<bool>
{
    internal new Task Task => base.Task;

    private protected override void CompleteWithSuccessNoResult() => CompleteWithResult(true);
}
