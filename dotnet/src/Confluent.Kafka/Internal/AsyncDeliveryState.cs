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
/// The state of an async send: besides the delivery, the pins of its key and value and its first stage, both of which
/// the hand-over ends. A hand-over inside the call, or before <see cref="FirstStage"/> looked, only records the
/// outcome; a later one completes the pending stage, Canceled when the caller's token has fired by then.
/// </summary>
internal sealed class AsyncDeliveryState : DeliveryState
{
    private const int InCall = 0;
    private const int Recorded = 1;
    private const int Pending = 2;

    private static readonly NativeMethods.ProducerSendCallback s_handOver = OnHandOver;

    private readonly CancellationToken _cancellationToken;
    private SendPins _pins;
    private GCHandle _handOver;
    private Exception? _failure;
    private TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>>? _firstStage;
    private int _stage;

    internal AsyncDeliveryState(
        DeliveryReporter reporter,
        IDeliveryCallback? callback,
        IAsyncDeliveryCallback? asyncCallback,
        string topic,
        int partition,
        CancellationToken cancellationToken)
        : base(reporter, callback, asyncCallback, topic, partition)
    {
        _cancellationToken = cancellationToken;
    }

    internal bool Send(SafeProducerHandle producer, in SerializedProducerRecord record, ref IntPtr outErr)
    {
        _handOver = GCHandle.Alloc(this);
        bool sent;
        try
        {
            sent = ProducerSendMarshal.SendWithHandOver(
                producer, in record, ref _pins, this, s_handOver, GCHandle.ToIntPtr(_handOver), ref outErr);
        }
        catch
        {
            Abandon();
            throw;
        }

        if (!sent)
        {
            Abandon();
        }

        return sent;
    }

    internal ValueTask<AsyncKafkaFuture<RecordMetadata>> FirstStage()
    {
        if (Volatile.Read(ref _stage) == InCall)
        {
            TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>> firstStage =
                new TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _firstStage, firstStage);
            if (Interlocked.CompareExchange(ref _stage, Pending, InCall) == InCall)
            {
                return new ValueTask<AsyncKafkaFuture<RecordMetadata>>(firstStage.Task);
            }
        }

        Exception? failure = Volatile.Read(ref _failure);
        return failure is null
            ? new ValueTask<AsyncKafkaFuture<RecordMetadata>>(new AsyncKafkaFuture<RecordMetadata>(Future))
            : new ValueTask<AsyncKafkaFuture<RecordMetadata>>(
                Task.FromException<AsyncKafkaFuture<RecordMetadata>>(failure));
    }

    private static void OnHandOver(IntPtr future, IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(opaque);
            AsyncDeliveryState state = (AsyncDeliveryState)handle.Target!;
            handle.Free();
            state.HandOver(future, error);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private void Abandon()
    {
        _pins.Release();
        _handOver.Free();
        Release();
    }

    private void HandOver(IntPtr future, IntPtr error)
    {
        Exception? failure;
        try
        {
            DestroyFuture(future);
            failure = KafkaException.FromFfiError(error, owned: true);
        }
        catch (Exception unreadable)
        {
            failure = unreadable;
        }

        _pins.Release();
        if (failure is not null)
        {
            KeepUntilDestroy();
        }

        Volatile.Write(ref _failure, failure);
        if (Interlocked.CompareExchange(ref _stage, Recorded, InCall) == InCall)
        {
            return;
        }

        TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>> firstStage = Volatile.Read(ref _firstStage)!;
        if (_cancellationToken.IsCancellationRequested)
        {
            firstStage.TrySetCanceled(_cancellationToken);
        }
        else if (failure is not null)
        {
            firstStage.TrySetException(failure);
        }
        else
        {
            firstStage.TrySetResult(new AsyncKafkaFuture<RecordMetadata>(Future));
        }
    }
}
