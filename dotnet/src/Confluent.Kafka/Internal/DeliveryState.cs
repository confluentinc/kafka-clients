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
/// One send's state, the <c>self</c> of its <c>kafka_producer_Callback_t</c>: the user's delivery callback, sync or
/// async, if any, and the record's future. <c>on_completion</c> copies the outcome and hands the state to the
/// producer's delivery chain, which runs the callback, completes the future and reports to the producer.
/// </summary>
internal class DeliveryState
{
    private const string CallbackThrew =
        "Confluent.Kafka: an IDeliveryCallback (OnCompletion) threw; the producer was told, and the record's " +
        "outcome is unchanged. Details: {0}";

    private const string AsyncCallbackFailed =
        "Confluent.Kafka: an IAsyncDeliveryCallback (OnCompletion) threw, or its task faulted or was canceled; the " +
        "producer was told, and the record's outcome is unchanged. Details: {0}";

    private static readonly NativeMethods.CallbackOnCompletion s_onCompletion = OnCompletion;

    private static readonly Action<Task, object?> s_onAsyncCallbackEnded = OnAsyncCallbackEnded;

    private readonly DeliveryReporter _reporter;
    private readonly IDeliveryCallback? _callback;
    private readonly IAsyncDeliveryCallback? _asyncCallback;
    private readonly string _topic;
    private readonly int _partition;
    private GCHandle _self;
    private int _released;
    private volatile bool _kept;
    private RecordMetadata? _delivered;
    private Exception? _failure;
    private long _callbackId;

    internal DeliveryState(DeliveryReporter reporter, IDeliveryCallback? callback, string topic, int partition)
        : this(reporter, callback, null, topic, partition)
    {
    }

    internal DeliveryState(
        DeliveryReporter reporter,
        IDeliveryCallback? callback,
        IAsyncDeliveryCallback? asyncCallback,
        string topic,
        int partition)
    {
        _reporter = reporter;
        _callback = callback;
        _asyncCallback = asyncCallback;
        _topic = topic;
        _partition = partition;
        Future = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
    }

    internal KafkaFutureImpl<RecordMetadata> Future { get; }

    internal static void DestroyFuture(IntPtr future)
    {
        if (future == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.KafkaFutureDestroy(future, ref outErr);
        FreeError(outErr);
    }

    internal IntPtr Register()
    {
        _self = GCHandle.Alloc(this);
        IntPtr outErr = IntPtr.Zero;
        IntPtr callback;
        try
        {
            callback = NativeMethods.CallbackNew(GCHandle.ToIntPtr(_self), s_onCompletion, ref outErr);
        }
        catch
        {
            Release();
            throw;
        }

        if (outErr != IntPtr.Zero)
        {
            Release();
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return callback;
    }

    // A failed send's callback may still fire, until the producer's destroy has returned.
    internal void KeepUntilDestroy()
    {
        _kept = true;
        _reporter.Keep(this);
    }

    internal void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
        {
            if (_kept)
            {
                _reporter.Forget(this);
            }

            if (_self.IsAllocated)
            {
                _self.Free();
            }
        }
    }

    internal bool Run(bool queued)
    {
        if (_asyncCallback is not null)
        {
            return RunAsyncCallback();
        }

        IntPtr report = IntPtr.Zero;
        bool completed = false;
        try
        {
            if (_callback is not null)
            {
                try
                {
                    _callback.OnCompletion(_delivered!, _failure);
                }
                catch (Exception thrown)
                {
                    report = ReportOf(thrown, CallbackThrew);
                }
            }

            // A completion queued behind an async callback reports before it completes the future, as that
            // callback's own end does.
            if (!queued)
            {
                CompleteFuture();
                completed = true;
            }
        }
        finally
        {
            Finish(report);
            if (!completed)
            {
                CompleteFuture();
            }
        }

        return false;
    }

    private static void OnCompletion(IntPtr self, IntPtr metadata, IntPtr error, long callbackId)
    {
        try
        {
            ((DeliveryState)GCHandle.FromIntPtr(self).Target!).Arrive(metadata, error, callbackId);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    private static Exception? FailureOf(IntPtr error)
    {
        try
        {
            return KafkaException.FromFfiError(error, owned: false);
        }
        catch (Exception unreadable)
        {
            return unreadable;
        }
    }

    private static IntPtr ReportOf(Exception thrown, string message)
    {
        try
        {
            System.Diagnostics.Trace.TraceError(message, thrown);
        }
        catch (Exception)
        {
            // A throwing TraceListener must not stop the report.
        }

        try
        {
            return ManagedExceptionCarrier.ToError(thrown);
        }
        catch (Exception)
        {
            return IntPtr.Zero;
        }
    }

    private static Exception? FailureOfTask(Task task)
    {
        if (task.Status == TaskStatus.RanToCompletion)
        {
            return null;
        }

        try
        {
            task.GetAwaiter().GetResult();
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static void OnAsyncCallbackEnded(Task task, object? state)
    {
        DeliveryState delivery = (DeliveryState)state!;
        delivery._reporter.Post(() =>
        {
            delivery.EndAsyncCallback(FailureOfTask(task));
            delivery._reporter.Resume();
        });
    }

    private void Arrive(IntPtr metadata, IntPtr error, long callbackId)
    {
        _callbackId = callbackId;
        _failure = FailureOf(error);
        try
        {
            _delivered = RecordMetadataMarshal.CopyOut(metadata);
        }
        catch (Exception unreadable)
        {
            // The callback still runs once, with Java's placeholder; a failed record keeps its own error.
            _delivered = new RecordMetadata(_topic, _partition, -1L, -1L);
            _failure ??= unreadable;
        }

        _reporter.Deliver(this);
    }

    private bool RunAsyncCallback()
    {
        Task? task;
        try
        {
            task = _reporter.CallAsync(_asyncCallback!, _delivered!, _failure);
        }
        catch (Exception thrown)
        {
            EndAsyncCallback(thrown);
            return false;
        }

        if (task is null || task.IsCompleted)
        {
            EndAsyncCallback(task is null ? null : FailureOfTask(task));
            return false;
        }

        task.ContinueWith(
            s_onAsyncCallbackEnded,
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return true;
    }

    private void EndAsyncCallback(Exception? thrown)
    {
        try
        {
            Finish(thrown is null ? IntPtr.Zero : ReportOf(thrown, AsyncCallbackFailed));
        }
        finally
        {
            CompleteFuture();
        }
    }

    private void CompleteFuture()
    {
        if (_failure is null)
        {
            Future.Complete(_delivered!);
        }
        else
        {
            Future.CompleteWithError(_failure);
        }
    }

    private void Finish(IntPtr report)
    {
        _reporter.Report(_callbackId, report);
        Release();
    }
}
