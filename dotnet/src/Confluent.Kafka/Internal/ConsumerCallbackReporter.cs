// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Reports the end of a listener or commit callback a consumer ran, through
/// <c>kafka_consumer_Consumer__set_callback_result</c>, and tracks whether the current thread or flow is inside one of
/// them or inside a blocking handle commit, whether an async one is still running, and the async commit callbacks
/// reported before they ended.
/// </summary>
internal sealed class ConsumerCallbackReporter
{
    [ThreadStatic]
    private static ThreadFrame? s_syncCallbacks;

    [ThreadStatic]
    private static ThreadFrame? s_handleCommits;

    private readonly IntPtr _view;
    private readonly object _gate = new object();
    private readonly AsyncLocal<bool> _inAsyncCallback = new AsyncLocal<bool>();
    private readonly Action? _asyncCallbackStarted;
    private bool _destroyed;
    private int _asyncCallbacksRunning;
    private int _keptRunning;
    private TaskCompletionSource<bool>? _keptEnded;
    private Exception? _keptError;
    private Exception? _deferredError;

    // The raw view, not the SafeHandle: the consumer's destroy runs the pending callbacks inside the handle's
    // release, where a SafeHandle parameter is refused, and it waits for their reports.
    internal ConsumerCallbackReporter(IntPtr view, Action? asyncCallbackStarted = null)
    {
        _view = view;
        _asyncCallbackStarted = asyncCallbackStarted;
    }

    // A sync callback is inside only on the thread running it, while it runs; an async one also after an await, as
    // the flag set around its start flows into its continuations.
    internal bool InCallback => _inAsyncCallback.Value || OnThisThread(s_syncCallbacks);

    internal bool RunsAsyncCallback => Volatile.Read(ref _asyncCallbacksRunning) != 0;

    internal bool InHandleCommit => OnThisThread(s_handleCommits);

    internal bool HasKeptCallbacks => Volatile.Read(ref _keptRunning) != 0 || Volatile.Read(ref _keptError) is not null;

    // An async callback runs from here, its synchronous part included, until ReportWhenEnded or ReportAndKeep sees its
    // Task end.
    internal CallbackScope Enter(bool asyncCallback)
    {
        if (asyncCallback)
        {
            Interlocked.Increment(ref _asyncCallbacksRunning);
            _asyncCallbackStarted?.Invoke();
            bool outer = _inAsyncCallback.Value;
            _inAsyncCallback.Value = true;
            return new CallbackScope(asyncCallback: true, outerAsync: outer, outerFrame: null);
        }

        ThreadFrame? outerFrame = s_syncCallbacks;
        s_syncCallbacks = new ThreadFrame(this, outerFrame);
        return new CallbackScope(asyncCallback: false, outerAsync: false, outerFrame);
    }

    internal HandleCommitScope EnterHandleCommit()
    {
        ThreadFrame? outer = s_handleCommits;
        s_handleCommits = new ThreadFrame(this, outer);
        return new HandleCommitScope(outer);
    }

    internal void Exit(CallbackScope scope)
    {
        if (scope.AsyncCallback)
        {
            _inAsyncCallback.Value = scope.OuterAsync;
        }
        else
        {
            s_syncCallbacks = scope.OuterFrame;
        }
    }

    internal void Report(long callbackId, Exception? failure) => Report(callbackId, ErrorOf(failure));

    internal void Report(long callbackId, IntPtr error)
    {
        lock (_gate)
        {
            if (_destroyed)
            {
                FreeError(error);
                return;
            }

            IntPtr outErr = IntPtr.Zero;
            try
            {
                NativeMethods.ConsumerSetCallbackResult(_view, callbackId, IntPtr.Zero, error, ref outErr);
            }
            catch (Exception)
            {
                FreeError(error);
            }

            FreeError(outErr);
        }
    }

    // The drain thread never waits on a callback's Task: a pending one reports from its continuation. An async
    // callback that threw before returning its Task ends with that failure.
    internal void ReportWhenEnded(long callbackId, Task? task, Exception? started, Action? reported = null)
    {
        if (started is not null || task is null || task.IsCompleted)
        {
            EndAsyncCallback(callbackId, started ?? FailureOf(task), reported);
            return;
        }

        task.ContinueWith(
            ended => EndAsyncCallback(callbackId, FailureOf(ended), reported),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    // Inside a blocking handle commit the thread that runs the callback waits in that commit, so the callback is
    // reported at once and its Task is kept for the call that delivered the outer callback to wait for, as Python
    // does.
    internal void ReportAndKeep(long callbackId, Task? task, Exception? started, Action? reported = null)
    {
        lock (_gate)
        {
            Interlocked.Increment(ref _keptRunning);
        }

        if (started is not null || task is null || task.IsCompleted)
        {
            Report(callbackId, IntPtr.Zero);
            EndKeptCallback(started ?? FailureOf(task), reported);
            return;
        }

        task.ContinueWith(
            ended => EndKeptCallback(FailureOf(ended), reported),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        Report(callbackId, IntPtr.Zero);
    }

    internal Task WhenKeptCallbacksEnd()
    {
        lock (_gate)
        {
            if (_keptRunning == 0)
            {
                return Task.CompletedTask;
            }

            _keptEnded ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            return _keptEnded.Task;
        }
    }

    internal void WaitForKeptCallbacks() => WhenKeptCallbacksEnd().GetAwaiter().GetResult();

    internal Exception? TakeKeptError()
    {
        lock (_gate)
        {
            Exception? error = _keptError;
            _keptError = null;
            return error;
        }
    }

    internal void KeepForTheNextCall(Exception? error)
    {
        lock (_gate)
        {
            if (error is not null && _deferredError is null)
            {
                _deferredError = error;
            }
        }
    }

    internal Exception? TakeKeptForTheNextCall()
    {
        lock (_gate)
        {
            Exception? error = _deferredError;
            _deferredError = null;
            return error;
        }
    }

    internal void MarkDestroyed()
    {
        lock (_gate)
        {
            _destroyed = true;
        }
    }

    private static Exception? FailureOf(Task? task)
    {
        if (task is null || task.Status == TaskStatus.RanToCompletion)
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

    private static IntPtr ErrorOf(Exception? failure)
    {
        if (failure is null)
        {
            return IntPtr.Zero;
        }

        try
        {
            return ManagedExceptionCarrier.ToError(failure);
        }
        catch (Exception)
        {
            return IntPtr.Zero;
        }
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    private void EndAsyncCallback(long callbackId, Exception? failure, Action? reported)
    {
        Interlocked.Decrement(ref _asyncCallbacksRunning);
        Report(callbackId, failure);
        reported?.Invoke();
    }

    private void EndKeptCallback(Exception? failure, Action? reported)
    {
        Interlocked.Decrement(ref _asyncCallbacksRunning);
        reported?.Invoke();
        TaskCompletionSource<bool>? ended = null;
        lock (_gate)
        {
            if (failure is not null && _keptError is null)
            {
                _keptError = failure;
            }

            if (Interlocked.Decrement(ref _keptRunning) == 0)
            {
                ended = _keptEnded;
                _keptEnded = null;
            }
        }

        ended?.TrySetResult(true);
    }

    private bool OnThisThread(ThreadFrame? top)
    {
        for (ThreadFrame? frame = top; frame is not null; frame = frame.Outer)
        {
            if (ReferenceEquals(frame.Reporter, this))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>What <see cref="Enter"/> replaced, for <see cref="Exit"/> to restore.</summary>
    internal readonly struct CallbackScope
    {
        internal CallbackScope(bool asyncCallback, bool outerAsync, ThreadFrame? outerFrame)
        {
            AsyncCallback = asyncCallback;
            OuterAsync = outerAsync;
            OuterFrame = outerFrame;
        }

        internal bool AsyncCallback { get; }

        internal bool OuterAsync { get; }

        internal ThreadFrame? OuterFrame { get; }
    }

    /// <summary>Ends the blocking handle commit <see cref="EnterHandleCommit"/> began on this thread.</summary>
    internal readonly struct HandleCommitScope : IDisposable
    {
        private readonly ThreadFrame? _outer;

        internal HandleCommitScope(ThreadFrame? outer) => _outer = outer;

        public void Dispose() => s_handleCommits = _outer;
    }

    /// <summary>
    /// One sync callback or blocking handle commit running on the current thread, and the one it runs inside, if any.
    /// </summary>
    internal sealed class ThreadFrame
    {
        internal ThreadFrame(ConsumerCallbackReporter reporter, ThreadFrame? outer)
        {
            Reporter = reporter;
            Outer = outer;
        }

        internal ConsumerCallbackReporter Reporter { get; }

        internal ThreadFrame? Outer { get; }
    }
}
