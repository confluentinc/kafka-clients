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
using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A <see cref="KafkaFutureImpl{T}"/> seen without its value type, which is how a future of no value (Java's
/// <c>KafkaFuture&lt;Void&gt;</c>) and <c>AllOf</c> over futures of different types hold one. It also holds the timed
/// and cancellable waits on a task, which the async futures share.
/// </summary>
internal abstract class KafkaFutureImpl
{
    private protected KafkaFutureImpl()
    {
    }

    internal abstract bool IsDone { get; }

    internal abstract bool IsCancelled { get; }

    internal abstract bool IsCompletedExceptionally { get; }

    internal abstract Exception? Failure { get; }

    internal static KafkaFutureImpl<TValue> CompletedFuture<TValue>(TValue value)
    {
        var future = new KafkaFutureImpl<TValue>();
        future.Complete(value);
        return future;
    }

    internal static KafkaFutureImpl<object?> AllOf(IReadOnlyList<KafkaFutureImpl> futures)
    {
        var result = new KafkaFutureImpl<object?>();
        if (futures.Count == 0)
        {
            result.Complete(null);
            return result;
        }

        int remaining = futures.Count;
        Exception? firstFailure = null;
        Action<KafkaFutureImpl> onDone = source =>
        {
            Exception? failure = source.Failure;
            if (failure is not null)
            {
                Interlocked.CompareExchange(ref firstFailure, failure, null);
            }

            if (Interlocked.Decrement(ref remaining) == 0)
            {
                Exception? first = Volatile.Read(ref firstFailure);
                if (first is null)
                {
                    result.Complete(null);
                }
                else
                {
                    result.CompleteWithError(first);
                }
            }
        };
        foreach (KafkaFutureImpl future in futures)
        {
            future.AddListener(onDone);
        }

        return result;
    }

    internal static Task WaitFor(Task task, TimeSpan timeout)
    {
        if (task.IsCompleted || timeout == Timeout.InfiniteTimeSpan)
        {
            return task;
        }

        if (timeout <= TimeSpan.Zero)
        {
            return Task.FromException(new TimeoutException());
        }

        return WithTimeout(task, timeout);
    }

    internal static Task WaitFor(Task task, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || !cancellationToken.CanBeCanceled)
        {
            return task;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        return WithCancellation(task, cancellationToken);
    }

    internal abstract bool Cancel(bool mayInterruptIfRunning);

    internal abstract void Wait();

    internal abstract void Wait(TimeSpan timeout);

    internal abstract void Wait(CancellationToken cancellationToken);

    internal abstract void GetNow();

    internal abstract Task WaitAsync();

    internal abstract Task WaitAsync(TimeSpan timeout);

    internal abstract Task WaitAsync(CancellationToken cancellationToken);

    internal abstract KafkaFutureImpl<TResult> ThenApply<TResult>(Func<TResult> function);

    internal abstract KafkaFutureImpl WhenComplete(Action<Exception?> action);

    internal abstract void AddListener(Action<KafkaFutureImpl> listener);

    private protected static long RemainingMilliseconds(Stopwatch clock, TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            return 0;
        }

        TimeSpan left = timeout - clock.Elapsed;
        return left <= TimeSpan.Zero ? 0 : (long)Math.Ceiling(left.TotalMilliseconds);
    }

    private protected static async Task AwaitWithin(Task task, TimeSpan timeout)
    {
        var clock = Stopwatch.StartNew();
        using var stopDelay = new CancellationTokenSource();
        while (true)
        {
            long left = RemainingMilliseconds(clock, timeout);
            if (left <= 0)
            {
                throw new TimeoutException();
            }

            Task delay = Task.Delay((int)Math.Min(left, int.MaxValue), stopDelay.Token);
            if (await Task.WhenAny(task, delay).ConfigureAwait(false) == task)
            {
                stopDelay.Cancel();
                return;
            }
        }
    }

    private protected static async Task AwaitUntil(Task task, CancellationToken cancellationToken)
    {
        var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(
                   static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true), cancelled))
        {
            if (await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false) != task)
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }
    }

    private static async Task WithTimeout(Task task, TimeSpan timeout)
    {
        await AwaitWithin(task, timeout).ConfigureAwait(false);
        await task.ConfigureAwait(false);
    }

    private static async Task WithCancellation(Task task, CancellationToken cancellationToken)
    {
        await AwaitUntil(task, cancellationToken).ConfigureAwait(false);
        await task.ConfigureAwait(false);
    }
}

/// <summary>
/// Java's <c>KafkaFutureImpl</c>: a future completed once, from any thread, the first completion winning; an
/// <see cref="OperationCanceledException"/> given as its error cancels it, and <c>Cancel</c> answers true only for its
/// own cancellation, not one a dependant took from its source or its function. A future made unable to cancel is never
/// cancelled: <c>Cancel</c> does nothing and such an error is an ordinary failure. Its dependants and
/// <c>WhenComplete</c> actions run inline on the completing thread, outside its lock, or inside the registering call
/// once it is done. An optional value source is read first by every state read but <c>IsDone</c> and <c>Cancel</c>,
/// and by every wait, while it is pending, settling it on the reading thread, and waits on it re-read the source in
/// slices. <c>IsDone</c> and <c>Cancel</c> only ask the source whether it is done, so they settle nothing and run no
/// listener, and while the future is pending and its source is done <c>Cancel</c> answers false. The
/// <see cref="Task{TResult}"/> of the async waits is made on the first of them and is the same every time.
/// </summary>
internal sealed class KafkaFutureImpl<T> : KafkaFutureImpl
{
    private const int Pending = 0;

    private const int Succeeded = 1;

    private const int Failed = 2;

    private const int Cancelled = 3;

    private const int SourceSliceMilliseconds = 100;

    private static readonly Action<object?> s_pulseWaiters = state =>
    {
        lock (state!)
        {
            Monitor.PulseAll(state);
        }
    };

    private readonly bool _mayCancel;

    private readonly IFutureValueSource<T>? _source;

    private volatile int _state;

    private bool _propagated;

    private T _value = default!;

    private ExceptionDispatchInfo? _failure;

    private List<Action<KafkaFutureImpl<T>>>? _listeners;

    private TaskCompletionSource<T>? _task;

    internal KafkaFutureImpl(bool mayCancel = true, IFutureValueSource<T>? source = null)
    {
        _mayCancel = mayCancel;
        _source = source;
    }

    internal override bool IsDone => _state != Pending || (_source is not null && _source.IsDone);

    internal override bool IsCancelled
    {
        get
        {
            ReadSource();
            return _state == Cancelled;
        }
    }

    internal override bool IsCompletedExceptionally
    {
        get
        {
            ReadSource();
            return _state >= Failed;
        }
    }

    internal override Exception? Failure => _failure?.SourceException;

    internal bool Complete(T value)
    {
        TaskCompletionSource<T>? task;
        List<Action<KafkaFutureImpl<T>>>? listeners;
        lock (this)
        {
            if (_state != Pending)
            {
                return false;
            }

            _value = value;
            _state = Succeeded;
            task = _task;
            listeners = _listeners;
            _listeners = null;
            Monitor.PulseAll(this);
        }

        Settled(task, listeners);
        return true;
    }

    internal bool CompleteWithError(Exception error) => Fail(StateFor(error), error, propagated: false);

    internal override bool Cancel(bool mayInterruptIfRunning)
    {
        if (!_mayCancel || (_state == Pending && _source is not null && _source.IsDone))
        {
            return false;
        }

        bool cancelled = _state == Pending && Fail(Cancelled, new OperationCanceledException(), propagated: false);
        return cancelled || (_state == Cancelled && !_propagated);
    }

    internal T Get()
    {
        AwaitSettled(Timeout.InfiniteTimeSpan, CancellationToken.None);
        return Answer();
    }

    internal T Get(TimeSpan timeout)
    {
        if (!AwaitSettled(timeout, CancellationToken.None))
        {
            throw new TimeoutException();
        }

        return Answer();
    }

    internal T Get(CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            return Get();
        }

        ReadSource();
        if (_state == Pending)
        {
            bool settled;
            using (cancellationToken.Register(s_pulseWaiters, this))
            {
                settled = AwaitSettled(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            if (!settled)
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }

        return Answer();
    }

    internal T GetNow(T valueIfAbsent)
    {
        ReadSource();
        return _state == Pending ? valueIfAbsent : Answer();
    }

    internal override void Wait() => _ = Get();

    internal override void Wait(TimeSpan timeout) => _ = Get(timeout);

    internal override void Wait(CancellationToken cancellationToken) => _ = Get(cancellationToken);

    internal override void GetNow() => _ = GetNow(default!);

    internal Task<T> GetAsync()
    {
        ReadSource();
        TaskCompletionSource<T>? task = Volatile.Read(ref _task);
        if (task is not null)
        {
            return task.Task;
        }

        bool settled;
        lock (this)
        {
            task = _task;
            if (task is null)
            {
                task = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _task, task);
            }

            settled = _state != Pending;
        }

        if (settled)
        {
            SetTask(task);
        }

        return task.Task;
    }

    internal static Task<T> WaitFor(Task<T> task, TimeSpan timeout)
    {
        if (task.IsCompleted || timeout == Timeout.InfiniteTimeSpan)
        {
            return task;
        }

        if (timeout <= TimeSpan.Zero)
        {
            return Task.FromException<T>(new TimeoutException());
        }

        return WithTimeout(task, timeout);
    }

    internal static Task<T> WaitFor(Task<T> task, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || !cancellationToken.CanBeCanceled)
        {
            return task;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        return WithCancellation(task, cancellationToken);
    }

    internal Task<T> GetAsync(TimeSpan timeout) => WaitFor(GetAsync(), timeout);

    internal Task<T> GetAsync(CancellationToken cancellationToken) => WaitFor(GetAsync(), cancellationToken);

    internal override Task WaitAsync() => GetAsync();

    internal override Task WaitAsync(TimeSpan timeout) => GetAsync(timeout);

    internal override Task WaitAsync(CancellationToken cancellationToken) => GetAsync(cancellationToken);

    internal KafkaFutureImpl<TResult> ThenApply<TResult>(Func<T, TResult> function)
    {
        var dependant = new KafkaFutureImpl<TResult>();
        Listen(source =>
        {
            Exception? failure = source.Failure;
            if (failure is not null)
            {
                dependant.Propagate(failure);
                return;
            }

            TResult result;
            try
            {
                result = function(source._value);
            }
            catch (Exception error)
            {
                dependant.Propagate(error);
                return;
            }

            dependant.Complete(result);
        });
        return dependant;
    }

    internal override KafkaFutureImpl<TResult> ThenApply<TResult>(Func<TResult> function) =>
        ThenApply(_ => function());

    internal KafkaFutureImpl<T> WhenComplete(Action<T?, Exception?> action)
    {
        var dependant = new KafkaFutureImpl<T>();
        Listen(source =>
        {
            Exception? failure = source.Failure;
            Exception? thrown = null;
            try
            {
                if (failure is null)
                {
                    action(source._value, null);
                }
                else
                {
                    action(default, failure);
                }
            }
            catch (Exception error)
            {
                thrown = error;
            }

            if (failure is not null)
            {
                dependant.Propagate(failure);
            }
            else if (thrown is not null)
            {
                dependant.Propagate(thrown);
            }
            else
            {
                dependant.Complete(source._value);
            }
        });
        return dependant;
    }

    internal override KafkaFutureImpl WhenComplete(Action<Exception?> action) =>
        WhenComplete((_, error) => action(error));

    internal override void AddListener(Action<KafkaFutureImpl> listener) => Listen(listener);

    public override string ToString()
    {
        ReadSource();
        int state = _state;
        string value = state == Succeeded && _value is not null
            ? Convert.ToString(_value, CultureInfo.InvariantCulture) ?? "null"
            : "null";
        Exception? error = state >= Failed ? Failure : null;
        string exception = error is null ? "null" : error.GetType().FullName + ": " + error.Message;
        bool done = error is not null || (state == Succeeded && _value is not null);
        return "KafkaFuture{value=" + value + ",exception=" + exception + ",done=" + (done ? "true" : "false") + "}";
    }

    private static async Task<T> WithTimeout(Task<T> task, TimeSpan timeout)
    {
        await AwaitWithin(task, timeout).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    private static async Task<T> WithCancellation(Task<T> task, CancellationToken cancellationToken)
    {
        await AwaitUntil(task, cancellationToken).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    private int StateFor(Exception error) =>
        error is OperationCanceledException && _mayCancel ? Cancelled : Failed;

    private bool Propagate(Exception error) => Fail(StateFor(error), error, propagated: true);

    private bool Fail(int state, Exception error, bool propagated)
    {
        var failure = ExceptionDispatchInfo.Capture(error);
        TaskCompletionSource<T>? task;
        List<Action<KafkaFutureImpl<T>>>? listeners;
        lock (this)
        {
            if (_state != Pending)
            {
                return false;
            }

            _failure = failure;
            _propagated = propagated;
            _state = state;
            task = _task;
            listeners = _listeners;
            _listeners = null;
            Monitor.PulseAll(this);
        }

        Settled(task, listeners);
        return true;
    }

    private void Settled(TaskCompletionSource<T>? task, List<Action<KafkaFutureImpl<T>>>? listeners)
    {
        if (task is not null)
        {
            SetTask(task);
        }

        if (listeners is not null)
        {
            foreach (Action<KafkaFutureImpl<T>> listener in listeners)
            {
                listener(this);
            }
        }
    }

    private void SetTask(TaskCompletionSource<T> task)
    {
        switch (_state)
        {
            case Succeeded:
                task.TrySetResult(_value);
                break;
            case Cancelled:
                task.TrySetCanceled(((OperationCanceledException)_failure!.SourceException).CancellationToken);
                break;
            default:
                task.TrySetException(_failure!.SourceException);
                break;
        }
    }

    private void Listen(Action<KafkaFutureImpl<T>> listener)
    {
        lock (this)
        {
            if (_state == Pending)
            {
                (_listeners ??= new List<Action<KafkaFutureImpl<T>>>()).Add(listener);
                return;
            }
        }

        listener(this);
    }

    private void ReadSource()
    {
        if (_source is null || _state != Pending)
        {
            return;
        }

        if (_source.TryRead(out T value, out Exception? error))
        {
            if (error is null)
            {
                Complete(value);
            }
            else
            {
                CompleteWithError(error);
            }
        }
    }

    private bool AwaitSettled(TimeSpan timeout, CancellationToken cancellationToken)
    {
        Stopwatch? clock = timeout == Timeout.InfiniteTimeSpan ? null : Stopwatch.StartNew();
        while (true)
        {
            ReadSource();
            lock (this)
            {
                if (_state != Pending)
                {
                    return true;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }

                int wait = _source is null ? Timeout.Infinite : SourceSliceMilliseconds;
                if (clock is not null)
                {
                    long left = RemainingMilliseconds(clock, timeout);
                    if (left <= 0)
                    {
                        return false;
                    }

                    if (wait == Timeout.Infinite || left < wait)
                    {
                        wait = (int)Math.Min(left, int.MaxValue);
                    }
                }

                Monitor.Wait(this, wait);
            }
        }
    }

    private T Answer()
    {
        _failure?.Throw();
        return _value;
    }
}

/// <summary>
/// The outcome a pending <see cref="KafkaFutureImpl{T}"/> reads before its own state: whether it is done, read without
/// waiting or settling the future, and its value or error when it is.
/// </summary>
internal interface IFutureValueSource<T>
{
    bool IsDone { get; }

    bool TryRead(out T value, out Exception? error);
}
