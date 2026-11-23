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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The fields and the logic of one future engine of either flavor, <see cref="KafkaFutureImpl{T}"/> or
/// <see cref="AsyncKafkaFutureImpl{T}"/>, held by value in the engine's one field. Every call is made on that field and
/// is handed the engine (<c>owner</c>), which it locks, waits and pulses on and hands to the listeners; the struct
/// never uses itself as an object. A copy is a second future that completes while the engine stays pending, so the
/// field is never readonly, assigned again, passed by value or returned, and the struct has no async instance method
/// and no iterator; its async helpers are static and never take the struct.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal struct FutureState<T>
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

    private readonly bool _send;

    private volatile int _state;

    private bool _propagated;

    private T _value = default!;

    private ExceptionDispatchInfo? _failure;

    private List<Action<IKafkaFutureImpl<T>>>? _listeners;

    private TaskCompletionSource<T>? _task;

    // The Java Get's task, one field for the send's footprint: its completion source when it was taken while the
    // future was pending, which the completion sets, or else the finished task itself.
    private object? _wrapped;

    internal FutureState(bool mayCancel, IFutureValueSource<T>? source, bool send)
    {
        _mayCancel = mayCancel;
        _source = source;
        _send = send;
    }

    internal bool IsDone => _state != Pending || (_source is not null && _source.IsDone);

    internal bool IsSettled => _state != Pending;

    internal bool IsCancelled(IKafkaFutureImpl<T> owner)
    {
        ReadSource(owner);
        return _state == Cancelled;
    }

    internal bool IsCompletedExceptionally(IKafkaFutureImpl<T> owner)
    {
        ReadSource(owner);
        return _state >= Failed;
    }

    internal Exception? Failure => _failure?.SourceException;

    internal bool Complete(IKafkaFutureImpl<T> owner, T value)
    {
        TaskCompletionSource<T>? task;
        TaskCompletionSource<T>? wrapped;
        List<Action<IKafkaFutureImpl<T>>>? listeners;
        lock (owner)
        {
            if (_state != Pending)
            {
                return false;
            }

            _value = value;
            _state = Succeeded;
            task = _task;
            wrapped = _wrapped as TaskCompletionSource<T>;
            listeners = _listeners;
            _listeners = null;
            Monitor.PulseAll(owner);
        }

        Settled(owner, task, wrapped, listeners);
        return true;
    }

    internal bool CompleteWithError(IKafkaFutureImpl<T> owner, Exception error) =>
        Fail(owner, StateFor(error), error, propagated: false);

    internal bool Propagate(IKafkaFutureImpl<T> owner, Exception error) =>
        Fail(owner, StateFor(error), error, propagated: true);

    internal bool Cancel(IKafkaFutureImpl<T> owner, bool mayInterruptIfRunning)
    {
        if (!_mayCancel || (_state == Pending && _source is not null && _source.IsDone))
        {
            return false;
        }

        bool cancelled =
            _state == Pending && Fail(owner, Cancelled, new OperationCanceledException(), propagated: false);
        return cancelled || (_state == Cancelled && !_propagated);
    }

    internal T GetUnwrapped(IKafkaFutureImpl<T> owner)
    {
        AwaitSettled(owner, Timeout.InfiniteTimeSpan, CancellationToken.None);
        return Answer();
    }

    internal T GetUnwrapped(IKafkaFutureImpl<T> owner, TimeSpan timeout)
    {
        if (!AwaitSettled(owner, timeout, CancellationToken.None))
        {
            throw TimedOut(timeout, _send);
        }

        return Answer();
    }

    internal T GetUnwrapped(IKafkaFutureImpl<T> owner, CancellationToken cancellationToken)
    {
        AwaitSettled(owner, cancellationToken);
        return Answer();
    }

    internal T GetNowUnwrapped(IKafkaFutureImpl<T> owner, T valueIfAbsent)
    {
        ReadSource(owner);
        return _state == Pending ? valueIfAbsent : Answer();
    }

    internal T GetWrapped(IKafkaFutureImpl<T> owner)
    {
        AwaitSettled(owner, Timeout.InfiniteTimeSpan, CancellationToken.None);
        return WrappedAnswer();
    }

    internal T GetWrapped(IKafkaFutureImpl<T> owner, TimeSpan timeout)
    {
        if (!AwaitSettled(owner, timeout, CancellationToken.None))
        {
            throw TimedOut(timeout, _send);
        }

        return WrappedAnswer();
    }

    internal T GetWrapped(IKafkaFutureImpl<T> owner, CancellationToken cancellationToken)
    {
        AwaitSettled(owner, cancellationToken);
        return WrappedAnswer();
    }

    internal T GetNowWrapped(IKafkaFutureImpl<T> owner, T valueIfAbsent)
    {
        ReadSource(owner);
        return _state == Pending ? valueIfAbsent : WrappedAnswer();
    }

    internal Task<T> GetAsync(IKafkaFutureImpl<T> owner)
    {
        ReadSource(owner);
        TaskCompletionSource<T>? task = Volatile.Read(ref _task);
        if (task is not null)
        {
            return task.Task;
        }

        bool settled;
        lock (owner)
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

    internal Task<T> WaitFor(Task<T> task, TimeSpan timeout)
    {
        if (task.IsCompleted || timeout == Timeout.InfiniteTimeSpan)
        {
            return task;
        }

        if (timeout <= TimeSpan.Zero)
        {
            return Task.FromException<T>(TimedOut(timeout, _send));
        }

        return WithTimeout(task, timeout, _send);
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

    internal Task<T> GetAsync(IKafkaFutureImpl<T> owner, TimeSpan timeout) => WaitFor(GetAsync(owner), timeout);

    internal Task<T> GetAsync(IKafkaFutureImpl<T> owner, CancellationToken cancellationToken) =>
        WaitFor(GetAsync(owner), cancellationToken);

    internal Task<T> GetWrappedAsync(IKafkaFutureImpl<T> owner)
    {
        Task<T>? task = WrappedTask(Volatile.Read(ref _wrapped));
        if (task is not null)
        {
            return task;
        }

        ReadSource(owner);
        lock (owner)
        {
            task = WrappedTask(_wrapped);
            if (task is null && _state == Pending)
            {
                var wrapped = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _wrapped, wrapped);
                return wrapped.Task;
            }
        }

        if (task is not null)
        {
            return task;
        }

        // Settled: the outcome needs no completion source, and the first of two racing callers wins.
        Task<T> done = _state == Failed ? Task.FromException<T>(new ExecutionException(Failure)) : GetAsync(owner);
        return WrappedTask(Interlocked.CompareExchange(ref _wrapped, done, null)) ?? done;
    }

    internal Task<T> GetWrappedAsync(IKafkaFutureImpl<T> owner, TimeSpan timeout) =>
        WaitFor(GetWrappedAsync(owner), timeout);

    internal Task<T> GetWrappedAsync(IKafkaFutureImpl<T> owner, CancellationToken cancellationToken) =>
        WaitFor(GetWrappedAsync(owner), cancellationToken);

    internal void ThenApply<TResult>(
        IKafkaFutureImpl<T> owner, IKafkaFutureImpl<TResult> dependant, Func<T, TResult> function)
    {
        Listen(owner, source =>
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
                result = function(source.GetNowUnwrapped(default!));
            }
            catch (Exception error)
            {
                dependant.Propagate(error);
                return;
            }

            dependant.Complete(result);
        });
    }

    internal void WhenComplete(IKafkaFutureImpl<T> owner, IKafkaFutureImpl<T> dependant, Action<T?, Exception?> action)
    {
        Listen(owner, source =>
        {
            Exception? failure = source.Failure;
            Exception? thrown = null;
            try
            {
                if (failure is null)
                {
                    action(source.GetNowUnwrapped(default!), null);
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
                dependant.Complete(source.GetNowUnwrapped(default!));
            }
        });
    }

    internal void AddListener(IKafkaFutureImpl<T> owner, Action<IKafkaFutureImpl> listener) => Listen(owner, listener);

    internal string ToString(IKafkaFutureImpl<T> owner)
    {
        ReadSource(owner);
        int state = _state;
        string value = state == Succeeded && _value is not null
            ? Convert.ToString(_value, CultureInfo.InvariantCulture) ?? "null"
            : "null";
        Exception? error = state >= Failed ? Failure : null;
        string exception = error is null ? "null" : JavaText.ToString(error);
        bool done = error is not null || (state == Succeeded && _value is not null);
        return "KafkaFuture{value=" + value + ",exception=" + exception + ",done=" + (done ? "true" : "false") + "}";
    }

    private static async Task<T> WithTimeout(Task<T> task, TimeSpan timeout, bool send)
    {
        if (!await CompletesWithin(task, timeout).ConfigureAwait(false))
        {
            throw TimedOut(timeout, send);
        }

        return await task.ConfigureAwait(false);
    }

    // Java's FutureRecordMetadata names the wait in milliseconds, its TimeUnit.toMillis truncating as Ticks do here;
    // Java's KafkaFutureImpl throws CompletableFuture's TimeoutException, which has no message.
    private static Exception TimedOut(TimeSpan timeout, bool send) =>
        send
            ? new Confluent.Kafka.TimeoutException(
                "Timeout after waiting for " +
                (timeout.Ticks / TimeSpan.TicksPerMillisecond).ToString(CultureInfo.InvariantCulture) + " ms.")
            : new Confluent.Kafka.TimeoutException();

    private static async Task<T> WithCancellation(Task<T> task, CancellationToken cancellationToken)
    {
        await AwaitUntil(task, cancellationToken).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    private int StateFor(Exception error) =>
        error is OperationCanceledException && _mayCancel ? Cancelled : Failed;

    private bool Fail(IKafkaFutureImpl<T> owner, int state, Exception error, bool propagated)
    {
        var failure = ExceptionDispatchInfo.Capture(error);
        TaskCompletionSource<T>? task;
        TaskCompletionSource<T>? wrapped;
        List<Action<IKafkaFutureImpl<T>>>? listeners;
        lock (owner)
        {
            if (_state != Pending)
            {
                return false;
            }

            _failure = failure;
            _propagated = propagated;
            _state = state;
            task = _task;
            wrapped = _wrapped as TaskCompletionSource<T>;
            listeners = _listeners;
            _listeners = null;
            Monitor.PulseAll(owner);
        }

        Settled(owner, task, wrapped, listeners);
        return true;
    }

    private void Settled(
        IKafkaFutureImpl<T> owner,
        TaskCompletionSource<T>? task,
        TaskCompletionSource<T>? wrapped,
        List<Action<IKafkaFutureImpl<T>>>? listeners)
    {
        if (task is not null)
        {
            SetTask(task);
        }

        if (wrapped is not null)
        {
            SetWrappedTask(wrapped);
        }

        if (listeners is not null)
        {
            foreach (Action<IKafkaFutureImpl<T>> listener in listeners)
            {
                listener(owner);
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

    private static Task<T>? WrappedTask(object? wrapped) =>
        wrapped is TaskCompletionSource<T> source ? source.Task : (Task<T>?)wrapped;

    // The Get task's outcome: a failure inside an ExecutionException, a value or a cancellation as SetTask gives it.
    private void SetWrappedTask(TaskCompletionSource<T> wrapped)
    {
        if (_state == Failed)
        {
            wrapped.TrySetException(new ExecutionException(_failure!.SourceException));
        }
        else
        {
            SetTask(wrapped);
        }
    }

    private void Listen(IKafkaFutureImpl<T> owner, Action<IKafkaFutureImpl<T>> listener)
    {
        lock (owner)
        {
            if (_state == Pending)
            {
                (_listeners ??= new List<Action<IKafkaFutureImpl<T>>>()).Add(listener);
                return;
            }
        }

        listener(owner);
    }

    private void ReadSource(IKafkaFutureImpl<T> owner)
    {
        if (_source is null || _state != Pending)
        {
            return;
        }

        if (_source.TryRead(out T value, out Exception? error))
        {
            if (error is null)
            {
                Complete(owner, value);
            }
            else
            {
                CompleteWithError(owner, error);
            }
        }
    }

    private bool AwaitSettled(IKafkaFutureImpl<T> owner, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Stopwatch? clock = timeout == Timeout.InfiniteTimeSpan ? null : Stopwatch.StartNew();
        while (true)
        {
            ReadSource(owner);
            lock (owner)
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

                Monitor.Wait(owner, wait);
            }
        }
    }

    private T Answer()
    {
        _failure?.Throw();
        return _value;
    }

    // Java's get and getNow: a cancelled future throws its cancellation as it is, and a failure goes inside an
    // ExecutionException. An OperationCanceledException given as the error of a future that can be cancelled cancels
    // it, so it is thrown as it is, as Java's KafkaFutureImpl throws a stored CancellationException; on a send's
    // future, which cannot be, it is a failure like any other, as Java's FutureRecordMetadata wraps every error.
    private T WrappedAnswer()
    {
        if (_state == Failed)
        {
            throw new ExecutionException(_failure!.SourceException);
        }

        return Answer();
    }

    private void AwaitSettled(IKafkaFutureImpl<T> owner, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            AwaitSettled(owner, Timeout.InfiniteTimeSpan, CancellationToken.None);
            return;
        }

        ReadSource(owner);
        if (_state == Pending)
        {
            bool settled;
            using (cancellationToken.Register(s_pulseWaiters, owner))
            {
                settled = AwaitSettled(owner, Timeout.InfiniteTimeSpan, cancellationToken);
            }

            if (!settled)
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }
    }

    private static long RemainingMilliseconds(Stopwatch clock, TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            return 0;
        }

        TimeSpan left = timeout - clock.Elapsed;
        return left <= TimeSpan.Zero ? 0 : (long)Math.Ceiling(left.TotalMilliseconds);
    }

    private static async Task<bool> CompletesWithin(Task task, TimeSpan timeout)
    {
        var clock = Stopwatch.StartNew();
        using var stopDelay = new CancellationTokenSource();
        while (true)
        {
            long left = RemainingMilliseconds(clock, timeout);
            if (left <= 0)
            {
                return false;
            }

            Task delay = Task.Delay((int)Math.Min(left, int.MaxValue), stopDelay.Token);
            if (await Task.WhenAny(task, delay).ConfigureAwait(false) == task)
            {
                stopDelay.Cancel();
                return true;
            }
        }
    }

    private static async Task AwaitUntil(Task task, CancellationToken cancellationToken)
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
}

/// <summary>
/// Java's <c>KafkaFuture.completedFuture</c> and <c>allOf</c>, and the <c>allOf(...).thenApply(...)</c> that gathers
/// the values of a map of futures, each over a future its caller made, so the caller chooses the engine.
/// </summary>
internal static class FutureState
{
    // Java's allOf dereferences each future, and Python's all_of refuses None with this text.
    internal const string ContainsNullMessage = "futures must not contain null";

    internal static TFuture CompletedFuture<TFuture, TValue>(TFuture future, TValue value)
        where TFuture : IKafkaFutureImpl<TValue>
    {
        future.Complete(value);
        return future;
    }

    internal static TFuture AllOf<TFuture>(TFuture result, IReadOnlyList<IKafkaFutureImpl> futures)
        where TFuture : IKafkaFutureImpl<object?>
    {
        if (futures.Count == 0)
        {
            result.Complete(null);
            return result;
        }

        int remaining = futures.Count;
        Exception? firstFailure = null;
        Action<IKafkaFutureImpl> onDone = source =>
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
        foreach (IKafkaFutureImpl future in futures)
        {
            future.AddListener(onDone);
        }

        return result;
    }

    internal static IKafkaFutureImpl<IReadOnlyDictionary<TKey, TValue>> GatherAll<TKey, TValue>(
        IKafkaFutureImpl<object?> all,
        IReadOnlyCollection<KeyValuePair<TKey, IKafkaFutureImpl<TValue>>> futures,
        IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        List<IKafkaFutureImpl> sources = new List<IKafkaFutureImpl>(futures.Count);
        foreach (KeyValuePair<TKey, IKafkaFutureImpl<TValue>> entry in futures)
        {
            sources.Add(entry.Value);
        }

        return AllOf(all, sources).ThenApplyFunc<IReadOnlyDictionary<TKey, TValue>>(_ =>
        {
            Dictionary<TKey, TValue> values = new Dictionary<TKey, TValue>(futures.Count, comparer);
            foreach (KeyValuePair<TKey, IKafkaFutureImpl<TValue>> entry in futures)
            {
                values[entry.Key] = entry.Value.GetNowUnwrapped(default!);
            }

            return values;
        });
    }
}
