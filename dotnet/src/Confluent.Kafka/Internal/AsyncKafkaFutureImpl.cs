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

using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>KafkaFutureImpl</c>, and the async <see cref="AsyncKafkaFuture{T}"/> itself: the future
/// <see cref="KafkaFutureImpl{T}"/> is, over the same <see cref="FutureState{T}"/>, whose waits hand out tasks. The
/// task of <c>ToCompletionStage</c> is made on the first call and is the same every time, and so is the one of
/// <c>Get()</c>. Its dependants are async futures too.
/// </summary>
internal sealed class AsyncKafkaFutureImpl<T> : AsyncKafkaFuture<T>, IKafkaFutureImpl<T>
{
    // The future's fields and logic, held in this object: never readonly and never copied, or the copy completes
    // while this future stays pending.
    private FutureState<T> _state;

    internal AsyncKafkaFutureImpl(bool mayCancel = true, IFutureValueSource<T>? source = null)
        : this(mayCancel, source, send: false)
    {
    }

    private AsyncKafkaFutureImpl(bool mayCancel, IFutureValueSource<T>? source, bool send)
    {
        _state = new FutureState<T>(mayCancel, source, send);
    }

    public bool IsSettled => _state.IsSettled;

    public Exception? Failure => _state.Failure;

    public bool Complete(T value) => _state.Complete(this, value);

    public bool CompleteWithError(Exception error) => _state.CompleteWithError(this, error);

    public bool Propagate(Exception error) => _state.Propagate(this, error);

    /// <summary>
    /// The future an async send returns, Java's <c>FutureRecordMetadata</c>: it is never cancelled, and its timed wait
    /// names the time it waited.
    /// </summary>
    internal static AsyncKafkaFutureImpl<T> ForSend() =>
        new AsyncKafkaFutureImpl<T>(mayCancel: false, source: null, send: true);

    public override Task<T> ToCompletionStage() => _state.GetAsync(this);

    public override AsyncKafkaFuture<TR> ThenApply<TR>(KafkaFuture.BaseFunction<T, TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return ThenApplyFunc<TR>(function.Invoke);
    }

    public override AsyncKafkaFuture<TR> ThenApply<TR>(Func<TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return ThenApplyFunc(_ => function());
    }

    public override AsyncKafkaFuture<T> WhenComplete(KafkaFuture.BiConsumer<T?, Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return WhenCompleteAction(action.Invoke);
    }

    public override AsyncKafkaFuture WhenComplete(Action<Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return WhenCompleteAction((_, error) => action(error));
    }

    public override bool Cancel(bool mayInterruptIfRunning) => _state.Cancel(this, mayInterruptIfRunning);

    public override Task<T> Get() => _state.GetWrappedAsync(this);

    public override Task<T> Get(TimeSpan timeout) => _state.GetWrappedAsync(this, timeout);

    public override Task<T> Get(CancellationToken cancellationToken) => _state.GetWrappedAsync(this, cancellationToken);

    public override T GetNow(T valueIfAbsent) => _state.GetNowWrapped(this, valueIfAbsent);

    public override void GetNow() => _ = _state.GetNowWrapped(this, default!);

    public override bool IsCancelled() => _state.IsCancelled(this);

    public override bool IsCompletedExceptionally() => _state.IsCompletedExceptionally(this);

    public override bool IsDone() => _state.IsDone;

    public T GetUnwrapped() => _state.GetUnwrapped(this);

    public T GetUnwrapped(TimeSpan timeout) => _state.GetUnwrapped(this, timeout);

    public T GetUnwrapped(CancellationToken cancellationToken) => _state.GetUnwrapped(this, cancellationToken);

    public T GetNowUnwrapped(T valueIfAbsent) => _state.GetNowUnwrapped(this, valueIfAbsent);

    internal AsyncKafkaFutureImpl<TResult> ThenApplyFunc<TResult>(Func<T, TResult> function)
    {
        var dependant = new AsyncKafkaFutureImpl<TResult>();
        _state.ThenApply(this, dependant, function);
        return dependant;
    }

    IKafkaFutureImpl<TResult> IKafkaFutureImpl<T>.ThenApplyFunc<TResult>(Func<T, TResult> function) =>
        ThenApplyFunc(function);

    internal AsyncKafkaFutureImpl<T> WhenCompleteAction(Action<T?, Exception?> action)
    {
        var dependant = new AsyncKafkaFutureImpl<T>();
        _state.WhenComplete(this, dependant, action);
        return dependant;
    }

    public void AddListener(Action<IKafkaFutureImpl> listener) => _state.AddListener(this, listener);

    public override string ToString() => _state.ToString(this);
}
