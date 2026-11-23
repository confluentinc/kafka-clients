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

using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>KafkaFutureImpl</c>, and the sync <see cref="KafkaFuture{T}"/> itself, whose waits block the calling
/// thread; <see cref="AsyncKafkaFutureImpl{T}"/> is the async one, over the same <see cref="FutureState{T}"/>. Its
/// dependants are sync futures too. It is a future completed once, from any thread, the first completion winning; an
/// <see cref="OperationCanceledException"/> given as its error cancels it, and
/// <c>Cancel</c> answers true only for its own cancellation, not one a dependant took from its source or its function.
/// A future made unable to cancel is never cancelled: <c>Cancel</c> does nothing and such an error is an ordinary
/// failure. Its dependants and <c>WhenComplete</c> actions run inline on the completing thread, outside its lock, or
/// inside the registering call once it is done. An optional value source is read first by every state read but
/// <c>IsDone()</c> and <c>Cancel</c>, and by every wait, while it is pending, settling it on the reading thread, and
/// waits on it re-read the source in slices. <c>IsDone()</c> and <c>Cancel</c> only ask the source whether it is done,
/// so they settle nothing and run no listener, and while the future is pending and its source is done <c>Cancel</c>
/// answers false. A timed wait that ends first throws Java's
/// <see cref="TimeoutException"/>, with no message, or for a send's future with the message of Java's
/// <c>FutureRecordMetadata</c>.
/// </summary>
internal sealed class KafkaFutureImpl<T> : KafkaFuture<T>, IKafkaFutureImpl<T>
{
    // The future's fields and logic, held in this object: never readonly and never copied, or the copy completes
    // while this future stays pending.
    private FutureState<T> _state;

    internal KafkaFutureImpl(bool mayCancel = true, IFutureValueSource<T>? source = null)
        : this(mayCancel, source, send: false)
    {
    }

    private KafkaFutureImpl(bool mayCancel, IFutureValueSource<T>? source, bool send)
    {
        _state = new FutureState<T>(mayCancel, source, send);
    }

    public bool IsSettled => _state.IsSettled;

    public Exception? Failure => _state.Failure;

    public bool Complete(T value) => _state.Complete(this, value);

    public bool CompleteWithError(Exception error) => _state.CompleteWithError(this, error);

    public bool Propagate(Exception error) => _state.Propagate(this, error);

    /// <summary>
    /// The future a send returns, Java's <c>FutureRecordMetadata</c>: it is never cancelled, and its timed wait names
    /// the time it waited.
    /// </summary>
    internal static KafkaFutureImpl<T> ForSend() => new KafkaFutureImpl<T>(mayCancel: false, source: null, send: true);

    public override KafkaFuture<TR> ThenApply<TR>(BaseFunction<T, TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return ThenApplyFunc<TR>(function.Invoke);
    }

    public override KafkaFuture<TR> ThenApply<TR>(Func<TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return ThenApplyFunc(_ => function());
    }

    public override KafkaFuture<T> WhenComplete(BiConsumer<T?, Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return WhenCompleteAction(action.Invoke);
    }

    public override KafkaFuture WhenComplete(Action<Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return WhenCompleteAction((_, error) => action(error));
    }

    public override bool Cancel(bool mayInterruptIfRunning) => _state.Cancel(this, mayInterruptIfRunning);

    public override T Get() => _state.GetWrapped(this);

    public override T Get(TimeSpan timeout) => _state.GetWrapped(this, timeout);

    public override T Get(CancellationToken cancellationToken) => _state.GetWrapped(this, cancellationToken);

    public override T GetNow(T valueIfAbsent) => _state.GetNowWrapped(this, valueIfAbsent);

    public override void GetNow() => _ = _state.GetNowWrapped(this, default!);

    public override bool IsCancelled() => _state.IsCancelled(this);

    public override bool IsCompletedExceptionally() => _state.IsCompletedExceptionally(this);

    public override bool IsDone() => _state.IsDone;

    public T GetUnwrapped() => _state.GetUnwrapped(this);

    public T GetUnwrapped(TimeSpan timeout) => _state.GetUnwrapped(this, timeout);

    public T GetUnwrapped(CancellationToken cancellationToken) => _state.GetUnwrapped(this, cancellationToken);

    public T GetNowUnwrapped(T valueIfAbsent) => _state.GetNowUnwrapped(this, valueIfAbsent);

    internal KafkaFutureImpl<TResult> ThenApplyFunc<TResult>(Func<T, TResult> function)
    {
        var dependant = new KafkaFutureImpl<TResult>();
        _state.ThenApply(this, dependant, function);
        return dependant;
    }

    internal KafkaFutureImpl<T> WhenCompleteAction(Action<T?, Exception?> action)
    {
        var dependant = new KafkaFutureImpl<T>();
        _state.WhenComplete(this, dependant, action);
        return dependant;
    }

    IKafkaFutureImpl<TResult> IKafkaFutureImpl<T>.ThenApplyFunc<TResult>(Func<T, TResult> function) =>
        ThenApplyFunc(function);

    public void AddListener(Action<IKafkaFutureImpl> listener) => _state.AddListener(this, listener);

    public override string ToString() => _state.ToString(this);
}

/// <summary>
/// The outcome a pending future engine of either flavor, <see cref="KafkaFutureImpl{T}"/> or
/// <see cref="AsyncKafkaFutureImpl{T}"/>, reads before its own state: whether it is done, read without waiting or
/// settling the future, and its value or error when it is.
/// </summary>
internal interface IFutureValueSource<T>
{
    bool IsDone { get; }

    bool TryRead(out T value, out Exception? error);
}
