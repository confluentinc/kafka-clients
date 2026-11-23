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

namespace Confluent.Kafka.Internal;

/// <summary>
/// A future engine of either flavor seen without its value type, which is how <c>AllOf</c> over futures of different
/// types and a listener hold one.
/// </summary>
internal interface IKafkaFutureImpl
{
    Exception? Failure { get; }

    void AddListener(Action<IKafkaFutureImpl> listener);
}

/// <summary>
/// A future engine of either flavor as its <see cref="FutureState{T}"/> and the shared code talk to it: the object the
/// state locks, waits and pulses on, the source its listeners read the value of, and the future a dependant or
/// <c>AllOf</c> completes. Its reads keep the failure itself, and its <c>ThenApplyFunc</c> dependant has the flavor of
/// the engine it is called on.
/// </summary>
internal interface IKafkaFutureImpl<T> : IKafkaFutureImpl
{
    bool IsSettled { get; }

    bool IsCancelled();

    bool Complete(T value);

    bool CompleteWithError(Exception error);

    bool Propagate(Exception error);

    T GetUnwrapped();

    T GetUnwrapped(TimeSpan timeout);

    T GetUnwrapped(CancellationToken cancellationToken);

    T GetNowUnwrapped(T valueIfAbsent);

    IKafkaFutureImpl<TResult> ThenApplyFunc<TResult>(Func<T, TResult> function);
}
