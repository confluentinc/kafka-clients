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

namespace Confluent.Kafka.Internal;

/// <summary>
/// A blocking source a <see cref="KafkaFuture"/> can wrap, and the base of every <see cref="SyncFutureSource{T}"/>:
/// a latch (<see cref="SyncCompletion{T}"/>) or a composite over other sources, waited on by the calling thread.
/// </summary>
internal abstract class SyncFutureSource
{
    internal abstract bool IsDone { get; }

    internal abstract void Wait();
}

/// <summary>A <see cref="SyncFutureSource"/> that completes with a value, which a <see cref="KafkaFuture{T}"/> can wrap.</summary>
internal abstract class SyncFutureSource<T> : SyncFutureSource
{
    internal abstract T Get();

    internal sealed override void Wait()
    {
        _ = Get();
    }
}
