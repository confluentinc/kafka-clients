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
/// The per-operation bridge context for an <b>async typed poll</b> — an
/// <see cref="OperationCompletionSource{TResult}"/> whose result is a
/// <see cref="ConsumerRecords{TKey, TValue}"/>, extended to <b>carry the two
/// deserializers</b> the completion trampoline needs to build that typed result (PLAN
/// M6/P1b §5). The poll callback is a single rooted <c>static</c> Cdecl delegate per closed
/// generic type (<c>TypedPollCallbacks&lt;TKey, TValue&gt;</c>), so it cannot capture per-op
/// serdes — it recovers this context from the <see cref="System.Runtime.InteropServices.GCHandle"/>
/// (<c>user_data</c>) and reads the serdes off it.
/// </summary>
/// <remarks>
/// All five completion invariants come from the base
/// <see cref="OperationCompletionSource{TResult}"/> (per-op <c>GCHandle</c> rooting, the
/// callback as sole owner of the free, <c>RunContinuationsAsynchronously</c>, the null-safe
/// error mapping, cancellation → <c>wakeup()</c>). This subclass only pins the serdes to the
/// op so the copy-out on the dispatcher thread can deserialize each key/value.
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
internal sealed class TypedPollCompletionSource<TKey, TValue>
    : OperationCompletionSource<ConsumerRecords<TKey, TValue>>
{
    internal TypedPollCompletionSource(
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        KeyDeserializer = keyDeserializer;
        ValueDeserializer = valueDeserializer;
    }

    /// <summary>The key deserializer applied per record during the copy-out.</summary>
    internal IDeserializer<TKey> KeyDeserializer { get; }

    /// <summary>The value deserializer applied per record during the copy-out.</summary>
    internal IDeserializer<TValue> ValueDeserializer { get; }
}
