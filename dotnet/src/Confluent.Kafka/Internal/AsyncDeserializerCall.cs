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

using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// How an asynchronous consumer calls one of its two deserializers, decided once when the consumer is built. A
/// built-in is called through its synchronous class, with no awaiting: over a span, or over a memory it may keep
/// when that class has the memory form. Any other class is awaited, with a memory it may keep when it is an
/// <see cref="IAsyncDeserializerWithRetention{T}"/> and with one revoked when its call completes otherwise.
/// </summary>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
internal sealed class AsyncDeserializerCall<T>
{
    internal AsyncDeserializerCall(IAsyncDeserializer<T> deserializer)
    {
        Deserializer = deserializer;
        if (deserializer is IAsyncBuiltInDeserializer<T> builtIn)
        {
            Inner = builtIn.Inner;
            InnerWithHeaders = Inner as IDeserializerWithHeaders<T>;
            InnerWithMemory = Inner as IDeserializerWithReadOnlyMemory<T>;
        }
        else
        {
            Retains = deserializer is IAsyncDeserializerWithRetention<T>;
        }
    }

    internal IAsyncDeserializer<T> Deserializer { get; }

    // The synchronous class of a built-in, null for any other deserializer, which is awaited.
    internal IDeserializer<T>? Inner { get; }

    internal IDeserializerWithHeaders<T>? InnerWithHeaders { get; }

    internal IDeserializerWithReadOnlyMemory<T>? InnerWithMemory { get; }

    // Whether an awaited deserializer may keep the memory it is given.
    internal bool Retains { get; }
}
