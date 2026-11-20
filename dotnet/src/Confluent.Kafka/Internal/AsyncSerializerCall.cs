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
/// How an asynchronous producer calls one of its two serializers, decided once when the producer is built. A built-in
/// is called through its synchronous class, with no awaiting. Any other class is awaited.
/// </summary>
/// <typeparam name="T">The type encoded.</typeparam>
internal sealed class AsyncSerializerCall<T>
{
    internal AsyncSerializerCall(IAsyncSerializer<T> serializer)
    {
        Serializer = serializer;
        if (serializer is IAsyncBuiltInSerializer<T> builtIn)
        {
            Inner = builtIn.Inner;
            InnerWithHeaders = Inner as ISerializerWithHeaders<T>;
        }
    }

    internal IAsyncSerializer<T> Serializer { get; }

    // The synchronous class of a built-in, null for any other serializer, which is awaited.
    internal ISerializer<T>? Inner { get; }

    internal ISerializerWithHeaders<T>? InnerWithHeaders { get; }
}
