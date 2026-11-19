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
/// An asynchronous built-in deserializer, which hands out the synchronous deserializer it calls, so that an
/// asynchronous consumer given only built-ins deserializes on the synchronous consumer's path: over a span, or, when
/// the inner one is an <see cref="IDeserializerWithReadOnlyMemory{T}"/>, over a memory it may keep. Only the binding's
/// own <c>Async</c> deserializers implement it: a user's class cannot.
/// </summary>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
internal interface IAsyncBuiltInDeserializer<T>
{
    IDeserializer<T> Inner { get; }
}
