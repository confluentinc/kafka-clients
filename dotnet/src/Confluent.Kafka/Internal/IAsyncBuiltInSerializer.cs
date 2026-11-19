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
/// An asynchronous built-in serializer, which hands out the synchronous serializer it calls, so that an asynchronous
/// producer given only built-ins serializes without a <see cref="System.Threading.Tasks.ValueTask{TResult}"/>. Only
/// the binding's own <c>Async</c> serializers implement it: a user's class cannot.
/// </summary>
/// <typeparam name="T">The type encoded.</typeparam>
internal interface IAsyncBuiltInSerializer<T>
{
    ISerializer<T> Inner { get; }
}
