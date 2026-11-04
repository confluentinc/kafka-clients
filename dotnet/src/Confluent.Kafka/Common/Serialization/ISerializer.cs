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

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts a <typeparamref name="T"/> to the bytes of a record key or value: Java's
/// <c>Serializer&lt;T&gt;</c>, without its headers overload and its <c>configure</c> and <c>close</c>.
/// </summary>
/// <typeparam name="T">The type encoded.</typeparam>
public interface ISerializer<T>
{
    /// <summary>Encodes <paramref name="data"/>, which may be <see langword="null"/>.</summary>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="data">The value to encode.</param>
    /// <returns>The bytes, or <see langword="null"/>, which is sent as a null key or value.</returns>
    byte[]? Serialize(string topic, T data);
}
