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
using System.Collections.Generic;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// A serializer and a deserializer for one type, Java's <c>Serde&lt;T&gt;</c>. <see cref="Serializer()"/> and
/// <see cref="Deserializer()"/> hand out two objects, each passed to the client that uses it: a producer or a
/// consumer given one of them disposes only that one. Disposing the serde (Java's <c>close()</c>) disposes the
/// serializer and the deserializer it holds. Its <c>configure</c> is
/// <see cref="SerdeExtensions.Configure{T}(ISerde{T}, IReadOnlyDictionary{string, object}, bool)"/>, which a serde
/// replaces by implementing <see cref="ISerdeWithConfigure{T}"/>.
/// </summary>
/// <typeparam name="T">The type encoded and decoded.</typeparam>
public interface ISerde<T> : IDisposable
{
    /// <summary>The serializer of this serde.</summary>
    /// <returns>The serializer.</returns>
    ISerializer<T> Serializer();

    /// <summary>The deserializer of this serde.</summary>
    /// <returns>The deserializer.</returns>
    IDeserializer<T> Deserializer();
}

/// <summary>
/// A serde that is set up from configs: it replaces Java's default <c>Serde.configure(Map, boolean)</c>, which does
/// nothing. <see cref="Serdes.WrapperSerde{T}"/> implements it by configuring its serializer and its deserializer.
/// </summary>
/// <remarks>
/// No client calls it. A <c>Configure</c> written on a class that implements only <see cref="ISerde{T}"/> is not called
/// by <see cref="SerdeExtensions.Configure{T}(ISerde{T}, IReadOnlyDictionary{string, object}, bool)"/>.
/// </remarks>
/// <typeparam name="T">The type encoded and decoded.</typeparam>
public interface ISerdeWithConfigure<T> : ISerde<T>
{
    /// <summary>Configures this class, which configures the underlying serializer and deserializer.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serde is used for the key or the value.</param>
    void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey);
}

/// <summary>Java's default methods of <c>Serde&lt;T&gt;</c>, for every <see cref="ISerde{T}"/>.</summary>
public static class SerdeExtensions
{
    /// <summary>
    /// Configures <paramref name="serde"/>, Java's <c>Serde.configure(Map, boolean)</c>:
    /// <see cref="ISerdeWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool)"/> when
    /// <paramref name="serde"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type encoded and decoded.</typeparam>
    /// <param name="serde">The serde.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serde is used for the key or the value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serde"/> is null.</exception>
    public static void Configure<T>(this ISerde<T> serde, IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        if (serde is null)
        {
            throw new ArgumentNullException(nameof(serde));
        }

        if (serde is ISerdeWithConfigure<T> configurable)
        {
            configurable.Configure(configs, isKey);
        }
    }
}
