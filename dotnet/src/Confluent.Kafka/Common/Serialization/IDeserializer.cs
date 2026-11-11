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

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts the bytes of a record key or value to a <typeparamref name="T"/>: Java's
/// <c>Deserializer&lt;T&gt;</c>. Its headers overloads are
/// <see cref="DeserializerExtensions.Deserialize{T}(IDeserializer{T}, string, Headers, ReadOnlySpan{byte})"/>, which a
/// deserializer replaces by implementing <see cref="IDeserializerWithHeaders{T}"/>, and its <c>configure</c> is
/// <see cref="DeserializerExtensions.Configure{T}(IDeserializer{T}, IReadOnlyDictionary{string, object}, bool)"/>, which
/// a deserializer replaces by implementing <see cref="IDeserializerWithConfigure{T}"/>. Java's <c>close</c> is
/// <see cref="IDisposable"/>: a consumer disposes a deserializer it was given that implements it, and a mock
/// consumer does not.
/// </summary>
/// <remarks>
/// The bytes are a <see cref="ReadOnlySpan{T}"/> over the fetched batch, where Java passes a <c>byte[]</c>, so a
/// record is not copied before it is deserialized.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IDeserializer<T>
{
    /// <summary>Decodes <paramref name="data"/> to a <typeparamref name="T"/>.</summary>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="data">The bytes, valid only during the call.</param>
    /// <returns>The decoded value; may be <see langword="null"/>. A consumer does not call the deserializer for an
    /// absent key or value: the record's field is then <see langword="default"/>.</returns>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>.</exception>
    T? Deserialize(string topic, ReadOnlySpan<byte> data);
}

/// <summary>
/// A deserializer that reads or adds the record's headers: it replaces Java's default
/// <c>Deserializer.deserialize(String, Headers, byte[])</c> and <c>deserialize(String, Headers, ByteBuffer)</c>, which call
/// the form without headers. A consumer calls this form for a deserializer that implements this interface, and
/// <see cref="IDeserializer{T}.Deserialize(string, ReadOnlySpan{byte})"/> otherwise.
/// </summary>
/// <remarks>
/// Java's two overloads are one form here: the bytes are a <see cref="ReadOnlySpan{T}"/>, for Java's <c>byte[]</c> and
/// <c>ByteBuffer</c> alike. A consumer refuses, when it is built, a deserializer that declares a public
/// <c>Deserialize(string, Headers, ReadOnlySpan&lt;byte&gt;)</c> but does not implement this interface, since it would
/// never call that method. The check reads the type's metadata, so under trimming or Native AOT, which can remove a
/// method nothing calls, it is best effort.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IDeserializerWithHeaders<T> : IDeserializer<T>
{
    /// <summary>Decodes <paramref name="data"/> to a <typeparamref name="T"/>.</summary>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>. The
    /// deserializer may change them as a side effect of deserializing; it is best practice to only add headers, and not
    /// to remove or change existing ones. A consumer passes the headers the record then carries, and that a
    /// <see cref="RecordDeserializationException"/> carries when a deserializer of the record throws.</param>
    /// <param name="data">The bytes, valid only during the call.</param>
    /// <returns>The decoded value; may be <see langword="null"/>. A consumer does not call the deserializer for an
    /// absent key or value: the record's field is then <see langword="default"/>.</returns>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>.</exception>
    T? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data);
}

/// <summary>
/// A deserializer that is set up from configs: it replaces Java's default <c>Deserializer.configure(Map, boolean)</c>,
/// which does nothing.
/// </summary>
/// <remarks>
/// No consumer calls it: as Java configures only a deserializer it builds from config, a deserializer given to a
/// consumer is set up by whoever builds it. A <c>Configure</c> written on a class that implements only
/// <see cref="IDeserializer{T}"/> is not called by
/// <see cref="DeserializerExtensions.Configure{T}(IDeserializer{T}, IReadOnlyDictionary{string, object}, bool)"/>, and
/// no check at construction reports it.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IDeserializerWithConfigure<T> : IDeserializer<T>
{
    /// <summary>Configures this class.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the deserializer is used for the key or the value.</param>
    void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey);
}

/// <summary>Java's default methods of <c>Deserializer&lt;T&gt;</c>, for every <see cref="IDeserializer{T}"/>.</summary>
public static class DeserializerExtensions
{
    /// <summary>
    /// Decodes <paramref name="data"/> with the record's headers, Java's
    /// <c>Deserializer.deserialize(String, Headers, byte[])</c> and <c>deserialize(String, Headers, ByteBuffer)</c>:
    /// <see cref="IDeserializerWithHeaders{T}.Deserialize(string, Headers, ReadOnlySpan{byte})"/> when
    /// <paramref name="deserializer"/> implements it, and otherwise
    /// <see cref="IDeserializer{T}.Deserialize(string, ReadOnlySpan{byte})"/>, the headers unused.
    /// </summary>
    /// <typeparam name="T">The type the bytes are decoded to.</typeparam>
    /// <param name="deserializer">The deserializer.</param>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>.</param>
    /// <param name="data">The bytes, valid only during the call.</param>
    /// <returns>The decoded value; may be <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deserializer"/> is null.</exception>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>.</exception>
    public static T? Deserialize<T>(this IDeserializer<T> deserializer, string topic, Headers headers, ReadOnlySpan<byte> data)
    {
        if (deserializer is null)
        {
            throw new ArgumentNullException(nameof(deserializer));
        }

        return deserializer is IDeserializerWithHeaders<T> withHeaders
            ? withHeaders.Deserialize(topic, headers, data)
            : deserializer.Deserialize(topic, data);
    }

    /// <summary>
    /// Configures <paramref name="deserializer"/>, Java's <c>Deserializer.configure(Map, boolean)</c>:
    /// <see cref="IDeserializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool)"/> when
    /// <paramref name="deserializer"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type the bytes are decoded to.</typeparam>
    /// <param name="deserializer">The deserializer.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the deserializer is used for the key or the value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="deserializer"/> is null.</exception>
    public static void Configure<T>(this IDeserializer<T> deserializer, IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        if (deserializer is null)
        {
            throw new ArgumentNullException(nameof(deserializer));
        }

        if (deserializer is IDeserializerWithConfigure<T> configurable)
        {
            configurable.Configure(configs, isKey);
        }
    }
}
