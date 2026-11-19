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

using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts a <typeparamref name="T"/> to the bytes of a record key or value: Java's
/// <c>Serializer&lt;T&gt;</c>. Its headers overload is
/// <see cref="SerializerExtensions.Serialize{T}(ISerializer{T}, string, Headers, T)"/>, which a serializer replaces by
/// implementing <see cref="ISerializerWithHeaders{T}"/>, and its <c>configure</c> is
/// <see cref="SerializerExtensions.Configure{T}(ISerializer{T}, IReadOnlyDictionary{string, object}, bool)"/>, which a
/// serializer replaces by implementing <see cref="ISerializerWithConfigure{T}"/>. Java's <c>close</c> is
/// <see cref="System.IDisposable"/>: a producer disposes a serializer it was given that implements it, and a mock
/// producer does not.
/// </summary>
/// <typeparam name="T">The type encoded.</typeparam>
public interface ISerializer<T>
{
    /// <summary>Encodes <paramref name="data"/>, which may be <see langword="null"/>.</summary>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="data">The value to encode; may be <see langword="null"/>, which the producer passes for a record
    /// with no key or no value.</param>
    /// <returns>
    /// The bytes, or <see langword="null"/>, which is sent as a null key or value. An empty memory is sent as empty bytes.
    /// A <see langword="null"/> <c>byte[]</c> converts to an empty memory, not to <see langword="null"/>: return
    /// <see langword="null"/> itself.
    /// </returns>
    /// <remarks>
    /// The producer reads the bytes where they are and copies them only into its record batch. It reads them until
    /// <c>Send</c> returns or throws, or, on an async producer, until <c>Send</c> throws or the <c>ValueTask</c> it returns
    /// has completed. Keep them unchanged until then. The key and the value are read after both serializers ran, so they
    /// must not share a buffer that the second call overwrites. A <see cref="System.Buffers.MemoryManager{T}"/> behind the
    /// memory must not call back into the producer: its <c>GetSpan</c> and <c>Pin</c> run inside <c>Send</c>. On an async
    /// producer its <see cref="System.Buffers.MemoryManager{T}.Unpin"/> runs inside <c>Send</c> too when the producer
    /// takes or refuses the record at once, and otherwise later, on the producer's completion thread or on the thread that
    /// closes the producer. If <c>Unpin</c> throws, the send's outcome is unchanged and the failure is written to
    /// <see cref="System.Diagnostics.Trace"/> as a warning.
    /// </remarks>
    ReadOnlyMemory<byte>? Serialize(string topic, T? data);
}

/// <summary>
/// A serializer that reads or adds the record's headers: it replaces Java's default
/// <c>Serializer.serialize(String, Headers, T)</c>, which calls the form without headers. A producer calls this form
/// for a serializer that implements this interface, and <see cref="ISerializer{T}.Serialize(string, T)"/> otherwise.
/// </summary>
/// <remarks>
/// A producer refuses, when it is built, a serializer that declares a public <c>Serialize(string, Headers, T)</c> but
/// does not implement this interface, since it would never call that method. The check reads the type's metadata, so
/// under trimming or Native AOT, which can remove a method nothing calls, it is best effort.
/// </remarks>
/// <typeparam name="T">The type encoded.</typeparam>
public interface ISerializerWithHeaders<T> : ISerializer<T>
{
    /// <summary>
    /// Encodes <paramref name="data"/>, which may be <see langword="null"/>. It is recommended to serialize
    /// <see langword="null"/> data to <see langword="null"/>.
    /// </summary>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>. The
    /// serializer may change them as a side effect of serializing; it is best practice to only add headers, and not to
    /// remove or change existing ones. <c>KafkaProducer</c> and <c>AsyncKafkaProducer</c> pass the record's own
    /// headers, the same object to the key's and the value's serializer, which the record carries when it is sent and
    /// which they make read-only once the serializers ran. <c>MockProducer</c> and <c>AsyncMockProducer</c> pass a new,
    /// empty headers object to each call and then drop it, as Java's <c>MockProducer</c> does, so what a serializer
    /// adds there does not reach the record.</param>
    /// <param name="data">The value to encode; may be <see langword="null"/>.</param>
    /// <returns>The bytes, or <see langword="null"/>, as <see cref="ISerializer{T}.Serialize(string, T)"/> returns
    /// them.</returns>
    ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, T? data);
}

/// <summary>
/// A serializer that is set up from configs: it replaces Java's default <c>Serializer.configure(Map, boolean)</c>, which
/// does nothing.
/// </summary>
/// <remarks>
/// No producer calls it: as Java configures only a serializer it builds from config, a serializer given to a producer
/// is set up by whoever builds it. A <c>Configure</c> written on a class that implements only
/// <see cref="ISerializer{T}"/> is not called by
/// <see cref="SerializerExtensions.Configure{T}(ISerializer{T}, IReadOnlyDictionary{string, object}, bool)"/>, and no
/// check at construction reports it.
/// </remarks>
/// <typeparam name="T">The type encoded.</typeparam>
public interface ISerializerWithConfigure<T> : ISerializer<T>
{
    /// <summary>Configures this class.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey);
}

/// <summary>Java's default methods of <c>Serializer&lt;T&gt;</c>, for every <see cref="ISerializer{T}"/>.</summary>
public static class SerializerExtensions
{
    /// <summary>
    /// Encodes <paramref name="data"/> with the record's headers, Java's <c>Serializer.serialize(String, Headers, T)</c>:
    /// <see cref="ISerializerWithHeaders{T}.Serialize(string, Headers, T)"/> when <paramref name="serializer"/>
    /// implements it, and otherwise <see cref="ISerializer{T}.Serialize(string, T)"/>, the headers unused.
    /// </summary>
    /// <typeparam name="T">The type encoded.</typeparam>
    /// <param name="serializer">The serializer.</param>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>.</param>
    /// <param name="data">The value to encode; may be <see langword="null"/>.</param>
    /// <returns>The bytes, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public static ReadOnlyMemory<byte>? Serialize<T>(this ISerializer<T> serializer, string topic, Headers headers, T? data)
    {
        if (serializer is null)
        {
            throw new ArgumentNullException(nameof(serializer));
        }

        return serializer is ISerializerWithHeaders<T> withHeaders
            ? withHeaders.Serialize(topic, headers, data)
            : serializer.Serialize(topic, data);
    }

    /// <summary>
    /// Configures <paramref name="serializer"/>, Java's <c>Serializer.configure(Map, boolean)</c>:
    /// <see cref="ISerializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool)"/> when
    /// <paramref name="serializer"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type encoded.</typeparam>
    /// <param name="serializer">The serializer.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public static void Configure<T>(this ISerializer<T> serializer, IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        if (serializer is null)
        {
            throw new ArgumentNullException(nameof(serializer));
        }

        if (serializer is ISerializerWithConfigure<T> configurable)
        {
            configurable.Configure(configs, isKey);
        }
    }
}
