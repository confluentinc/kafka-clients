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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts a <typeparamref name="T"/> to the bytes of a record key or value without blocking a thread: the form of
/// Java's <c>Serializer&lt;T&gt;</c> that an asynchronous producer takes, for a serializer that waits, as one that
/// reads a schema registry does. It has one method, Java's <c>serialize(String, Headers, T)</c>, so the headers are
/// always passed. Java's <c>configure</c> is
/// <see cref="AsyncSerializerExtensions.Configure{T}(IAsyncSerializer{T}, IReadOnlyDictionary{string, object}, bool)"/>,
/// which a serializer replaces by implementing <see cref="IAsyncSerializerWithConfigure{T}"/>. Java's <c>close</c> is
/// <see cref="IAsyncDisposable"/>, or else <see cref="IDisposable"/>: a producer disposes a serializer it was given
/// through the first of the two it implements, and a mock producer does not.
/// </summary>
/// <remarks>
/// Each built-in serializer has a class of this interface, named <c>Async</c> and the serializer's name
/// (<see cref="AsyncStringSerializer"/>). A serializer of one's own that does not wait is given to an asynchronous
/// producer through a class of this interface that calls it and returns a completed <see cref="ValueTask{TResult}"/>.
/// </remarks>
/// <typeparam name="T">The type encoded.</typeparam>
public interface IAsyncSerializer<T>
{
    /// <summary>
    /// Encodes <paramref name="data"/>, which may be <see langword="null"/>. It is recommended to serialize
    /// <see langword="null"/> data to <see langword="null"/>.
    /// </summary>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>. The
    /// serializer may change them as a side effect of serializing; it is best practice to only add headers, and not to
    /// remove or change existing ones.</param>
    /// <param name="data">The value to encode; may be <see langword="null"/>, which the producer passes for a record
    /// with no key or no value.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The bytes, or <see langword="null"/>, which is sent as a null key or value. An empty memory is sent as empty
    /// bytes. A <see langword="null"/> <c>byte[]</c> converts to an empty memory, not to <see langword="null"/>: return
    /// <see langword="null"/> itself. The bytes stay unchanged for as long as
    /// <see cref="ISerializer{T}.Serialize(string, T)"/> says.
    /// </returns>
    ValueTask<ReadOnlyMemory<byte>?> Serialize(string topic, Headers headers, T? data, CancellationToken cancellationToken);
}

/// <summary>
/// An asynchronous serializer that is set up from configs: it replaces Java's default
/// <c>Serializer.configure(Map, boolean)</c>, which does nothing. <c>Configure</c> does not wait, as on
/// <see cref="ISerializerWithConfigure{T}"/>.
/// </summary>
/// <remarks>
/// No producer calls it: as Java configures only a serializer it builds from config, a serializer given to a producer
/// is set up by whoever builds it. A <c>Configure</c> written on a class that implements only
/// <see cref="IAsyncSerializer{T}"/> is not called by
/// <see cref="AsyncSerializerExtensions.Configure{T}(IAsyncSerializer{T}, IReadOnlyDictionary{string, object}, bool)"/>,
/// and no check at construction reports it.
/// </remarks>
/// <typeparam name="T">The type encoded.</typeparam>
public interface IAsyncSerializerWithConfigure<T> : IAsyncSerializer<T>
{
    /// <summary>Configures this class.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey);
}

/// <summary>
/// Java's default methods of <c>Serializer&lt;T&gt;</c> that <see cref="IAsyncSerializer{T}"/> does not declare, for
/// every <see cref="IAsyncSerializer{T}"/>.
/// </summary>
public static class AsyncSerializerExtensions
{
    /// <summary>
    /// Configures <paramref name="serializer"/>, Java's <c>Serializer.configure(Map, boolean)</c>:
    /// <see cref="IAsyncSerializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool)"/> when
    /// <paramref name="serializer"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type encoded.</typeparam>
    /// <param name="serializer">The serializer.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public static void Configure<T>(this IAsyncSerializer<T> serializer, IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        if (serializer is null)
        {
            throw new ArgumentNullException(nameof(serializer));
        }

        if (serializer is IAsyncSerializerWithConfigure<T> configurable)
        {
            configurable.Configure(configs, isKey);
        }
    }
}
