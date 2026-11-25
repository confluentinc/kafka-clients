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

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts the bytes of a record key or value to a <typeparamref name="T"/> without blocking a thread: the form of
/// Java's <c>Deserializer&lt;T&gt;</c> that an asynchronous consumer takes, for a deserializer that waits, as one that
/// reads a schema registry does. Its method is Java's <c>deserialize(String, byte[])</c>, over a memory; its headers
/// overload is
/// <see cref="AsyncDeserializerExtensions.Deserialize{T}(IAsyncDeserializer{T}, string, IHeaders, ReadOnlyMemory{byte}, CancellationToken)"/>,
/// Java's <c>deserialize(String, Headers, ByteBuffer)</c>, which a deserializer replaces by implementing
/// <see cref="IAsyncDeserializerWithHeaders{T}"/>. Java's <c>configure</c> is
/// <see cref="AsyncDeserializerExtensions.Configure{T}(IAsyncDeserializer{T}, IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>,
/// which a deserializer replaces by implementing <see cref="IAsyncDeserializerWithConfigure{T}"/>. Java's <c>close</c>
/// is <see cref="IAsyncDisposable"/> and nothing else: a consumer disposes a deserializer, given or built from its
/// configuration, only through it and refuses one that is <see cref="IDisposable"/> without it; a mock consumer
/// holds none.
/// </summary>
/// <remarks>
/// Each built-in deserializer has a class of this interface, named <c>Async</c> and the deserializer's name
/// (<see cref="AsyncStringDeserializer"/>). A deserializer of one's own that does not wait is given to an asynchronous
/// consumer through a class of this interface that calls it and returns a completed <see cref="ValueTask{TResult}"/>.
/// An asynchronous consumer calls a built-in class without awaiting, and awaits any other one record at a time, the
/// key before the value, with the token its <c>Poll</c> was given.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IAsyncDeserializer<T>
{
    /// <summary>Decodes <paramref name="data"/> to a <typeparamref name="T"/>.</summary>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="data">The bytes, valid until the returned <see cref="ValueTask{TResult}"/> completes, so across
    /// an <see langword="await"/> inside the call. After that the memory, a slice of it and a span read from it must
    /// not be used: a value that outlives the call copies what it needs. A consumer revokes the memory when the call completes, after
    /// which reading it or a slice of it throws <see cref="ObjectDisposedException"/>, while a pin taken during the
    /// call stays valid until it is released. Where Java's <c>ByteBuffer</c> has a position and a limit, this
    /// is exactly the key or the value, from its first byte to its last.</param>
    /// <param name="cancellationToken">Cancels the call. A consumer passes the token of its <c>Poll</c>, which then
    /// ends with <see cref="OperationCanceledException"/> and returns the batch's records again on the next one.</param>
    /// <returns>The decoded value; may be <see langword="null"/>. A consumer does not call the deserializer for an
    /// absent key or value: the record's field is then <see langword="default"/>.</returns>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>. It is thrown by
    /// the call or carried by the returned <see cref="ValueTask{TResult}"/>.</exception>
    ValueTask<T?> Deserialize(string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
}

/// <summary>
/// An asynchronous deserializer that reads, or changes, the headers of the record it decodes: Java's
/// <c>deserialize(String, Headers, ByteBuffer)</c>, which replaces the form without headers when the deserializer
/// implements this interface, as <see cref="IDeserializerWithHeaders{T}"/> does on a synchronous one.
/// </summary>
/// <remarks>
/// A consumer calls this form, and not the one without headers, of a deserializer that implements it. A consumer
/// refuses a deserializer that declares a public
/// <c>Deserialize(string, IHeaders, ReadOnlyMemory&lt;byte&gt;, CancellationToken)</c> without this interface, which it
/// would never call.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IAsyncDeserializerWithHeaders<T> : IAsyncDeserializer<T>
{
    /// <summary>Decodes <paramref name="data"/> to a <typeparamref name="T"/>.</summary>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>. The
    /// deserializer may change them as a side effect of deserializing; it is best practice to only add headers, and not
    /// to remove or change existing ones.</param>
    /// <param name="data">The bytes, valid until the returned <see cref="ValueTask{TResult}"/> completes, so across
    /// an <see langword="await"/> inside the call. After that the memory, a slice of it and a span read from it must
    /// not be used: a value that outlives the call copies what it needs, unless the deserializer implements
    /// <see cref="IAsyncDeserializerWithRetention{T}"/>. A consumer revokes the memory when the call completes, after
    /// which reading it or a slice of it throws <see cref="ObjectDisposedException"/>, while a pin taken during the
    /// call stays valid until it is released. Where Java's <c>ByteBuffer</c> has a position and a limit, this
    /// is exactly the key or the value, from its first byte to its last.</param>
    /// <param name="cancellationToken">Cancels the call. A consumer passes the token of its <c>Poll</c>, which then
    /// ends with <see cref="OperationCanceledException"/> and returns the batch's records again on the next one.</param>
    /// <returns>The decoded value; may be <see langword="null"/>. A consumer does not call the deserializer for an
    /// absent key or value: the record's field is then <see langword="default"/>.</returns>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>. It is thrown by
    /// the call or carried by the returned <see cref="ValueTask{TResult}"/>.</exception>
    ValueTask<T?> Deserialize(
        string topic, IHeaders headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
}

/// <summary>
/// An asynchronous deserializer that may keep or return the memory it is given. The interface has no member of its
/// own: it changes what
/// <see cref="IAsyncDeserializerWithHeaders{T}.Deserialize(string, IHeaders, ReadOnlyMemory{byte}, CancellationToken)"/>,
/// the form a consumer calls, may do with its <c>data</c>.
/// </summary>
/// <remarks>
/// The memory is the consumer's own bytes, not a copy. It stays valid for as long as it, or a slice of it, is
/// reachable, and it keeps every record polled with it in memory until then: the garbage collector releases them, and
/// nothing is disposed. A span read from the memory does not keep the bytes valid by itself: hold
/// <c>using MemoryHandle pin = memory.Pin();</c> while reading one. It is the asynchronous form of
/// <see cref="IDeserializerWithReadOnlyMemory{T}"/>.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IAsyncDeserializerWithRetention<T> : IAsyncDeserializerWithHeaders<T>
{
}

/// <summary>
/// An asynchronous deserializer that is set up from configs: it replaces Java's default
/// <c>Deserializer.configure(Map, boolean)</c>, which does nothing. <c>Configure</c> may wait, as one that reads a schema
/// registry does, where the <c>Configure</c> of <see cref="IDeserializerWithConfigure{T}"/> returns when it is done.
/// </summary>
/// <remarks>
/// A consumer calls and awaits it only on a deserializer it builds, from the function its configuration holds under
/// <c>key.deserializer</c> or <c>value.deserializer</c>: as Java configures only a deserializer it builds from config,
/// a deserializer given to a consumer is set up by whoever builds it. A <c>Configure</c> written on a class that
/// implements only
/// <see cref="IAsyncDeserializer{T}"/> is not called by
/// <see cref="AsyncDeserializerExtensions.Configure{T}(IAsyncDeserializer{T}, IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>,
/// and no check at construction reports it.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IAsyncDeserializerWithConfigure<T> : IAsyncDeserializer<T>
{
    /// <summary>Configures this class.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the deserializer is used for the key or the value.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>A task that completes when the deserializer is configured.</returns>
    ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken = default);
}

/// <summary>
/// Java's default methods of <c>Deserializer&lt;T&gt;</c> that <see cref="IAsyncDeserializer{T}"/> does not declare,
/// for every <see cref="IAsyncDeserializer{T}"/>.
/// </summary>
public static class AsyncDeserializerExtensions
{
    /// <summary>
    /// Decodes <paramref name="data"/>, Java's <c>Deserializer.deserialize(String, Headers, ByteBuffer)</c>:
    /// <see cref="IAsyncDeserializerWithHeaders{T}.Deserialize(string, IHeaders, ReadOnlyMemory{byte}, CancellationToken)"/>
    /// when <paramref name="deserializer"/> implements it, and otherwise
    /// <see cref="IAsyncDeserializer{T}.Deserialize(string, ReadOnlyMemory{byte}, CancellationToken)"/> without the
    /// headers, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type the bytes are decoded to.</typeparam>
    /// <param name="deserializer">The deserializer.</param>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="headers">The record's headers, which may be empty but are never <see langword="null"/>.</param>
    /// <param name="data">The bytes, under the contract of the form called.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The task of the form called.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deserializer"/> is null.</exception>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>. It is thrown by
    /// the call or carried by the returned <see cref="ValueTask{TResult}"/>.</exception>
    public static ValueTask<T?> Deserialize<T>(
        this IAsyncDeserializer<T> deserializer,
        string topic,
        IHeaders headers,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        if (deserializer is null)
        {
            throw new ArgumentNullException(nameof(deserializer));
        }

        return deserializer is IAsyncDeserializerWithHeaders<T> withHeaders
            ? withHeaders.Deserialize(topic, headers, data, cancellationToken)
            : deserializer.Deserialize(topic, data, cancellationToken);
    }

    /// <summary>
    /// Configures <paramref name="deserializer"/>, Java's <c>Deserializer.configure(Map, boolean)</c>:
    /// <see cref="IAsyncDeserializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>
    /// when <paramref name="deserializer"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type the bytes are decoded to.</typeparam>
    /// <param name="deserializer">The deserializer.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the deserializer is used for the key or the value.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The task of the deserializer's <c>Configure</c>, or a completed one when it does not implement the interface.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="deserializer"/> is null.</exception>
    public static ValueTask Configure<T>(
        this IAsyncDeserializer<T> deserializer,
        IReadOnlyDictionary<string, object?> configs,
        bool isKey,
        CancellationToken cancellationToken = default)
    {
        if (deserializer is null)
        {
            throw new ArgumentNullException(nameof(deserializer));
        }

        return deserializer is IAsyncDeserializerWithConfigure<T> configurable
            ? configurable.Configure(configs, isKey, cancellationToken)
            : default;
    }
}
