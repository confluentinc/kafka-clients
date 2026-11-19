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
/// <see cref="AsyncSerializerExtensions.Configure{T}(IAsyncSerializer{T}, IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>,
/// which a serializer replaces by implementing <see cref="IAsyncSerializerWithConfigure{T}"/>. Java's <c>close</c> is
/// <see cref="IAsyncDisposable"/> and nothing else: a producer disposes a serializer, given or built from its
/// configuration, only through it and refuses one that is <see cref="IDisposable"/> without it, and a mock producer
/// disposes none.
/// </summary>
/// <remarks>
/// <para>
/// Each built-in serializer has a class of this interface, named <c>Async</c> and the serializer's name
/// (<see cref="AsyncStringSerializer"/>). A serializer of one's own that does not wait is given to an asynchronous
/// producer through a class of this interface that calls it and returns a completed <see cref="ValueTask{TResult}"/>.
/// </para>
/// <para>
/// A producer given two built-in classes serializes inside its <c>Send</c> call and waits for neither. A producer
/// given any other class awaits the key serializer and then the value serializer, and the record enters the producer
/// when both have completed, so <c>Send</c>'s first stage covers the wait. A failure, thrown by the call or carried
/// by the <see cref="ValueTask{TResult}"/>, ends that first stage, and nothing is sent; an
/// <see cref="Confluent.Kafka.Common.Errors.ApiException"/> on an <c>AsyncKafkaProducer</c> instead fails the record
/// through the future <c>Send</c> hands back, as a built-in serializer's does and as Java's
/// <c>KafkaProducer.doSend</c>.
/// </para>
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
    /// remove or change existing ones. A mock producer passes a new, empty <see cref="Headers"/> that it then drops,
    /// as Java's <c>MockProducer</c> does.</param>
    /// <param name="data">The value to encode; may be <see langword="null"/>, which the producer passes for a record
    /// with no key or no value.</param>
    /// <param name="cancellationToken">Cancels the call: the token the producer's <c>Send</c> was given. An
    /// <see cref="OperationCanceledException"/> from the serializer ends <c>Send</c>'s first stage as cancelled.</param>
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
/// <c>Serializer.configure(Map, boolean)</c>, which does nothing. <c>Configure</c> may wait, as one that reads a schema
/// registry does, where the <c>Configure</c> of <see cref="ISerializerWithConfigure{T}"/> returns when it is done.
/// </summary>
/// <remarks>
/// A producer calls and awaits it only on a serializer it builds, from the function its configuration holds under
/// <c>key.serializer</c> or <c>value.serializer</c>: as Java configures only a serializer it builds from config, a
/// serializer given to a producer is set up by whoever builds it. A <c>Configure</c> written on a class that
/// implements only
/// <see cref="IAsyncSerializer{T}"/> is not called by
/// <see cref="AsyncSerializerExtensions.Configure{T}(IAsyncSerializer{T}, IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>,
/// and no check at construction reports it.
/// </remarks>
/// <typeparam name="T">The type encoded.</typeparam>
public interface IAsyncSerializerWithConfigure<T> : IAsyncSerializer<T>
{
    /// <summary>Configures this class.</summary>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>A task that completes when the serializer is configured.</returns>
    ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken);
}

/// <summary>
/// Java's default methods of <c>Serializer&lt;T&gt;</c> that <see cref="IAsyncSerializer{T}"/> does not declare, for
/// every <see cref="IAsyncSerializer{T}"/>.
/// </summary>
public static class AsyncSerializerExtensions
{
    /// <summary>
    /// Configures <paramref name="serializer"/>, Java's <c>Serializer.configure(Map, boolean)</c>:
    /// <see cref="IAsyncSerializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool, CancellationToken)"/>
    /// when <paramref name="serializer"/> implements it, and otherwise nothing, as Java's default.
    /// </summary>
    /// <typeparam name="T">The type encoded.</typeparam>
    /// <param name="serializer">The serializer.</param>
    /// <param name="configs">Configs in key/value pairs.</param>
    /// <param name="isKey">Whether the serializer is used for the key or the value.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The task of the serializer's <c>Configure</c>, or a completed one when it does not implement the interface.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is null.</exception>
    public static ValueTask Configure<T>(
        this IAsyncSerializer<T> serializer,
        IReadOnlyDictionary<string, object?> configs,
        bool isKey,
        CancellationToken cancellationToken = default)
    {
        if (serializer is null)
        {
            throw new ArgumentNullException(nameof(serializer));
        }

        return serializer is IAsyncSerializerWithConfigure<T> configurable
            ? configurable.Configure(configs, isKey, cancellationToken)
            : default;
    }
}
