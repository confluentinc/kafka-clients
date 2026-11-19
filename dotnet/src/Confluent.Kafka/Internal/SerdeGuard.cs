// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Refuses, when a client is built, a serializer or deserializer with a public method the client would never call: a
/// headers form or a memory form without the interface that makes the client call it, or a <c>Close()</c> without
/// <see cref="IDisposable"/>. For an asynchronous serializer or deserializer, whose one method a client always calls,
/// it is a serde that is <see cref="IDisposable"/>, or has a public <c>DisposeAsync()</c>, <c>Dispose()</c> or
/// <c>Close()</c>, without <see cref="IAsyncDisposable"/>, the one interface an asynchronous client disposes it
/// through.
/// </summary>
internal static class SerdeGuard
{
    internal static void Check<T>(ISerializer<T> serializer, string client, bool disposes)
    {
        if (serializer is not ISerializerWithHeaders<T>
            && DefaultMethodGuard.Declares(serializer, "Serialize", typeof(string), typeof(Headers), typeof(T)))
        {
            string type = DefaultMethodGuard.TypeText(typeof(T));
            throw DefaultMethodGuard.NeverCalled(
                serializer, "Serialize(string, Headers, " + type + ")", "ISerializerWithHeaders<" + type + ">", client);
        }

        CheckClose(serializer, client, disposes);
    }

    internal static void Check<T>(IDeserializer<T> deserializer, string client, bool disposes)
    {
        if (deserializer is not IDeserializerWithHeaders<T>
            && DefaultMethodGuard.Declares(deserializer, "Deserialize", typeof(string), typeof(Headers), typeof(ReadOnlySpan<byte>)))
        {
            string type = DefaultMethodGuard.TypeText(typeof(T));
            throw DefaultMethodGuard.NeverCalled(
                deserializer, "Deserialize(string, Headers, ReadOnlySpan<byte>)", "IDeserializerWithHeaders<" + type + ">", client);
        }

        if (deserializer is not IDeserializerWithReadOnlyMemory<T>
            && DefaultMethodGuard.Declares(deserializer, "Deserialize", typeof(string), typeof(Headers), typeof(ReadOnlyMemory<byte>)))
        {
            string type = DefaultMethodGuard.TypeText(typeof(T));
            throw DefaultMethodGuard.NeverCalled(
                deserializer,
                "Deserialize(string, Headers, ReadOnlyMemory<byte>)",
                "IDeserializerWithReadOnlyMemory<" + type + ">",
                client);
        }

        CheckClose(deserializer, client, disposes);
    }

    internal static void Check<T>(IAsyncSerializer<T> serializer, string client, bool disposes) =>
        CheckAsyncClose(serializer, client, disposes);

    internal static void Check<T>(IAsyncDeserializer<T> deserializer, string client, bool disposes) =>
        CheckAsyncClose(deserializer, client, disposes);

    // An asynchronous client disposes a serde only through IAsyncDisposable. One that is also IDisposable is taken, as
    // .NET pairs the two, and only its DisposeAsync() is called. One that is IDisposable alone would never be disposed,
    // so it is refused, as is a public DisposeAsync(), Dispose() or Close() on a serde that is not IAsyncDisposable.
    private static void CheckAsyncClose(object serde, string client, bool disposes)
    {
        if (!disposes || serde is IAsyncDisposable)
        {
            return;
        }

        if (DefaultMethodGuard.Declares(serde, "DisposeAsync"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "DisposeAsync()", "IAsyncDisposable", client);
        }

        if (serde is IDisposable)
        {
            throw NeverDisposed(serde, client);
        }

        if (DefaultMethodGuard.Declares(serde, "Dispose"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Dispose()", "IAsyncDisposable", client);
        }

        if (DefaultMethodGuard.Declares(serde, "Close"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Close()", "IAsyncDisposable", client);
        }
    }

    // The interface itself, not a method's name: an explicit IDisposable.Dispose() is refused as a public one is.
    private static IllegalArgumentException NeverDisposed(object serde, string client) =>
        new IllegalArgumentException(
            DefaultMethodGuard.TypeText(serde.GetType()) + " implements IDisposable but does not implement IAsyncDisposable, "
                + "so the " + client + " would never dispose it.");

    // The mocks never dispose a serde, so only the clients that do refuse a Close() they would not call.
    private static void CheckClose(object serde, string client, bool disposes)
    {
        if (disposes && serde is not IDisposable && DefaultMethodGuard.Declares(serde, "Close"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Close()", "IDisposable", client);
        }
    }
}
