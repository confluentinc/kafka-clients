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
/// it is a <c>DisposeAsync()</c>, a <c>Dispose()</c> or a <c>Close()</c> without the interface the client disposes
/// it through.
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

    // An asynchronous client disposes a serde through IAsyncDisposable, or else through IDisposable. One that is both
    // is taken, as .NET pairs the two, although its Dispose() is then not called.
    private static void CheckAsyncClose(object serde, string client, bool disposes)
    {
        if (!disposes)
        {
            return;
        }

        if (serde is not IAsyncDisposable && DefaultMethodGuard.Declares(serde, "DisposeAsync"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "DisposeAsync()", "IAsyncDisposable", client);
        }

        if (serde is not IDisposable && DefaultMethodGuard.Declares(serde, "Dispose"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Dispose()", "IDisposable", client);
        }

        if (serde is not IAsyncDisposable && serde is not IDisposable && DefaultMethodGuard.Declares(serde, "Close"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Close()", "IAsyncDisposable or IDisposable", client);
        }
    }

    // The mocks never dispose a serde, so only the clients that do refuse a Close() they would not call.
    private static void CheckClose(object serde, string client, bool disposes)
    {
        if (disposes && serde is not IDisposable && DefaultMethodGuard.Declares(serde, "Close"))
        {
            throw DefaultMethodGuard.NeverCalled(serde, "Close()", "IDisposable", client);
        }
    }
}
