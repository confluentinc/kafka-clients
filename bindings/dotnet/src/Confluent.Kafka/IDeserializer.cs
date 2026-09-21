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

namespace Confluent.Kafka;

/// <summary>
/// Converts Kafka wire bytes to a value of type <typeparamref name="T"/> — the .NET
/// realization of Java's
/// <c>org.apache.kafka.common.serialization.Deserializer&lt;T&gt;</c>
/// (<c>deserialize(String topic, byte[] data)</c>, <c>Deserializer.java:55</c>). A pure
/// managed, binding-/user-layer concern: the C ABI is bytes-only (CLAUDE.md §4).
/// </summary>
/// <remarks>
/// <para>
/// <b>Signature deviation from Java — deliberate, and the zero-copy lock</b>
/// (recorded here in the consumer-threading §28 style). Java's method takes a
/// <c>byte[] data</c>; ours takes a <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/>.
/// A <c>byte[]</c> parameter forces a per-record heap copy out of the native fetch
/// batch; the span instead <em>borrows the native slice in place</em> — the receive-path
/// zero-copy contract (ffi-marshalling.md §B4, consumer-threading.md §27). Because a
/// <see cref="ReadOnlySpan{T}"/> is a <c>ref struct</c>, it cannot be boxed, stored in a
/// field, captured by a lambda, sent across a thread, or held across an <c>await</c>, so
/// it <em>provably cannot outlive</em> the batch the typed poll path (P1b) borrows it
/// from. The only allocation is the owned <typeparamref name="T"/> this method returns —
/// exactly Java's own per-record allocation, one virtual call per record (DoD §11).
/// </para>
/// <para>
/// <b>Synchronous.</b> There is no async deserialize overload: a <c>ref struct</c> span
/// cannot cross an <c>await</c> point, and deserialization is CPU-bound. This is also
/// Java-faithful — Java's <c>Deserializer&lt;T&gt;</c> is synchronous (its
/// Schema-Registry serdes deserialize over blocking, cached HTTP through this same sync
/// method). An async deserialize idiom (as in the confluent-kafka-dotnet ecosystem
/// client) is a .NET-specific divergence from the Java shape and is deferred; if a
/// Schema-Registry path later needs one, it is decided then. This note commits to
/// nothing beyond "sync only for now".
/// </para>
/// <para>
/// <b>Headers overload deferred.</b> Only the header-less form ships. Java's
/// <c>default T deserialize(String topic, Headers headers, byte[] data)</c>
/// (<c>Deserializer.java:84</c>) can be added later non-breakingly as a C#
/// default-interface-method that forwards to this header-less method — mirroring Java's
/// own defaulting.
/// </para>
/// </remarks>
/// <typeparam name="T">The value type this deserializer decodes to.</typeparam>
public interface IDeserializer<T>
{
    /// <summary>
    /// Decodes <paramref name="data"/> to a value of type <typeparamref name="T"/>.
    /// </summary>
    /// <param name="topic">
    /// The topic the value was consumed from. Passed through for parity with Java
    /// (topic-aware deserializers); the built-in serdes ignore it.
    /// </param>
    /// <param name="data">
    /// The wire bytes, borrowing the native fetch-batch slice in place (see the type
    /// remarks). Must be read within the call — never captured to outlive it.
    /// </param>
    /// <returns>The decoded, owned value.</returns>
    /// <exception cref="SerializationException">
    /// The bytes are malformed for <typeparamref name="T"/> (e.g. a wrong-length
    /// fixed-width payload), matching Java's <c>SerializationException</c>.
    /// </exception>
    T Deserialize(string topic, ReadOnlySpan<byte> data);
}
