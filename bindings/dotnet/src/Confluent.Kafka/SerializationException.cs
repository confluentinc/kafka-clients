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
/// The exception raised when (de)serialization of a record key or value fails — the
/// .NET realization of Java's
/// <c>org.apache.kafka.common.errors.SerializationException</c>.
/// </summary>
/// <remarks>
/// <para>
/// A flat subclass of <see cref="KafkaException"/>, mirroring Java's hierarchy
/// (<c>SerializationException extends KafkaException</c>) and the binding's flat
/// error-model convention (ffi-marshalling.md §A5/§B5). Being a
/// <see cref="KafkaException"/>, it is caught by an existing
/// <c>catch (KafkaException)</c>. Its classification fields
/// (<see cref="KafkaException.Code"/>, <see cref="KafkaException.IsRetriable"/>)
/// are left at their defaults — a serde failure originates in the binding/user
/// layer, not from a <c>kafka_common_Error_t</c> handle, so it carries no core
/// error code.
/// </para>
/// <para>
/// The built-in <see cref="Serdes"/> throw this on malformed input (e.g. a fixed-width
/// payload of the wrong length). It is left non-<c>sealed</c> to mirror Java, where
/// serde errors form a small hierarchy under this type.
/// </para>
/// </remarks>
public class SerializationException : KafkaException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SerializationException"/> class
    /// with a default message.
    /// </summary>
    public SerializationException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SerializationException"/> class
    /// with the specified message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public SerializationException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SerializationException"/> class
    /// with the specified message and the underlying cause.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">
    /// The exception that is the cause of the current exception (e.g. the format
    /// error from parsing), or <see langword="null"/> if none is specified.
    /// </param>
    public SerializationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
