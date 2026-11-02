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

namespace Confluent.Kafka.Common.Errors;

/// <summary>
/// A failed serialization or deserialization of a record key or value, Java's <c>SerializationException</c>:
/// a <see cref="KafkaException"/> whose <see cref="KafkaException.Code"/> is 0.
/// </summary>
public class SerializationException : KafkaException
{
    /// <summary>Initializes an exception with a default message.</summary>
    public SerializationException()
    {
    }

    /// <summary>Initializes an exception with a message.</summary>
    public SerializationException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes an exception with a message and a cause.</summary>
    public SerializationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes an exception with a cause, whose type's full name and message, Java's <c>cause.toString()</c>,
    /// become the message, or the default message for a <see langword="null"/> cause.
    /// </summary>
    public SerializationException(Exception? cause)
        : base(cause)
    {
    }
}
