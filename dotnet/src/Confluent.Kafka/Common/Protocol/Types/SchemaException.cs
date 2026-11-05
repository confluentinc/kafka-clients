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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Protocol.Types;

/// <summary>
/// Thrown if the protocol schema validation fails while parsing request or response. Java's
/// <c>org.apache.kafka.common.protocol.types.SchemaException</c>.
/// </summary>
public class SchemaException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>Initializes the exception, as Java's <c>SchemaException(String message)</c>.</summary>
    public SchemaException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>SchemaException(String message, Throwable cause)</c>.</summary>
    public SchemaException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal SchemaException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -14;
}
