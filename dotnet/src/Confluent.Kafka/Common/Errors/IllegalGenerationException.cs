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

namespace Confluent.Kafka.Common.Errors;

/// <summary>Java's <c>org.apache.kafka.common.errors.IllegalGenerationException</c>.</summary>
public class IllegalGenerationException : global::Confluent.Kafka.Common.Errors.ApplicationRecoverableException
{
    /// <summary>Initializes the exception, as Java's <c>IllegalGenerationException()</c>.</summary>
    public IllegalGenerationException()
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>IllegalGenerationException(String message, Throwable cause)</c>.
    /// </summary>
    public IllegalGenerationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>IllegalGenerationException(String message)</c>.</summary>
    public IllegalGenerationException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>IllegalGenerationException(Throwable cause)</c>.</summary>
    public IllegalGenerationException(Exception? innerException)
        : base(innerException)
    {
    }

    internal IllegalGenerationException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 22;
}
