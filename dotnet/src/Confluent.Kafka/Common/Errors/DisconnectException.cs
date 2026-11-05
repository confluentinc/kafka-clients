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

/// <summary>
/// Server disconnected before a request could be completed. Java's
/// <c>org.apache.kafka.common.errors.DisconnectException</c>.
/// </summary>
public class DisconnectException : global::Confluent.Kafka.Common.Errors.RetriableException
{
    /// <summary>
    /// The instance Java's <c>INSTANCE</c> holds. An error the client throws is never this instance, so compare by
    /// type. .NET records a stack trace on every throw, where Java's <c>ApiException</c> records none.
    /// </summary>
    public static readonly DisconnectException Instance = new DisconnectException();

    /// <summary>Initializes the exception, as Java's <c>DisconnectException()</c>.</summary>
    public DisconnectException()
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>DisconnectException(String message, Throwable cause)</c>.
    /// </summary>
    public DisconnectException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>DisconnectException(String message)</c>.</summary>
    public DisconnectException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>DisconnectException(Throwable cause)</c>.</summary>
    public DisconnectException(Exception? innerException)
        : base(innerException)
    {
    }

    internal DisconnectException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -11;
}
