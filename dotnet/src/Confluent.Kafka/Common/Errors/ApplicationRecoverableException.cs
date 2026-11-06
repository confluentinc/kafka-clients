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
/// Indicates that the error is fatal to the producer, and the application needs to restart the producer after handling
/// the error. Depending on the application, different recovery strategies (e.g., re-balancing task, restoring from
/// checkpoints) may be employed. Java's <c>org.apache.kafka.common.errors.ApplicationRecoverableException</c>.
/// </summary>
public abstract class ApplicationRecoverableException : global::Confluent.Kafka.Common.Errors.ApiException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>ApplicationRecoverableException(String message, Throwable cause)</c>.
    /// </summary>
    protected ApplicationRecoverableException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApplicationRecoverableException(String message)</c>.</summary>
    protected ApplicationRecoverableException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApplicationRecoverableException(Throwable cause)</c>.</summary>
    protected ApplicationRecoverableException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApplicationRecoverableException()</c>.</summary>
    protected ApplicationRecoverableException()
    {
    }

    internal ApplicationRecoverableException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }
}
