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
/// In the context of the group coordinator, the broker returns this error code for metadata or offset commit requests
/// if the group metadata topic has not been created yet. Java's
/// <c>org.apache.kafka.common.errors.CoordinatorNotAvailableException</c>.
/// </summary>
public class CoordinatorNotAvailableException : global::Confluent.Kafka.Common.Errors.RefreshRetriableException
{
    /// <summary>
    /// The instance Java's <c>INSTANCE</c> holds. An error the client throws is never this instance, so compare by
    /// type. .NET records a stack trace on every throw, where Java's <c>ApiException</c> records none.
    /// </summary>
    public static readonly CoordinatorNotAvailableException INSTANCE = new CoordinatorNotAvailableException();

    private CoordinatorNotAvailableException()
    {
    }

    /// <summary>Initializes the exception, as Java's <c>CoordinatorNotAvailableException(String message)</c>.</summary>
    public CoordinatorNotAvailableException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>CoordinatorNotAvailableException(String message, Throwable cause)</c>.
    /// </summary>
    public CoordinatorNotAvailableException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal CoordinatorNotAvailableException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 15;
}
