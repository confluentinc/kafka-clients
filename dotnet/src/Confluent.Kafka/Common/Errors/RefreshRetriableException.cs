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
/// Indicates that an operation failed due to outdated or invalid metadata, requiring a refresh (e.g., refreshing
/// producer metadata) before retrying the request. The request can be modified or updated with fresh metadata before
/// being retried. Java's <c>org.apache.kafka.common.errors.RefreshRetriableException</c>.
/// </summary>
public abstract class RefreshRetriableException : global::Confluent.Kafka.Common.Errors.RetriableException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>RefreshRetriableException(String message, Throwable cause)</c>.
    /// </summary>
    protected RefreshRetriableException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RefreshRetriableException(String message)</c>.</summary>
    protected RefreshRetriableException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RefreshRetriableException(Throwable cause)</c>.</summary>
    protected RefreshRetriableException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RefreshRetriableException()</c>.</summary>
    protected RefreshRetriableException()
    {
    }

    internal RefreshRetriableException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }
}
