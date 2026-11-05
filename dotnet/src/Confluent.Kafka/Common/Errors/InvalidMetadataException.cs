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
/// An exception that may indicate the client's metadata is out of date. Java's
/// <c>org.apache.kafka.common.errors.InvalidMetadataException</c>.
/// </summary>
public abstract class InvalidMetadataException : global::Confluent.Kafka.Common.Errors.RefreshRetriableException
{
    /// <summary>Initializes the exception, as Java's <c>InvalidMetadataException()</c>.</summary>
    protected InvalidMetadataException()
    {
    }

    /// <summary>Initializes the exception, as Java's <c>InvalidMetadataException(String message)</c>.</summary>
    protected InvalidMetadataException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>InvalidMetadataException(String message, Throwable cause)</c>.
    /// </summary>
    protected InvalidMetadataException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>InvalidMetadataException(Throwable cause)</c>.</summary>
    protected InvalidMetadataException(Exception? innerException)
        : base(innerException)
    {
    }

    internal InvalidMetadataException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }
}
