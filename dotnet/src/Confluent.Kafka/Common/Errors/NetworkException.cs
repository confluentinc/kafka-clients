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
/// A misc. network-related IOException occurred when making a request. This could be because the client's metadata is
/// out of date and it is making a request to a node that is now dead. Java's
/// <c>org.apache.kafka.common.errors.NetworkException</c>.
/// </summary>
public class NetworkException : global::Confluent.Kafka.Common.Errors.InvalidMetadataException
{
    /// <summary>Initializes the exception, as Java's <c>NetworkException()</c>.</summary>
    public NetworkException()
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>NetworkException(String message, Throwable cause)</c>.
    /// </summary>
    public NetworkException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>NetworkException(String message)</c>.</summary>
    public NetworkException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>NetworkException(Throwable cause)</c>.</summary>
    public NetworkException(Exception? innerException)
        : base(innerException)
    {
    }

    internal NetworkException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 13;
}
