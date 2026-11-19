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
/// Any API exception that is part of the public protocol and should be a subclass of this class and be part of this
/// package. Java's <c>org.apache.kafka.common.errors.ApiException</c>.
/// </summary>
public class ApiException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>Initializes the exception, as Java's <c>ApiException(String message, Throwable cause)</c>.</summary>
    public ApiException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApiException(String message)</c>.</summary>
    public ApiException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApiException(Throwable cause)</c>.</summary>
    public ApiException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ApiException()</c>.</summary>
    public ApiException()
    {
    }

    internal ApiException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -6;
}
