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
/// Exception thrown due to a request that illegally refers to the same resource twice (for example, trying to both
/// create and delete the same SCRAM credential for a particular user in a single request). Java's
/// <c>org.apache.kafka.common.errors.DuplicateResourceException</c>.
/// </summary>
public class DuplicateResourceException : global::Confluent.Kafka.Common.Errors.ApiException
{
    /// <summary>Initializes the exception, as Java's <c>DuplicateResourceException(String message)</c>.</summary>
    public DuplicateResourceException(string? message)
        : this(null, message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>DuplicateResourceException(String message, Throwable cause)</c>.
    /// </summary>
    public DuplicateResourceException(string? message, Exception? innerException)
        : this(null, message, innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>DuplicateResourceException(String resource, String message)</c>.
    /// </summary>
    public DuplicateResourceException(string? resource, string? message)
        : base(message)
    {
        Resource = resource;
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>DuplicateResourceException(String resource, String message, Throwable
    /// cause)</c>.
    /// </summary>
    public DuplicateResourceException(string? resource, string? message, Exception? innerException)
        : base(message, innerException)
    {
        Resource = resource;
    }

    internal DuplicateResourceException(CoreError _, string? message, Exception? innerException, string? resource)
        : base(_, message, innerException)
    {
        Resource = resource;
    }

    /// <summary>Java's <c>resource()</c>.</summary>
    public string? Resource { get; }

    internal override int FfiId => 92;
}
