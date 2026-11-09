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
/// Exception thrown due to a request for a resource that does not exist. Java's
/// <c>org.apache.kafka.common.errors.ResourceNotFoundException</c>.
/// </summary>
public class ResourceNotFoundException : global::Confluent.Kafka.Common.Errors.ApiException
{
    private readonly string? _resource;

    /// <summary>Initializes the exception, as Java's <c>ResourceNotFoundException(String message)</c>.</summary>
    public ResourceNotFoundException(string? message)
        : this(null, message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ResourceNotFoundException(String message, Throwable cause)</c>.
    /// </summary>
    public ResourceNotFoundException(string? message, Exception? innerException)
        : this(null, message, innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ResourceNotFoundException(String resource, String message)</c>.
    /// </summary>
    public ResourceNotFoundException(string? resource, string? message)
        : base(message)
    {
        _resource = resource;
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ResourceNotFoundException(String resource, String message, Throwable
    /// cause)</c>.
    /// </summary>
    public ResourceNotFoundException(string? resource, string? message, Exception? innerException)
        : base(message, innerException)
    {
        _resource = resource;
    }

    internal ResourceNotFoundException(CoreError _, string? message, Exception? innerException, string? resource)
        : base(_, message, innerException)
    {
        _resource = resource;
    }

    /// <summary>Java's <c>resource()</c>.</summary>
    /// <returns>The resource.</returns>
    public string? Resource() => _resource;

    internal override int FfiId => 91;
}
