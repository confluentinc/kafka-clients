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

namespace Confluent.Kafka.Common.Requests;

/// <summary>
/// Raised if the correlationId in a response header does not match the expected value from the request header. Java's
/// <c>org.apache.kafka.common.requests.CorrelationIdMismatchException</c>.
/// </summary>
public class CorrelationIdMismatchException : global::Confluent.Kafka.IllegalStateException
{
    private readonly int _requestCorrelationId;

    private readonly int _responseCorrelationId;

    /// <summary>
    /// Initializes the exception, as Java's <c>CorrelationIdMismatchException(String message, int requestCorrelationId,
    /// int responseCorrelationId)</c>.
    /// </summary>
    public CorrelationIdMismatchException(string? message, int requestCorrelationId, int responseCorrelationId)
        : base(message)
    {
        _requestCorrelationId = requestCorrelationId;
        _responseCorrelationId = responseCorrelationId;
    }

    internal CorrelationIdMismatchException(CoreError _, string? message, Exception? innerException, int requestCorrelationId, int responseCorrelationId)
        : base(_, message, innerException)
    {
        _requestCorrelationId = requestCorrelationId;
        _responseCorrelationId = responseCorrelationId;
    }

    /// <summary>Java's <c>requestCorrelationId()</c>.</summary>
    /// <returns>The request correlation id.</returns>
    public int RequestCorrelationId() => _requestCorrelationId;

    /// <summary>Java's <c>responseCorrelationId()</c>.</summary>
    /// <returns>The response correlation id.</returns>
    public int ResponseCorrelationId() => _responseCorrelationId;
}
