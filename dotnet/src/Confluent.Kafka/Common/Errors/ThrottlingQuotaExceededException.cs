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
/// Exception thrown if an operation on a resource exceeds the throttling quota. Java's
/// <c>org.apache.kafka.common.errors.ThrottlingQuotaExceededException</c>.
/// </summary>
public class ThrottlingQuotaExceededException : global::Confluent.Kafka.Common.Errors.RetriableException
{
    /// <summary>Initializes the exception, as Java's <c>ThrottlingQuotaExceededException(String message)</c>.</summary>
    public ThrottlingQuotaExceededException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ThrottlingQuotaExceededException(int throttleTimeMs, String
    /// message)</c>.
    /// </summary>
    public ThrottlingQuotaExceededException(int throttleTimeMs, string? message)
        : base(message)
    {
        ThrottleTimeMs = throttleTimeMs;
    }

    internal ThrottlingQuotaExceededException(CoreError _, string? message, Exception? innerException, int throttleTimeMs)
        : base(_, message, innerException)
    {
        ThrottleTimeMs = throttleTimeMs;
    }

    /// <summary>Java's <c>throttleTimeMs()</c>.</summary>
    public int ThrottleTimeMs { get; }

    internal override int FfiId => 89;
}
