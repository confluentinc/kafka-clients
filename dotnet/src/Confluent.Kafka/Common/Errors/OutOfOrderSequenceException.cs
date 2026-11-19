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
/// This exception indicates that the broker received an unexpected sequence number from the producer, which means that
/// data may have been lost. If the producer is configured for idempotence only (i.e. if <c>enable.idempotence</c> is
/// set and no <c>transactional.id</c> is configured), it is possible to continue sending with the same producer
/// instance, but doing so risks reordering of sent records. For transactional producers, this is a fatal error and you
/// should close the producer. Java's <c>org.apache.kafka.common.errors.OutOfOrderSequenceException</c>.
/// </summary>
public class OutOfOrderSequenceException : global::Confluent.Kafka.Common.Errors.ApiException
{
    /// <summary>Initializes the exception, as Java's <c>OutOfOrderSequenceException(String msg)</c>.</summary>
    public OutOfOrderSequenceException(string? message)
        : base(message)
    {
    }

    internal OutOfOrderSequenceException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 45;
}
