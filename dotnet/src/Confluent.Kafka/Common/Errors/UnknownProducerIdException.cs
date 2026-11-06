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
/// This exception is raised by the broker if it could not locate the producer metadata associated with the producerId
/// in question. This could happen if, for instance, the producer's records were deleted because their retention time
/// had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the
/// broker, and future appends by the producer will return this exception. Java's
/// <c>org.apache.kafka.common.errors.UnknownProducerIdException</c>.
/// </summary>
public class UnknownProducerIdException : global::Confluent.Kafka.Common.Errors.OutOfOrderSequenceException
{
    /// <summary>Initializes the exception, as Java's <c>UnknownProducerIdException(String message)</c>.</summary>
    public UnknownProducerIdException(string? message)
        : base(message)
    {
    }

    internal UnknownProducerIdException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 59;
}
