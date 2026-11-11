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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// Exception thrown when an offset commit fails with a retriable error. This exception is generated on the client side
/// upon receiving retriable error codes from the Group Coordinator in a commit response. Java's
/// <c>org.apache.kafka.clients.consumer.RetriableCommitFailedException</c>.
/// </summary>
public class RetriableCommitFailedException : global::Confluent.Kafka.Common.Errors.RetriableException
{
    /// <summary>Initializes the exception, as Java's <c>RetriableCommitFailedException(Throwable t)</c>.</summary>
    public RetriableCommitFailedException(Exception? innerException)
        : base("Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.", innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RetriableCommitFailedException(String message)</c>.</summary>
    public RetriableCommitFailedException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>RetriableCommitFailedException(String message, Throwable t)</c>.
    /// </summary>
    public RetriableCommitFailedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal RetriableCommitFailedException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -23;
}
