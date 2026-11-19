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
/// The replica is not available for the requested topic partition. This may be a transient exception during
/// reassignments. From version 2.6 onwards, Fetch requests and other requests intended only for the leader or follower
/// of the topic partition return <c>NotLeaderOrFollowerException</c> if the broker is a not a replica of the partition.
/// Java's <c>org.apache.kafka.common.errors.ReplicaNotAvailableException</c>.
/// </summary>
public class ReplicaNotAvailableException : global::Confluent.Kafka.Common.Errors.InvalidMetadataException
{
    /// <summary>Initializes the exception, as Java's <c>ReplicaNotAvailableException(String message)</c>.</summary>
    public ReplicaNotAvailableException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ReplicaNotAvailableException(String message, Throwable cause)</c>.
    /// </summary>
    public ReplicaNotAvailableException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ReplicaNotAvailableException(Throwable cause)</c>.</summary>
    public ReplicaNotAvailableException(Exception? innerException)
        : base(innerException)
    {
    }

    internal ReplicaNotAvailableException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 9;
}
