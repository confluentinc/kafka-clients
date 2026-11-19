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
/// Indicates that the leader is not able to guarantee monotonically increasing offsets due to the high watermark
/// lagging behind the epoch start offset after a recent leader election. Java's
/// <c>org.apache.kafka.common.errors.OffsetNotAvailableException</c>.
/// </summary>
public class OffsetNotAvailableException : global::Confluent.Kafka.Common.Errors.RetriableException
{
    /// <summary>Initializes the exception, as Java's <c>OffsetNotAvailableException(String message)</c>.</summary>
    public OffsetNotAvailableException(string? message)
        : base(message)
    {
    }

    internal OffsetNotAvailableException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 78;
}
