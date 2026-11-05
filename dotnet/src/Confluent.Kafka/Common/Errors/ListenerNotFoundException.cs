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
/// The leader does not have an endpoint corresponding to the listener on which metadata was requested. This could
/// indicate a broker configuration error or a transient error when listeners are updated dynamically and client
/// requests are processed before all brokers have updated their listeners. This is currently used only for missing
/// listeners on leader brokers, but may be used for followers in future. Java's
/// <c>org.apache.kafka.common.errors.ListenerNotFoundException</c>.
/// </summary>
public class ListenerNotFoundException : global::Confluent.Kafka.Common.Errors.InvalidMetadataException
{
    /// <summary>Initializes the exception, as Java's <c>ListenerNotFoundException(String message)</c>.</summary>
    public ListenerNotFoundException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ListenerNotFoundException(String message, Throwable cause)</c>.
    /// </summary>
    public ListenerNotFoundException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal ListenerNotFoundException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 72;
}
