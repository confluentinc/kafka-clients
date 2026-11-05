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
/// This fatal exception indicates that another producer with the same <c>transactional.id</c> has been started. It is
/// only possible to have one producer instance with a <c>transactional.id</c> at any given time, and the latest one to
/// be started "fences" the previous instances so that they can no longer make transactional requests. When you
/// encounter this exception, you must close the producer instance. Java's
/// <c>org.apache.kafka.common.errors.ProducerFencedException</c>.
/// </summary>
public class ProducerFencedException : global::Confluent.Kafka.Common.Errors.ApplicationRecoverableException
{
    /// <summary>Initializes the exception, as Java's <c>ProducerFencedException(String msg)</c>.</summary>
    public ProducerFencedException(string? message)
        : base(message)
    {
    }

    internal ProducerFencedException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 90;
}
