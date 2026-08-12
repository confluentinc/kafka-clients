// Copyright 2025 Confluent Inc.
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

using System;

namespace Confluent.Kafka;

/// <summary>
/// The non-blocking consumer surface shared by every consumer flavor — the members
/// that are non-blocking in Java's consumer implementation and therefore stay
/// synchronous regardless of the async/sync split. It is the common base of
/// <see cref="IAsyncConsumer"/> (the async surface) and reserves the shape for a
/// future sync <c>IConsumer</c> surface, so both carry these two members with the
/// same signatures.
/// </summary>
public interface IConsumerCommon
{
    /// <summary>
    /// Interrupts a blocked operation on this consumer (Java <c>wakeup()</c>) — the
    /// in-flight <see cref="IAsyncConsumer.Poll"/> (etc.) faults with a
    /// <see cref="KafkaException"/> (Wakeup, one-shot). Non-blocking in Java, so it stays
    /// synchronous; it is the one member deliberately callable from another thread (the
    /// single-owner model's cross-thread escape). Best-effort: a no-op once the consumer
    /// is closing.
    /// </summary>
    void Wakeup();

    /// <summary>
    /// Returns the current consumer group metadata (Java <c>groupMetadata()</c>). A
    /// non-blocking getter in Java, so it stays synchronous (a method, matching Java's
    /// method-not-property shape).
    /// </summary>
    /// <returns>A snapshot of the group membership.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The consumer was accessed concurrently (it is not safe for multi-threaded access).
    /// </exception>
    ConsumerGroupMetadata GroupMetadata();
}
