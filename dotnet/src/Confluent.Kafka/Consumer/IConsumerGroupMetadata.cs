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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The consumer group information a consumer's <c>GroupMetadata()</c> returns and a producer's
/// <c>SendOffsetsToTransaction</c> takes, Java's <c>org.apache.kafka.clients.consumer.ConsumerGroupMetadata</c>.
/// </summary>
/// <remarks>
/// <para>
/// An interface, as Java's becomes in Kafka 5.0 (KIP-1136), whose constructors are deprecated since 4.2.
/// </para>
/// <para>
/// The instance a consumer returns is a snapshot. Before the consumer has joined a group, <see cref="GroupId()"/> is
/// the configured <c>group.id</c>, <see cref="GenerationId()"/> is <c>-1</c>, <see cref="MemberId()"/> is empty, and
/// <see cref="GroupInstanceId()"/> is the configured <c>group.instance.id</c>, or <see langword="null"/>; a mock
/// consumer returns Java's mock values. It compares by value, as Java's <c>equals</c>, and can be passed to
/// <c>SendOffsetsToTransaction</c> also after its consumer is closed.
/// </para>
/// <para>
/// An implementation of your own can be passed to <c>SendOffsetsToTransaction</c> too. The producer reads each of its
/// values once, on the calling thread, when the call is made; a string holding a NUL character is refused with
/// <see cref="System.ArgumentException"/>, as the native client cannot receive it unchanged.
/// </para>
/// </remarks>
public interface IConsumerGroupMetadata
{
    /// <summary>The consumer group id (the configured <c>group.id</c>).</summary>
    /// <returns>The consumer group id.</returns>
    string GroupId();

    /// <summary>
    /// The group generation id, or <c>-1</c> when unknown (before the consumer has joined a group).
    /// </summary>
    /// <returns>The group generation id, or <c>-1</c> when unknown.</returns>
    int GenerationId();

    /// <summary>
    /// The member id assigned by the coordinator, or the empty string before the consumer has joined a group.
    /// </summary>
    /// <returns>The member id, or the empty string before the consumer has joined a group.</returns>
    string MemberId();

    /// <summary>
    /// The static group instance id (the configured <c>group.instance.id</c>), or <see langword="null"/> when the
    /// consumer is not a static member.
    /// </summary>
    /// <returns>The static group instance id, or <see langword="null"/> when the consumer is not a static
    /// member.</returns>
    string? GroupInstanceId();
}
