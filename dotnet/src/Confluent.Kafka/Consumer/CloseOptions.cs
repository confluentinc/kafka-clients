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

using System;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The options of <see cref="IConsumer{TKey, TValue}.Close(CloseOptions)"/> — Java's <c>CloseOptions</c>: the
/// maximum time to wait for the close and the group membership operation applied on leaving.
/// </summary>
public sealed class CloseOptions
{
    private TimeSpan? _timeout;

    private Confluent.Kafka.Consumer.GroupMembershipOperation _operation = Confluent.Kafka.Consumer.GroupMembershipOperation.Default;

    private CloseOptions()
    {
    }

    /// <summary>Creates options with <paramref name="timeout"/> — Java's static <c>timeout(Duration)</c>.</summary>
    /// <param name="timeout">The maximum time to wait for the consumer to close; null applies the default close timeout.</param>
    /// <returns>New options with that timeout and <see cref="Confluent.Kafka.Consumer.GroupMembershipOperation.Default"/>.</returns>
    public static CloseOptions Timeout(TimeSpan? timeout) => new CloseOptions().WithTimeout(timeout);

    /// <summary>
    /// Creates options with <paramref name="operation"/> — Java's static
    /// <c>groupMembershipOperation(GroupMembershipOperation)</c>.
    /// </summary>
    /// <param name="operation">The group membership operation to apply on close.</param>
    /// <returns>New options with that operation and no timeout.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="operation"/> is not a defined <see cref="Confluent.Kafka.Consumer.GroupMembershipOperation"/> member.
    /// </exception>
    public static CloseOptions GroupMembershipOperation(Confluent.Kafka.Consumer.GroupMembershipOperation operation) =>
        new CloseOptions().WithGroupMembershipOperation(operation);

    /// <summary>Sets the timeout — Java's <c>withTimeout(Duration)</c>.</summary>
    /// <param name="timeout">The maximum time to wait for the consumer to close; null applies the default close timeout.</param>
    /// <returns>These options.</returns>
    public CloseOptions WithTimeout(TimeSpan? timeout)
    {
        _timeout = timeout;
        return this;
    }

    /// <summary>Sets the group membership operation — Java's <c>withGroupMembershipOperation(GroupMembershipOperation)</c>.</summary>
    /// <param name="operation">The group membership operation to apply on close.</param>
    /// <returns>These options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="operation"/> is not a defined <see cref="Confluent.Kafka.Consumer.GroupMembershipOperation"/> member.
    /// </exception>
    public CloseOptions WithGroupMembershipOperation(Confluent.Kafka.Consumer.GroupMembershipOperation operation)
    {
        if (!Enum.IsDefined(typeof(Confluent.Kafka.Consumer.GroupMembershipOperation), operation))
        {
            throw new ArgumentOutOfRangeException(
                nameof(operation), operation, "operation must be a defined GroupMembershipOperation member.");
        }

        _operation = operation;
        return this;
    }

    /// <summary>The group membership operation — Java's <c>groupMembershipOperation()</c>.</summary>
    /// <returns>The operation; <see cref="Confluent.Kafka.Consumer.GroupMembershipOperation.Default"/> unless set.</returns>
    public Confluent.Kafka.Consumer.GroupMembershipOperation GroupMembershipOperation() => _operation;

    /// <summary>The timeout — Java's <c>timeout()</c>.</summary>
    /// <returns>The timeout, or null when the default close timeout applies.</returns>
    public TimeSpan? Timeout() => _timeout;
}
