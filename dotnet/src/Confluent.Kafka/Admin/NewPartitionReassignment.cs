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
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The target replica set for one partition in a call to
/// <see cref="IAdmin.AlterPartitionReassignments"/> — Java's
/// <c>org.apache.kafka.clients.admin.NewPartitionReassignment</c>.
/// </summary>
/// <remarks>
/// <para>
/// An empty replica list is rejected, as Java rejects it, so a cancellation is a
/// <see langword="null"/> entry in <see cref="IAdmin.AlterPartitionReassignments"/> (Java's
/// <c>Optional.empty()</c>), never an empty list.
/// </para>
/// <para>
/// A null list throws <see cref="ArgumentNullException"/> and an empty one
/// <see cref="ArgumentException"/>, both with Java's message, where Java throws one
/// <c>IllegalArgumentException</c>: a recorded deviation, which a
/// <c>catch (ArgumentException)</c> still covers.
/// </para>
/// </remarks>
public sealed class NewPartitionReassignment
{
    private const string NoReplicasMessage = "Cannot create a new partition reassignment without any replicas";

    private readonly ReadOnlyCollection<int> _targetReplicas;

    /// <summary>
    /// Creates a reassignment onto <paramref name="targetReplicas"/> — Java's constructor.
    /// </summary>
    /// <param name="targetReplicas">
    /// The broker ids the partition's replicas are to move to, preferred leader first.
    /// Must not be empty. The list is copied.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="targetReplicas"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="targetReplicas"/> is empty.</exception>
    public NewPartitionReassignment(IReadOnlyList<int> targetReplicas)
    {
        if (targetReplicas is null)
        {
            throw new ArgumentNullException(nameof(targetReplicas), NoReplicasMessage);
        }

        if (targetReplicas.Count == 0)
        {
            throw new ArgumentException(NoReplicasMessage, nameof(targetReplicas));
        }

        _targetReplicas = new List<int>(targetReplicas).AsReadOnly();
    }

    /// <summary>
    /// The broker ids the partition's replicas are to move to — Java's
    /// <c>targetReplicas()</c>. Never empty, and read-only, as Java's <c>List.copyOf</c> result.
    /// </summary>
    public IReadOnlyList<int> TargetReplicas => _targetReplicas;
}
