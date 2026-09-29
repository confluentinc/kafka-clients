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

using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// Carries Java's <b>default</b> <c>ConsumerRebalanceListener.onPartitionsLost</c>
/// implementation — delegate to <c>onPartitionsRevoked</c> — for implementers that want it.
/// Derive from this and override only <see cref="OnPartitionsRevoked"/> and
/// <see cref="OnPartitionsAssigned"/>; override <see cref="OnPartitionsLost"/> as well to
/// distinguish a loss from a clean revocation.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this type exists (a deliberate deviation from the Java type list,
/// <c>definition-of-done.md</c> §7).</b> Java puts the <c>onPartitionsLost</c> default
/// <em>on the interface</em>. C# default interface methods require .NET Standard 2.1 / C# 8,
/// and this binding's floor is <b>netstandard2.0</b> (it must load on net462), so a default
/// interface method is not expressible here. Dropping the default would make every
/// implementer hand-write a delegating <c>OnPartitionsLost</c> — losing a Java behaviour the
/// binding exists to restore (<c>bindings/CLAUDE.md</c> §2). This abstract base is the
/// idiomatic C# carrier for it, so the type exists <em>because of</em> a Java behaviour
/// rather than adding one. Chosen as P6-D1 option (b).
/// </para>
/// <para>
/// Using it is optional: <see cref="IConsumerRebalanceListener"/> stays directly
/// implementable, and everything the interface documents about threading, throwing and
/// registration lifetime applies unchanged.
/// </para>
/// </remarks>
public abstract class ConsumerRebalanceListenerBase : IConsumerRebalanceListener
{
    /// <inheritdoc/>
    public abstract void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <inheritdoc/>
    public abstract void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Java's default <c>onPartitionsLost</c>: delegates to
    /// <see cref="OnPartitionsRevoked"/>. Override to treat a loss differently from a clean
    /// revocation.
    /// </summary>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    public virtual void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        OnPartitionsRevoked(partitions);
}
