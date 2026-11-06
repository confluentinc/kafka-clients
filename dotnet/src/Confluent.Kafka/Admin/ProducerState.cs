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

using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The state of one active producer on a partition — Java's
/// <c>org.apache.kafka.clients.admin.ProducerState</c>.
/// </summary>
/// <remarks>
/// The two nullable getters are Java's <c>OptionalInt</c> and <c>OptionalLong</c>: absent is
/// <see langword="null"/>, never <c>0</c> or <c>-1</c>, although the four scalar getters use
/// <c>-1</c> as their sentinel.
/// </remarks>
public sealed class ProducerState
{
    private readonly long _producerId;
    private readonly int _producerEpoch;
    private readonly int _lastSequence;
    private readonly long _lastTimestamp;
    private readonly long? _currentTransactionStartOffset;
    private readonly int? _coordinatorEpoch;

    /// <summary>Creates a producer state — Java's constructor.</summary>
    /// <param name="producerId">The producer id.</param>
    /// <param name="producerEpoch">The producer epoch.</param>
    /// <param name="lastSequence">The last sequence number written by this producer.</param>
    /// <param name="lastTimestamp">The timestamp of the last record written by this producer.</param>
    /// <param name="coordinatorEpoch">The transaction coordinator epoch, or <see langword="null"/>.</param>
    /// <param name="currentTransactionStartOffset">
    /// The first offset of the in-progress transaction, or <see langword="null"/> when none is in progress.
    /// </param>
    public ProducerState(
        long producerId,
        int producerEpoch,
        int lastSequence,
        long lastTimestamp,
        int? coordinatorEpoch,
        long? currentTransactionStartOffset)
    {
        _producerId = producerId;
        _producerEpoch = producerEpoch;
        _lastSequence = lastSequence;
        _lastTimestamp = lastTimestamp;
        _coordinatorEpoch = coordinatorEpoch;
        _currentTransactionStartOffset = currentTransactionStartOffset;
    }

    /// <summary>The producer id — Java's <c>producerId()</c>.</summary>
    /// <returns>The producer id.</returns>
    public long ProducerId() => _producerId;

    /// <summary>
    /// The producer epoch — Java's <c>producerEpoch()</c>, an <see langword="int"/> here as in
    /// Java, unlike <see cref="AbortTransactionSpec.ProducerEpoch()"/>.
    /// </summary>
    /// <returns>The producer epoch — Java's <c>producerEpoch()</c>, an <see langword="int"/> here as in Java,
    /// unlike <see cref="AbortTransactionSpec.ProducerEpoch()"/>.</returns>
    public int ProducerEpoch() => _producerEpoch;

    /// <summary>The last written sequence number — Java's <c>lastSequence()</c>.</summary>
    /// <returns>The last written sequence number.</returns>
    public int LastSequence() => _lastSequence;

    /// <summary>The last written timestamp — Java's <c>lastTimestamp()</c>.</summary>
    /// <returns>The last written timestamp.</returns>
    public long LastTimestamp() => _lastTimestamp;

    /// <summary>
    /// The first offset of the in-progress transaction, or <see langword="null"/> when none is
    /// in progress — Java's <c>currentTransactionStartOffset()</c>.
    /// </summary>
    /// <returns>The first offset of the in-progress transaction, or <see langword="null"/> when none is in progress
    ///.</returns>
    public long? CurrentTransactionStartOffset() => _currentTransactionStartOffset;

    /// <summary>
    /// The transaction coordinator epoch, or <see langword="null"/> when absent — Java's
    /// <c>coordinatorEpoch()</c>.
    /// </summary>
    /// <returns>The transaction coordinator epoch, or <see langword="null"/> when absent.</returns>
    public int? CoordinatorEpoch() => _coordinatorEpoch;

    /// <summary>Value equality over all six fields — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same producer state.</returns>
    public override bool Equals(object? obj) =>
        obj is ProducerState other
        && ProducerId() == other.ProducerId()
        && ProducerEpoch() == other.ProducerEpoch()
        && LastSequence() == other.LastSequence()
        && LastTimestamp() == other.LastTimestamp()
        && CoordinatorEpoch() == other.CoordinatorEpoch()
        && CurrentTransactionStartOffset() == other.CurrentTransactionStartOffset();

    /// <summary>The hash of all six fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = ProducerId().GetHashCode();
            hash = (hash * 31) + ProducerEpoch();
            hash = (hash * 31) + LastSequence();
            hash = (hash * 31) + LastTimestamp().GetHashCode();
            hash = (hash * 31) + (CoordinatorEpoch()?.GetHashCode() ?? 0);
            return (hash * 31) + (CurrentTransactionStartOffset()?.GetHashCode() ?? 0);
        }
    }

    /// <summary>
    /// A diagnostic rendering — Java's <c>toString()</c>, where the two optional fields print
    /// as Java's <c>OptionalInt[55]</c> or <c>OptionalInt.empty</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ProducerState(producerId={0}, producerEpoch={1}, lastSequence={2}, lastTimestamp={3}"
                + ", coordinatorEpoch={4}, currentTransactionStartOffset={5})",
            _producerId,
            _producerEpoch,
            _lastSequence,
            _lastTimestamp,
            _coordinatorEpoch.HasValue
                ? "OptionalInt[" + _coordinatorEpoch.Value.ToString(CultureInfo.InvariantCulture) + "]"
                : "OptionalInt.empty",
            _currentTransactionStartOffset.HasValue
                ? "OptionalLong[" + _currentTransactionStartOffset.Value.ToString(CultureInfo.InvariantCulture) + "]"
                : "OptionalLong.empty");
}
