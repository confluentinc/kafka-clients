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
/// <c>org.apache.kafka.clients.admin.ProducerState</c> (<c>:23</c>).
/// </summary>
/// <remarks>
/// ⚠ The two nullable properties are Java's <c>OptionalInt</c>/<c>OptionalLong</c> (<c>:28-29</c>):
/// absent is <see langword="null"/>, never <c>0</c> or <c>-1</c>. The four scalar siblings do
/// use <c>-1</c> as their out-of-range sentinel, so the two encodings must not be conflated.
/// </remarks>
public sealed class ProducerState
{
    /// <summary>Creates a producer state — Java's constructor (<c>:31-45</c>).</summary>
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
        ProducerId = producerId;
        ProducerEpoch = producerEpoch;
        LastSequence = lastSequence;
        LastTimestamp = lastTimestamp;
        CoordinatorEpoch = coordinatorEpoch;
        CurrentTransactionStartOffset = currentTransactionStartOffset;
    }

    /// <summary>The producer id — Java's <c>producerId()</c> (<c>:47</c>).</summary>
    public long ProducerId { get; }

    /// <summary>
    /// The producer epoch — Java's <c>producerEpoch()</c> (<c>:51</c>), an <see langword="int"/>
    /// here as in Java, unlike <see cref="AbortTransactionSpec.ProducerEpoch"/>.
    /// </summary>
    public int ProducerEpoch { get; }

    /// <summary>The last written sequence number — Java's <c>lastSequence()</c> (<c>:55</c>).</summary>
    public int LastSequence { get; }

    /// <summary>The last written timestamp — Java's <c>lastTimestamp()</c> (<c>:59</c>).</summary>
    public long LastTimestamp { get; }

    /// <summary>
    /// The first offset of the in-progress transaction, or <see langword="null"/> when none is
    /// in progress — Java's <c>currentTransactionStartOffset()</c> (<c>:63</c>, <c>OptionalLong</c>).
    /// </summary>
    public long? CurrentTransactionStartOffset { get; }

    /// <summary>
    /// The transaction coordinator epoch, or <see langword="null"/> when absent — Java's
    /// <c>coordinatorEpoch()</c> (<c>:67</c>, <c>OptionalInt</c>).
    /// </summary>
    public int? CoordinatorEpoch { get; }

    /// <summary>Value equality over all six fields — Java's <c>equals</c> (<c>:72</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same producer state.</returns>
    public override bool Equals(object? obj) =>
        obj is ProducerState other
        && ProducerId == other.ProducerId
        && ProducerEpoch == other.ProducerEpoch
        && LastSequence == other.LastSequence
        && LastTimestamp == other.LastTimestamp
        && CoordinatorEpoch == other.CoordinatorEpoch
        && CurrentTransactionStartOffset == other.CurrentTransactionStartOffset;

    /// <summary>The hash of all six fields — Java's <c>hashCode</c> (<c>:85</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = ProducerId.GetHashCode();
            hash = (hash * 31) + ProducerEpoch;
            hash = (hash * 31) + LastSequence;
            hash = (hash * 31) + LastTimestamp.GetHashCode();
            hash = (hash * 31) + (CoordinatorEpoch?.GetHashCode() ?? 0);
            return (hash * 31) + (CurrentTransactionStartOffset?.GetHashCode() ?? 0);
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:91</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ProducerState(producerId={0}, producerEpoch={1}, lastSequence={2}, lastTimestamp={3}"
                + ", coordinatorEpoch={4}, currentTransactionStartOffset={5})",
            ProducerId,
            ProducerEpoch,
            LastSequence,
            LastTimestamp,
            CoordinatorEpoch.HasValue ? CoordinatorEpoch.Value.ToString(CultureInfo.InvariantCulture) : "null",
            CurrentTransactionStartOffset.HasValue
                ? CurrentTransactionStartOffset.Value.ToString(CultureInfo.InvariantCulture)
                : "null");
}
