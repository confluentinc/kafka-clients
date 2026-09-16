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
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A description of one log directory on one broker — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.LogDirDescription</c>
/// (<c>LogDirDescription.java:31</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b><see cref="Error"/> is a FIELD on a successfully-returned description, not a
/// failure of the query.</b> It means <em>this log directory</em> is offline or unreadable
/// while the broker's query itself succeeded — Java documents
/// <c>KafkaStorageException</c> ("the log directory is offline") and
/// <c>UnknownServerException</c> as its two values (<c>:54-59</c>). The per-broker
/// task in <see cref="DescribeLogDirsResult.Descriptions"/> therefore
/// <b>succeeds</b>, carrying a description whose <see cref="Error"/> is non-null. Faulting
/// the task here would be a behavioural divergence — the broker answered.
/// </para>
/// <para>
/// ⚠⚠ <b><c>IsCordoned</c> is DELIBERATELY ABSENT (M15/P3 decision D15, §15
/// Gap 1).</b> Java has <c>isCordoned()</c> (<c>:94</c>) and the Rust core implements it
/// (<c>src/admin/log_dir_description.rs:113</c>), but the C ABI exports no accessor for it
/// — verified: <c>grep -ci "cordoned"</c> over the header is <b>0</b>, against a
/// control-positive <c>kafka_admin_LogDirDescription_total_bytes</c> of <b>1</b>. It is
/// <b>not faked</b>: a stubbed <c>false</c> would assert <em>"this log directory is not
/// cordoned"</em> when the truth is <em>"the binding cannot know"</em>, and a wrong answer
/// is worse than an absent one. <b>Absence is the honest encoding</b>, and a test pins that
/// no such member exists so a later refactor cannot quietly add a fake one. This makes the
/// type knowingly incomplete against Java (<c>definition-of-done.md</c> §2), which is
/// recorded rather than hidden.
/// </para>
/// <para>
/// <b>Accessors are properties</b> (decision D18) — each is a pure managed field read.
/// </para>
/// </remarks>
public sealed class LogDirDescription
{
    /// <summary>
    /// Initializes a description — the shape of Java's constructors
    /// (<c>:38</c>, <c>:42</c>, <c>:46</c>), minus the <c>isCordoned</c> flag the ABI does
    /// not expose.
    /// </summary>
    /// <param name="error">
    /// The directory-level error, or <see langword="null"/> if the directory is healthy.
    /// </param>
    /// <param name="replicaInfos">The replicas hosted in this directory.</param>
    /// <param name="totalBytes">
    /// The volume's total size, or <see langword="null"/> if the broker did not report it.
    /// </param>
    /// <param name="usableBytes">
    /// The volume's usable size, or <see langword="null"/> if the broker did not report it.
    /// </param>
    /// <remarks>
    /// <see langword="internal"/> because the only producer is the result marshaller.
    /// Java's constructors are public, but they take the <c>isCordoned</c> flag this type
    /// cannot carry (directly, or by defaulting it) — publishing a constructor that
    /// silently drops it would put the fake back in through the other door.
    /// </remarks>
    internal LogDirDescription(
        KafkaException? error,
        IReadOnlyDictionary<TopicPartition, ReplicaInfo> replicaInfos,
        long? totalBytes,
        long? usableBytes)
    {
        Error = error;
        ReplicaInfos = replicaInfos;
        TotalBytes = totalBytes;
        UsableBytes = usableBytes;
    }

    /// <summary>
    /// The directory-level error, or <see langword="null"/> when the directory is healthy —
    /// Java's <c>error()</c> (<c>:61</c>), whose type is <c>ApiException</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="KafkaException"/> is this binding's established mapping for Java's
    /// <c>ApiException</c> (the M15/P1 <c>TopicMetadataAndConfig</c> precedent); no second
    /// exception type is introduced for it. ⚠ Non-null here does <b>not</b> mean the query
    /// failed — see the type remarks.
    /// </remarks>
    public KafkaException? Error { get; }

    /// <summary>
    /// The replicas hosted in this directory, keyed by topic partition — Java's
    /// <c>replicaInfos()</c> (<c>:69</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, ReplicaInfo> ReplicaInfos { get; }

    /// <summary>
    /// The total size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>totalBytes()</c> (<c>:78</c>), an <c>OptionalLong</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <c>OptionalLong</c> has no netstandard2.0 equivalent, so the mapping is
    /// <c>long?</c>, and the ABI's <c>-1</c> — Java's <c>UNKNOWN_VOLUME_BYTES</c>, which its
    /// constructor turns into <c>OptionalLong.empty()</c> (<c>:49</c>) — becomes
    /// <see langword="null"/>. ⚠ <b>Contrast <see cref="DeletedRecords.LowWatermark"/>,
    /// where <c>-1</c> is a legitimate value and not a sentinel at all.</b> Here a volume
    /// size cannot legitimately be negative, and the header names <c>-1</c> as the "did not
    /// report" marker. <c>0</c> is a real size and stays <c>0</c>.
    /// </remarks>
    public long? TotalBytes { get; }

    /// <summary>
    /// The usable size in bytes of the volume this directory is on, or
    /// <see langword="null"/> when the broker did not report it — Java's
    /// <c>usableBytes()</c> (<c>:87</c>).
    /// </summary>
    /// <remarks>
    /// <inheritdoc cref="TotalBytes" path="/remarks"/>
    /// </remarks>
    public long? UsableBytes { get; }

    /// <summary>
    /// A diagnostic rendering following Java's <c>toString()</c> (<c>:98</c>).
    /// </summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// ⚠ Java's rendering ends with <c>, isCordoned=…)</c> (<c>:105</c>). That field is
    /// omitted here for the same reason the accessor is (D15): rendering a value the
    /// binding cannot know would be a fabrication in diagnostic output, which is exactly
    /// where a reader would trust it.
    /// </remarks>
    public override string ToString()
    {
        StringBuilder replicas = new StringBuilder("{");
        bool first = true;
        foreach (KeyValuePair<TopicPartition, ReplicaInfo> replica in ReplicaInfos)
        {
            if (!first)
            {
                replicas.Append(", ");
            }

            first = false;
            replicas.Append(replica.Key).Append('=').Append(replica.Value);
        }

        replicas.Append('}');

        return string.Format(
            CultureInfo.InvariantCulture,
            "LogDirDescription(replicaInfos={0}, error={1}, totalBytes={2}, usableBytes={3})",
            replicas,
            Error is null ? "null" : Error.Message,
            TotalBytes is null ? "empty" : TotalBytes.Value.ToString(CultureInfo.InvariantCulture),
            UsableBytes is null ? "empty" : UsableBytes.Value.ToString(CultureInfo.InvariantCulture));
    }
}
