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
using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one topic in the cluster — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.TopicListing</c>.
/// </summary>
/// <remarks>
/// The three accessors are properties rather than methods, matching
/// <see cref="TopicDescription"/>: each is a pure managed field read that does no
/// P/Invoke and cannot throw, which is the case CLAUDE.md §3's "non-blocking getter →
/// sync property" row is written for. (The consumer's <c>Assignment()</c> family are
/// methods for the opposite reason — each marshals a fresh snapshot across the ABI.)
/// </remarks>
public sealed class TopicListing
{
    /// <summary>
    /// Initializes a listing — Java's
    /// <c>TopicListing(String name, Uuid topicId, boolean internal)</c>.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="topicId">The topic id.</param>
    /// <param name="isInternal">Whether the topic is internal to Kafka.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public TopicListing(string name, Uuid topicId, bool isInternal)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        TopicId = topicId;
        IsInternal = isInternal;
    }

    /// <summary>The topic name — Java's <c>name()</c>.</summary>
    public string Name { get; }

    /// <summary>The topic id — Java's <c>topicId()</c>.</summary>
    public Uuid TopicId { get; }

    /// <summary>
    /// Whether the topic is internal to Kafka (for example <c>__consumer_offsets</c>) —
    /// Java's <c>isInternal()</c>. Internal topics are listed only when
    /// <see cref="ListTopicsOptions.ListInternal"/> is set.
    /// </summary>
    public bool IsInternal { get; }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(name={0}, topicId={1}, internal={2})",
            Name,
            TopicId,
            IsInternal);
}
