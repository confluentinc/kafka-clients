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

namespace Confluent.Kafka;

/// <summary>
/// A collection of topics identified either by name or by id — Java's
/// <c>org.apache.kafka.common.TopicCollection</c>, the argument type of
/// <see cref="Admin.IAdmin.DeleteTopics(TopicCollection, Admin.DeleteTopicsOptions?)"/> and
/// <see cref="Admin.IAdmin.DescribeTopics(TopicCollection, Admin.DescribeTopicsOptions?)"/>.
/// </summary>
/// <remarks>
/// The two nested classes are the only subclasses: the outer constructor is private, as in
/// Java. The nested constructors are internal, where Java's are private, because C# does not let
/// the outer factories reach a nested type's private constructor.
/// </remarks>
public abstract class TopicCollection
{
    private TopicCollection()
    {
    }

    /// <summary>
    /// A collection of topics identified by topic id — Java's <c>ofTopicIds</c>.
    /// </summary>
    /// <param name="topics">The topic ids. Copied, so later mutation cannot affect a request.</param>
    /// <returns>The collection.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="topics"/> is null.</exception>
    public static TopicIdCollection OfTopicIds(IEnumerable<Uuid> topics) => new TopicIdCollection(topics);

    /// <summary>
    /// A collection of topics identified by topic name — Java's <c>ofTopicNames</c>.
    /// </summary>
    /// <param name="topics">The topic names. Copied, so later mutation cannot affect a request.</param>
    /// <returns>The collection.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <remarks>
    /// A <see langword="null"/> element is accepted here, as Java's <c>ofTopicNames</c> accepts
    /// one. A recorded deviation:
    /// <see cref="Admin.IAdmin.DeleteTopics(TopicCollection, Admin.DeleteTopicsOptions?)"/> and
    /// <see cref="Admin.IAdmin.DescribeTopics(TopicCollection, Admin.DescribeTopicsOptions?)"/>
    /// reject the whole call with <see cref="System.ArgumentException"/>, where Java fails only
    /// that key's future with <c>InvalidTopicException</c>.
    /// </remarks>
    public static TopicNameCollection OfTopicNames(IEnumerable<string> topics) =>
        new TopicNameCollection(topics);

    /// <summary>
    /// Topics identified by their topic id — Java's <c>TopicCollection.TopicIdCollection</c>.
    /// </summary>
    public sealed class TopicIdCollection : TopicCollection
    {
        private readonly ReadOnlyCollection<Uuid> _topicIds;

        internal TopicIdCollection(IEnumerable<Uuid> topics)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            _topicIds = new ReadOnlyCollection<Uuid>(new List<Uuid>(topics));
        }

        /// <summary>The topic ids — Java's <c>topicIds()</c>.</summary>
        /// <returns>The topic ids, in the order supplied, as a read-only view, as Java's.</returns>
        public IReadOnlyCollection<Uuid> TopicIds() => _topicIds;
    }

    /// <summary>
    /// Topics identified by their name — Java's <c>TopicCollection.TopicNameCollection</c>.
    /// </summary>
    public sealed class TopicNameCollection : TopicCollection
    {
        private readonly ReadOnlyCollection<string> _topicNames;

        internal TopicNameCollection(IEnumerable<string> topics)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            _topicNames = new ReadOnlyCollection<string>(new List<string>(topics));
        }

        /// <summary>The topic names — Java's <c>topicNames()</c>.</summary>
        /// <returns>The topic names, in the order supplied, as a read-only view, as Java's.</returns>
        public IReadOnlyCollection<string> TopicNames() => _topicNames;
    }
}
