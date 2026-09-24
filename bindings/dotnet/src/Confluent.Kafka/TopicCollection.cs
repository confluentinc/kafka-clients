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

namespace Confluent.Kafka;

/// <summary>
/// A collection of topics identified <b>either</b> by name <b>or</b> by id — the .NET
/// realization of Java's <c>org.apache.kafka.common.TopicCollection</c>. It is the
/// argument type of <see cref="Admin.IAdmin.DeleteTopics"/> and
/// <see cref="Admin.IAdmin.DescribeTopics"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a type rather than two overloads.</b> "Names xor ids" is an invariant, and this
/// shape makes it unrepresentable to violate: the two nested subclasses are the only
/// inhabitants, so an RPC can dispatch on the runtime type and pick the matching ABI
/// entry point without a runtime "which one did you mean" check. The C ABI expresses the
/// same invariant by giving each form its own function
/// (<c>..._delete_topics</c> / <c>..._delete_topics_by_ids</c>) rather than a tagged
/// input struct.
/// </para>
/// <para>
/// <b>Namespace.</b> Java puts this in <c>org.apache.kafka.common</c>, not
/// <c>…clients.admin</c>, so it lives at the root <c>Confluent.Kafka</c> namespace beside
/// <see cref="Uuid"/> and <see cref="Node"/> rather than under
/// <c>Confluent.Kafka.Admin</c>.
/// </para>
/// <para>
/// <b>Subclassing is blocked, but by a slightly different mechanism than Java's.</b>
/// Java makes the outer constructor <c>private</c> and the two nested constructors
/// <c>private</c> too — legal there because Java's private access is symmetric across a
/// whole top-level class, so the outer static factories can still call the nested
/// constructors. C# is <b>not</b> symmetric: a nested type may reach its containing
/// type's private members, but the containing type may <em>not</em> reach a nested type's
/// (measured — it is a CS0122). So the outer constructor stays <c>private</c>, which is
/// what actually blocks subclassing from outside, and the two nested constructors are
/// <c>internal</c> — the closest reachable form, and still not constructible by a caller
/// outside this assembly. Java's javadoc — <em>"Subclassing this class beyond the classes
/// provided here is not supported"</em> — therefore remains a compile-time fact for every
/// consumer of the package, which is what it is there to guarantee.
/// </para>
/// </remarks>
public abstract class TopicCollection
{
    /// <summary>
    /// Private, so the only subclasses are the two nested ones (Java's
    /// <c>TopicCollection()</c> is <c>private</c> for the same reason).
    /// </summary>
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
    public static TopicNameCollection OfTopicNames(IEnumerable<string> topics) =>
        new TopicNameCollection(topics);

    /// <summary>
    /// Topics identified by their topic id — Java's <c>TopicCollection.TopicIdCollection</c>.
    /// </summary>
    public sealed class TopicIdCollection : TopicCollection
    {
        private readonly List<Uuid> _topicIds;

        /// <summary>
        /// Copies the caller's collection, as Java's <c>new ArrayList&lt;&gt;(topics)</c>
        /// does — a request must not be able to change under the caller's feet.
        /// <c>internal</c>, not <c>private</c>: see the subclassing note in the type
        /// remarks.
        /// </summary>
        /// <param name="topics">The topic ids. Must not be null.</param>
        /// <exception cref="System.ArgumentNullException"><paramref name="topics"/> is null.</exception>
        /// <remarks>
        /// The guard is explicit rather than left to <c>List&lt;T&gt;</c>'s own throw: the BCL
        /// constructor names <em>its</em> parameter (<c>collection</c>), which is an
        /// implementation detail of the copy and not a parameter this API has. The binding
        /// names its own parameter everywhere else — including the sibling types this phase
        /// ships — and callers filter on <c>ParamName</c>. The parameter is also named
        /// <c>topics</c>, not <c>topicIds</c>, so the name reported is the one the public
        /// factory documents whichever path reaches here.
        /// </remarks>
        internal TopicIdCollection(IEnumerable<Uuid> topics)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            _topicIds = new List<Uuid>(topics);
        }

        /// <summary>
        /// The topic ids — Java's <c>topicIds()</c>. A <b>method</b>, not a property,
        /// because Java's is a method and it hands back a snapshot view.
        /// </summary>
        /// <returns>The topic ids, in the order supplied.</returns>
        public IReadOnlyCollection<Uuid> TopicIds() => _topicIds;
    }

    /// <summary>
    /// Topics identified by their name — Java's <c>TopicCollection.TopicNameCollection</c>.
    /// </summary>
    public sealed class TopicNameCollection : TopicCollection
    {
        private readonly List<string> _topicNames;

        /// <inheritdoc cref="TopicIdCollection(IEnumerable{Uuid})"/>
        internal TopicNameCollection(IEnumerable<string> topics)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            _topicNames = new List<string>(topics);
        }

        /// <summary>
        /// The topic names — Java's <c>topicNames()</c>. A <b>method</b>, not a property,
        /// for the same reason as <see cref="TopicIdCollection.TopicIds"/>.
        /// </summary>
        /// <returns>The topic names, in the order supplied.</returns>
        public IReadOnlyCollection<string> TopicNames() => _topicNames;
    }
}
