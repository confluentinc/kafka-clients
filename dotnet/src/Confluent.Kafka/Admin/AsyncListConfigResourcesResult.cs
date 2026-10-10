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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ListConfigResources"/> — the .NET realization of Java's
/// <c>ListConfigResourcesResult</c>: <b>one</b> future over the whole listing, handed
/// back the moment the request is submitted (<c>ListConfigResourcesResult.java:42</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Exactly one accessor, because Java declares exactly one.</b> Java's class holds a
/// single <c>KafkaFuture&lt;Collection&lt;ConfigResource&gt;&gt;</c> field and exposes it
/// through <c>all()</c> — there is no per-resource view and no map. Adding a second
/// projection would be public surface Java does not have
/// (<c>definition-of-done.md</c> §7).
/// </para>
/// <para>
/// ⚠ <b>A <em>collection</em>, not a map — which is why the walker gained a callable.</b>
/// <c>kafka_admin_ListConfigResourcesResult_t</c> exposes <c>count</c> / <c>get_type</c> /
/// <c>get_name</c> / <c>destroy</c>: no key, and no <c>get_error</c> of any kind. The
/// absence of the error accessor is the ABI stating the semantics — either the call fails,
/// through the callback's own <c>error</c>, or the whole listing succeeds. See
/// <c>KeyedResultMarshal.CompleteList</c>.
/// </para>
/// <para>
/// <b>Order.</b> Entries arrive sorted by <c>(type id, name)</c> and are handed on
/// unchanged. Java returns a <c>Collection</c>, so order is not part of the contract —
/// do not rely on it, but nothing here shuffles or re-sorts it either.
/// </para>
/// </remarks>
/// <seealso cref="ListConfigResourcesResult"/>
public sealed class AsyncListConfigResourcesResult
{
    private readonly Task<IReadOnlyCollection<ConfigResource>> _future;

    /// <summary>
    /// Wraps the single future — Java's package-private
    /// <c>ListConfigResourcesResult(KafkaFuture&lt;Collection&lt;ConfigResource&gt;&gt;)</c>
    /// (<c>:32</c>).
    /// </summary>
    internal AsyncListConfigResourcesResult(Task<IReadOnlyCollection<ConfigResource>> future)
    {
        _future = future;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncListConfigResourcesResult From(
        SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation) =>
        new AsyncListConfigResourcesResult(operation.Task);

    /// <summary>
    /// The full set of config resources — Java's <c>all()</c> (<c>:42</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the resources, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// It wraps the underlying task itself, so every call returns an equal future over the <b>same</b> task —
    /// no second task whose fault could go unobserved. Java's <c>all()</c> re-wraps the
    /// field in a fresh <c>KafkaFutureImpl</c> via <c>whenComplete</c> (<c>:43-51</c>),
    /// which yields an identical outcome; returning the source directly is the shipped
    /// <see cref="AsyncListTopicsResult.NamesToListings"/> reading of the same pattern.
    /// </remarks>
    public AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> All() => new AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>>(_future);
}
