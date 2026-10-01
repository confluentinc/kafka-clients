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
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>listGroups</c> call — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsResult</c> (<c>ListGroupsResult.java:31</c>):
/// <b>three</b> awaitable views of <b>one</b> listing, handed back the moment the request is
/// submitted.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="All"/> is not <see cref="Valid"/>.</b> A <c>listGroups</c> call can partially
/// succeed — some brokers answer with listings while others answer with an error — and Java
/// splits that one mixed outcome three ways (<c>:43-58</c>): <see cref="All"/> yields every
/// listing but <b>fails</b> if any error occurred, <see cref="Valid"/> yields the listings
/// regardless, and <see cref="Errors"/> yields the errors. Reaching for <see cref="All"/> where
/// <see cref="Valid"/> was meant turns a partial success into a thrown exception; the other way
/// round silently hides one.
/// </para>
/// <para>
/// ⚠ <b>The listings and the errors are independent, and nothing here pairs them.</b> They are
/// two lists with <em>separate</em> lengths — not parallel arrays, and with no positional
/// correspondence at all: three errors can accompany one listing, or none. Each surfaces as its
/// own <see cref="IReadOnlyCollection{T}"/> of its own element type, carrying its own
/// <c>Count</c>. No count is ever exposed apart from the collection it belongs to, so neither
/// this class nor a caller can index one list by the other's length.
/// </para>
/// <para>
/// <b>Java's <c>Collection&lt;Throwable&gt;</c> becomes
/// <c>IReadOnlyCollection&lt;KafkaException&gt;</c>.</b> Every per-group error crossing the ABI
/// is a native error handle, which this binding surfaces as the one flat
/// <see cref="KafkaException"/> — there is no exception hierarchy left to widen for. The
/// narrower element type is also exactly what <see cref="All"/> throws, so the two accessors
/// cannot disagree.
/// </para>
/// <para>
/// ⚠ <b>"Never fails with an error" is about per-group errors, not about the call.</b> Java's
/// javadoc says <see cref="Valid"/> and <see cref="Errors"/> never fail (<c>:76</c>,
/// <c>:92</c>), and in Java that holds because a per-group error is an <em>element</em> of the
/// one source future's value rather than a failure of it. It says nothing about the enclosing
/// call failing: Java completes those two futures only from inside <c>thenApply</c>
/// (<c>:40</c>), which does not run when the source future fails — so a failed call would leave
/// all three permanently incomplete. A task that never completes is a hang, not a contract, so
/// when the call itself fails this binding faults all three instead. For per-group errors the
/// behaviour is exactly as Java documents it.
/// </para>
/// <para>
/// <b>Every accessor is a method although each yields a value</b>, matching
/// <see cref="ListTopicsResult.NamesToListings"/> and <see cref="ListConfigResourcesResult.All"/>
/// — each starts work (an <c>await</c> of the shared source) and returns a fresh
/// <see cref="Task{TResult}"/> rather than reading a field, which is not what a property
/// promises.
/// </para>
/// </remarks>
public sealed class ListGroupsResult
{
    private readonly Task<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> _future;

    /// <summary>
    /// Wraps the one awaitable the call completes — Java's package-private
    /// <c>ListGroupsResult(KafkaFuture&lt;Collection&lt;Object&gt;&gt;)</c> (<c>:36</c>).
    /// </summary>
    /// <param name="future">
    /// The task the call completes with the two lists: the groups that were listed, and the
    /// errors that occurred.
    /// </param>
    /// <remarks>
    /// ⚠ <b>Two typed lists, where Java takes one <c>Collection&lt;Object&gt;</c>.</b> Java mixes
    /// listings and <c>Throwable</c>s into a single heterogeneous collection and sorts them apart
    /// at run time with <c>instanceof</c> (<c>:43-49</c>) — a shape C# could only reproduce as
    /// <c>object</c> plus the same unchecked casts, and one the ABI does not force, since the
    /// native result already hands the two over separately. Splitting them in the signature moves
    /// Java's run-time partition to compile time and leaves the three accessors doing nothing but
    /// selecting.
    /// </remarks>
    internal ListGroupsResult(
        Task<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> future)
    {
        _future = future;
    }

    /// <summary>
    /// Every group listing, or the first error that occurred — Java's <c>all()</c> (<c>:69</c>).
    /// </summary>
    /// <returns>
    /// A task yielding all of the listings, or faulting with the <b>first</b> error when any
    /// occurred (<c>:52-53</c>).
    /// </returns>
    /// <remarks>
    /// ⚠ <b>All-or-nothing: one error and no listing is yielded at all</b>, not even the ones
    /// that were fetched successfully, and the later errors are not reported either. Use
    /// <see cref="Valid"/> for the partial results and <see cref="Errors"/> for everything that
    /// went wrong.
    /// </remarks>
    public async Task<IReadOnlyCollection<GroupListing>> All()
    {
        (IReadOnlyCollection<GroupListing> valid, IReadOnlyCollection<KafkaException> errors) =
            await _future.ConfigureAwait(false);

        if (errors.Count > 0)
        {
            // Java hands the exception object itself to completeExceptionally (:53). Capturing
            // and rethrowing preserves that object rather than re-originating it here, so a
            // second All() cannot rewrite the stack trace the first one already reported.
            ExceptionDispatchInfo.Capture(errors.First()).Throw();
        }

        return valid;
    }

    /// <summary>
    /// Just the listings that were fetched, errors ignored — Java's <c>valid()</c> (<c>:82</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the listings that succeeded, empty when none did. It does not fault for a
    /// per-group error.
    /// </returns>
    /// <remarks>
    /// These are Java's "partial results" (<c>:78-80</c>): when this yields a short list, pair it
    /// with <see cref="Errors"/> to find out what is missing — nothing about this collection
    /// alone distinguishes "there are only two groups" from "eight more could not be fetched".
    /// </remarks>
    public async Task<IReadOnlyCollection<GroupListing>> Valid() =>
        (await _future.ConfigureAwait(false)).Valid;

    /// <summary>
    /// Just the errors that occurred — Java's <c>errors()</c> (<c>:95</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the errors, empty when none occurred. It does not fault for a per-group
    /// error — an error is an element here, not a failure.
    /// </returns>
    /// <remarks>
    /// A non-empty result means listings are very likely missing from <see cref="Valid"/>
    /// (<c>:89-90</c>). Only the first of these is ever thrown by <see cref="All"/>, so this is
    /// the only view that reports all of them.
    /// </remarks>
    public async Task<IReadOnlyCollection<KafkaException>> Errors() =>
        (await _future.ConfigureAwait(false)).Errors;
}
