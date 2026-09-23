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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The inner <c>(filter, result)</c> walk behind <c>deleteAcls</c>' per-filter value.
/// </summary>
/// <remarks>
/// <para>
/// <c>deleteAcls</c> is result shape 1 whose per-key value is itself an indexed list, so the
/// second axis lives entirely inside a <c>readValue</c> reader:
/// <c>KeyedResultMarshal</c>'s readers are typed <c>(IntPtr result, int index) -&gt; TValue</c>
/// and the walker is deliberately agnostic to what a reader does with the root. No walker
/// callable is added or changed (PLAN §1.2).
/// </para>
/// <para>
/// ⚠⚠ <b>The two error accessors are independent, and only <c>get_error</c> is a fault.</b>
/// The outer <c>get_error(i)</c> is the filter's future failing and faults that filter's
/// <c>Task</c> — it is read by <see cref="KeyedResultMarshal"/>, not here. The inner
/// <c>get_result_error(i, j)</c> is Java's <c>FilterResult.error()</c> and is a stored
/// <b>value</b> inside a successfully completed <c>FilterResults</c>
/// (<c>confluent_kafka.h:7744-7747</c>, <c>:7793-7796</c>). Both are <c>const</c>, so both
/// are read with <see cref="KafkaException.FromBorrowedHandle"/> and neither is destroyed:
/// const-ness answers ownership, and the Java return type answers fault-versus-value.
/// </para>
/// </remarks>
internal static class DeleteAclsResultMarshal
{
    /// <summary><c>int32_t (*)(const *Result_t *, int32_t index)</c>.</summary>
    internal delegate int IndexedCountAccessor(IntPtr result, int index);

    /// <summary>
    /// <c>const T *(*)(const *Result_t *, int32_t index, int32_t resultIndex)</c> — the
    /// two-index accessor shape the inner axis needs.
    /// </summary>
    internal delegate IntPtr NestedAccessor(IntPtr result, int index, int resultIndex);

    /// <summary>
    /// Builds the per-filter value reader over one result type's own inner accessors.
    /// </summary>
    /// <param name="getResultCount">That result's <c>get_result_count(i)</c>.</param>
    /// <param name="getBinding">That result's <c>get_binding(i, j)</c>.</param>
    /// <param name="getResultError">That result's <c>get_result_error(i, j)</c>.</param>
    /// <param name="readBinding">
    /// The borrowed-binding copy-out — <see cref="AclRowMarshal.ReadBinding"/> in production.
    /// A parameter because the ABI offers no way to construct a binding handle, so the walk
    /// is otherwise unreachable for anything but an all-errors result.
    /// </param>
    /// <remarks>
    /// Each ABI accessor is captured as a direct delegate parameter so the reader-wiring guard
    /// can read the symbols back off the closure.
    /// </remarks>
    internal static Func<IntPtr, int, DeleteAclsResult.FilterResults> FilterResultsReader(
        IndexedCountAccessor getResultCount,
        NestedAccessor getBinding,
        NestedAccessor getResultError,
        Func<IntPtr, AclBinding> readBinding) =>
        (result, index) =>
        {
            int count = getResultCount(result, index);
            List<DeleteAclsResult.FilterResult> values =
                new List<DeleteAclsResult.FilterResult>(count < 0 ? 0 : count);

            for (int resultIndex = 0; resultIndex < count; resultIndex++)
            {
                IntPtr error = getResultError(result, index, resultIndex);
                if (error != IntPtr.Zero)
                {
                    // ⚠ BORROWED, and a VALUE — not this filter's fault. FromBorrowedHandle
                    // copies code/message/flags out and retains no pointer, so the stored
                    // exception outliving the walk is correct; the handle dies with the root.
                    values.Add(
                        new DeleteAclsResult.FilterResult(KafkaException.FromBorrowedHandle(error)!));
                    continue;
                }

                // Complementary to the error above: for an in-range entry precisely one of
                // the two is non-null, so a null here with a null error is a malformed row
                // and the copy-out rejects it rather than inventing a binding.
                values.Add(
                    new DeleteAclsResult.FilterResult(
                        readBinding(getBinding(result, index, resultIndex))));
            }

            return new DeleteAclsResult.FilterResults(values);
        };
}
