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
/// (header, <c>kafka_admin_DeleteAclsResult_get_error</c>,
/// <c>kafka_admin_DeleteAclsResult_get_result_error</c>). Both are <c>const</c>, so both are read
/// with <see cref="KafkaException.FromBorrowedHandle"/> and neither is destroyed: const-ness
/// answers ownership, and the Java return type answers fault-versus-value.
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
                values.Add(
                    ReadEntry(
                        getBinding(result, index, resultIndex),
                        getResultError(result, index, resultIndex),
                        readBinding));
            }

            return new DeleteAclsResult.FilterResults(values);
        };

    /// <summary>
    /// The per-key twin of <see cref="FilterResultsReader"/>, over the OWNED
    /// <c>kafka_admin_DeleteAclsFilterResults_t</c> a per-key callback is handed: one index
    /// axis instead of two, and the accessors take the value handle itself.
    /// </summary>
    /// <param name="getCount">That value's <c>count()</c>.</param>
    /// <param name="getBinding">That value's <c>get_binding(j)</c>.</param>
    /// <param name="getError">That value's <c>get_error(j)</c>.</param>
    /// <param name="readBinding">
    /// The borrowed-binding copy-out — <see cref="AclRowMarshal.ReadBinding"/> in production.
    /// </param>
    /// <remarks>
    /// ⚠ The inner error stays <b>borrowed</b> (<c>const</c>, dies with the value handle) and
    /// stays a stored <em>value</em> rather than a fault: only the callback's own top-level
    /// error became owned under the per-key ABI.
    /// </remarks>
    internal static Func<IntPtr, DeleteAclsResult.FilterResults> FilterResultsPerKeyReader(
        KeyedResultMarshal.CountAccessor getCount,
        KeyedResultMarshal.IndexedAccessor getBinding,
        KeyedResultMarshal.IndexedAccessor getError,
        Func<IntPtr, AclBinding> readBinding) =>
        value =>
        {
            int count = getCount(value);
            List<DeleteAclsResult.FilterResult> values =
                new List<DeleteAclsResult.FilterResult>(count < 0 ? 0 : count);

            for (int index = 0; index < count; index++)
            {
                values.Add(ReadEntry(getBinding(value, index), getError(value, index), readBinding));
            }

            return new DeleteAclsResult.FilterResults(values);
        };

    /// <summary>
    /// One inner entry, from its two <b>independent</b> accessors — shared by both readers so
    /// the rule below cannot diverge between the aggregate and the per-key walk.
    /// </summary>
    /// <param name="binding">The entry's borrowed binding pointer, or null.</param>
    /// <param name="error">The entry's borrowed error handle, or null.</param>
    /// <param name="readBinding">The borrowed-binding copy-out, handed only a non-null pointer.</param>
    /// <remarks>
    /// <para>
    /// ⚠⚠ <b>The binding is read whether or not the entry carries an error.</b> A matched ACL
    /// whose delete failed has both: Java builds every entry as
    /// <c>new FilterResult(aclBinding, aclError.exception(...))</c>
    /// (<c>KafkaAdminClient.java:2705-2708</c>) and the core stores both the same way, so
    /// skipping the binding once an error is seen would drop the ACL the failure is about
    /// (M15/P13.2 G4-1). The header says the same
    /// (<c>kafka_admin_DeleteAclsResult_get_binding</c>: the binding and the result error are
    /// not exclusive, as in Java's <c>FilterResult</c>).
    /// </para>
    /// <para>
    /// An entry with <b>neither</b> is a malformed row and is rejected with the same text
    /// <see cref="AclRowMarshal.ReadBinding"/> uses, rather than turned into an entry that
    /// reports an unnamed ACL as deleted. <paramref name="readBinding"/> is never handed the
    /// null pointer.
    /// </para>
    /// </remarks>
    private static DeleteAclsResult.FilterResult ReadEntry(
        IntPtr binding, IntPtr error, Func<IntPtr, AclBinding> readBinding)
    {
        if (binding == IntPtr.Zero && error == IntPtr.Zero)
        {
            throw new KafkaException(AclRowMarshal.NoBindingWithinCountMessage);
        }

        // ⚠ BORROWED, and a VALUE — not this filter's fault. FromBorrowedHandle copies
        // code/message/flags out and retains no pointer, so the stored exception outliving
        // the walk is correct; the handle dies with its root.
        KafkaException? failure = KafkaException.FromBorrowedHandle(error);
        AclBinding? matched = binding == IntPtr.Zero ? null : readBinding(binding);
        return new DeleteAclsResult.FilterResult(matched, failure);
    }
}
