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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The seven-array row projector shared by the ACL RPCs, plus the reader that restores
/// Java's nesting from the ABI's flat <c>kafka_common_AclBinding_t</c>.
/// </summary>
/// <remarks>
/// <para>
/// <c>create_acls</c> and <c>delete_acls</c> have byte-identical signatures — seven
/// parallel arrays, row <c>i</c> describing one binding or filter — so one projection rule
/// serves both (PLAN §4.1).
/// </para>
/// <para>
/// ⚠⚠ <b><see langword="null"/> and <c>""</c> are DIFFERENT wire values, and this is the
/// only place that rule is implemented.</b> On a <em>filter</em> row a null name means
/// "match any resource name" and travels as <see cref="IntPtr.Zero"/>, while <c>""</c> is
/// a filter on the literal empty name and must travel as a <b>non-null</b> pointer to a
/// NUL byte (<c>confluent_kafka.h:7528-7530</c>). Collapsing the two turns a filter on
/// <c>""</c> into match-everything, which on <c>delete_acls</c> deletes ACLs the caller
/// never asked to delete. Concrete rows never carry a null (the value types reject one at
/// construction), so both paths go through <see cref="PinName"/> and cannot diverge.
/// </para>
/// <para>
/// The string pins are <b>call-scoped</b> (ffi §A4): the ABI copies every row out during
/// the submit, so <see cref="Rows"/> is disposed in the caller's <c>finally</c> and no pin
/// outlives the call. The <c>int[]</c> / <c>IntPtr[]</c> arrays are blittable and pinned
/// by the interop marshaller for the duration of the P/Invoke, so they need no pin here.
/// </para>
/// </remarks>
internal static class AclRowMarshal
{
    /// <summary>
    /// The production <c>kafka_common_AclBindingFilter_*</c> set. Shared by every ACL result,
    /// so — unlike a result's own <c>get_filter</c> — it is not cross-wirable.
    /// </summary>
    internal static readonly FilterAccessors NativeFilterAccessors = new FilterAccessors(
        NativeMethods.AclBindingFilterResourceType,
        NativeMethods.AclBindingFilterResourceName,
        NativeMethods.AclBindingFilterPatternType,
        NativeMethods.AclBindingFilterPrincipal,
        NativeMethods.AclBindingFilterHost,
        NativeMethods.AclBindingFilterOperation,
        NativeMethods.AclBindingFilterPermissionType);

    /// <summary>
    /// Pins one row string under the null-versus-empty rule: <see langword="null"/> stays a
    /// null pointer, and <c>""</c> becomes a pointer to a NUL byte.
    /// </summary>
    /// <param name="value">The name, principal or host — null only on a filter row.</param>
    /// <param name="pinned">The call-scoped pin list the caller releases.</param>
    /// <returns>The pointer for this row's slot.</returns>
    internal static IntPtr PinName(string? value, List<Utf8Marshal.PinnedUtf8String> pinned)
    {
        if (value is null)
        {
            return IntPtr.Zero;
        }

        // Utf8Marshal.Pin("") pins a one-byte array holding the terminator, so its address
        // is non-null — the distinction the remarks above depend on.
        Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(value);
        pinned.Add(name);
        return name.Pointer;
    }

    /// <summary>Projects concrete bindings onto the seven arrays.</summary>
    /// <param name="bindings">The de-duplicated request keys, in request order.</param>
    internal static Rows Pin(IReadOnlyList<AclBinding> bindings)
    {
        Rows rows = new Rows(bindings.Count);
        try
        {
            for (int i = 0; i < bindings.Count; i++)
            {
                ResourcePattern pattern = bindings[i].Pattern;
                AccessControlEntry entry = bindings[i].Entry;
                rows.Set(
                    i,
                    (int)pattern.ResourceType,
                    pattern.Name,
                    (int)pattern.PatternType,
                    entry.Principal,
                    entry.Host,
                    (int)entry.Operation,
                    (int)entry.PermissionType);
            }

            return rows;
        }
        catch
        {
            rows.Dispose();
            throw;
        }
    }

    /// <summary>Projects filters onto the same seven arrays.</summary>
    /// <param name="filters">The de-duplicated request keys, in request order.</param>
    /// <remarks>
    /// Identical to the concrete overload except that the three strings are nullable — see
    /// the null-versus-empty rule in the type remarks.
    /// </remarks>
    internal static Rows Pin(IReadOnlyList<AclBindingFilter> filters)
    {
        Rows rows = new Rows(filters.Count);
        try
        {
            for (int i = 0; i < filters.Count; i++)
            {
                ResourcePatternFilter pattern = filters[i].PatternFilter;
                AccessControlEntryFilter entry = filters[i].EntryFilter;
                rows.Set(
                    i,
                    (int)pattern.ResourceType,
                    pattern.Name,
                    (int)pattern.PatternType,
                    entry.Principal,
                    entry.Host,
                    (int)entry.Operation,
                    (int)entry.PermissionType);
            }

            return rows;
        }
        catch
        {
            rows.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Builds the per-index key reader for one ACL result type, over that result's own
    /// <c>get_binding</c>.
    /// </summary>
    /// <param name="getBinding">
    /// That result's <c>get_binding(i)</c> — the only accessor that differs between the ACL
    /// results, and therefore the one a cross-wiring guard reads back off the closure.
    /// </param>
    internal static Func<IntPtr, int, AclBinding> BindingReader(
        KeyedResultMarshal.IndexedAccessor getBinding) =>
        (result, index) => ReadBinding(getBinding(result, index));

    /// <summary>
    /// Copies one borrowed <c>kafka_common_AclBinding_t</c> out into the nested managed
    /// shape, reassembling <see cref="ResourcePattern"/> from accessors 1-3 and
    /// <see cref="AccessControlEntry"/> from accessors 4-7 (PLAN D36).
    /// </summary>
    /// <param name="binding">The borrowed binding pointer; it dies with the result root.</param>
    /// <exception cref="KafkaException">
    /// The ABI produced no binding, or no string, for an index inside its own count.
    /// </exception>
    internal static AclBinding ReadBinding(IntPtr binding)
    {
        if (binding == IntPtr.Zero)
        {
            throw new KafkaException(
                "The admin result produced no ACL binding for an index within its own count.");
        }

        ResourcePattern pattern = new ResourcePattern(
            (ResourceType)NativeMethods.AclBindingResourceType(binding),
            ReadRequired(NativeMethods.AclBindingResourceName(binding), "resource name"),
            (PatternType)NativeMethods.AclBindingPatternType(binding));

        AccessControlEntry entry = new AccessControlEntry(
            ReadRequired(NativeMethods.AclBindingPrincipal(binding), "principal"),
            ReadRequired(NativeMethods.AclBindingHost(binding), "host"),
            (AclOperation)NativeMethods.AclBindingOperation(binding),
            (AclPermissionType)NativeMethods.AclBindingPermissionType(binding));

        return new AclBinding(pattern, entry);
    }

    /// <summary>
    /// Builds the per-index key reader for one ACL result type keyed by a <b>filter</b>, over
    /// that result's own <c>get_filter</c>.
    /// </summary>
    /// <param name="getFilter">That result's <c>get_filter(i)</c>.</param>
    internal static Func<IntPtr, int, AclBindingFilter> FilterReader(
        KeyedResultMarshal.IndexedAccessor getFilter) =>
        (result, index) => ReadFilter(getFilter(result, index));

    /// <summary>
    /// Copies one borrowed <c>kafka_common_AclBindingFilter_t</c> out into the nested managed
    /// shape, reassembling <see cref="ResourcePatternFilter"/> from accessors 1-3 and
    /// <see cref="AccessControlEntryFilter"/> from accessors 4-7 (PLAN D36).
    /// </summary>
    /// <param name="filter">The borrowed filter pointer; it dies with the result root.</param>
    /// <exception cref="KafkaException">
    /// The ABI produced no filter for an index inside its own count.
    /// </exception>
    /// <remarks>
    /// ⚠ The three strings are read <b>without</b> a null guard, unlike
    /// <see cref="ReadBinding"/>'s: on a filter a null pointer means "match any" and must
    /// come back as <see langword="null"/>, distinct from a pointer to the empty string
    /// (<c>confluent_kafka.h:7528-7530</c>).
    /// </remarks>
    internal static AclBindingFilter ReadFilter(IntPtr filter) =>
        ReadFilter(filter, NativeFilterAccessors);

    /// <summary>
    /// <see cref="ReadFilter(IntPtr)"/> over an injected accessor set.
    /// </summary>
    /// <param name="filter">The borrowed filter pointer, or a stand-in under an injected set.</param>
    /// <param name="accessors">The seven flat accessors to decode it with.</param>
    /// <remarks>
    /// The set is a parameter, unlike <see cref="ReadBinding"/>'s hard-wired one, because this
    /// decode carries a rule the binding decode does not — a null string is <c>null</c>, not
    /// <c>""</c> — and the ABI offers no way to construct a filter handle to assert it on.
    /// </remarks>
    internal static AclBindingFilter ReadFilter(IntPtr filter, FilterAccessors accessors)
    {
        if (filter == IntPtr.Zero)
        {
            throw new KafkaException(
                "The admin result produced no ACL filter for an index within its own count.");
        }

        ResourcePatternFilter pattern = new ResourcePatternFilter(
            (ResourceType)accessors.ResourceType(filter),
            Utf8Marshal.PtrToString(accessors.ResourceName(filter)),
            (PatternType)accessors.PatternType(filter));

        AccessControlEntryFilter entry = new AccessControlEntryFilter(
            Utf8Marshal.PtrToString(accessors.Principal(filter)),
            Utf8Marshal.PtrToString(accessors.Host(filter)),
            (AclOperation)accessors.Operation(filter),
            (AclPermissionType)accessors.PermissionType(filter));

        return new AclBindingFilter(pattern, entry);
    }

    /// <summary>
    /// Copies a borrowed, NUL-terminated string the header documents as "never null on a
    /// binding", rejecting a null rather than substituting one.
    /// </summary>
    private static string ReadRequired(IntPtr value, string field) =>
        Utf8Marshal.PtrToString(value)
        ?? throw new KafkaException($"The admin result produced no {field} for an ACL binding.");

    /// <summary>
    /// The seven flat <c>kafka_common_AclBindingFilter_*</c> accessors, as one set.
    /// </summary>
    internal sealed class FilterAccessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        internal FilterAccessors(
            Func<IntPtr, int> resourceType,
            Func<IntPtr, IntPtr> resourceName,
            Func<IntPtr, int> patternType,
            Func<IntPtr, IntPtr> principal,
            Func<IntPtr, IntPtr> host,
            Func<IntPtr, int> operation,
            Func<IntPtr, int> permissionType)
        {
            ResourceType = resourceType;
            ResourceName = resourceName;
            PatternType = patternType;
            Principal = principal;
            Host = host;
            Operation = operation;
            PermissionType = permissionType;
        }

        /// <summary><c>patternFilter().resourceType().code()</c>.</summary>
        internal Func<IntPtr, int> ResourceType { get; }

        /// <summary><c>patternFilter().name()</c>, borrowed, or null for "match any".</summary>
        internal Func<IntPtr, IntPtr> ResourceName { get; }

        /// <summary><c>patternFilter().patternType().code()</c>.</summary>
        internal Func<IntPtr, int> PatternType { get; }

        /// <summary><c>entryFilter().principal()</c>, borrowed, or null for "match any".</summary>
        internal Func<IntPtr, IntPtr> Principal { get; }

        /// <summary><c>entryFilter().host()</c>, borrowed, or null for "match any".</summary>
        internal Func<IntPtr, IntPtr> Host { get; }

        /// <summary><c>entryFilter().operation().code()</c>.</summary>
        internal Func<IntPtr, int> Operation { get; }

        /// <summary><c>entryFilter().permissionType().code()</c>.</summary>
        internal Func<IntPtr, int> PermissionType { get; }
    }

    /// <summary>
    /// One submit's seven arrays plus the call-scoped string pins behind three of them.
    /// </summary>
    internal sealed class Rows : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pinned;

        internal Rows(int count)
        {
            Count = count;
            ResourceTypes = new int[count];
            ResourceNames = new IntPtr[count];
            PatternTypes = new int[count];
            Principals = new IntPtr[count];
            Hosts = new IntPtr[count];
            Operations = new int[count];
            PermissionTypes = new int[count];

            // Three strings per row, and a filter row may contribute fewer.
            _pinned = new List<Utf8Marshal.PinnedUtf8String>(count * 3);
        }

        /// <summary>The row count every array is sized to.</summary>
        internal int Count { get; }

        /// <summary>Column 0 — <c>pattern().resourceType().code()</c>.</summary>
        internal int[] ResourceTypes { get; }

        /// <summary>Column 1 — <c>pattern().name()</c>, or null on a wildcard filter.</summary>
        internal IntPtr[] ResourceNames { get; }

        /// <summary>Column 2 — <c>pattern().patternType().code()</c>.</summary>
        internal int[] PatternTypes { get; }

        /// <summary>Column 3 — <c>entry().principal()</c>, or null on a wildcard filter.</summary>
        internal IntPtr[] Principals { get; }

        /// <summary>Column 4 — <c>entry().host()</c>, or null on a wildcard filter.</summary>
        internal IntPtr[] Hosts { get; }

        /// <summary>Column 5 — <c>entry().operation().code()</c>.</summary>
        internal int[] Operations { get; }

        /// <summary>Column 6 — <c>entry().permissionType().code()</c>.</summary>
        internal int[] PermissionTypes { get; }

        /// <summary>Fills row <paramref name="index"/>, pinning its three strings.</summary>
        internal void Set(
            int index,
            int resourceType,
            string? resourceName,
            int patternType,
            string? principal,
            string? host,
            int operation,
            int permissionType)
        {
            ResourceTypes[index] = resourceType;
            ResourceNames[index] = PinName(resourceName, _pinned);
            PatternTypes[index] = patternType;
            Principals[index] = PinName(principal, _pinned);
            Hosts[index] = PinName(host, _pinned);
            Operations[index] = operation;
            PermissionTypes[index] = permissionType;
        }

        /// <summary>Releases every string pin — call-scoped, in the submit's <c>finally</c>.</summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String name in _pinned)
            {
                name.Dispose();
            }

            _pinned.Clear();
        }
    }
}
