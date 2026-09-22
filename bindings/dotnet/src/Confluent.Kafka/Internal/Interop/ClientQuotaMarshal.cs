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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The filter projector and the readers shared by the client-quota RPCs.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The entity's name is a TERNARY, and this is where it is implemented.</b>
/// <c>get_entry_name</c> returns null both out of range and when the entry names the
/// built-in default entity; inside <c>0..entry_count-1</c> a null therefore means "default
/// entity" — Java's null map value — never a skipped entry and never <c>""</c>
/// (<c>confluent_kafka.h:7622-7628</c>).
/// </para>
/// <para>
/// The string pins are <b>call-scoped</b> (ffi §A4): the ABI copies the filter out during
/// the submit, so <see cref="FilterRows"/> is disposed in the caller's <c>finally</c>. The
/// <c>int[]</c> / <c>IntPtr[]</c> arrays are blittable and pinned by the interop marshaller
/// for the duration of the P/Invoke.
/// </para>
/// </remarks>
internal static class ClientQuotaMarshal
{
    /// <summary>
    /// <c>bool (*)(const *Result_t *, int32_t index, int32_t quotaIndex, double *out)</c> —
    /// the presence-returning out-parameter accessor.
    /// </summary>
    internal delegate bool QuotaValueAccessor(IntPtr result, int index, int quotaIndex, out double value);

    /// <summary>
    /// <c>const char *(*)(const *Result_t *, int32_t index, int32_t quotaIndex)</c>.
    /// </summary>
    internal delegate IntPtr QuotaKeyAccessor(IntPtr result, int index, int quotaIndex);

    /// <summary>
    /// <c>int32_t (*)(const *Result_t *, int32_t index)</c> — the inner axis' own count.
    /// </summary>
    internal delegate int QuotaCountAccessor(IntPtr result, int index);

    /// <summary>
    /// The production <c>kafka_common_ClientQuotaEntity_*</c> set. Shared by every quota
    /// result, so — unlike a result's own <c>get_entity</c> — it is not cross-wirable.
    /// </summary>
    internal static readonly EntityAccessors NativeEntityAccessors = new EntityAccessors(
        NativeMethods.ClientQuotaEntityEntryCount,
        NativeMethods.ClientQuotaEntityGetEntryType,
        NativeMethods.ClientQuotaEntityGetEntryName);

    /// <summary>Projects a filter's components onto the three parallel arrays.</summary>
    /// <param name="filter">The filter to submit; its components keep request order.</param>
    /// <exception cref="ArgumentException">
    /// An <see cref="ClientQuotaMatchType.Exact"/> component carries no match name — the ABI
    /// rejects it (<c>confluent_kafka.h:8213-8214</c>), so it is caught before any pin.
    /// </exception>
    internal static FilterRows Pin(ClientQuotaFilter filter)
    {
        List<ClientQuotaFilterComponent> components =
            new List<ClientQuotaFilterComponent>(filter.Components);

        FilterRows rows = new FilterRows(components.Count, filter.Strict);
        try
        {
            for (int i = 0; i < components.Count; i++)
            {
                ClientQuotaFilterComponent component = components[i];
                if (component.MatchType == ClientQuotaMatchType.Exact && component.MatchName is null)
                {
                    throw new ArgumentException(
                        "An exact-match client-quota filter component must carry a match name.",
                        nameof(filter));
                }

                rows.Set(
                    i,
                    component.EntityType,
                    (int)component.MatchType,
                    // Read only for EXACT; the other two carry no name by construction.
                    component.MatchType == ClientQuotaMatchType.Exact ? component.MatchName : null);
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
    /// Projects a batch of alterations onto the seven <c>alter_client_quotas</c> arrays, four
    /// of which are ragged arrays-of-arrays.
    /// </summary>
    /// <param name="alterations">The alterations to submit, in request order.</param>
    internal static AlterationRows PinAlterations(IReadOnlyList<ClientQuotaAlteration> alterations)
    {
        AlterationRows rows = new AlterationRows(alterations.Count);
        try
        {
            for (int i = 0; i < alterations.Count; i++)
            {
                rows.Set(i, alterations[i]);
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
    /// Builds the per-index entity reader for one quota result, over that result's own
    /// <c>get_entity</c>.
    /// </summary>
    /// <param name="getEntity">
    /// That result's <c>get_entity(i)</c> — the only accessor that differs between the quota
    /// results, and therefore the one a cross-wiring guard reads back off the closure.
    /// </param>
    internal static Func<IntPtr, int, ClientQuotaEntity> EntityReader(
        KeyedResultMarshal.IndexedAccessor getEntity) =>
        (result, index) => ReadEntity(getEntity(result, index));

    /// <summary>
    /// Copies one borrowed <c>kafka_common_ClientQuotaEntity_t</c> out into the managed
    /// value type.
    /// </summary>
    /// <param name="entity">The borrowed entity pointer; it dies with the result root.</param>
    internal static ClientQuotaEntity ReadEntity(IntPtr entity) =>
        ReadEntity(entity, NativeEntityAccessors);

    /// <summary>
    /// <see cref="ReadEntity(IntPtr)"/> over an injected accessor set.
    /// </summary>
    /// <param name="entity">The borrowed entity pointer, or a stand-in under an injected set.</param>
    /// <param name="accessors">The three flat accessors to decode it with.</param>
    /// <remarks>
    /// The set is a parameter because this decode carries the null-means-default rule and the
    /// ABI offers no way to construct an entity handle to assert it on.
    /// </remarks>
    /// <exception cref="KafkaException">
    /// The ABI produced no entity, or no entity type, for an index inside its own count.
    /// </exception>
    internal static ClientQuotaEntity ReadEntity(IntPtr entity, EntityAccessors accessors)
    {
        if (entity == IntPtr.Zero)
        {
            throw new KafkaException(
                "The admin result produced no client-quota entity for an index within its own count.");
        }

        int count = accessors.EntryCount(entity);
        Dictionary<string, string?> entries =
            new Dictionary<string, string?>(count < 0 ? 0 : count, StringComparer.Ordinal);

        for (int index = 0; index < count; index++)
        {
            string type = Utf8Marshal.PtrToString(accessors.GetEntryType(entity, index))
                ?? throw new KafkaException(
                    "The admin result produced no entity type for a client-quota entity entry.");

            // ⚠ Inside the count a null name is Java's null map value — the built-in default
            // entity for the type — never a skip and never "" (h:7622-7628).
            entries.Add(type, Utf8Marshal.PtrToString(accessors.GetEntryName(entity, index)));
        }

        return new ClientQuotaEntity(entries);
    }

    /// <summary>
    /// Builds the per-entity quota-map reader: the whole inner <c>(i, j)</c> axis, walked
    /// inside the reader so the aggregate walker needs no second index.
    /// </summary>
    /// <param name="getQuotaCount">That result's <c>get_quota_count(i)</c>.</param>
    /// <param name="getQuotaKey">That result's <c>get_quota_key(i, j)</c>.</param>
    /// <param name="getQuotaValue">That result's <c>get_quota_value(i, j, out)</c>.</param>
    /// <remarks>
    /// Each ABI accessor is captured as a direct delegate parameter so the reader-wiring
    /// guard can read the symbols back off the closure.
    /// </remarks>
    internal static Func<IntPtr, int, IReadOnlyDictionary<string, double>> QuotaMapReader(
        QuotaCountAccessor getQuotaCount,
        QuotaKeyAccessor getQuotaKey,
        QuotaValueAccessor getQuotaValue) =>
        (result, index) =>
        {
            int count = getQuotaCount(result, index);
            Dictionary<string, double> quotas =
                new Dictionary<string, double>(count < 0 ? 0 : count, StringComparer.Ordinal);

            for (int quotaIndex = 0; quotaIndex < count; quotaIndex++)
            {
                string key = Utf8Marshal.PtrToString(getQuotaKey(result, index, quotaIndex))
                    ?? throw new KafkaException(
                        "The admin result produced no quota key for an index within its own count.");

                // ⚠ Presence is the RETURN VALUE — 0 and every negative are legal values, so
                // no sentinel could carry the distinction (h:7868-7876).
                if (!getQuotaValue(result, index, quotaIndex, out double value))
                {
                    throw new KafkaException(
                        "The admin result produced no quota value for an index within its own count.");
                }

                quotas.Add(key, value);
            }

            return quotas;
        };

    /// <summary>
    /// The three flat <c>kafka_common_ClientQuotaEntity_*</c> accessors, as one set.
    /// </summary>
    internal sealed class EntityAccessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        internal EntityAccessors(
            Func<IntPtr, int> entryCount,
            Func<IntPtr, int, IntPtr> getEntryType,
            Func<IntPtr, int, IntPtr> getEntryName)
        {
            EntryCount = entryCount;
            GetEntryType = getEntryType;
            GetEntryName = getEntryName;
        }

        /// <summary><c>entries().size()</c>.</summary>
        internal Func<IntPtr, int> EntryCount { get; }

        /// <summary><c>entries()</c> key at an index, borrowed.</summary>
        internal Func<IntPtr, int, IntPtr> GetEntryType { get; }

        /// <summary><c>entries()</c> value at an index, borrowed, or null for the default entity.</summary>
        internal Func<IntPtr, int, IntPtr> GetEntryName { get; }
    }

    /// <summary>
    /// One submit's three arrays plus the call-scoped string pins behind two of them.
    /// </summary>
    internal sealed class FilterRows : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pinned;

        internal FilterRows(int count, bool strict)
        {
            Count = count;
            Strict = strict;
            EntityTypes = new IntPtr[count];
            MatchTypes = new int[count];
            MatchNames = new IntPtr[count];

            // One entity type per row, plus a match name on the EXACT rows only.
            _pinned = new List<Utf8Marshal.PinnedUtf8String>(count * 2);
        }

        /// <summary>The row count every array is sized to.</summary>
        internal int Count { get; }

        /// <summary>Java's <c>containsOnly(...)</c> rather than <c>contains(...)</c>.</summary>
        internal bool Strict { get; }

        /// <summary>Column 0 — <c>entityType()</c>, never null.</summary>
        internal IntPtr[] EntityTypes { get; }

        /// <summary>Column 1 — the wire match type: 0 EXACT, 1 DEFAULT, 2 SPECIFIED.</summary>
        internal int[] MatchTypes { get; }

        /// <summary>Column 2 — the match name, read only on an EXACT row.</summary>
        internal IntPtr[] MatchNames { get; }

        /// <summary>Fills row <paramref name="index"/>, pinning its strings.</summary>
        internal void Set(int index, string entityType, int matchType, string? matchName)
        {
            EntityTypes[index] = AclRowMarshal.PinName(entityType, _pinned);
            MatchTypes[index] = matchType;
            MatchNames[index] = AclRowMarshal.PinName(matchName, _pinned);
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

    /// <summary>
    /// One <c>alter_client_quotas</c> submit: seven outer arrays, of which four are
    /// <b>ragged</b> arrays-of-arrays, plus the call-scoped pins behind every inner buffer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Row <c>i</c>'s entity is <c>EntityCounts[i]</c> <c>(type, name)</c> pairs and its ops
    /// are <c>OpCounts[i]</c> triples, so the two levels have <em>independent</em> lengths —
    /// following the <c>listConsumerGroupOffsets</c> precedent (PLAN D32).
    /// </para>
    /// <para>
    /// The op-presence flags are pinned <c>byte[]</c>s of 0/1, never <c>bool[]</c>s: C's
    /// <c>bool</c> is one byte and .NET's default marshaller would widen it.
    /// </para>
    /// </remarks>
    internal sealed class AlterationRows : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pinned;
        private readonly List<GCHandle> _pinnedArrays;

        internal AlterationRows(int count)
        {
            Count = count;
            EntityTypes = new IntPtr[count];
            EntityNames = new IntPtr[count];
            EntityCounts = new int[count];
            OpKeys = new IntPtr[count];
            OpValues = new IntPtr[count];
            OpHasValues = new IntPtr[count];
            OpCounts = new int[count];

            _pinned = new List<Utf8Marshal.PinnedUtf8String>(count * 3);
            _pinnedArrays = new List<GCHandle>(count * 5);
        }

        /// <summary>The alteration count every outer array is sized to.</summary>
        internal int Count { get; }

        /// <summary>Row <c>i</c>'s entity types — <c>char**</c>, <c>EntityCounts[i]</c> long.</summary>
        internal IntPtr[] EntityTypes { get; }

        /// <summary>
        /// Row <c>i</c>'s entity names — <c>char**</c>, with a NULL entry naming the built-in
        /// default entity for its type.
        /// </summary>
        internal IntPtr[] EntityNames { get; }

        /// <summary>How many <c>(type, name)</c> pairs row <c>i</c>'s entity has.</summary>
        internal int[] EntityCounts { get; }

        /// <summary>Row <c>i</c>'s op keys — <c>char**</c>, <c>OpCounts[i]</c> long.</summary>
        internal IntPtr[] OpKeys { get; }

        /// <summary>Row <c>i</c>'s op values — <c>double*</c>, read only where the flag is set.</summary>
        internal IntPtr[] OpValues { get; }

        /// <summary>Row <c>i</c>'s op presence flags — <c>bool*</c>, one byte per op.</summary>
        internal IntPtr[] OpHasValues { get; }

        /// <summary>How many ops row <c>i</c> carries.</summary>
        internal int[] OpCounts { get; }

        /// <summary>Fills row <paramref name="index"/>, pinning both of its inner levels.</summary>
        internal void Set(int index, ClientQuotaAlteration alteration)
        {
            IReadOnlyDictionary<string, string?> entries = alteration.Entity.Entries;
            IntPtr[] types = new IntPtr[entries.Count];
            IntPtr[] names = new IntPtr[entries.Count];
            int entry = 0;
            foreach (KeyValuePair<string, string?> pair in entries)
            {
                types[entry] = AclRowMarshal.PinName(pair.Key, _pinned);

                // ⚠ A null name is Java's null map value — the built-in DEFAULT entity for the
                // type — which is neither omitting the type nor the name "" (h:8287-8290).
                names[entry] = AclRowMarshal.PinName(pair.Value, _pinned);
                entry++;
            }

            EntityCounts[index] = entries.Count;
            EntityTypes[index] = PinArray(types);
            EntityNames[index] = PinArray(names);

            IReadOnlyCollection<ClientQuotaAlteration.Op> ops = alteration.Ops;
            OpCounts[index] = ops.Count;
            if (ops.Count == 0)
            {
                // A NULL inner pointer with a count of 0; the core null-checks before reading.
                OpKeys[index] = IntPtr.Zero;
                OpValues[index] = IntPtr.Zero;
                OpHasValues[index] = IntPtr.Zero;
                return;
            }

            IntPtr[] keys = new IntPtr[ops.Count];
            double[] values = new double[ops.Count];
            byte[] hasValues = new byte[ops.Count];
            int op = 0;
            foreach (ClientQuotaAlteration.Op alterationOp in ops)
            {
                keys[op] = AclRowMarshal.PinName(alterationOp.Key, _pinned);

                // ⚠⚠ THE WRITE PATH'S SILENT-CORRUPTION POINT. A cleared flag is Java's
                // Op(key, null) — REMOVE this quota — while a set flag with value 0 SETS it to
                // 0. Every double, 0 included, is a legal quota value, so no sentinel could
                // carry the distinction (h:8291-8295): the flag is the only channel.
                hasValues[op] = alterationOp.Value.HasValue ? (byte)1 : (byte)0;
                values[op] = alterationOp.Value ?? 0d;
                op++;
            }

            OpKeys[index] = PinArray(keys);
            OpValues[index] = PinArray(values);
            OpHasValues[index] = PinArray(hasValues);
        }

        /// <summary>
        /// Releases every string pin and every inner-array pin — call-scoped, in the submit's
        /// <c>finally</c>.
        /// </summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String name in _pinned)
            {
                name.Dispose();
            }

            _pinned.Clear();

            foreach (GCHandle pin in _pinnedArrays)
            {
                pin.Free();
            }

            _pinnedArrays.Clear();
        }

        private IntPtr PinArray(Array inner)
        {
            GCHandle pin = GCHandle.Alloc(inner, GCHandleType.Pinned);
            _pinnedArrays.Add(pin);
            return pin.AddrOfPinnedObject();
        }
    }
}
