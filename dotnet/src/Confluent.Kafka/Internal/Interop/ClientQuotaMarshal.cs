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
/// Builds the owned <c>kafka_common_quota_ClientQuotaFilter_t</c>, <c>ClientQuotaAlteration_t</c> and
/// <c>ClientQuotaEntity_t</c> handles the client-quota RPCs take, and copies the entities and quota maps their
/// futures deliver into managed values.
/// </summary>
internal static class ClientQuotaMarshal
{
    private delegate void NativeDestroy(IntPtr native, ref IntPtr outErr);

    internal static IntPtr NewFilter(ClientQuotaFilter filter)
    {
        List<IntPtr> components = new List<IntPtr>(filter.Components.Count);
        IntPtr list = IntPtr.Zero;
        try
        {
            foreach (ClientQuotaFilterComponent component in filter.Components)
            {
                components.Add(NewComponent(component));
            }

            list = NewList(components);
            IntPtr outErr = IntPtr.Zero;
            IntPtr native = filter.Strict
                ? NativeMethods.ClientQuotaFilterContainsOnly(list, ref outErr)
                : NativeMethods.ClientQuotaFilterContains(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            Destroy(list, NativeMethods.ListDestroy);
            foreach (IntPtr component in components)
            {
                Destroy(component, NativeMethods.ClientQuotaFilterComponentDestroy);
            }
        }
    }

    internal static void DestroyFilter(IntPtr filter) => Destroy(filter, NativeMethods.ClientQuotaFilterDestroy);

    internal static IntPtr NewAlteration(ClientQuotaAlteration alteration)
    {
        IntPtr entity = NewEntity(alteration.Entity);
        try
        {
            List<IntPtr> ops = new List<IntPtr>(alteration.Ops.Count);
            IntPtr list = IntPtr.Zero;
            try
            {
                foreach (ClientQuotaAlteration.Op op in alteration.Ops)
                {
                    ops.Add(NewOp(op));
                }

                list = NewList(ops);
                IntPtr outErr = IntPtr.Zero;
                IntPtr native = NativeMethods.ClientQuotaAlterationNew(entity, list, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return native;
            }
            finally
            {
                Destroy(list, NativeMethods.ListDestroy);
                foreach (IntPtr op in ops)
                {
                    Destroy(op, NativeMethods.ClientQuotaAlterationOpDestroy);
                }
            }
        }
        finally
        {
            Destroy(entity, NativeMethods.ClientQuotaEntityDestroy);
        }
    }

    internal static void DestroyAlteration(IntPtr alteration) =>
        Destroy(alteration, NativeMethods.ClientQuotaAlterationDestroy);

    internal static IntPtr NewEntity(ClientQuotaEntity entity)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(entity.Entries.Count * 2);
        IntPtr map = IntPtr.Zero;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string?> entry in entity.Entries)
            {
                Utf8Marshal.PinnedUtf8String type = Utf8Marshal.Pin(entry.Key);
                pins.Add(type);
                IntPtr name = IntPtr.Zero;
                if (entry.Value is not null)
                {
                    Utf8Marshal.PinnedUtf8String pinnedName = Utf8Marshal.Pin(entry.Value);
                    pins.Add(pinnedName);
                    name = pinnedName.Pointer;
                }

                NativeMethods.MapPut(map, type.Pointer, name, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            IntPtr native = NativeMethods.ClientQuotaEntityNew(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            Destroy(map, NativeMethods.MapDestroy);
            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }
        }
    }

    internal static void DestroyEntity(IntPtr entity) => Destroy(entity, NativeMethods.ClientQuotaEntityDestroy);

    internal static ClientQuotaEntity CopyOutEntity(IntPtr entity)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr entries = NativeMethods.ClientQuotaEntityEntries(entity, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.MapSize(entries, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Dictionary<string, string?> copied = new Dictionary<string, string?>(count, StringComparer.Ordinal);
            for (int i = 0; i < count; i++)
            {
                IntPtr type = NativeMethods.MapKey(entries, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr name = NativeMethods.MapValue(entries, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);

                // A NULL name is Java's null map value: the built-in default entity for the type.
                copied.Add(Utf8Marshal.PtrToString(type) ?? string.Empty, Utf8Marshal.PtrToString(name));
            }

            return new ClientQuotaEntity(copied);
        }
        finally
        {
            Destroy(entries, NativeMethods.MapDestroy);
        }
    }

    internal static IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> CopyOutEntities(
        IntPtr entities)
    {
        Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> copied =
            new Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>();
        if (entities == IntPtr.Zero)
        {
            return copied;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(entities, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        for (int i = 0; i < count; i++)
        {
            IntPtr entity = NativeMethods.MapKey(entities, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr quotas = NativeMethods.MapValue(entities, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[CopyOutEntity(entity)] = CopyOutQuotas(quotas);
        }

        return copied;
    }

    private static IReadOnlyDictionary<string, double> CopyOutQuotas(IntPtr quotas)
    {
        Dictionary<string, double> copied = new Dictionary<string, double>(StringComparer.Ordinal);
        if (quotas == IntPtr.Zero)
        {
            return copied;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(quotas, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        for (int i = 0; i < count; i++)
        {
            IntPtr name = NativeMethods.MapKey(quotas, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(quotas, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[Utf8Marshal.PtrToString(name) ?? string.Empty] =
                BitConverter.Int64BitsToDouble(Marshal.ReadInt64(value));
        }

        return copied;
    }

    private static IntPtr NewComponent(ClientQuotaFilterComponent component)
    {
        using Utf8Marshal.PinnedUtf8String entityType = Utf8Marshal.Pin(component.EntityType);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        switch (component.MatchType)
        {
            case ClientQuotaMatchType.Exact:
                using (Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(component.MatchName ?? string.Empty))
                {
                    native = NativeMethods.ClientQuotaFilterComponentOfEntity(entityType.Pointer, name.Pointer, ref outErr);
                }

                break;
            case ClientQuotaMatchType.Default:
                native = NativeMethods.ClientQuotaFilterComponentOfDefaultEntity(entityType.Pointer, ref outErr);
                break;
            case ClientQuotaMatchType.Specified:
                native = NativeMethods.ClientQuotaFilterComponentOfEntityType(entityType.Pointer, ref outErr);
                break;
            default:
                throw new InvalidOperationException("Unknown client quota match type " + component.MatchType + ".");
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr NewOp(ClientQuotaAlteration.Op op)
    {
        using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(op.Key);
        IntPtr outErr = IntPtr.Zero;

        // A null value removes the quota; the C ABI spells Java's null as NaN.
        IntPtr native = NativeMethods.ClientQuotaAlterationOpNew(key.Pointer, op.Value ?? double.NaN, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr NewList(List<IntPtr> elements)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ListNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            foreach (IntPtr element in elements)
            {
                NativeMethods.ListAdd(list, element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
        }
        catch
        {
            Destroy(list, NativeMethods.ListDestroy);
            throw;
        }

        return list;
    }

    private static void Destroy(IntPtr native, NativeDestroy destroy)
    {
        if (native == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        destroy(native, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
