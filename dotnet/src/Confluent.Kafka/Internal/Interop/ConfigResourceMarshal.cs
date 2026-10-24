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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds a <c>kafka_common_config_ConfigResource_t</c> from a <see cref="ConfigResource"/> and copies one
/// out, mapping the type through Java's <c>ConfigResource.Type</c> id (<c>forId</c> degrades an id this
/// client does not know to <see cref="ConfigResourceType.Unknown"/>).
/// </summary>
internal static class ConfigResourceMarshal
{
    internal static ConfigResourceType TypeFromId(int id) =>
        Enum.IsDefined(typeof(ConfigResourceType), id) ? (ConfigResourceType)id : ConfigResourceType.Unknown;

    internal static IntPtr NativeType(ConfigResourceType type)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.ConfigResourceTypeForId((sbyte)TypeFromId((int)type), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static IntPtr Build(ConfigResource resource)
    {
        IntPtr type = NativeType(resource.Type);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(resource.Name))
        {
            native = NativeMethods.ConfigResourceNew(type, name.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static ConfigResource CopyOut(IntPtr resource)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr type = NativeMethods.ConfigResourceGetType(resource, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte id = NativeMethods.ConfigResourceTypeId(type, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr name = NativeMethods.ConfigResourceName(resource, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ConfigResource(TypeFromId(id), Utf8Marshal.PtrToString(name) ?? string.Empty);
    }

    internal static void Destroy(IntPtr resource)
    {
        if (resource == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConfigResourceDestroy(resource, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
