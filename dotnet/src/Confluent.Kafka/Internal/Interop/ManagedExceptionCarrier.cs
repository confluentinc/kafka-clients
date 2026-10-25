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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Carries a managed exception across the core as a <c>local_callback</c> error whose opaque pointer is a
/// <see cref="GCHandle"/> to it, freed once by the core through the rooted release callback.
/// </summary>
internal static class ManagedExceptionCarrier
{
    /// <summary>The release callback through which the core frees the carried <see cref="GCHandle"/>.</summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ReleaseCallback(IntPtr opaque);

    private const string UnreadableMessage = "callback raised an exception";

    private static readonly ReleaseCallback s_release = Release;

    internal static IntPtr ToError(Exception exception)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(MessageOf(exception));
        GCHandle handle = GCHandle.Alloc(exception);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorLocalCallback(pinned.Pointer, GCHandle.ToIntPtr(handle), s_release, ref outErr);

        // On a caught panic the handle is leaked, as Python's binding leaks its reference: whether the core
        // already released it is unknown, and a second Free would be worse than a leak.
        return error != IntPtr.Zero ? error : outErr;
    }

    internal static Exception? Carried(IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr view = NativeMethods.ErrorLocalCallbackError(error, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (view == IntPtr.Zero)
        {
            return null;
        }

        IntPtr opaque = NativeMethods.LocalCallbackErrorOpaque(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return opaque == IntPtr.Zero ? null : (Exception?)GCHandle.FromIntPtr(opaque).Target;
    }

    private static string MessageOf(Exception exception)
    {
        string? message;
        try
        {
            message = exception.Message;
        }
        catch (Exception)
        {
            return UnreadableMessage;
        }

        return string.IsNullOrEmpty(message) ? exception.GetType().FullName! : message;
    }

    private static void Release(IntPtr opaque)
    {
        try
        {
            GCHandle.FromIntPtr(opaque).Free();
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }
}
