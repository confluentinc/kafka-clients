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
/// Builds an RPC's C options: <c>&lt;X&gt;Options_new</c>, which carries the core's defaults, then one
/// <c>set_*</c> call for each setter that ran on the managed options, in the order they last ran.
/// </summary>
internal static class AdminOptionsMarshal
{
    internal delegate IntPtr New(ref IntPtr outErr);

    internal delegate void Set<TOptions>(IntPtr native, TOptions options, string setter, ref IntPtr outErr);

    internal delegate void Destroy(IntPtr native, ref IntPtr outErr);

    internal static IntPtr Build<TOptions>(
        New create, TOptions? options, OptionSetters? setters, Set<TOptions> set, Destroy destroy)
        where TOptions : class
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = create(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (options is null || setters is null)
        {
            return native;
        }

        try
        {
            foreach (string setter in setters.InOrder)
            {
                set(native, options, setter, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
        }
        catch
        {
            Free(native, destroy);
            throw;
        }

        return native;
    }

    internal static void Free(IntPtr native, Destroy destroy)
    {
        IntPtr outErr = IntPtr.Zero;
        destroy(native, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
