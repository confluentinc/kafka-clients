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
/// The handle of an admin <c>*Result_t</c>, held by its managed Result for that Result's life and by each future the
/// Result owns until that future's delivery; the last release destroys it, from a finalizer too, since a Result's
/// destroy runs no callbacks and blocks nothing.
/// </summary>
internal sealed class SafeAdminResultHandle : SafeHandleZeroIsInvalid
{
    private readonly Destroy _destroy;

    internal SafeAdminResultHandle(IntPtr result, Destroy destroy)
    {
        _destroy = destroy;
        SetHandle(result);
    }

    internal delegate void Destroy(IntPtr result, ref IntPtr outErr);

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        _destroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
