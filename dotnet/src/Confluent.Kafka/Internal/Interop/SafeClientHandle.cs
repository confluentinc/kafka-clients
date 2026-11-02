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
using System.Diagnostics;
using System.Threading;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// A client's owned handle: <c>handle</c> is the view every operation takes, and the class pointer is kept beside it
/// for the class-only calls, which take a reference on the handle through <see cref="DangerousAddRefClass"/>, and for
/// the destroy. The destroy runs only after <see cref="CloseExplicitly"/>; a handle released any other way, by
/// finalization included, leaks the client and writes one warning, as Java and Python leak a client never closed.
/// </summary>
internal abstract class SafeClientHandle : SafeHandleZeroIsInvalid
{
    private readonly IntPtr _classPointer;

    private int _explicitTeardown;

    protected SafeClientHandle(IntPtr view, IntPtr classPointer)
    {
        _classPointer = classPointer;
        SetHandle(view);
    }

    // Exists because the class-only calls need the class pointer back: DangerousAddRef returns nothing, and
    // DangerousGetHandle returns the view.
    internal IntPtr DangerousAddRefClass(ref bool success)
    {
        DangerousAddRef(ref success);
        return _classPointer;
    }

    internal void CloseExplicitly()
    {
        Interlocked.Exchange(ref _explicitTeardown, 1);
        Dispose();
    }

    protected abstract void Destroy(IntPtr classPointer);

    protected sealed override bool ReleaseHandle()
    {
        if (Volatile.Read(ref _explicitTeardown) == 1)
        {
            Destroy(_classPointer);
            return true;
        }

        TraceLeak();
        return true;
    }

    private void TraceLeak()
    {
        try
        {
            Trace.TraceWarning(
                "Confluent.Kafka: {0} 0x{1:X} was released without Close or Dispose; the native client is leaked.",
                GetType().Name,
                _classPointer.ToInt64());
        }
        catch (Exception)
        {
            // A throwing TraceListener must not escape the release.
        }
    }
}
