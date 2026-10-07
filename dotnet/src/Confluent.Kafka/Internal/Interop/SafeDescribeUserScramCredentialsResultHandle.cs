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
/// Owned handle over a <c>kafka_admin_DescribeUserScramCredentialsResult_t</c>, retained
/// past its completion callback so <c>DescribeUserScramCredentialsResult.Description(user)</c>
/// can be served on demand (ffi §B2 Category 3 — a borrow-root held past the call rather
/// than freed in it).
/// </summary>
/// <remarks>
/// <para>
/// Release is the <see cref="System.Runtime.InteropServices.SafeHandle"/> critical
/// finalizer, and the public result type deliberately does <b>not</b> expose
/// <see cref="IDisposable"/>: Java's <c>DescribeUserScramCredentialsResult</c> is not
/// closeable, and <c>..._Result_destroy</c> is a plain <c>Box::from_raw</c> drop with no
/// runtime teardown, so ffi §A2's "prefer <c>Dispose</c> over the finalizer" — which is
/// about the producer's <em>blocking</em> destroy — does not reach this case. Adding
/// <see cref="IDisposable"/> later is non-breaking.
/// </para>
/// <para>
/// The retained root is independent of the <c>AdminClient</c>: the core result it wraps
/// holds its resolved response future by value, and reading it needs no tokio runtime, so
/// <c>Description</c> keeps working after the client is disposed.
/// </para>
/// </remarks>
internal sealed class SafeDescribeUserScramCredentialsResultHandle : SafeHandleZeroIsInvalid
{
    private SafeDescribeUserScramCredentialsResultHandle()
    {
    }

    /// <summary>
    /// Takes ownership of a raw result root obtained from the completion callback.
    /// </summary>
    /// <param name="result">The non-null result root to adopt.</param>
    /// <returns>The owning handle.</returns>
    /// <remarks>
    /// ⚠ Either this returns an owning handle or it threw <b>before</b> taking ownership —
    /// the allocation is the only fallible step and <c>SetHandle</c> cannot throw. The
    /// caller's ownership baton covers the throwing case (ffi §B6).
    /// </remarks>
    internal static SafeDescribeUserScramCredentialsResultHandle Adopt(IntPtr result)
    {
        SafeDescribeUserScramCredentialsResultHandle adopted =
            new SafeDescribeUserScramCredentialsResultHandle();
        adopted.SetHandle(result);
        return adopted;
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.DescribeUserScramCredentialsResultDestroy(handle);
        return true;
    }
}
