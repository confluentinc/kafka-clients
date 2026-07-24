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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// Shared base for the binding's owned opaque handles: a <see cref="SafeHandle"/>
/// whose only invalid value is <see cref="IntPtr.Zero"/> (PLAN D2). The ABI's
/// <c>*_destroy</c> functions are all null-safe, and no constructor returns
/// <c>-1</c>, so <c>SafeHandleZeroOrMinusOneIsInvalid</c>
/// (which also treats <c>-1</c> as invalid) is <b>not</b> our contract. This is
/// confluent-kafka-dotnet's <c>SafeHandleZeroIsInvalid</c> pattern (ffi §A2/§B2).
/// </summary>
internal abstract class SafeHandleZeroIsInvalid : SafeHandle
{
    /// <summary>
    /// Initializes the base with a zero (invalid) handle. Ownership is always
    /// <see langword="true"/>: the binding owns every handle it wraps, so the
    /// runtime is responsible for its release (the release contract is on
    /// <see cref="IsInvalid"/>).
    /// </summary>
    protected SafeHandleZeroIsInvalid()
        : base(IntPtr.Zero, ownsHandle: true)
    {
    }

    /// <summary>
    /// A handle is invalid iff it is <see cref="IntPtr.Zero"/>. The CLR consults this to
    /// gate release: per the documented <see cref="SafeHandle.ReleaseHandle"/> contract,
    /// ReleaseHandle "is guaranteed to be called only once and only if the handle is
    /// valid as defined by the IsInvalid property" — so a zero handle (e.g. a fallible
    /// constructor's failure return) is inert and never freed.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;
}
