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
/// The base of the binding's owning <see cref="SafeHandle"/>s, for which only <see cref="IntPtr.Zero"/> is invalid.
/// </summary>
internal abstract class SafeHandleZeroIsInvalid : SafeHandle
{
    protected SafeHandleZeroIsInvalid()
        : base(IntPtr.Zero, ownsHandle: true)
    {
    }

    public override bool IsInvalid => handle == IntPtr.Zero;
}
