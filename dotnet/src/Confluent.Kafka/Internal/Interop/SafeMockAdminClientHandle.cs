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
/// The handle of a <c>kafka_admin_MockAdminClient_t</c>, held as its borrowed <c>Admin</c> view and destroyed with
/// <c>kafka_admin_MockAdminClient_destroy_err</c>.
/// </summary>
internal sealed class SafeMockAdminClientHandle : SafeAdminClientHandle
{
    internal SafeMockAdminClientHandle(IntPtr view, IntPtr mock)
        : base(view, mock)
    {
    }

    private protected override void DestroyClass(IntPtr classPointer)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MockAdminClientDestroy(classPointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }
}
