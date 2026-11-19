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
/// An admin client's owned handle over its <c>kafka_admin_Admin_t</c> view, which every admin operation takes; the
/// action given to <see cref="ReleaseAfterDestroy"/> runs only once the class's destroy has returned.
/// </summary>
internal abstract class SafeAdminClientHandle : SafeClientHandle
{
    private Action? _afterDestroy;

    private protected SafeAdminClientHandle(IntPtr view, IntPtr classPointer)
        : base(view, classPointer)
    {
    }

    internal void ReleaseAfterDestroy(Action release) => _afterDestroy = release;

    protected sealed override void Destroy(IntPtr classPointer)
    {
        try
        {
            DestroyClass(classPointer);
        }
        finally
        {
            _afterDestroy?.Invoke();
        }
    }

    private protected abstract void DestroyClass(IntPtr classPointer);
}
