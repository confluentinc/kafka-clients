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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Owned handle over a <c>kafka_admin_AdminClient_t</c> — the client handle returned
/// <b>directly</b> by <c>kafka_admin_AdminClient_new</c> <em>and</em> by
/// <c>kafka_admin_MockAdminClient_new</c> (ffi §B2, Category 1). The mock is not a
/// separate ABI type: both constructors hand back the same opaque
/// <c>kafka_admin_AdminClient_t*</c>, so one handle type wraps both. The interop
/// marshaller invokes the private parameterless ctor and sets the handle atomically on
/// return (the M2/P2 hardening), so the binding never wraps a raw pointer itself; a
/// null native return simply yields an <c>IsInvalid</c> handle whose
/// <see cref="ReleaseHandle"/> is skipped (no spurious <c>AdminClient_destroy</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This handle's reference count is load-bearing.</b>
/// <c>kafka_admin_AdminClient_destroy</c> does not wait for a synchronous call still
/// running on another thread, and does not wait for every in-flight <c>_async</c>
/// operation. Since master #233 it waits for an in-flight close and fails the
/// single-callback operations it tracks with a <c>TIMEOUT</c> error, joining their
/// tasks, before it shuts the runtime down, drops the client and <em>detaches</em> the
/// dispatcher. The per-key operations are not tracked, so one in flight at destroy may
/// never fire its callbacks. So every async submit takes a <b>span-the-op</b>
/// <see cref="System.Runtime.InteropServices.SafeHandle.DangerousAddRef(ref bool)"/>
/// here (released by the completion callback in
/// <c>AdminOperation.FreeGcHandle</c>), a synchronous call holds the marshaller's
/// call-scoped one (ffi §A2), and because <see cref="ReleaseHandle"/> runs only at
/// count zero, a <c>Dispose</c> racing an in-flight op <b>defers</b> the native destroy
/// instead of pulling the client out from under the operation.
/// </para>
/// <para>
/// A deferred destroy is therefore normal operation, not a defect: disposing while an
/// op is in flight returns promptly, and the native release happens when that op's
/// callback releases the last reference — on whichever thread the callback ran (the
/// handle's dispatcher, a tokio worker, or, for an inline completion, the submitting
/// thread). On a tokio worker, the destroy moves its wait and free to a thread of its
/// own and returns at once (master #233). This is the same trade the consumer's
/// Category-6 handle records: a deferred release is a retention, whereas a raw pointer
/// outliving its client is corruption.
/// </para>
/// <para>
/// <see cref="ReleaseHandle"/> is the bare <c>AdminClient_destroy</c>. The
/// <b>graceful</b> teardown — a <c>AdminClient_close</c> / <c>close_async</c> that
/// joins the background task, followed by the destroy — is orchestrated by
/// <c>NativeAdminClient</c> before this handle is disposed.
/// </para>
/// </remarks>
internal sealed class SafeAdminHandle : SafeHandleZeroIsInvalid
{
    private SafeAdminHandle()
    {
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        // ⚠ Structurally excluded from the SafeHandle-as-parameter convention that the
        // synchronous admin calls use (ffi §A2): passing `this` would make the marshaller
        // DangerousAddRef a handle that is already mid-release. The protected `handle`
        // field is the only correct argument here.
        NativeMethods.AdminClientDestroy(handle);
        return true;
    }
}
