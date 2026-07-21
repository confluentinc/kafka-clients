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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// Owned handle over a <c>kafka_consumer_Consumer_t</c> — the client handle from
/// <c>kafka_consumer_KafkaConsumer_new</c> / <c>MockConsumer_new</c> (ffi §B2,
/// Category 1).
/// </summary>
/// <remarks>
/// <see cref="ReleaseHandle"/> is the <b>last-resort</b> bare
/// <c>Consumer_destroy</c>, which is fire-and-forget: it cancels any in-flight op
/// and does NOT join the background task. The <b>graceful</b> teardown — a
/// <c>Consumer_close</c> / <c>close_with_timeout</c> that joins the task, followed
/// by destroy — is orchestrated by the owning lifecycle wrapper before this handle
/// is disposed (ffi §B2/§B7). A bare destroy on its own is correct for freeing the
/// native memory but skips the join; the wrapper's <c>Dispose</c> is the normal
/// path.
/// </remarks>
internal sealed class SafeConsumerHandle : SafeHandleZeroIsInvalid
{
    private SafeConsumerHandle()
    {
    }

    /// <summary>
    /// Wraps a raw consumer handle returned by a consumer constructor. The handle
    /// must be non-<see cref="IntPtr.Zero"/> (the constructor's failure is surfaced
    /// as a <see cref="KafkaException"/> before this is called).
    /// </summary>
    internal static SafeConsumerHandle FromRaw(IntPtr raw)
    {
        var handle = new SafeConsumerHandle();
        handle.SetHandle(raw);
        return handle;
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.ConsumerDestroy(handle);
        return true;
    }
}
