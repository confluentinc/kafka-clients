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
/// Owned handle over a <c>kafka_consumer_Consumer_t</c> — the client handle returned
/// <b>directly</b> by <c>kafka_consumer_KafkaConsumer_new</c> / <c>MockConsumer_new</c>
/// (ffi §B2, Category 1). The interop marshaller invokes the private parameterless
/// ctor and sets the handle atomically on return (M2/P2), so the binding never wraps
/// a raw pointer itself and there is no create→<c>SetHandle</c> allocation-gap window;
/// a null native return simply yields an <c>IsInvalid</c> handle whose
/// <see cref="ReleaseHandle"/> is skipped (no spurious <c>Consumer_destroy</c>).
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

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.ConsumerDestroy(handle);
        return true;
    }
}
