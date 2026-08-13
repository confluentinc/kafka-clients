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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The shared public-layer <b>graceful close → destroy</b> orchestration for the producer
/// (M11/P2 Dispose upgrade, ffi-marshalling.md §A7). Layered <b>above</b>
/// <see cref="NativeProducer"/>: every method here closes gracefully first, then routes the
/// destroy through <see cref="NativeProducer.Dispose"/> (which stays the pinned M11/P1
/// <c>Producer_destroy</c>-only teardown) — so the upgrade is purely additive and P1's teardown
/// pin is left byte-for-byte. The public <c>AsyncKafkaProducer</c> / <c>AsyncMockProducer</c>
/// wrappers own the one-shot close latch and call into these bodies (both use the identical
/// intricate teardown, so it lives once here rather than duplicated per wrapper — the analog of
/// the consumer's single <c>NativeConsumer</c>-owned close→destroy, moved up a layer because
/// the producer's <see cref="NativeProducer"/> pin must stay destroy-only).
/// </summary>
/// <remarks>
/// <b>A close error never prevents the destroy.</b> Every method destroys in a <c>finally</c>,
/// so the handle is always freed even when the graceful close throws (§A7). The graceful close
/// is bridged over <see cref="NativeProducer.CloseWithCallback"/> (async) /
/// <see cref="NativeProducer.CloseSync"/> (sync); its span-the-op <see cref="System.Runtime.InteropServices.SafeHandle"/>
/// ref defers the native destroy until the close's completion callback fires, so the subsequent
/// <see cref="NativeProducer.Dispose"/> is use-after-free-safe even on the timed-deadline path
/// where the close is still in flight when destroy is requested.
/// </remarks>
internal static class ProducerTeardown
{
    /// <summary>
    /// Graceful async close that <b>surfaces</b> a close failure (Java <c>close()</c>), then
    /// destroy. The public <c>Close(CancellationToken)</c> body.
    /// </summary>
    internal static async Task CloseGracefulThenDestroyAsync(NativeProducer native)
    {
        try
        {
            // Once the close is in flight the graceful join runs to completion — pass no token
            // (mirrors the consumer's Close). Surface any close error (unlike disposal).
            await native.CloseWithCallback().ConfigureAwait(false);
        }
        finally
        {
            // Producer_destroy, exactly once — even if the close threw.
            native.Dispose();
        }
    }

    /// <summary>
    /// Best-effort async close that <b>swallows</b> a <see cref="KafkaException"/> (disposal is
    /// not the place to surface a close failure), then destroy. The public
    /// <c>DisposeAsync</c> body — the primary teardown path.
    /// </summary>
    internal static async ValueTask CloseBestEffortThenDestroyAsync(NativeProducer native)
    {
        try
        {
            await native.CloseWithCallback().ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Best-effort teardown — disposal must not surface a close error (that is Close()'s
            // job). A close failure still proceeds to destroy in the finally.
        }
        finally
        {
            native.Dispose();
        }
    }

    /// <summary>
    /// Best-effort <b>synchronous</b> graceful close (swallows its own error inside
    /// <see cref="NativeProducer.CloseSync"/>), then destroy. The public blocking <c>Dispose</c>
    /// body (there is no <c>Producer_close_with_timeout</c> ABI, so the sync leg is the plain
    /// <c>Producer_close</c>).
    /// </summary>
    internal static void CloseSyncThenDestroy(NativeProducer native)
    {
        try
        {
            native.CloseSync();
        }
        finally
        {
            native.Dispose();
        }
    }

    /// <summary>
    /// Graceful async close raced against a <b>.NET-side deadline</b> (Java <c>close(Duration)</c>),
    /// then destroy — there is no <c>Producer_close_with_timeout</c> ABI, so the timeout is a
    /// managed race over <c>Producer_close_async</c>, not a native timed close (§A7). On the
    /// deadline the awaiter completes (best-effort return) and the native close continues to
    /// completion in the background, freeing its own rooting; the subsequent destroy is deferred
    /// (by the span-the-op ref) until that straggler callback fires. A close that resolves first
    /// surfaces its result/error; user cancellation (distinct from the timeout) throws
    /// <see cref="OperationCanceledException"/>.
    /// </summary>
    internal static async Task CloseWithDeadlineThenDestroyAsync(
        NativeProducer native,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        try
        {
            Task closeTask = native.CloseWithCallback();

            // Linked so the caller's token also trips the delay; Zero timeout → the delay
            // completes immediately (a valid "don't wait" deadline).
            using CancellationTokenSource deadlineCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Task delayTask = Task.Delay(timeout, deadlineCts.Token);

            Task winner = await Task.WhenAny(closeTask, delayTask).ConfigureAwait(false);
            if (winner == closeTask)
            {
                // Close resolved within the deadline. Cancel the delay timer, then surface the
                // close result/error.
                deadlineCts.Cancel();
                await closeTask.ConfigureAwait(false);
                return;
            }

            // The deadline elapsed (or the caller canceled) before the close resolved. Observe
            // the still-in-flight close's eventual fault so it is not an unobserved-Task
            // exception; the native close continues + frees its own rooting (span-the-op ref),
            // so the destroy in the finally is deferred and use-after-free-safe.
            ObserveEventually(closeTask);

            // User cancellation → OperationCanceledException; a plain timeout → best-effort
            // return (Java close(Duration) does not throw on timeout).
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            native.Dispose();
        }
    }

    private static void ObserveEventually(Task task) =>
        _ = task.ContinueWith(
            static t => { _ = t.Exception; },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
