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
using System.Runtime.ExceptionServices;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A sync projection (M15/P14 S1, PLAN §3.3): a source whose value is a function of another source's — Java's
/// <c>KafkaFuture.thenApply</c>, the sync twin of the async results' <c>Project</c>.
/// </summary>
/// <typeparam name="TIn">The value of the source it projects.</typeparam>
/// <typeparam name="TOut">The projected value.</typeparam>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Lazy, on the caller's thread.</b> Nothing runs until the first <see cref="Get"/>, which waits for the source and
/// then applies the function on the calling thread. The function is pure, so applying it when it is first asked for is
/// equivalent to Java applying it when the source completes; and no thread-pool thread is ever involved.
/// </description></item>
/// <item><description>
/// <b>Applied at most once, memoized.</b> The function runs under this instance's lock, so concurrent first callers
/// apply it once between them. Its value — or the exception it threw, kept as an <see cref="ExceptionDispatchInfo"/> —
/// is what every <see cref="Get"/> returns or rethrows from then on: the same value instance, the same exception
/// instance.
/// </description></item>
/// <item><description>
/// <b>A source failure is the source's.</b> If the source failed, <see cref="Get"/> rethrows the source's own exception
/// instance (the source rethrows the same one on every call) and the function never runs. Nothing the source throws is
/// memoized here, so a source that refuses one caller's wait does not fail the projection for any other caller.
/// </description></item>
/// <item><description>
/// <b>Done when the source is.</b> <see cref="IsDone"/> is the source's: the projection adds no wait of its own.
/// </description></item>
/// <item><description>
/// <b>Locks on <c>this</c></b>, as <see cref="SyncCompletion{T}"/> does: the instance is internal and never handed to
/// user code, so nothing else can lock on it.
/// </description></item>
/// </list>
/// </remarks>
internal sealed class ProjectedSyncFuture<TIn, TOut> : SyncFutureSource<TOut>
{
    private readonly SyncFutureSource<TIn> _source;
    private Func<TIn, TOut>? _project;
    private TOut _value = default!;
    private ExceptionDispatchInfo? _error;
    private volatile bool _done;

    /// <summary>Creates a projection of <paramref name="source"/> through <paramref name="project"/>.</summary>
    /// <param name="source">The source whose value is projected.</param>
    /// <param name="project">The projection: pure, applied at most once.</param>
    internal ProjectedSyncFuture(SyncFutureSource<TIn> source, Func<TIn, TOut> project)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _project = project ?? throw new ArgumentNullException(nameof(project));
    }

    /// <inheritdoc/>
    internal override bool IsDone => _source.IsDone;

    /// <inheritdoc/>
    internal override TOut Get()
    {
        if (!_done)
        {
            // Wait for the source first, outside the lock: a source failure propagates from here as the source's own
            // instance, and is not memoized.
            Apply(_source.Get());
        }

        _error?.Throw();
        return _value;
    }

    private void Apply(TIn input)
    {
        lock (this)
        {
            if (_done)
            {
                return;
            }

            try
            {
                _value = _project!(input);
            }
            catch (Exception exception)
            {
                _error = ExceptionDispatchInfo.Capture(exception);
            }

            // The function is never needed again; drop it (and whatever it captured).
            _project = null;

            // Published last, as SyncCompletion publishes `_done`: a reader that sees it set sees the outcome.
            _done = true;
        }
    }
}
