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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The send of a record whose serializer threw an <see cref="ApiException"/>, as Java's <c>doSend</c> answers it:
/// the delivery callback, if any, runs once on the caller's thread with placeholder metadata and the exception, and
/// the send returns a future failed with it, Java's <c>FutureFailure</c>. The record never reaches the core, so no
/// byte is pinned and no handle is allocated for it.
/// </summary>
internal static class FutureFailure
{
    private const string AsyncCallbackFailed =
        "Confluent.Kafka: an IAsyncCallback (OnCompletion) task faulted or was canceled after a serializer " +
        "failed the record; the record's outcome is unchanged. Details: {0}";

    internal static KafkaFuture<RecordMetadata> Send(string topic, int? partition, ApiException error, ICallback? callback)
    {
        // A throw from the callback leaves the send, as it leaves Java's.
        callback?.OnCompletion(Placeholder(topic, partition), error);
        return new KafkaFuture<RecordMetadata>(Failed(error));
    }

    internal static ValueTask<AsyncKafkaFuture<RecordMetadata>> SendAsync(
        string topic,
        int? partition,
        ApiException error,
        ICallback? callback,
        IAsyncCallback? asyncCallback,
        CancellationToken cancellationToken,
        PendingCallbacks? pending = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RecordMetadata placeholder = Placeholder(topic, partition);
        if (asyncCallback is null)
        {
            callback?.OnCompletion(placeholder, error);
            return new ValueTask<AsyncKafkaFuture<RecordMetadata>>(new AsyncKafkaFuture<RecordMetadata>(Failed(error)));
        }

        // As on the delivery path, the future fails once the callback's task has ended.
        KafkaFutureImpl<RecordMetadata> future = KafkaFutureImpl<RecordMetadata>.ForSend();
        Task task = pending is null
            ? asyncCallback.OnCompletion(placeholder, error)
            : pending.Call(asyncCallback, placeholder, error);
        if (task is null || task.IsCompleted)
        {
            EndAsyncCallback(task, future, error);
        }
        else
        {
            Task ended = task.ContinueWith(
                done => EndAsyncCallback(done, future, error),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            pending?.Add(ended);
        }

        return new ValueTask<AsyncKafkaFuture<RecordMetadata>>(new AsyncKafkaFuture<RecordMetadata>(future));
    }

    /// <summary>
    /// The async callbacks of a producer's serializer failures still running, which its <c>Flush</c> and
    /// <c>Close</c> wait for as Java's wait for the callbacks of the records they cover. Nothing of them crosses the
    /// C ABI, so the core's flush and close cannot see them. A wait made from inside one of them does not wait for
    /// them, or it would wait for itself.
    /// </summary>
    internal sealed class PendingCallbacks
    {
        private readonly HashSet<Task> _running = new HashSet<Task>();
        private readonly AsyncLocal<bool> _inside = new AsyncLocal<bool>();

        internal Task Call(IAsyncCallback callback, RecordMetadata metadata, Exception exception)
        {
            // A nested call restores the outer flag: this write changes the caller's context.
            bool outer = _inside.Value;
            _inside.Value = true;
            try
            {
                return callback.OnCompletion(metadata, exception);
            }
            finally
            {
                _inside.Value = outer;
            }
        }

        internal void Add(Task ended)
        {
            lock (_running)
            {
                _running.Add(ended);
            }

            ended.ContinueWith(
                done =>
                {
                    lock (_running)
                    {
                        _running.Remove(done);
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        /// <summary>The callbacks running now, ended when the last of them ends; ended at once when there are none.</summary>
        internal Task Snapshot()
        {
            if (_inside.Value)
            {
                return Task.CompletedTask;
            }

            lock (_running)
            {
                if (_running.Count == 0)
                {
                    return Task.CompletedTask;
                }

                Task[] running = new Task[_running.Count];
                _running.CopyTo(running);
                return Task.WhenAll(running);
            }
        }

        internal void Wait() => Snapshot().GetAwaiter().GetResult();

        /// <summary>Awaits <paramref name="first"/>, then the callbacks running when this was called.</summary>
        internal Task After(Task first, CancellationToken cancellationToken)
        {
            Task callbacks = Snapshot();
            return callbacks.IsCompleted ? first : AfterBoth(first, callbacks, cancellationToken);
        }

        private static async Task AfterBoth(Task first, Task callbacks, CancellationToken cancellationToken)
        {
            await first.ConfigureAwait(false);
            if (callbacks.IsCompleted || !cancellationToken.CanBeCanceled)
            {
                await callbacks.ConfigureAwait(false);
                return;
            }

            TaskCompletionSource<bool> canceled =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(() => canceled.TrySetResult(true)))
            {
                if (await Task.WhenAny(callbacks, canceled.Task).ConfigureAwait(false) != callbacks)
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }
    }

    // The core's placeholder for a record failed before it is appended: the record's partition, or -1.
    private static RecordMetadata Placeholder(string topic, int? partition) =>
        new RecordMetadata(new TopicPartition(topic, partition ?? -1), -1L, -1L, -1, -1);

    private static KafkaFutureImpl<RecordMetadata> Failed(ApiException error)
    {
        KafkaFutureImpl<RecordMetadata> future = KafkaFutureImpl<RecordMetadata>.ForSend();
        future.CompleteWithError(error);
        return future;
    }

    private static void EndAsyncCallback(Task? task, KafkaFutureImpl<RecordMetadata> future, ApiException error)
    {
        try
        {
            if (task is not null && task.Status != TaskStatus.RanToCompletion)
            {
                Exception failure = task.Exception?.GetBaseException() ?? new TaskCanceledException(task);
                System.Diagnostics.Trace.TraceError(AsyncCallbackFailed, failure);
            }
        }
        catch (Exception)
        {
            // A throwing TraceListener must not leave the future pending.
        }
        finally
        {
            future.CompleteWithError(error);
        }
    }
}
