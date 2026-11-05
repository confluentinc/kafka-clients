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
        "Confluent.Kafka: an IAsyncDeliveryCallback (OnCompletion) task faulted or was canceled after a serializer " +
        "failed the record; the record's outcome is unchanged. Details: {0}";

    internal static KafkaFuture<RecordMetadata> Send(string topic, int? partition, ApiException error, IDeliveryCallback? callback)
    {
        // A throw from the callback leaves the send, as it leaves Java's.
        callback?.OnCompletion(Placeholder(topic, partition), error);
        return new KafkaFuture<RecordMetadata>(Failed(error));
    }

    internal static ValueTask<AsyncKafkaFuture<RecordMetadata>> SendAsync(
        string topic,
        int? partition,
        ApiException error,
        IDeliveryCallback? callback,
        IAsyncDeliveryCallback? asyncCallback,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RecordMetadata placeholder = Placeholder(topic, partition);
        if (asyncCallback is null)
        {
            callback?.OnCompletion(placeholder, error);
            return new ValueTask<AsyncKafkaFuture<RecordMetadata>>(new AsyncKafkaFuture<RecordMetadata>(Failed(error)));
        }

        // As on the delivery path, the future fails once the callback's task has ended.
        KafkaFutureImpl<RecordMetadata> future = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        Task task = asyncCallback.OnCompletion(placeholder, error);
        if (task is null || task.IsCompleted)
        {
            EndAsyncCallback(task, future, error);
        }
        else
        {
            task.ContinueWith(
                ended => EndAsyncCallback(ended, future, error),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        return new ValueTask<AsyncKafkaFuture<RecordMetadata>>(new AsyncKafkaFuture<RecordMetadata>(future));
    }

    // The core's placeholder for a record failed before it is appended: the record's partition, or -1.
    private static RecordMetadata Placeholder(string topic, int? partition) =>
        new RecordMetadata(new TopicPartition(topic, partition ?? -1), -1L, -1L, -1, -1);

    private static KafkaFutureImpl<RecordMetadata> Failed(ApiException error)
    {
        KafkaFutureImpl<RecordMetadata> future = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
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
