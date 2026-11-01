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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A native <c>CreateTopicsResult_t</c>: its per-topic <c>values()</c> futures, and each per-topic getter's future read
/// from the C accessor when asked, linked to the topic's future; the accessor's future fails for a topic not in the request.
/// </summary>
internal sealed class CreateTopicsRefinements
{
    private static readonly Func<IntPtr, Config> s_copyOutConfig = ConfigEntriesMarshal.CopyOut;

    private static readonly Func<IntPtr, Uuid> s_copyOutUuid = UuidMarshal.CopyOut;

    private static readonly Func<IntPtr, int> s_readInt32 = static value => Marshal.ReadInt32(value);

    internal CreateTopicsRefinements(SafeAdminResultHandle result, IReadOnlyDictionary<string, KafkaFutureImpl<object?>> values)
    {
        Result = result;
        Values = values;
    }

    private delegate IntPtr TopicFuture(IntPtr result, IntPtr topic, ref IntPtr outErr);

    internal SafeAdminResultHandle Result { get; }

    internal IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values { get; }

    internal KafkaFutureImpl<Config> Config(string topic) =>
        Refine(topic, NativeMethods.CreateTopicsResultConfig, s_copyOutConfig);

    internal KafkaFutureImpl<Uuid> TopicId(string topic) =>
        Refine(topic, NativeMethods.CreateTopicsResultTopicId, s_copyOutUuid);

    internal KafkaFutureImpl<int> NumPartitions(string topic) =>
        Refine(topic, NativeMethods.CreateTopicsResultNumPartitions, s_readInt32);

    internal KafkaFutureImpl<int> ReplicationFactor(string topic) =>
        Refine(topic, NativeMethods.CreateTopicsResultReplicationFactor, s_readInt32);

    private KafkaFutureImpl<T> Refine<T>(string topic, TopicFuture accessor, Func<IntPtr, T> convert)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        AdminStrings.Validate(topic, nameof(topic));
        IntPtr future;
        bool added = false;
        try
        {
            Result.DangerousAddRef(ref added);
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(topic);
            IntPtr outErr = IntPtr.Zero;
            future = accessor(Result.DangerousGetHandle(), name.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            if (added)
            {
                Result.DangerousRelease();
            }
        }

        KafkaFutureImpl<T> source = NativeFutureSource<T>.Start(SafeKafkaFutureHandle.Owned(future), convert);
        if (!Values.TryGetValue(topic, out KafkaFutureImpl<object?>? key))
        {
            return source;
        }

        KafkaFutureImpl<T> linked = source.ThenApply(static value => value);
        key.AddListener(settled =>
        {
            if (settled.IsCancelled && settled.Failure is { } cancellation)
            {
                linked.Propagate(cancellation);
            }
        });
        return linked;
    }
}
