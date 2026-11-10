// Copyright 2026 Confluent Inc.
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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// An async subscribe started while another subscribe on the same consumer has not settled: refused before the
/// native call, with no registration, and the in-flight mark released on every outcome of the first.
/// </summary>
public sealed class PublicAsyncConsumerOverlappingSubscribeTests
{
    private const string Topic = "overlapping-subscribe-topic";

    private const string Concurrent = "KafkaConsumer is not safe for multi-threaded access.";

    private const string MutuallyExclusive = "Subscription to topics, partitions and pattern are mutually exclusive";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_settle = TimeSpan.FromSeconds(2);

    private static readonly string[] s_topics = { Topic };

    private static readonly SubscriptionPattern s_pattern = new SubscriptionPattern("overlapping-subscribe-.*");

    private static readonly Form[] s_subscribingForms =
    {
        Form.Topics,
        Form.TopicsWithAListener,
        Form.TopicsWithAnAsyncListener,
        Form.Pattern,
        Form.PatternWithAListener,
        Form.PatternWithAnAsyncListener,
    };

    /// <summary>The async consumer a case drives.</summary>
    public enum Kind
    {
        AsyncKafkaConsumer,
        AsyncMockConsumer,
    }

    /// <summary>A subscribe form: topics, a pattern or no topics, without a listener or with either kind.</summary>
    public enum Form
    {
        Topics,
        TopicsWithAListener,
        TopicsWithAnAsyncListener,
        Pattern,
        PatternWithAListener,
        PatternWithAnAsyncListener,
        NoTopics,
        NoTopicsWithAListener,
        NoTopicsWithAnAsyncListener,
    }

    public static TheoryData<Kind, Form, Form> Overlaps()
    {
        Form[] forms = (Form[])Enum.GetValues(typeof(Form));
        TheoryData<Kind, Form, Form> data = new TheoryData<Kind, Form, Form>();
        foreach (Kind kind in new[] { Kind.AsyncKafkaConsumer, Kind.AsyncMockConsumer })
        {
            foreach (Form first in forms)
            {
                foreach (Form second in forms)
                {
                    data.Add(kind, first, second);
                }
            }
        }

        return data;
    }

    public static TheoryData<Kind, Form> KindsAndForms()
    {
        TheoryData<Kind, Form> data = new TheoryData<Kind, Form>();
        foreach (Kind kind in new[] { Kind.AsyncKafkaConsumer, Kind.AsyncMockConsumer })
        {
            foreach (Form form in s_subscribingForms)
            {
                data.Add(kind, form);
            }
        }

        return data;
    }

    public static TheoryData<Form> Forms()
    {
        TheoryData<Form> data = new TheoryData<Form>();
        foreach (Form form in s_subscribingForms)
        {
            data.Add(form);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Overlaps))]
    public async Task ASubscribeStartedBeforeAnotherHasSettled_IsRefusedBeforeTheNativeCall(
        Kind kind, Form first, Form second)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            NativeConsumer native = Native(consumer);
            Task firstSubscribe;
            using (new ConsumerPumpStall(consumer))
            {
                firstSubscribe = Subscribe(consumer, first, NewListener(first));
                (WeakReference? secondListener, Exception? refused) = StartUnreferenced(consumer, second);

                ConcurrentModificationException concurrent = Assert.IsType<ConcurrentModificationException>(refused);
                Assert.Equal(Concurrent, concurrent.Message);
                Assert.Null(native.CurrentListenerRegistration);
                Assert.Equal(0, native.RetainedListenerRegistrationCount);
                await AssertCollected(secondListener);
                Assert.False(firstSubscribe.IsCompleted);
            }

            await TestTimeout.Run(() => firstSubscribe, s_deadline);
            Assert.Equal(KeepsItsListener(kind, first), native.CurrentListenerRegistration is not null);
            Assert.Equal(0, native.RetainedListenerRegistrationCount);

            await TestTimeout.Run(() => Subscribe(consumer, first, NewListener(first)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(KindsAndForms))]
    public async Task AfterASubscribeTheConsumerRefused_TheNextSubscribeReachesTheConsumer(Kind kind, Form first)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            Form other = IsPattern(first) ? Form.Topics : Form.Pattern;
            await TestTimeout.Run(() => Subscribe(consumer, other, null), s_deadline);
            Task refusedSubscribe = Subscribe(consumer, first, NewListener(first));
            IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
                () => TestTimeout.Run(() => refusedSubscribe, s_deadline));
            Assert.Equal(MutuallyExclusive, refused.Message);

            IllegalStateException again = await Assert.ThrowsAsync<IllegalStateException>(
                () => TestTimeout.Run(() => Subscribe(consumer, first, NewListener(first)), s_deadline));

            Assert.Equal(MutuallyExclusive, again.Message);
            await TestTimeout.Run(() => Subscribe(consumer, other, null), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(Forms))]
    public async Task OnAKafkaConsumer_AfterASubscribeRefusedForAMissingGroupId_TheNextReachesTheConsumer(Form first)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(Kind.AsyncKafkaConsumer, groupId: false);
        try
        {
            for (int attempt = 0; attempt < 2; attempt++)
            {
                Task subscribed = Subscribe(consumer, first, NewListener(first));
                InvalidGroupIdException refused = await Assert.ThrowsAsync<InvalidGroupIdException>(
                    () => TestTimeout.Run(() => subscribed, s_deadline));

                Assert.Equal(GroupIdNotDefined, refused.Message);
            }
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(Forms))]
    public async Task OnAKafkaConsumer_AfterASubscribeTheCoreRefusedAsConcurrent_TheNextIsAccepted(Form first)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(Kind.AsyncKafkaConsumer);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);
            Task<ConsumerRecords<byte[], byte[]>> poll = consumer.Poll(s_deadline);
            Task subscribed = Subscribe(consumer, first, NewListener(first));
            ConcurrentModificationException refused = await Assert.ThrowsAsync<ConcurrentModificationException>(
                () => TestTimeout.Run(() => subscribed, s_deadline));
            Assert.Equal(Concurrent, refused.Message);
            consumer.Wakeup();
            await Assert.ThrowsAsync<WakeupException>(() => TestTimeout.Run(() => poll, s_deadline));
            await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);

            await TestTimeout.Run(() => Subscribe(consumer, first, NewListener(first)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(KindsAndForms))]
    public async Task AfterASubscribeWhoseTokenWasCancelled_TheNextIsAccepted(Kind kind, Form first)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task subscribed;
            using (new ConsumerPumpStall(consumer))
            {
                subscribed = Subscribe(consumer, first, NewListener(first), cancel.Token);
                cancel.Cancel();
            }

            await Assert.ThrowsAsync<OperationCanceledException>(() => TestTimeout.Run(() => subscribed, s_deadline));

            await TestTimeout.Run(() => Subscribe(consumer, first, NewListener(first)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [InlineData(Kind.AsyncKafkaConsumer)]
    [InlineData(Kind.AsyncMockConsumer)]
    public async Task AfterASubscribeWithATokenAlreadyCancelled_TheNextIsAccepted(Kind kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            cancel.Cancel();
            Assert.Throws<OperationCanceledException>(
                () => { _ = Subscribe(consumer, Form.TopicsWithAListener, NewListener(Form.TopicsWithAListener), cancel.Token); });

            await TestTimeout.Run(
                () => Subscribe(consumer, Form.TopicsWithAListener, NewListener(Form.TopicsWithAListener)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [InlineData(Kind.AsyncKafkaConsumer)]
    [InlineData(Kind.AsyncMockConsumer)]
    public async Task AfterASubscribeWhoseNativeCallThrew_TheNextIsAccepted(Kind kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            NativeConsumer native = Native(consumer);
            InvalidOperationException thrown = new InvalidOperationException("the native call threw");
            bool markedWhenThrown = false;
            TargetInvocationException failed = Assert.Throws<TargetInvocationException>(() => Submit(
                native,
                (_, _) =>
                {
                    markedWhenThrown = SubscribeInFlight(native) is not null;
                    throw thrown;
                },
                settle: null));

            Assert.Same(thrown, failed.InnerException);
            Assert.True(markedWhenThrown);
            await TestTimeout.Run(
                () => Subscribe(consumer, Form.TopicsWithAListener, NewListener(Form.TopicsWithAListener)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [InlineData(Kind.AsyncKafkaConsumer)]
    [InlineData(Kind.AsyncMockConsumer)]
    public async Task AfterASubscribeWhoseSettleThrew_TheNextIsAccepted(Kind kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(kind);
        try
        {
            NativeConsumer native = Native(consumer);
            int settles = 0;
            bool markedWhenThrown = false;
            ConsumerCompletion<bool> completion = Submit(
                native,
                (arguments, opaque) => NativeMethods.ConsumerSubscribeWithTopicsCb(
                    native.Handle, arguments.StringList(s_topics), ConsumerCompletion.Void, opaque),
                _ =>
                {
                    markedWhenThrown = SubscribeInFlight(native) is not null;
                    Interlocked.Increment(ref settles);
                    throw new InvalidOperationException("the settle threw");
                });

            await TestTimeout.Run(() => Finish(native, completion), s_deadline);
            Assert.Equal(1, Volatile.Read(ref settles));
            Assert.True(markedWhenThrown);
            await TestTimeout.Run(
                () => Subscribe(consumer, Form.TopicsWithAListener, NewListener(Form.TopicsWithAListener)), s_deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static IAsyncConsumer<byte[], byte[]> Create(Kind kind, bool groupId = true)
    {
        if (kind == Kind.AsyncMockConsumer)
        {
            return new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray(), Serdes.ByteArray());
        }

        Dictionary<string, object?> config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
        };
        if (groupId)
        {
            config["group.id"] = "overlapping-subscribe-group";
        }

        return new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray(), Serdes.ByteArray());
    }

    private static NativeConsumer Native(object consumer) =>
        (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;

    private static ConsumerCompletion<bool> Submit(
        NativeConsumer native,
        Action<NativeCallArguments, IntPtr> submit,
        Action<IntPtr>? settle)
    {
        Dictionary<string, object?> given = new Dictionary<string, object?>
        {
            ["cancellationToken"] = CancellationToken.None,
            ["convert"] = new Func<IntPtr, long, bool>((_, _) => true),
            ["submit"] = submit,
            ["settle"] = settle,
            ["subscribes"] = true,
        };
        MethodInfo method = PrivateGeneric("Submit");
        ParameterInfo[] parameters = method.GetParameters();
        Assert.Equal(given.Count, parameters.Count(parameter => given.ContainsKey(parameter.Name!)));
        object?[] arguments = parameters
            .Select(parameter => given.TryGetValue(parameter.Name!, out object? value) ? value : parameter.DefaultValue)
            .ToArray();
        return (ConsumerCompletion<bool>)method.Invoke(native, arguments)!;
    }

    private static Task Finish(NativeConsumer native, ConsumerCompletion<bool> completion) =>
        (Task)PrivateGeneric("Finish").Invoke(
            native, new object[] { completion, NativeConsumer.AsyncOperation.Subscribe, CancellationToken.None })!;

    private static MethodInfo PrivateGeneric(string name) =>
        Assert.Single(
                typeof(NativeConsumer).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance),
                method => method.Name == name && method.IsGenericMethodDefinition)
            .MakeGenericMethod(typeof(bool));

    private static object? SubscribeInFlight(NativeConsumer native) =>
        typeof(NativeConsumer)
            .GetField("_subscribeInFlight", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(native);

    private static bool IsPattern(Form form) =>
        form is Form.Pattern or Form.PatternWithAListener or Form.PatternWithAnAsyncListener;

    private static bool HasAListener(Form form) =>
        form is Form.TopicsWithAListener
            or Form.TopicsWithAnAsyncListener
            or Form.PatternWithAListener
            or Form.PatternWithAnAsyncListener
            or Form.NoTopicsWithAListener
            or Form.NoTopicsWithAnAsyncListener;

    private static bool KeepsItsListener(Kind kind, Form form) =>
        HasAListener(form)
            && !(kind == Kind.AsyncKafkaConsumer
                && form is Form.NoTopics or Form.NoTopicsWithAListener or Form.NoTopicsWithAnAsyncListener);

    private static object? NewListener(Form form) => form switch
    {
        Form.TopicsWithAListener or Form.PatternWithAListener or Form.NoTopicsWithAListener => new NoOpListener(),
        Form.TopicsWithAnAsyncListener or Form.PatternWithAnAsyncListener or Form.NoTopicsWithAnAsyncListener =>
            new NoOpAsyncListener(),
        _ => null,
    };

    private static Task Subscribe(
        IAsyncConsumer<byte[], byte[]> consumer,
        Form form,
        object? listener,
        CancellationToken cancellationToken = default) => form switch
        {
            Form.Topics => consumer.Subscribe(s_topics, cancellationToken),
            Form.TopicsWithAListener =>
                consumer.Subscribe(s_topics, (IConsumerRebalanceListener)listener!, cancellationToken),
            Form.TopicsWithAnAsyncListener =>
                consumer.Subscribe(s_topics, (IAsyncConsumerRebalanceListener)listener!, cancellationToken),
            Form.Pattern => consumer.Subscribe(s_pattern, cancellationToken),
            Form.PatternWithAListener =>
                consumer.Subscribe(s_pattern, (IConsumerRebalanceListener)listener!, cancellationToken),
            Form.PatternWithAnAsyncListener =>
                consumer.Subscribe(s_pattern, (IAsyncConsumerRebalanceListener)listener!, cancellationToken),
            Form.NoTopics => consumer.Subscribe(Array.Empty<string>(), cancellationToken),
            Form.NoTopicsWithAListener =>
                consumer.Subscribe(Array.Empty<string>(), (IConsumerRebalanceListener)listener!, cancellationToken),
            _ => consumer.Subscribe(Array.Empty<string>(), (IAsyncConsumerRebalanceListener)listener!, cancellationToken),
        };

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference? Listener, Exception? Refused) StartUnreferenced(
        IAsyncConsumer<byte[], byte[]> consumer,
        Form form)
    {
        object? listener = NewListener(form);
        WeakReference? weak = listener is null ? null : new WeakReference(listener);
        try
        {
            _ = Subscribe(consumer, form, listener);
            return (weak, null);
        }
        catch (Exception exception)
        {
            return (weak, exception);
        }
    }

    private static async Task AssertCollected(WeakReference? listener)
    {
        if (listener is null)
        {
            return;
        }

        Stopwatch watch = Stopwatch.StartNew();
        do
        {
            await Task.Yield();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        while (listener.IsAlive && watch.Elapsed < s_settle);

        Assert.False(listener.IsAlive, "the refused subscribe's listener is still referenced");
    }

    /// <summary>A rebalance listener that does nothing.</summary>
    private sealed class NoOpListener : IConsumerRebalanceListener
    {
        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An asynchronous rebalance listener that does nothing.</summary>
    private sealed class NoOpAsyncListener : IAsyncConsumerRebalanceListener
    {
        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }
}
