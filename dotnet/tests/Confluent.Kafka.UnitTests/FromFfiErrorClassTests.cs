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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

using FutureTimeoutException = Confluent.Kafka.TimeoutException;
using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A core error is read into the class of its id: errors built by the core's factories, id -1 told apart by the
/// core's <c>is_api_error</c>, the wrap of a carried callback exception, every registry entry, an id the binding does
/// not know, and the one class an owner-check rejection and the core's concurrent-access error share.
/// </summary>
public sealed class FromFfiErrorClassTests
{
    private const string NonAsciiMessage = "café-Ω-日本語-😀";

    private const string UnknownServerErrorMessage =
        "The server experienced an unexpected error when processing the request.";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const string Topic = "from-ffi-error-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>A common error factory that builds an error from a UTF-8 message.</summary>
    private delegate IntPtr MessageFactory(IntPtr message, ref IntPtr outErr);

    private static readonly Dictionary<string, (MessageFactory Factory, int Code, Type Expected)> s_localFactories = new()
    {
        ["local_concurrent_modification"] =
            (NativeMethods.ErrorLocalConcurrentModification, -2, typeof(ConcurrentModificationException)),
        ["local_illegal_argument"] = (NativeMethods.ErrorLocalIllegalArgument, -3, typeof(IllegalArgumentException)),
        ["local_illegal_state"] = (NativeMethods.ErrorLocalIllegalState, -4, typeof(IllegalStateException)),
        ["local_timeout"] = (NativeMethods.ErrorLocalTimeout, -5, typeof(FutureTimeoutException)),
        ["local_unsupported_operation"] =
            (NativeMethods.ErrorLocalUnsupportedOperation, -31, typeof(UnsupportedOperationException)),
    };

    [Theory]
    [InlineData("local_concurrent_modification")]
    [InlineData("local_illegal_argument")]
    [InlineData("local_illegal_state")]
    [InlineData("local_timeout")]
    [InlineData("local_unsupported_operation")]
    public void ALocalFactory_GivesItsJavaNamedClass_WithTheCoreMessage(string row)
    {
        (MessageFactory factory, int code, Type expected) = s_localFactories[row];
        IntPtr error = Build(factory, NonAsciiMessage);
        Assert.Equal(code, NativeMethods.Code(error));
        Assert.Equal(NonAsciiMessage, Utf8Marshal.PtrToString(NativeMethods.Message(error)));

        Exception read = KafkaException.FromFfiError(error, owned: true)!;

        Assert.IsType(expected, read);
        Assert.Equal(code, ErrorClasses.Ids[expected]);
        Assert.Equal(NonAsciiMessage, read.Message);
        Assert.False(read is KafkaException, $"{expected.FullName} is a JDK class, not a KafkaException.");
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void TheNullPointerId_IsAnArgumentNullException_WithTheCoreMessage_AndNoParamName()
    {
        IntPtr error = Build(NativeMethods.ErrorLocalNullPointer, NonAsciiMessage);
        Assert.Equal(-32, NativeMethods.Code(error));

        Exception read = KafkaException.FromFfiError(error, owned: true)!;

        // Java's NullPointerException has no class here: a .NET ArgumentNullException, as Python's TypeError.
        ArgumentNullException argument = Assert.IsType<ArgumentNullException>(read);
        Assert.Null(argument.ParamName);
        Assert.Equal(NonAsciiMessage, read.Message);
        Assert.Null(read.InnerException);
    }

    [Fact]
    public void ABareKafkaError_IsExactlyAKafkaException_WithIdMinusOne()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafka(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        Assert.Equal(-1, NativeMethods.Code(error));
        Assert.Equal(0, NativeMethods.ErrorIsApiError(error, ref outErr));
        Assert.Equal(IntPtr.Zero, outErr);

        Exception read = KafkaException.FromFfiError(error, owned: true)!;

        KafkaException bare = Assert.IsType<KafkaException>(read);
        Assert.Equal(-1, bare.Code);
        Assert.Equal(UnknownServerErrorMessage, bare.Message);
        Assert.Null(bare.InnerException);
    }

    [Fact]
    public void AKafkaFactory_GivesItsGeneratedClass()
    {
        IntPtr error = Build(NativeMethods.ErrorTimeout, NonAsciiMessage);

        Exception read = KafkaException.FromFfiError(error, owned: true)!;

        KafkaTimeoutException timeout = Assert.IsType<KafkaTimeoutException>(read);
        Assert.Equal(7, timeout.Code);
        Assert.Equal(NonAsciiMessage, timeout.Message);
        Assert.IsAssignableFrom<RetriableException>(timeout);
        Assert.Null(timeout.InnerException);
    }

    [Fact]
    public void IdMinusOne_IsUnknownServerException_WhenItIsAnApiError_AndABareKafkaException_Otherwise()
    {
        var cause = new InvalidOperationException("cause");

        Exception api = KafkaException.OfClass(-1, isApiError: true, "broker", cause, IntPtr.Zero);
        Exception bare = KafkaException.OfClass(-1, isApiError: false, "local", cause, IntPtr.Zero);

        UnknownServerException server = Assert.IsType<UnknownServerException>(api);
        Assert.IsAssignableFrom<ApiException>(server);
        Assert.Equal(-1, server.Code);
        Assert.Equal("broker", server.Message);
        Assert.Same(cause, server.InnerException);
        KafkaException kafka = Assert.IsType<KafkaException>(bare);
        Assert.Equal(-1, kafka.Code);
        Assert.Equal("local", kafka.Message);
        Assert.Same(cause, kafka.InnerException);
    }

    [Fact]
    public void TheWrapOfACarriedException_ThatIsNotAKafkaException_IsExactlyAKafkaException()
    {
        var thrown = new ListenerException("listener failed: Ω");
        IntPtr wrap = BuildWithSource("LocalCallbackError: listener failed: Ω", ManagedExceptionCarrier.ToError(thrown));
        Assert.Equal(-1, NativeMethods.Code(wrap));
        IntPtr outErr = IntPtr.Zero;
        Assert.Equal(0, NativeMethods.ErrorIsApiError(wrap, ref outErr));
        Assert.Equal(IntPtr.Zero, outErr);

        Exception read = KafkaException.FromFfiError(wrap, owned: true)!;

        KafkaException kafka = Assert.IsType<KafkaException>(read);
        Assert.Equal(-1, kafka.Code);
        Assert.Equal(typeof(ListenerException).FullName + ": listener failed: Ω", kafka.Message);
        Assert.Same(thrown, kafka.InnerException);
    }

    [Fact]
    public void TheWrapOfACarriedKafkaException_IsThatException()
    {
        var thrown = new UnknownTopicOrPartitionException("listener failed: Ω");
        IntPtr wrap = BuildWithSource("LocalCallbackError: listener failed: Ω", ManagedExceptionCarrier.ToError(thrown));

        Assert.Same(thrown, KafkaException.FromFfiError(wrap, owned: true));
    }

    [Fact]
    public void EveryRegistryEntry_BuildsItsClass_WithTheMessageAndTheCause()
    {
        var cause = new InvalidOperationException("cause");
        var problems = new List<string>();

        foreach (int code in ErrorClasses.Factories.Keys)
        {
            foreach (bool isApiError in new[] { false, true })
            {
                Exception read = KafkaException.OfClass(code, isApiError, "m", cause, IntPtr.Zero);
                if (!ErrorClasses.Ids.TryGetValue(read.GetType(), out int id) || id != code)
                {
                    problems.Add($"Id {code} builds {read.GetType().FullName}, whose id is not {code}.");
                }
                else if (read.Message != "m" || !ReferenceEquals(cause, read.InnerException))
                {
                    problems.Add($"Id {code} ({read.GetType().FullName}) does not keep its message and cause.");
                }
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData(99999)]
    [InlineData(-9999)]
    public void AnUnknownId_FallsBackToAKafkaException_WithCodeMinusOne(int code)
    {
        var cause = new InvalidOperationException("cause");

        Exception read = KafkaException.OfClass(code, isApiError: false, "from a newer core", cause, IntPtr.Zero);

        KafkaException kafka = Assert.IsType<KafkaException>(read);
        Assert.Equal(-1, kafka.Code);
        Assert.Equal("from a newer core", kafka.Message);
        Assert.Same(cause, kafka.InnerException);
    }

    [Fact]
    public async Task AnOwnerCheckRejection_AndTheCoresConcurrentAccessError_HaveTheSameClass()
    {
        var tp = new TopicPartition(Topic, 0);
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray(), Serdes.ByteArray());
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);
            Exception? fromTheCore = null;
            Exception? fromTheOwnerCheck = null;
            await TestTimeout.Run(
                () => consumer.CommitAsync(
                    new Dictionary<TopicPartition, OffsetAndMetadata>(),
                    new DelegateCallback(() =>
                    {
                        fromTheCore = Record.Exception(() => consumer.Subscription());
                        RunOnAnotherThread(() => fromTheOwnerCheck = Record.Exception(() => consumer.Assignment()));
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata>()), s_deadline);

            Exception built = KafkaException.FromFfiError(
                Build(NativeMethods.ErrorLocalConcurrentModification, ConcurrentMessage), owned: true)!;
            foreach (Exception? refused in new[] { fromTheCore, fromTheOwnerCheck, built })
            {
                ConcurrentModificationException concurrent = Assert.IsType<ConcurrentModificationException>(refused);
                Assert.Equal(ConcurrentMessage, concurrent.Message);
            }

            Assert.Equal(-2, ErrorClasses.Ids[typeof(ConcurrentModificationException)]);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static IntPtr Build(MessageFactory factory, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = factory(pinned.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private static IntPtr BuildWithSource(string message, IntPtr source)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafkaMessageSource(pinned.Pointer, source, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private static void RunOnAnotherThread(Action action)
    {
        Exception? failure = null;
        Thread other = new Thread(() => failure = Record.Exception(action));
        other.IsBackground = true;
        other.Start();
        if (!other.Join(s_deadline))
        {
            throw new System.TimeoutException("the other thread's call did not return");
        }

        if (failure is not null)
        {
            throw new InvalidOperationException("the other thread failed", failure);
        }
    }

    private static Dictionary<string, object?> Config() => new Dictionary<string, object?>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "from-ffi-error-group",
        ["enable.auto.commit"] = "false",
    };

    /// <summary>An exception type that is not a <see cref="KafkaException"/>, carried through the core.</summary>
    private sealed class ListenerException : Exception
    {
        public ListenerException(string message)
            : base(message)
        {
        }
    }

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
