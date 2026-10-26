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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ListTopics</c> on the four admin clients over one native <c>names_to_listings</c> future, its <c>Listings</c> and
/// <c>Names</c> projections, the C options built from the setters that ran, and the native future's reads, deliveries
/// and release.
/// </summary>
public sealed class PublicAdminListTopicsTests
{
    private const BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;

    private const string Orders = "orders";
    private const string Offsets = "__consumer_offsets";

    private const string Mock = "mock";
    private const string AsyncMock = "async-mock";
    private const string Real = "real";
    private const string AsyncReal = "async-real";

    private const string ThreadExited = "The AdminClient thread has exited. Call: listTopics";

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    [Theory]
    [InlineData(null)]
    [InlineData(false)]
    [InlineData(true)]
    public void AMock_ListsEveryTopicWithItsFlagAndId_WhateverListInternalSays(bool? listInternal)
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            Seed(admin);
            ListTopicsResult result = admin.ListTopics(OptionsFor(listInternal));

            AssertSeeded((IReadOnlyDictionary<string, TopicListing>)BlockingCall.RunGet(() => result.NamesToListings().Get())!);
            AssertListings((IReadOnlyCollection<TopicListing>)BlockingCall.RunGet(() => result.Listings().Get())!);
            AssertNames((IReadOnlyCollection<string>)BlockingCall.RunGet(() => result.Names().Get())!);
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnAsyncMock_ListsEveryTopicWithItsFlagAndId_WhateverListInternalSays(bool? listInternal)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        try
        {
            Seed(admin);
            AsyncListTopicsResult result = admin.ListTopics(OptionsFor(listInternal));

            AssertSeeded(await TestTimeout.Run(() => result.NamesToListings().Get(), BlockingCall.Deadline));
            AssertListings(await TestTimeout.Run(() => result.Listings().Get(), BlockingCall.Deadline));
            AssertNames(await TestTimeout.Run(() => result.Names().Get(), BlockingCall.Deadline));
        }
        finally
        {
            await TestTimeout.DisposeAsync(admin);
        }
    }

    [Fact]
    public void AnEmptyMock_ListsNoTopics_AndItsProjectionsAreEmpty()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            ListTopicsResult result = admin.ListTopics();

            Assert.Empty((IReadOnlyDictionary<string, TopicListing>)BlockingCall.RunGet(() => result.NamesToListings().Get())!);
            Assert.Empty((IReadOnlyCollection<TopicListing>)BlockingCall.RunGet(() => result.Listings().Get())!);
            Assert.Empty((IReadOnlyCollection<string>)BlockingCall.RunGet(() => result.Names().Get())!);
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public async Task TheAsyncRealClient_FailsTheFutureAndBothProjections_WithTheCoresErrorOnClose()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
        try
        {
            AsyncListTopicsResult result = admin.ListTopics(new ListTopicsOptions { ListInternal = true });
            Assert.False(result.NamesToListings().IsDone);

            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);

            AssertThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => result.NamesToListings().Get(), BlockingCall.Deadline)));
            AssertThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => result.Listings().Get(), BlockingCall.Deadline)));
            AssertThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => result.Names().Get(), BlockingCall.Deadline)));
        }
        finally
        {
            await TestTimeout.DisposeAsync(admin);
        }
    }

    [Theory]
    [InlineData(Mock)]
    [InlineData(AsyncMock)]
    [InlineData(Real)]
    [InlineData(AsyncReal)]
    public async Task ListTopicsAfterDispose_ThrowsObjectDisposed_NamingTheClient(string kind)
    {
        object admin = Create(kind);
        if (admin is IAsyncDisposable asyncAdmin)
        {
            await TestTimeout.DisposeAsync(asyncAdmin);
        }
        else
        {
            TestTimeout.Dispose((IDisposable)admin);
        }

        ObjectDisposedException thrown = Assert.Throws<ObjectDisposedException>(() =>
        {
            _ = admin is IAdmin sync ? (object)sync.ListTopics() : ((IAsyncAdmin)admin).ListTopics();
        });
        string expected = kind is Mock or AsyncMock ? nameof(MockAdminClient) : nameof(KafkaAdminClient);
        Assert.Equal(expected, thrown.ObjectName);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheDelivery_RunsOnTheFlavoursDrainThread(bool async)
    {
        object admin = async ? AsyncMockAdminClient.Create().Build() : MockAdminClient.Create().Build();
        try
        {
            using ManualResetEventSlim ran = new ManualResetEventSlim();
            string? thread = null;
            using (new DrainGate(CompletionOf(admin)))
            {
                if (admin is IAdmin sync)
                {
                    _ = sync.ListTopics().NamesToListings().WhenComplete((_, _) =>
                    {
                        thread = Thread.CurrentThread.Name;
                        ran.Set();
                    });
                }
                else
                {
                    _ = ((IAsyncAdmin)admin).ListTopics().NamesToListings().WhenComplete((_, _) =>
                    {
                        thread = Thread.CurrentThread.Name;
                        ran.Set();
                    });
                }

                Assert.False(ran.IsSet);
            }

            Assert.True(ran.Wait(BlockingCall.Deadline));
            Assert.Equal(async ? "confluent-kafka-admin-pump" : "kafka-admin-client-thread", thread);
        }
        finally
        {
            await DisposeAny(admin);
        }
    }

    [Fact]
    public void IsDoneAndGetNow_AnswerFromTheHandle_BeforeTheDeliveryRuns()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            Seed(admin);
            CompletionThread completion = CompletionOf(admin);
            int listened = 0;
            string? listenedOn = null;
            SafeKafkaFutureHandle handle;
            using (new DrainGate(completion))
            {
                KafkaFuture<IReadOnlyDictionary<string, TopicListing>> future = admin.ListTopics().NamesToListings();
                handle = HandleOf(ImplOf(future));
                _ = future.WhenComplete((_, _) =>
                {
                    Interlocked.Increment(ref listened);
                    listenedOn = Thread.CurrentThread.Name;
                });

                Assert.True(future.IsDone);
                Assert.False(ImplOf(future).IsSettled);
                Assert.Equal(0, Volatile.Read(ref listened));

                IReadOnlyDictionary<string, TopicListing> absent = new Dictionary<string, TopicListing>();
                IReadOnlyDictionary<string, TopicListing> now = future.GetNow(absent);
                Assert.NotSame(absent, now);
                AssertSeeded(now);
                Assert.True(ImplOf(future).IsSettled);
                Assert.Equal(1, Volatile.Read(ref listened));
                Assert.Equal(Thread.CurrentThread.Name, listenedOn);
            }

            FlushUntil(completion, () => handle.IsClosed);
            Assert.True(handle.IsClosed);
            Assert.Equal(1, Volatile.Read(ref listened));
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public void AWhenCompleteOnA_ThatGetsADoneB_AnswersOnTheDrainThread_BeforeBsOwnDelivery()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            Seed(admin);
            List<string> events = new List<string>();
            IReadOnlyDictionary<string, TopicListing>? fromOther = null;
            string? thread = null;
            string? first = null;
            int claimed = 0;
            using ManualResetEventSlim ran = new ManualResetEventSlim();
            using (new DrainGate(CompletionOf(admin)))
            {
                KafkaFuture<IReadOnlyDictionary<string, TopicListing>> a = admin.ListTopics().NamesToListings();
                KafkaFuture<IReadOnlyDictionary<string, TopicListing>> b = admin.ListTopics().NamesToListings();
                Register(a, "a", b);
                Register(b, "b", a);
            }

            Assert.True(ran.Wait(BlockingCall.Deadline));
            Assert.Equal("kafka-admin-client-thread", thread);
            string other = first == "a" ? "b" : "a";
            Assert.Equal(new[] { first + "-start", other, first + "-end" }, events);
            AssertSeeded(fromOther!);

            void Register(
                KafkaFuture<IReadOnlyDictionary<string, TopicListing>> self,
                string name,
                KafkaFuture<IReadOnlyDictionary<string, TopicListing>> peer)
            {
                _ = self.WhenComplete((_, _) =>
                {
                    if (Interlocked.CompareExchange(ref claimed, 1, 0) != 0)
                    {
                        events.Add(name);
                        return;
                    }

                    first = name;
                    events.Add(name + "-start");
                    thread = Thread.CurrentThread.Name;
                    fromOther = peer.Get();
                    events.Add(name + "-end");
                    ran.Set();
                });
            }
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AClosedClient_SettlesItsPendingFuture_AndReleasesItsHandle_WhileTheResultLivesOn(bool async)
    {
        SafeKafkaFutureHandle handle;
        SafeAdminResultHandle native;
        if (async)
        {
            AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
            AsyncListTopicsResult result = admin.ListTopics();
            handle = HandleOf(ImplOf(result.NamesToListings()));
            native = result.Native!;
            Assert.False(handle.IsClosed);

            await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), BlockingCall.Deadline);
            await TestTimeout.DisposeAsync(admin);

            Assert.True(result.NamesToListings().IsCompletedExceptionally);
            AssertThreadExited(await Record.ExceptionAsync(() => TestTimeout.Run(() => result.Names().Get(), BlockingCall.Deadline)));
        }
        else
        {
            KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
            ListTopicsResult result = admin.ListTopics();
            handle = HandleOf(ImplOf(result.NamesToListings()));
            native = result.Native!;
            Assert.False(handle.IsClosed);

            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));
            TestTimeout.Dispose(admin);

            Assert.True(result.NamesToListings().IsCompletedExceptionally);
            BlockingCall.AssertThrew(BlockingCall.RunGet(() => result.Names().Get()), 7, ThreadExited);
        }

        Assert.True(handle.IsClosed);
        Assert.False(native.IsClosed);
        native.Dispose();
    }

    [Fact]
    public void TheResultHandle_OutlivesItsDeliveredFuture_AndNothingRootsIt()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            (SafeKafkaFutureHandle future, WeakReference result) = ListAndAbandon(admin);
            FlushUntil(CompletionOf(admin), () => future.IsClosed);

            Assert.True(future.IsClosed);
            Assert.True(Collected(result, () => true));
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public void TheResultHandlesFinalizer_FreesTheNativeResult()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            StrongBox<int> destroyed = new StrongBox<int>();
            WeakReference result = AbandonRawResult(admin, destroyed);

            Assert.True(Collected(result, () => Volatile.Read(ref destroyed.Value) == 1));
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0), ReadBack(null));
        Assert.Equal((-1, (sbyte)0), ReadBack(new ListTopicsOptions()));
        Assert.Empty(new ListTopicsOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal((expected, (sbyte)0), ReadBack(new ListTopicsOptions { TimeoutMs = timeoutMs }));
    }

    [Fact]
    public void ListInternal_ReachesTheCOptions_AndASetterRunTwiceSendsItsLastValue()
    {
        Assert.Equal((-1, (sbyte)1), ReadBack(new ListTopicsOptions { ListInternal = true }));

        ListTopicsOptions options = new ListTopicsOptions { ListInternal = true, TimeoutMs = 10 };
        options.ListInternal = false;
        options.TimeoutMs = 20;
        Assert.Equal((20, (sbyte)0), ReadBack(options));
    }

    [Fact]
    public void OnlyTheSettersThatRan_ReachTheCOptions_InTheOrderTheyLastRan()
    {
        ListTopicsOptions options = new ListTopicsOptions();
        options.ListInternal = true;
        options.TimeoutMs = 5;
        options.ListInternal = false;
        Assert.Equal(new[] { nameof(ListTopicsOptions.TimeoutMs), nameof(ListTopicsOptions.ListInternal) }, options.Setters.InOrder);

        ListTopicsOptions timeoutOnly = new ListTopicsOptions { TimeoutMs = null };
        Assert.Equal(new[] { nameof(ListTopicsOptions.TimeoutMs) }, Built(timeoutOnly));
        Assert.Equal(new[] { nameof(ListTopicsOptions.TimeoutMs), nameof(ListTopicsOptions.ListInternal) }, Built(options));
        Assert.Empty(Built(new ListTopicsOptions()));
    }

    [Fact]
    public void ASetterThatFails_FreesTheCOptions_AndRethrows()
    {
        InvalidOperationException failure = new InvalidOperationException("setter");
        int destroyed = 0;
        ListTopicsOptions options = new ListTopicsOptions { ListInternal = true };

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => AdminOptionsMarshal.Build(
            NativeMethods.ListTopicsOptionsNew,
            options,
            options.Setters,
            (IntPtr native, ListTopicsOptions o, string setter, ref IntPtr outErr) => throw failure,
            (IntPtr native, ref IntPtr outErr) =>
            {
                destroyed++;
                NativeMethods.ListTopicsOptionsDestroy(native, ref outErr);
            }));

        Assert.Same(failure, thrown);
        Assert.Equal(1, destroyed);
    }

    private static ListTopicsOptions? OptionsFor(bool? listInternal) =>
        listInternal is null ? null : new ListTopicsOptions { ListInternal = listInternal.Value };

    private static object Create(string kind) => kind switch
    {
        Mock => MockAdminClient.Create().Build(),
        AsyncMock => AsyncMockAdminClient.Create().Build(),
        Real => new KafkaAdminClient(s_unreachable),
        _ => new AsyncKafkaAdminClient(s_unreachable),
    };

    private static async Task DisposeAny(object admin)
    {
        if (admin is IAsyncDisposable asyncAdmin)
        {
            await TestTimeout.DisposeAsync(asyncAdmin);
        }
        else
        {
            TestTimeout.Dispose((IDisposable)admin);
        }
    }

    private static void Seed(object admin)
    {
        AddTopic(admin, Orders, isInternal: false);
        AddTopic(admin, Offsets, isInternal: true);
    }

    private static void AssertSeeded(IReadOnlyDictionary<string, TopicListing> listings)
    {
        Assert.Equal(new[] { Offsets, Orders }, listings.Keys.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(Orders, listings[Orders].Name);
        Assert.False(listings[Orders].IsInternal);
        Assert.Equal(Offsets, listings[Offsets].Name);
        Assert.True(listings[Offsets].IsInternal);
        Assert.NotEqual(Uuid.Zero, listings[Orders].TopicId);
        Assert.NotEqual(Uuid.Zero, listings[Offsets].TopicId);
        Assert.NotEqual(listings[Orders].TopicId, listings[Offsets].TopicId);
    }

    private static void AssertListings(IReadOnlyCollection<TopicListing> listings)
    {
        Assert.Equal(2, listings.Count);
        Assert.Equal(new[] { Offsets, Orders }, listings.Select(listing => listing.Name).OrderBy(name => name, StringComparer.Ordinal));
        Assert.True(listings.Single(listing => listing.Name == Offsets).IsInternal);
    }

    private static void AssertNames(IReadOnlyCollection<string> names) =>
        Assert.Equal(new[] { Offsets, Orders }, names.OrderBy(name => name, StringComparer.Ordinal));

    private static void AssertThreadExited(Exception? thrown)
    {
        KafkaException error = Assert.IsType<KafkaException>(thrown);
        Assert.Equal(7, error.Code);
        Assert.Equal(ThreadExited, error.Message);
        Assert.True(error.IsRetriable);
    }

    private static NativeAdminClient NativeOf(object admin) =>
        (NativeAdminClient)admin.GetType().GetField("_native", Private)!.GetValue(admin)!;

    private static SafeAdminClientHandle HandleOf(NativeAdminClient native) =>
        (SafeAdminClientHandle)typeof(NativeAdminClient).GetField("_handle", Private)!.GetValue(native)!;

    private static CompletionThread CompletionOf(object admin) =>
        (CompletionThread)typeof(NativeAdminClient).GetField("_completion", Private)!.GetValue(NativeOf(admin))!;

    private static KafkaFutureImpl<T> ImplOf<T>(KafkaFuture<T> future) =>
        (KafkaFutureImpl<T>)typeof(KafkaFuture<T>).GetField("_impl", Private)!.GetValue(future)!;

    private static KafkaFutureImpl<T> ImplOf<T>(AsyncKafkaFuture<T> future) =>
        (KafkaFutureImpl<T>)typeof(AsyncKafkaFuture<T>).GetField("_impl", Private)!.GetValue(future)!;

    private static SafeKafkaFutureHandle HandleOf<T>(KafkaFutureImpl<T> future)
    {
        object source = typeof(KafkaFutureImpl<T>).GetField("_source", Private)!.GetValue(future)!;
        return (SafeKafkaFutureHandle)typeof(NativeFutureSource<T>).GetField("_handle", Private)!.GetValue(source)!;
    }

    private static void AddTopic(object admin, string name, bool isInternal)
    {
        SafeAdminClientHandle handle = HandleOf(NativeOf(admin));
        bool added = false;
        try
        {
            IntPtr mock = handle.DangerousAddRefClass(ref added);
            using Utf8Marshal.PinnedUtf8String text = Utf8Marshal.Pin(name);
            IntPtr outErr = IntPtr.Zero;
            Native.MockAdminClientAddTopic(mock, isInternal ? (sbyte)1 : (sbyte)0, text.Pointer, IntPtr.Zero, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (SafeKafkaFutureHandle Future, WeakReference Result) ListAndAbandon(MockAdminClient admin)
    {
        ListTopicsResult result = admin.ListTopics();
        _ = BlockingCall.RunGet(() => result.NamesToListings().Get());
        Assert.False(result.Native!.IsClosed);
        return (HandleOf(ImplOf(result.NamesToListings())), new WeakReference(result.Native));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference AbandonRawResult(MockAdminClient admin, StrongBox<int> destroyed)
    {
        IntPtr options = NativeAdminClient.NewListTopicsOptions(null);
        IntPtr raw;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            raw = NativeMethods.AdminListTopicsWithOptions(HandleOf(NativeOf(admin)), options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            NativeAdminClient.FreeListTopicsOptions(options);
        }

        return new WeakReference(new SafeAdminResultHandle(raw, (IntPtr result, ref IntPtr outErr) =>
        {
            Interlocked.Increment(ref destroyed.Value);
            NativeMethods.ListTopicsResultDestroy(result, ref outErr);
        }));
    }

    private static bool Collected(WeakReference target, Func<bool> released)
    {
        DateTime deadline = DateTime.UtcNow + BlockingCall.Deadline;
        while (DateTime.UtcNow < deadline)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            if (!target.IsAlive && released())
            {
                return true;
            }

            Thread.Sleep(10);
        }

        return false;
    }

    private static void Flush(CompletionThread completion)
    {
        using ManualResetEventSlim drained = new ManualResetEventSlim();
        completion.PostAfterDrain(drained.Set);
        Assert.True(drained.Wait(BlockingCall.Deadline));
    }

    // The core queues a future's delivery from a task of its own, so one drain can run before it is queued.
    private static void FlushUntil(CompletionThread completion, Func<bool> done)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        do
        {
            Flush(completion);
        }
        while (!done() && elapsed.Elapsed < BlockingCall.Deadline);
    }

    private static (int TimeoutMs, sbyte ListInternal) ReadBack(ListTopicsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListTopicsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.ListTopicsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte listInternal = Native.ListTopicsOptionsShouldListInternal(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, listInternal);
        }
        finally
        {
            NativeAdminClient.FreeListTopicsOptions(native);
        }
    }

    private static List<string> Built(ListTopicsOptions options)
    {
        List<string> ran = new List<string>();
        IntPtr native = AdminOptionsMarshal.Build(
            NativeMethods.ListTopicsOptionsNew,
            options,
            options.Setters,
            (IntPtr n, ListTopicsOptions o, string setter, ref IntPtr outErr) => ran.Add(setter),
            NativeMethods.ListTopicsOptionsDestroy);
        NativeAdminClient.FreeListTopicsOptions(native);
        return ran;
    }

    /// <summary>Holds a client's drain thread inside a posted action until disposed.</summary>
    private sealed class DrainGate : IDisposable
    {
        private readonly ManualResetEventSlim _release = new ManualResetEventSlim();

        internal DrainGate(CompletionThread completion)
        {
            using ManualResetEventSlim entered = new ManualResetEventSlim();
            completion.Post(() =>
            {
                entered.Set();
                _release.Wait(BlockingCall.Deadline);
            });
            Assert.True(entered.Wait(BlockingCall.Deadline));
        }

        public void Dispose() => _release.Set();
    }

    /// <summary>The C calls only these tests make: seeding the mock, and reading the C options back.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_MockAdminClient_add_topic", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MockAdminClientAddTopic(
            IntPtr self, sbyte isInternal, IntPtr name, IntPtr partitions, IntPtr configs, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListTopicsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListTopicsOptions_should_list_internal_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte ListTopicsOptionsShouldListInternal(IntPtr self, ref IntPtr outErr);
    }
}
