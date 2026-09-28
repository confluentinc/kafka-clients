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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// S1 (M17/P1, CP1): the 18 transaction and idempotency P/Invoke declarations of PLAN §4.4 are
/// exactly right, and the 14 synchronous ones resolve against the native library.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why structural.</b> A behavioural test cannot reliably see a missing <c>[MarshalAs(I1)]</c> on
/// the hosts this suite runs on. A one-byte C <c>bool</c> argument gets a register of its own, so it
/// reads back correctly either way (measured in M15/P2b, see
/// <see cref="AdminNativeMethodsMarshallingTests"/>), and a four-byte read of a one-byte return is
/// wrong only when the bits above that byte happen to be set. So the entry point, the calling
/// convention, every <c>I1</c> and the parameter shapes, names included, are asserted by reflection,
/// against a table transcribed from the prototypes in the generated <c>confluent_kafka.h</c>.
/// </para>
/// <para>
/// <b>Why also called.</b> A wrong <c>EntryPoint</c> throws <see cref="EntryPointNotFoundException"/>
/// only when the method is first called, so the call is the proof that it resolves. The smoke calls
/// each of the 14 synchronous declarations: the control operations and the mock helpers on a mock
/// producer, the group-metadata constructor and the five predicates on handles of their own. The
/// four async submits need a completion context; S6 (CP4) resolves them through production's own
/// submit path.
/// </para>
/// </remarks>
public sealed class ProducerTransactionNativeMethodsTests
{
    private const string BeginTransactionAsyncSymbol = "kafka_producer_Producer_begin_transaction_async";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// PLAN §4.4, row by row: the C# name, the ABI symbol, and the C# signature that maps the
    /// header prototype through ffi §0.1's type map (<c>SafeProducerHandle</c> for the sync
    /// operations and the mock helpers, a raw <c>IntPtr</c> for the async submits). Every parameter
    /// carries the header's name, camelCased. For row 12's three out-params that keeps the header's
    /// <c>out_</c> prefix, where §4.4's column writes <c>offset</c> / <c>leaderEpoch</c> /
    /// <c>metadata</c> (a deviation the CP1 gate record states).
    /// </summary>
    private static readonly Declaration[] s_declarations =
    {
        new Declaration(1, "ProducerInitTransactions", "kafka_producer_Producer_init_transactions", "IntPtr (SafeProducerHandle producer)"),
        new Declaration(2, "ProducerBeginTransaction", "kafka_producer_Producer_begin_transaction", "IntPtr (SafeProducerHandle producer)"),
        new Declaration(
            3,
            "ProducerSendOffsetsToTransaction",
            "kafka_producer_Producer_send_offsets_to_transaction",
            "IntPtr (SafeProducerHandle producer, IntPtr[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, "
                + "IntPtr[] metadata, int count, IntPtr groupMetadata)"),
        new Declaration(4, "ProducerCommitTransaction", "kafka_producer_Producer_commit_transaction", "IntPtr (SafeProducerHandle producer)"),
        new Declaration(5, "ProducerAbortTransaction", "kafka_producer_Producer_abort_transaction", "IntPtr (SafeProducerHandle producer)"),
        new Declaration(
            6,
            "ProducerInitTransactionsAsync",
            "kafka_producer_Producer_init_transactions_async",
            "void (IntPtr producer, ProducerCallbacks.OperationCallback callback, IntPtr userData)"),
        new Declaration(
            7,
            "ProducerSendOffsetsToTransactionAsync",
            "kafka_producer_Producer_send_offsets_to_transaction_async",
            "void (IntPtr producer, IntPtr[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, "
                + "IntPtr[] metadata, int count, IntPtr groupMetadata, ProducerCallbacks.OperationCallback callback, IntPtr userData)"),
        new Declaration(
            8,
            "ProducerCommitTransactionAsync",
            "kafka_producer_Producer_commit_transaction_async",
            "void (IntPtr producer, ProducerCallbacks.OperationCallback callback, IntPtr userData)"),
        new Declaration(
            9,
            "ProducerAbortTransactionAsync",
            "kafka_producer_Producer_abort_transaction_async",
            "void (IntPtr producer, ProducerCallbacks.OperationCallback callback, IntPtr userData)"),
        new Declaration(
            10,
            "MockProducerSetCommitTransactionError",
            "kafka_producer_MockProducer_set_commit_transaction_error",
            "bool (SafeProducerHandle producer, bool clear, int errorCode, IntPtr errorMessage)"),
        new Declaration(11, "MockProducerSentOffsets", "kafka_producer_MockProducer_sent_offsets", "bool (SafeProducerHandle producer)"),
        new Declaration(
            12,
            "MockProducerCommittedOffset",
            "kafka_producer_MockProducer_committed_offset",
            "bool (SafeProducerHandle producer, IntPtr groupId, IntPtr topic, int partition, "
                + "out long outOffset, out int outLeaderEpoch, [Out] byte[] outMetadata, int metadataCap)"),
        new Declaration(
            13,
            "ConsumerGroupMetadataNew",
            "kafka_consumer_ConsumerGroupMetadata_new",
            "IntPtr (IntPtr groupId, int generationId, IntPtr memberId, IntPtr groupInstanceId)"),
        new Declaration(14, "IsTransactionAbortableError", "kafka_common_Error_is_transaction_abortable_error", "bool (IntPtr error)"),
        new Declaration(15, "IsApplicationRecoverableError", "kafka_common_Error_is_application_recoverable_error", "bool (IntPtr error)"),
        new Declaration(16, "IsInvalidConfigurationError", "kafka_common_Error_is_invalid_configuration_error", "bool (IntPtr error)"),
        new Declaration(17, "IsAuthorizationError", "kafka_common_Error_is_authorization_error", "bool (IntPtr error)"),
        new Declaration(18, "IsOutOfOrderSequenceError", "kafka_common_Error_is_out_of_order_sequence_error", "bool (IntPtr error)"),
    };

    /// <summary>
    /// Each of the 18 exists exactly once, is a P/Invoke into <c>confluent_kafka</c>, names its full
    /// ABI symbol as its <c>EntryPoint</c> (without it the marshaller would probe the short C# name
    /// and throw at run time, ffi §0.1) and is <c>Cdecl</c>, matching the Rust exports'
    /// <c>extern "C"</c>.
    /// </summary>
    [Fact]
    public void TheEighteenDeclarations_BindTheirSymbols_WithCdecl()
    {
        Assert.Equal(18, s_declarations.Length);
        Assert.Equal(18, s_declarations.Select(declaration => declaration.EntryPoint).Distinct().Count());

        List<string> expected = new List<string>();
        List<string> actual = new List<string>();
        foreach (Declaration declaration in s_declarations)
        {
            expected.Add($"#{declaration.Row} {declaration.Name} -> confluent_kafka!{declaration.EntryPoint}, Cdecl");

            MethodInfo[] matches = NativeMethodsNamed(declaration.Name);
            if (matches.Length != 1)
            {
                actual.Add($"#{declaration.Row} {declaration.Name}: {matches.Length} declarations");
                continue;
            }

            MethodInfo method = matches[0];
            DllImportAttribute? import = method.Attributes.HasFlag(MethodAttributes.PinvokeImpl)
                ? method.GetCustomAttribute<DllImportAttribute>()
                : null;
            actual.Add(import is null
                ? $"#{declaration.Row} {declaration.Name}: not a P/Invoke"
                : $"#{declaration.Row} {method.Name} -> {import.Value}!{import.EntryPoint}, {import.CallingConvention}");
        }

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Each declaration's return type and parameters, in order, are exactly the table's: each
    /// parameter's type, its by-reference or <c>[Out]</c> mark, and its name. Rows 1-5 and 10-12 take
    /// the <see cref="SafeProducerHandle"/> first (the sync convention), rows 6-9 take a raw
    /// <see cref="IntPtr"/> first and end with the rooted void-result callback plus <c>userData</c>
    /// (the async span-the-op convention), and rows 13-18 take only raw handles and scalars. The marks
    /// are part of the shape, so row 12's three caller-owned out-params cannot silently become
    /// in-params. So are the names: P/Invoke passes arguments by position, so where parameters share a
    /// type (row 13's three strings, for one) the name is the declaration's only statement of which
    /// header parameter sits at which position, and a same-typed reorder would pass on types alone.
    /// </summary>
    [Fact]
    public void TheEighteenDeclarations_HaveTheHeaderParameterShapes()
    {
        List<string> expected = s_declarations
            .Select(declaration => $"#{declaration.Row} {declaration.Name}: {declaration.Shape}")
            .ToList();
        List<string> actual = s_declarations
            .Select(declaration => $"#{declaration.Row} {declaration.Name}: {ShapeOf(SingleNativeMethod(declaration.Name))}")
            .ToList();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Every C <c>bool</c> on the 18 is marshalled as one byte: the returns of rows 10-12 and 14-18,
    /// and row 10's <c>clear</c> argument (ffi §0.1: the default is a four-byte Win32 <c>BOOL</c>).
    /// The exact counts are asserted too, so the sweep cannot pass by seeing nothing.
    /// </summary>
    [Fact]
    public void EveryBoolOnTheEighteen_IsMarshalledAsI1()
    {
        List<string> unmarked = new List<string>();
        int boolReturns = 0;
        int boolParameters = 0;

        foreach (Declaration declaration in s_declarations)
        {
            MethodInfo method = SingleNativeMethod(declaration.Name);
            if (method.ReturnType == typeof(bool))
            {
                boolReturns++;
                if (MarshalAsOf(method.ReturnParameter) != UnmanagedType.I1)
                {
                    unmarked.Add($"{method.Name}(return)");
                }
            }

            foreach (ParameterInfo parameter in method.GetParameters())
            {
                if (parameter.ParameterType == typeof(bool) || parameter.ParameterType == typeof(bool).MakeByRefType())
                {
                    boolParameters++;
                    if (MarshalAsOf(parameter) != UnmanagedType.I1)
                    {
                        unmarked.Add($"{method.Name}({parameter.Name})");
                    }
                }
            }
        }

        Assert.Equal(new List<string>(), unmarked);
        Assert.Equal(8, boolReturns);
        Assert.Equal(1, boolParameters);
    }

    /// <summary>
    /// PLAN B8 / Q5: <c>kafka_producer_Producer_begin_transaction_async</c> exists in the header but is
    /// deliberately not bound, because Java's <c>beginTransaction()</c> never blocks and so has no
    /// async member to back.
    /// </summary>
    [Fact]
    public void BeginTransactionAsync_IsNotDeclared()
    {
        Dictionary<string, MethodInfo[]> byEntryPoint = AllImports()
            .GroupBy(EntryPointOf)
            .ToDictionary(group => group.Key, group => group.ToArray());

        Assert.False(
            byEntryPoint.ContainsKey(BeginTransactionAsyncSymbol),
            $"{BeginTransactionAsyncSymbol} must stay unbound (PLAN B8, Q5)");

        // Control-positive: the same sweep does see the sync sibling and the other four async
        // control operations, so the absence above is not an artifact of the sweep.
        Assert.True(byEntryPoint.ContainsKey("kafka_producer_Producer_begin_transaction"));
        Assert.True(byEntryPoint.ContainsKey("kafka_producer_Producer_init_transactions_async"));
        Assert.True(byEntryPoint.ContainsKey("kafka_producer_Producer_send_offsets_to_transaction_async"));
        Assert.True(byEntryPoint.ContainsKey("kafka_producer_Producer_commit_transaction_async"));
        Assert.True(byEntryPoint.ContainsKey("kafka_producer_Producer_abort_transaction_async"));
    }

    /// <summary>
    /// The producer's transaction surface is closed: every <c>kafka_producer_*</c> declaration whose
    /// symbol mentions a transaction is one of §4.4's ten (the nine control operations and the
    /// commit-error hook), each bound exactly once. The other Java mock transaction hooks have no C
    /// symbol (PLAN B7), so a declaration outside this set would be a new decision, not a typo.
    /// </summary>
    [Fact]
    public void TheProducerTransactionFamily_IsExactlyTheBoundSymbols()
    {
        List<string> expected = new List<string>
        {
            "kafka_producer_MockProducer_set_commit_transaction_error",
            "kafka_producer_Producer_abort_transaction",
            "kafka_producer_Producer_abort_transaction_async",
            "kafka_producer_Producer_begin_transaction",
            "kafka_producer_Producer_commit_transaction",
            "kafka_producer_Producer_commit_transaction_async",
            "kafka_producer_Producer_init_transactions",
            "kafka_producer_Producer_init_transactions_async",
            "kafka_producer_Producer_send_offsets_to_transaction",
            "kafka_producer_Producer_send_offsets_to_transaction_async",
        };

        List<string> actual = AllImports()
            .Select(EntryPointOf)
            .Where(entryPoint =>
                entryPoint.StartsWith("kafka_producer_", StringComparison.Ordinal)
                && entryPoint.IndexOf("transaction", StringComparison.Ordinal) >= 0)
            .OrderBy(entryPoint => entryPoint, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Resolution smoke for rows 1-5 and 10-13 on a <see cref="NativeProducer.CreateMock"/> handle: the
    /// five sync control operations in a valid order (init; begin; send-offsets with
    /// <c>count == 0</c> and a transient group metadata; commit; begin; abort) and the three mock
    /// helpers, each called for real. A wrong entry point throws only when called, so the call is the
    /// proof.
    /// </summary>
    /// <remarks>
    /// The commit-error hook is installed and then cleared before <c>initTransactions</c>, its
    /// setup-only position (the header). The commit then succeeds, which shows the clear reached the
    /// core, and the installed code 120 never surfaces. With nothing staged, <c>sentOffsets()</c> is
    /// <see langword="false"/> after an empty send-offsets (Java's <c>MockProducer</c> returns for an
    /// empty map before it sets the flag, <c>MockProducer.java:194-200</c>) and the committed-offset
    /// lookup finds nothing.
    /// </remarks>
    [Fact]
    public void TheSyncControlOperations_AndTheMockHelpers_ResolveAndRun_OnAMockHandle()
    {
        NativeProducer producer = NativeProducer.CreateMock();
        try
        {
            TestTimeout.Run(() => RunTransactionSequence(producer.Handle), s_deadline);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    /// <summary>
    /// Row 13 resolves, and the native reads its arguments in the header's order: the call passes a
    /// distinct value for each field, by position in the header's order, and each is read back
    /// through the shipped accessors before the owned handle is destroyed. A non-ASCII group id also
    /// covers the UTF-8 input path (ffi §A3), and a null instance id reads back as absent. Passing by
    /// position, the call cannot see which names the declaration gives its parameters, so a
    /// same-typed swap there leaves this fact green; the declaration's order and names are
    /// <see cref="TheEighteenDeclarations_HaveTheHeaderParameterShapes"/>'s job.
    /// </summary>
    [Fact]
    public void ConsumerGroupMetadataNew_Resolves_AndPassesItsFieldsInHeaderOrder()
    {
        IntPtr metadata = NewGroupMetadata("s1-group-ü", 7, "s1-member", "s1-instance");
        try
        {
            Assert.NotEqual(IntPtr.Zero, metadata);
            Assert.Equal("s1-group-ü", Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(metadata)));
            Assert.Equal(7, NativeMethods.ConsumerGroupMetadataGenerationId(metadata));
            Assert.Equal("s1-member", Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(metadata)));
            Assert.Equal("s1-instance", Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupInstanceId(metadata)));
        }
        finally
        {
            NativeMethods.ConsumerGroupMetadataDestroy(metadata);
        }

        IntPtr noInstance = NewGroupMetadata("s1-group", -1, string.Empty, null);
        try
        {
            Assert.NotEqual(IntPtr.Zero, noInstance);
            Assert.Equal("s1-group", Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(noInstance)));
            Assert.Equal(-1, NativeMethods.ConsumerGroupMetadataGenerationId(noInstance));
            Assert.Equal(string.Empty, Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(noInstance)));
            Assert.Equal(IntPtr.Zero, NativeMethods.ConsumerGroupMetadataGroupInstanceId(noInstance));
        }
        finally
        {
            NativeMethods.ConsumerGroupMetadataDestroy(noInstance);
        }
    }

    /// <summary>
    /// Rows 14-18 resolve, and each answers <see langword="false"/> for a null handle (the header).
    /// </summary>
    [Fact]
    public void TheFivePredicates_Resolve_AndAnswerFalseForANullHandle()
    {
        Assert.Equal(
            new[]
            {
                "IsTransactionAbortableError=False",
                "IsApplicationRecoverableError=False",
                "IsInvalidConfigurationError=False",
                "IsAuthorizationError=False",
                "IsOutOfOrderSequenceError=False",
            },
            ReadPredicates(IntPtr.Zero));
    }

    /// <summary>
    /// On a <c>TransactionAbortable</c> error (code 120, built with <c>kafka_common_Error_new</c>) only
    /// the transaction-abortable predicate holds. So this row breaks if
    /// <see cref="NativeMethods.IsTransactionAbortableError"/> swaps entry points with one of the
    /// other four, but not under a swap among those four, which all read <see langword="false"/>
    /// here; <see cref="TheEighteenDeclarations_BindTheirSymbols_WithCdecl"/> catches every swap. The
    /// handle is borrowed by the predicates and destroyed afterwards. The code is read back first: the
    /// header maps an unassigned code to <c>UnknownServerError</c> (-1), which would make the row
    /// meaningless.
    /// </summary>
    [Fact]
    public void TheFivePredicates_OnATransactionAbortableError_OnlyTheTransactionAbortableOneHolds()
    {
        IntPtr error = NativeMethods.KafkaErrorNew(120, IntPtr.Zero);
        try
        {
            Assert.NotEqual(IntPtr.Zero, error);
            Assert.Equal(120, NativeMethods.Code(error));
            Assert.Equal(
                new[]
                {
                    "IsTransactionAbortableError=True",
                    "IsApplicationRecoverableError=False",
                    "IsInvalidConfigurationError=False",
                    "IsAuthorizationError=False",
                    "IsOutOfOrderSequenceError=False",
                },
                ReadPredicates(error));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    private static void RunTransactionSequence(SafeProducerHandle producer)
    {
        using (Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("S1 installed commit error"))
        {
            Assert.True(NativeMethods.MockProducerSetCommitTransactionError(producer, false, 120, message.Pointer));
        }

        Assert.True(NativeMethods.MockProducerSetCommitTransactionError(producer, true, 0, IntPtr.Zero));

        AssertSucceeded(NativeMethods.ProducerInitTransactions(producer), "init_transactions");
        AssertSucceeded(NativeMethods.ProducerBeginTransaction(producer), "begin_transaction");

        IntPtr groupMetadata = NewGroupMetadata("s1-group", -1, string.Empty, null);
        try
        {
            // count == 0 reads no array (the header); the zero-length arrays are the shape
            // NativeConsumer.WithPinnedCommitOffsets hands over for an empty map.
            AssertSucceeded(
                NativeMethods.ProducerSendOffsetsToTransaction(
                    producer, new IntPtr[0], new int[0], new long[0], new int[0], new IntPtr[0], 0, groupMetadata),
                "send_offsets_to_transaction");
        }
        finally
        {
            NativeMethods.ConsumerGroupMetadataDestroy(groupMetadata);
        }

        Assert.False(NativeMethods.MockProducerSentOffsets(producer));

        AssertSucceeded(NativeMethods.ProducerCommitTransaction(producer), "commit_transaction");

        using (Utf8Marshal.PinnedUtf8String groupId = Utf8Marshal.Pin("s1-group"))
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin("s1-topic"))
        {
            byte[] metadataBuffer = new byte[16];
            Assert.False(NativeMethods.MockProducerCommittedOffset(
                producer, groupId.Pointer, topic.Pointer, 0, out _, out _, metadataBuffer, metadataBuffer.Length));
        }

        AssertSucceeded(NativeMethods.ProducerBeginTransaction(producer), "begin_transaction (second transaction)");
        AssertSucceeded(NativeMethods.ProducerAbortTransaction(producer), "abort_transaction");
    }

    /// <summary>
    /// Reads and frees a control operation's owned error handle (<see cref="KafkaException.FromHandle"/>
    /// frees it exactly once, and maps <see cref="IntPtr.Zero"/> to <see langword="null"/>), failing
    /// with the core's code and message if there was one.
    /// </summary>
    private static void AssertSucceeded(IntPtr error, string operation)
    {
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            Assert.Fail($"{operation} returned an error: code {failure.Code}, message '{failure.Message}'");
        }
    }

    /// <summary>
    /// Builds a transient owned group-metadata handle; the strings are pinned only for the call,
    /// because the core copies them (the D12 input-handle rule). The caller destroys the handle.
    /// </summary>
    private static IntPtr NewGroupMetadata(string groupId, int generationId, string memberId, string? groupInstanceId)
    {
        using Utf8Marshal.PinnedUtf8String group = Utf8Marshal.Pin(groupId);
        using Utf8Marshal.PinnedUtf8String member = Utf8Marshal.Pin(memberId);
        using Utf8Marshal.PinnedUtf8String? instance = groupInstanceId is null ? null : Utf8Marshal.Pin(groupInstanceId);
        return NativeMethods.ConsumerGroupMetadataNew(
            group.Pointer, generationId, member.Pointer, instance?.Pointer ?? IntPtr.Zero);
    }

    private static string[] ReadPredicates(IntPtr error) => new[]
    {
        $"IsTransactionAbortableError={NativeMethods.IsTransactionAbortableError(error)}",
        $"IsApplicationRecoverableError={NativeMethods.IsApplicationRecoverableError(error)}",
        $"IsInvalidConfigurationError={NativeMethods.IsInvalidConfigurationError(error)}",
        $"IsAuthorizationError={NativeMethods.IsAuthorizationError(error)}",
        $"IsOutOfOrderSequenceError={NativeMethods.IsOutOfOrderSequenceError(error)}",
    };

    private static MethodInfo[] AllImports() =>
        typeof(NativeMethods)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Attributes.HasFlag(MethodAttributes.PinvokeImpl))
            .ToArray();

    private static string EntryPointOf(MethodInfo method) =>
        method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint ?? method.Name;

    private static MethodInfo[] NativeMethodsNamed(string name) =>
        typeof(NativeMethods)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Name == name)
            .ToArray();

    private static MethodInfo SingleNativeMethod(string name)
    {
        MethodInfo[] matches = NativeMethodsNamed(name);
        Assert.True(matches.Length == 1, $"expected exactly one NativeMethods.{name}, found {matches.Length}");
        return matches[0];
    }

    /// <summary>
    /// Renders a declaration as <c>ReturnType (ParameterType parameterName, …)</c>, with <c>out</c> for
    /// a by-reference out-param and <c>[Out]</c> for an out-marked array, so it reads like §4.4's
    /// signature column. The names are rendered because P/Invoke binds arguments by position: without
    /// them, a reorder among parameters of the same type is invisible.
    /// </summary>
    private static string ShapeOf(MethodInfo method)
    {
        IEnumerable<string> parameters = method.GetParameters().Select(parameter =>
        {
            Type type = parameter.ParameterType;
            string typeName = type.IsByRef
                ? (parameter.IsOut ? "out " : "ref ") + TypeName(type.GetElementType()!)
                : (parameter.IsOut ? "[Out] " : string.Empty) + TypeName(type);
            return $"{typeName} {parameter.Name}";
        });

        return $"{TypeName(method.ReturnType)} ({string.Join(", ", parameters)})";
    }

    private static string TypeName(Type type)
    {
        if (type.IsArray)
        {
            return TypeName(type.GetElementType()!) + "[]";
        }

        if (type == typeof(void))
        {
            return "void";
        }

        if (type == typeof(bool))
        {
            return "bool";
        }

        if (type == typeof(byte))
        {
            return "byte";
        }

        if (type == typeof(int))
        {
            return "int";
        }

        if (type == typeof(long))
        {
            return "long";
        }

        return type.DeclaringType is null ? type.Name : $"{type.DeclaringType.Name}.{type.Name}";
    }

    private static UnmanagedType? MarshalAsOf(ParameterInfo parameter)
    {
        object[] attributes = parameter.GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false);
        return attributes.Length == 0 ? null : ((MarshalAsAttribute)attributes[0]).Value;
    }

    /// <summary>One row of PLAN §4.4.</summary>
    private sealed class Declaration
    {
        internal Declaration(int row, string name, string entryPoint, string shape)
        {
            Row = row;
            Name = name;
            EntryPoint = entryPoint;
            Shape = shape;
        }

        internal int Row { get; }

        internal string Name { get; }

        internal string EntryPoint { get; }

        internal string Shape { get; }
    }
}
