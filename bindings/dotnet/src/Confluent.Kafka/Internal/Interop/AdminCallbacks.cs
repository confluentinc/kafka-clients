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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The managed side of the admin C ABI's completion callbacks — kept-alive, classic
/// <c>Cdecl</c> delegates, the only portable mechanism on the netstandard2.0 floor
/// (ffi §0.1). Admin is ffi §B6's <b>hookless one-shot per-operation</b> family: no
/// admin entry point takes a <c>user_data_destroy</c>, so the callback is the
/// <b>sole owner</b> of the per-operation <c>GCHandle</c> free.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>An admin callback can fire on one of three threads, and one of them is
/// yours.</b> Normally it runs on the client's dispatcher thread. It runs
/// <b>synchronously on the submitting thread, before the entry point returns</b>, when
/// the RPC cannot be submitted at all. And it runs on a tokio worker thread if the
/// dispatcher died. The header therefore disclaims serialisation outright: "callbacks
/// are not guaranteed to be serialised on one thread. Do not hold a lock across this
/// call and re-acquire it in the callback, and publish everything the callback needs
/// (including <c>user_data</c>) before calling rather than after."
/// </para>
/// <para>
/// <b>How wide the inline path is, cited precisely.</b> For the entry points this class
/// serves, the header documents exactly one trigger: <em>a NULL <c>admin</c>
/// handle</em>. The family-wide trigger set is larger and does include
/// argument-marshaling failure on ordinary bad input — an unparseable base64 topic id,
/// an unknown <c>AlterConfigOp.OpType</c> code — but that statement lives in
/// <c>src/ffi/admin.rs:56-63</c>, a <c>//!</c> module doc <b>cbindgen does not emit</b>,
/// and its triggers belong to entry points later phases will declare. This class is the
/// family-wide one, so it is written for the wider set deliberately: every consequence
/// below is a no-cost invariant, and being ready for an inline callback that cannot
/// happen yet costs nothing.
/// </para>
/// <para>
/// Three consequences are enforced here and at the submit site: the delegates are
/// <c>static readonly</c> so the GC cannot collect a thunk native still holds; each body
/// is a <b>total no-throw boundary</b>, because an exception unwinding into Rust is
/// undefined behaviour and — on the inline path — there is no caller frame willing to
/// catch it; and every source is built with <c>RunContinuationsAsynchronously</c>,
/// without which an awaiter's continuation would run inside the caller's own P/Invoke.
/// </para>
/// <para>
/// <b>Ownership of what the callback is handed.</b> The <c>error</c>
/// <em>parameter</em> is a non-const, <b>owned</b> handle and is freed via
/// <see cref="KafkaException.FromHandle(IntPtr)"/>; the <c>result</c> is an owned
/// borrow-root destroyed exactly once in the <c>finally</c>; and every value read out of
/// that result — including a <b>per-key error</b> — is <b>borrowed</b> and must never be
/// destroyed (see <see cref="KeyedResultMarshal"/>).
/// </para>
/// <para>
/// <b>Readers, not accessors, carry the key and the value (M15/P2a, M15/P2b).</b> Each
/// RPC's key and value are read by a hoisted <c>Func&lt;IntPtr, int, T&gt;</c> over the
/// result handle and the index, because neither axis is universal: <c>deleteRecords</c>
/// has no <c>get_key</c> (its key is composed from two accessors) and no pointer-shaped
/// value (its value is an inline <c>int64_t</c>). Hoisting them into
/// <c>static readonly</c> fields is what keeps a walk allocation-free.
/// </para>
/// </remarks>
internal static class AdminCallbacks
{
    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_close_callback_t</c>:
    /// <c>void (*)(kafka_common_KafkaError_t* error, void* user_data)</c>. Null
    /// <paramref name="error"/> is success.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CloseCallback(IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_topics_callback_t</c>:
    /// <c>void (*)(kafka_admin_CreateTopicsResult_t* result,
    /// kafka_common_KafkaError_t* error, void* user_data)</c>. Exactly one of the two is
    /// non-null and the callback owns it. ⚠ A <b>per-topic</b> failure arrives inside
    /// <paramref name="result"/>, not as <paramref name="error"/>: a non-null
    /// <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreateTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_topics_callback_t</c>:
    /// <c>void (*)(kafka_admin_DeleteTopicsResult_t* result,
    /// kafka_common_KafkaError_t* error, void* user_data)</c>. Shared by <b>both</b>
    /// delete entry points — the by-name and the by-id one — because they produce the same
    /// result type. ⚠ A <b>per-topic</b> failure arrives inside
    /// <paramref name="result"/>; a non-null <paramref name="error"/> means the request
    /// could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_describe_topics_callback_t</c>,
    /// shared by both describe entry points for the same reason as
    /// <see cref="DeleteTopicsCallback"/>.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DescribeTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_list_topics_callback_t</c>.
    /// ⚠ Unlike every other admin callback here, <paramref name="error"/> is the
    /// <b>only</b> failure channel: the result type has no <c>get_error</c>, so there is
    /// no per-topic failure to carry (result shape 3).
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ListTopicsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_create_partitions_callback_t</c>.
    /// ⚠ A <b>per-topic</b> failure arrives inside <paramref name="result"/>; a non-null
    /// <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CreatePartitionsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The C signature for <c>kafka_admin_AdminClient_delete_records_callback_t</c>.
    /// ⚠ A <b>per-partition</b> failure arrives inside <paramref name="result"/>; a
    /// non-null <paramref name="error"/> means the request could not be submitted at all.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void DeleteRecordsCallback(IntPtr result, IntPtr error, IntPtr userData);

    /// <summary>
    /// The single rooted instance passed to every <c>close_async</c> submission. Rooted
    /// for the process lifetime, so the native thunk never dangles (ffi §B6 keep-alive).
    /// </summary>
    internal static readonly CloseCallback Close = OnClose;

    /// <summary>
    /// The single rooted instance passed to every <c>create_topics_async</c>
    /// submission.
    /// </summary>
    internal static readonly CreateTopicsCallback CreateTopics = OnCreateTopics;

    /// <summary>
    /// <c>createTopics</c>' universal accessors — <c>count</c> and the borrowed per-key
    /// <c>get_error</c>. Built once, so walking a result allocates no delegates.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so a test can walk a real result with the <b>same</b>
    /// accessor set production uses (<c>definition-of-done.md</c> §12): a test that
    /// assembled its own could keep passing after production started pointing at a
    /// different function.
    /// </remarks>
    internal static readonly KeyedResultMarshal.Accessors CreateTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.CreateTopicsResultCount,
            NativeMethods.CreateTopicsResultGetError);

    /// <summary>
    /// <c>createTopics</c>' key reader — Java keys this result by topic <b>name</b>
    /// (<c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt; values()</c>), so the borrowed
    /// <c>get_key(i)</c> string is the key with no parsing. Hoisted for the same reason
    /// as <see cref="CreateTopicsAccessors"/>: no delegate is allocated per walk.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> CreateTopicsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.CreateTopicsResultGetKey(result, index));

    /// <summary>
    /// <c>createTopics</c>' per-key value reader: <c>get_value(i)</c> yields a
    /// <b>borrowed child handle</b>, which is copied out into an owned managed object
    /// before the root dies. Hoisted for the same reason as the accessors — and shared
    /// with the tests that drive the walker directly.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicMetadataAndConfig> TopicMetadataAndConfigValue =
        static (result, index) =>
            TopicMetadataAndConfigMarshal.CopyOut(NativeMethods.CreateTopicsResultGetValue(result, index));

    /// <summary>
    /// The single rooted instance passed to <b>both</b> <c>delete_topics_async</c> and
    /// <c>delete_topics_by_ids_async</c>. One trampoline serves both because the ABI hands
    /// back the same result type; which key type the awaiters are keyed by travels in the
    /// <c>user_data</c> context, not in the delegate.
    /// </summary>
    internal static readonly DeleteTopicsCallback DeleteTopicsByName = OnDeleteTopicsByName;

    /// <summary>
    /// The by-<b>id</b> rooted instance. It is a separate delegate from
    /// <see cref="DeleteTopicsByName"/> only because the two recover a differently-typed
    /// context out of <c>user_data</c> (<c>Uuid</c> keys versus <c>string</c> keys); the
    /// ABI signature is identical.
    /// </summary>
    internal static readonly DeleteTopicsCallback DeleteTopicsById = OnDeleteTopicsById;

    /// <inheritdoc cref="DeleteTopicsByName"/>
    internal static readonly DescribeTopicsCallback DescribeTopicsByName = OnDescribeTopicsByName;

    /// <inheritdoc cref="DeleteTopicsById"/>
    internal static readonly DescribeTopicsCallback DescribeTopicsById = OnDescribeTopicsById;

    /// <summary>
    /// The single rooted instance passed to every <c>list_topics_async</c> submission.
    /// </summary>
    internal static readonly ListTopicsCallback ListTopics = OnListTopics;

    /// <summary>
    /// The single rooted instance passed to every <c>create_partitions_async</c>
    /// submission.
    /// </summary>
    internal static readonly CreatePartitionsCallback CreatePartitions = OnCreatePartitions;

    /// <summary>
    /// The single rooted instance passed to every <c>delete_records_async</c> submission.
    /// </summary>
    internal static readonly DeleteRecordsCallback DeleteRecords = OnDeleteRecords;

    /// <summary>
    /// <c>deleteTopics</c>' universal accessors. Result <b>shape 2</b>: the ABI declares
    /// no <c>DeleteTopicsResult_get_value</c>, because Java's per-key future is
    /// <c>KafkaFuture&lt;Void&gt;</c> and a null error <em>is</em> the success value —
    /// which is stated by routing through the value-less <c>Complete</c> overload, not by
    /// nulling anything here.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DeleteTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteTopicsResultCount,
            NativeMethods.DeleteTopicsResultGetError);

    /// <summary>
    /// <c>describeTopics</c>' universal accessors — result shape 1 (a value per key).
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DescribeTopicsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DescribeTopicsResultCount,
            NativeMethods.DescribeTopicsResultGetError);

    /// <summary>
    /// <c>deleteTopics</c>' by-<b>name</b> key reader: the borrowed <c>get_key(i)</c>
    /// string is the key, as it is for <c>createTopics</c>.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> DeleteTopicsNameKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteTopicsResultGetKey(result, index));

    /// <summary>
    /// <c>deleteTopics</c>' by-<b>id</b> key reader — the other half of the base64
    /// topic-id round trip. The header is explicit that "result keys are the same base64"
    /// strings the request supplied, so the key is <c>get_key(i)</c> parsed back through
    /// <see cref="Uuid.Parse"/>; a caller who passed <c>Uuid</c>s gets <c>Uuid</c>s back.
    /// This is what <c>KeyedAdminOperation</c>'s generic key exists for.
    /// </summary>
    internal static readonly Func<IntPtr, int, Uuid> DeleteTopicsIdKey =
        static (result, index) =>
            Uuid.Parse(KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteTopicsResultGetKey(result, index)));

    /// <inheritdoc cref="DeleteTopicsNameKey"/>
    internal static readonly Func<IntPtr, int, string> DescribeTopicsNameKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeTopicsResultGetKey(result, index));

    /// <inheritdoc cref="DeleteTopicsIdKey"/>
    internal static readonly Func<IntPtr, int, Uuid> DescribeTopicsIdKey =
        static (result, index) =>
            Uuid.Parse(KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeTopicsResultGetKey(result, index)));

    /// <summary>
    /// The per-key value reader for <c>describeTopics</c>, hoisted for the same reason
    /// as <see cref="TopicMetadataAndConfigValue"/>.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicDescription> TopicDescriptionValue =
        static (result, index) =>
            TopicDescriptionMarshal.CopyOut(NativeMethods.DescribeTopicsResultGetValue(result, index));

    /// <summary>
    /// <c>listTopics</c>' key reader (result shape 3). The map is keyed by topic name,
    /// exactly as Java's <c>Map&lt;String, TopicListing&gt;</c> is.
    /// </summary>
    internal static readonly Func<IntPtr, int, string> ListTopicsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.ListTopicsResultGetKey(result, index));

    /// <summary>
    /// <c>listTopics</c>' value reader: <c>get_value(i)</c> yields a borrowed
    /// <c>TopicListing_t</c>, copied out before the root dies.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicListing> TopicListingValue =
        static (result, index) =>
            TopicListingMarshal.CopyOut(NativeMethods.ListTopicsResultGetValue(result, index));

    /// <summary>
    /// <c>createPartitions</c>' universal accessors — result <b>shape 2</b>, like
    /// <c>deleteTopics</c>: Java's per-topic future is <c>KafkaFuture&lt;Void&gt;</c> and
    /// the ABI declares no <c>_get_value</c>.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors CreatePartitionsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.CreatePartitionsResultCount,
            NativeMethods.CreatePartitionsResultGetError);

    /// <summary><c>createPartitions</c>' key reader — the topic name.</summary>
    internal static readonly Func<IntPtr, int, string> CreatePartitionsKey =
        static (result, index) =>
            KeyedResultMarshal.ReadStringKey(NativeMethods.CreatePartitionsResultGetKey(result, index));

    /// <summary>
    /// <c>deleteRecords</c>' universal accessors. The <c>get_error</c> here is the
    /// <b>authoritative</b> success/failure signal for the whole RPC — see
    /// <see cref="DeletedRecordsValue"/>.
    /// </summary>
    internal static readonly KeyedResultMarshal.Accessors DeleteRecordsAccessors =
        new KeyedResultMarshal.Accessors(
            NativeMethods.DeleteRecordsResultCount,
            NativeMethods.DeleteRecordsResultGetError);

    /// <summary>
    /// <c>deleteRecords</c>' <b>composite</b> key reader — the sub-shape M15/P2a's
    /// <c>(result, index)</c> key seam exists for. This result declares no
    /// <c>get_key</c>; the key is <c>(get_topic(i), get_partition(i))</c>, reassembled
    /// into the shipped <see cref="TopicPartition"/> that Java keys the map by.
    /// </summary>
    internal static readonly Func<IntPtr, int, TopicPartition> DeleteRecordsKey =
        static (result, index) => new TopicPartition(
            KeyedResultMarshal.ReadStringKey(NativeMethods.DeleteRecordsResultGetTopic(result, index)),
            NativeMethods.DeleteRecordsResultGetPartition(result, index));

    /// <summary>
    /// <c>deleteRecords</c>' <b>inline-scalar</b> value reader — the sub-shape M15/P2b's
    /// value seam exists for. There is no borrowed child handle to copy out of:
    /// <c>get_low_watermark(i)</c> <em>is</em> the value, an <c>int64_t</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This reader is reached only when the entry's <c>get_error</c> was null</b> —
    /// the walker checks the error first, so the <c>-1</c> the header mentions is never
    /// interpreted here. That matters because <c>-1</c> is overloaded three ways
    /// ("that partition failed", "index out of range", and a genuine watermark of
    /// <c>-1</c>), so it cannot serve as a verdict. A <c>-1</c> reaching this reader is a
    /// <b>success</b> carrying <c>-1</c>.
    /// </remarks>
    internal static readonly Func<IntPtr, int, DeletedRecords> DeletedRecordsValue =
        static (result, index) =>
            new DeletedRecords(NativeMethods.DeleteRecordsResultGetLowWatermark(result, index));

    /// <summary>
    /// The result-root destroys, hoisted for the same reason as the accessor sets: a
    /// method group converted at the call site would allocate a delegate per completion.
    /// All are null-safe, so the trampoline's <c>finally</c> can call them
    /// unconditionally.
    /// </summary>
    private static readonly Action<IntPtr> s_destroyCreateTopicsResult = NativeMethods.CreateTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteTopicsResult = NativeMethods.DeleteTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDescribeTopicsResult = NativeMethods.DescribeTopicsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyCreatePartitionsResult =
        NativeMethods.CreatePartitionsResultDestroy;

    /// <inheritdoc cref="s_destroyCreateTopicsResult"/>
    private static readonly Action<IntPtr> s_destroyDeleteRecordsResult = NativeMethods.DeleteRecordsResultDestroy;

    private static void OnClose(IntPtr error, IntPtr userData)
    {
        OperationCompletionSource? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (OperationCompletionSource)handle.Target!;
            context.Complete(error);
        }
        catch (Exception exception)
        {
            // No-throw boundary: never unwind into native. Surface via the Task if the
            // context was recovered; otherwise there is nothing to fault.
            context?.TrySetException(exception);
        }
        finally
        {
            // Sole owner of the GCHandle free and the span-the-op reference release, on
            // every path (ffi §B6 hookless one-shot).
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The one completion body every keyed admin trampoline delegates to, so the
    /// ownership rules are stated once instead of once per RPC.
    /// </summary>
    /// <remarks>
    /// The <c>finally</c> discharges three obligations on <b>every</b> path — including
    /// the inline ones and the no-throw path: the owned result root is destroyed exactly
    /// once (null-safe, so the top-level-error branch is a no-op); any awaiter the result
    /// failed to account for is faulted, so no caller can be left holding a <c>Task</c>
    /// that never completes; and the rooting <c>GCHandle</c> plus the span-the-op client
    /// reference are released. The destroy runs strictly <em>after</em> the walk, because
    /// every value the walk reads is borrowed from that root.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">
    /// The submit failure, or <c>IntPtr.Zero</c>. ⚠ <b>OWNED</b> — freed here with
    /// <see cref="KafkaException.FromHandle(IntPtr)"/>, the mirror image of the per-key
    /// errors inside a result, which are borrowed and must never be freed.
    /// </param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="readValue">That RPC's value reader.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteKeyed<TKey, TValue>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.Accessors accessors,
        Func<IntPtr, int, TKey> readKey,
        Func<IntPtr, int, TValue> readValue,
        Action<IntPtr> destroyResult)
        where TKey : notnull
    {
        KeyedAdminOperation<TKey, TValue>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (KeyedAdminOperation<TKey, TValue>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                // The request could not be submitted at all: there is no result table, so
                // every requested key fails with this one error.
                context.FailAll(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.Complete(result, accessors, context, readKey, readValue);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Tasks.
            context?.FailAll(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    /// <summary>
    /// The <b>shape-2</b> twin of <see cref="CompleteKeyed{TKey, TValue}"/>: identical in
    /// every respect except that the walk carries no per-key value, because the RPC's
    /// result type has no <c>_get_value</c> function.
    /// </summary>
    /// <remarks>
    /// It exists as its own method rather than as a null argument so the shape is stated
    /// by the type system: it accepts only a <see cref="VoidKeyedAdminOperation{TKey}"/>,
    /// so a value-carrying operation cannot be routed here and have its value dropped.
    /// </remarks>
    /// <param name="result">The owned result root, or <c>IntPtr.Zero</c> on a submit failure.</param>
    /// <param name="error">The submit failure, or <c>IntPtr.Zero</c>. <b>OWNED</b>.</param>
    /// <param name="userData">The per-operation <c>GCHandle</c>.</param>
    /// <param name="accessors">That RPC's universal accessors.</param>
    /// <param name="readKey">That RPC's key reader.</param>
    /// <param name="destroyResult">That RPC's <c>*Result_destroy</c>.</param>
    private static void CompleteKeyedVoid<TKey>(
        IntPtr result,
        IntPtr error,
        IntPtr userData,
        KeyedResultMarshal.Accessors accessors,
        Func<IntPtr, int, TKey> readKey,
        Action<IntPtr> destroyResult)
        where TKey : notnull
    {
        VoidKeyedAdminOperation<TKey>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (VoidKeyedAdminOperation<TKey>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                context.FailAll(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.Complete(result, accessors, context, readKey);
            }
        }
        catch (Exception exception)
        {
            context?.FailAll(exception);
        }
        finally
        {
            destroyResult(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }

    private static void OnCreateTopics(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            CreateTopicsAccessors,
            CreateTopicsKey,
            TopicMetadataAndConfigValue,
            s_destroyCreateTopicsResult);

    private static void OnDeleteTopicsByName(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            DeleteTopicsAccessors,
            DeleteTopicsNameKey,
            s_destroyDeleteTopicsResult);

    private static void OnDeleteTopicsById(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            DeleteTopicsAccessors,
            DeleteTopicsIdKey,
            s_destroyDeleteTopicsResult);

    private static void OnDescribeTopicsByName(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeTopicsAccessors,
            DescribeTopicsNameKey,
            TopicDescriptionValue,
            s_destroyDescribeTopicsResult);

    private static void OnDescribeTopicsById(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DescribeTopicsAccessors,
            DescribeTopicsIdKey,
            TopicDescriptionValue,
            s_destroyDescribeTopicsResult);

    private static void OnCreatePartitions(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyedVoid(
            result,
            error,
            userData,
            CreatePartitionsAccessors,
            CreatePartitionsKey,
            s_destroyCreatePartitionsResult);

    private static void OnDeleteRecords(IntPtr result, IntPtr error, IntPtr userData) =>
        CompleteKeyed(
            result,
            error,
            userData,
            DeleteRecordsAccessors,
            DeleteRecordsKey,
            DeletedRecordsValue,
            s_destroyDeleteRecordsResult);

    /// <summary>
    /// The <b>shape-3</b> trampoline: one awaiter, no per-key error channel.
    /// </summary>
    /// <remarks>
    /// The differences from the keyed trampolines are exactly the two the shape implies.
    /// The callback's <c>error</c> is the <em>only</em> failure channel, so it faults the
    /// single awaiter rather than fanning out across keys; and any failure during the
    /// walk does the same, because there is no per-key <see cref="System.Threading.Tasks.Task"/>
    /// to attribute it to. The <c>finally</c>'s three obligations are unchanged.
    /// </remarks>
    private static void OnListTopics(IntPtr result, IntPtr error, IntPtr userData)
    {
        SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>? context = null;
        try
        {
            GCHandle handle = GCHandle.FromIntPtr(userData);
            context = (SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>)handle.Target!;

            if (error != IntPtr.Zero)
            {
                // ⚠ OWNED — FromHandle frees it exactly once (the mirror image of the
                // per-key errors inside a keyed result, which are borrowed).
                context.SetException(KafkaException.FromHandle(error)!);
            }
            else
            {
                KeyedResultMarshal.CompleteAggregate(
                    result,
                    NativeMethods.ListTopicsResultCount,
                    context,
                    ListTopicsKey,
                    TopicListingValue,
                    StringComparer.Ordinal);
            }
        }
        catch (Exception exception)
        {
            // No-throw boundary. On the inline path there is not even a caller frame that
            // would catch this, so it must be absorbed here and surfaced through the Task.
            context?.SetException(exception);
        }
        finally
        {
            NativeMethods.ListTopicsResultDestroy(result);
            context?.FailUncompleted();
            context?.FreeGcHandle();
        }
    }
}
