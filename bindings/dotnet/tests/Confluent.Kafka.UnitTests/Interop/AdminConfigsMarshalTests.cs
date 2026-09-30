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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives M15/P3 Stage 2's value tree over a <b>real</b> native per-key <c>Config_t</c> —
/// two levels of borrowed pointers (<c>Config_t</c> → <c>ConfigEntry_t</c> → synonym
/// arrays) that one <c>Config_destroy</c> invalidates together.
/// </summary>
/// <remarks>
/// <para>
/// The production trampoline destroys the value in its <c>finally</c> — correctly — so it
/// never lets a caller inspect the borrowed pointers afterwards, and "everything was
/// copied out before the value died" is exactly what has to be proven. So these tests
/// submit <c>describe_configs_async</c> directly with a capturing callback, keep the value
/// alive, walk it with <em>production's</em> marshaller, destroy the value, and only then
/// read the result.
/// </para>
/// <para>
/// ⚠ <b>Under M15/P9's per-key ABI both the value and the error are OWNED</b> — there is
/// no result root to borrow either from. That inverts the shape-1 walk this file used to
/// drive, where the per-resource error was <c>const</c> and died with the root. Reading
/// the error with <see cref="KafkaException.FromBorrowedHandle"/> here would leak it.
/// </para>
/// </remarks>
public sealed class AdminConfigsMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "cfg-marshal-topic";

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.DescribeConfigsCallback s_capture = OnCapture;

    /// <inheritdoc cref="s_capture"/>
    private static readonly AdminCallbacks.DescribeConfigsCallback s_perKey = OnPerKey;

    /// <summary>
    /// ⚠ <b>The test that catches a lazily-held borrowed pointer, and nothing else will.</b>
    /// The whole <see cref="Config"/> is read <em>after</em> <c>Config_destroy</c> has
    /// invalidated every pointer it came from.
    /// </summary>
    [Fact]
    public async Task TheValueTree_IsCopiedOut_AndSurvivesTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(
                new[]
                {
                    new NewTopic(Topic, 1, 1)
                    {
                        Configs = new Dictionary<string, string>
                        {
                            ["cleanup.policy"] = "compact",
                            ["retention.ms"] = "604800000",
                        },
                    },
                },
                options: null).All(),
            s_deadline);

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        CaptureState capture = SubmitAndCapture(admin, resource);

        Config config;
        try
        {
            // The key crossed as the composite (type id, name) scalar pair, and there is no
            // per-key error, so the value is present.
            Assert.Equal((int)ConfigResourceType.Topic, capture.ResourceType);
            Assert.Equal(Topic, capture.ResourceName);
            Assert.Equal(IntPtr.Zero, capture.Error);

            config = AdminCallbacks.ConfigPerKeyValue(capture.Value);
        }
        finally
        {
            NativeMethods.ConfigDestroy(capture.Value);
        }

        // The value is gone. Everything below reads only owned managed state.
        Assert.Equal(2, config.Entries.Count);

        ConfigEntry cleanup = Assert.IsType<ConfigEntry>(config.Get("cleanup.policy"));
        Assert.Equal("cleanup.policy", cleanup.Name);
        Assert.Equal("compact", cleanup.Value);
        Assert.False(cleanup.IsSensitive);
        Assert.False(cleanup.IsReadOnly);
        Assert.Empty(cleanup.Synonyms);

        ConfigEntry retention = Assert.IsType<ConfigEntry>(config.Get("retention.ms"));
        Assert.Equal("604800000", retention.Value);

        // The mock builds entries through Java's two-argument ConfigEntry constructor, so
        // it reports UNKNOWN for both name-encoded enums and no documentation. That the
        // NAMES decode at all is asserted directly against the mapping below — the mock
        // cannot produce any other name.
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, cleanup.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, cleanup.Type);
        Assert.Null(cleanup.Documentation);
        Assert.False(cleanup.IsDefault);
    }

    /// <summary>
    /// A failing key arrives with an <b>owned</b> error and a NULL value, and the message is
    /// the mock's, asserted exactly (<c>definition-of-done.md</c> §3).
    /// </summary>
    /// <remarks>
    /// ⚠ THE INJECTION POINT for <c>describeConfigs</c> under the per-key ABI, and the
    /// inverse of what this test asserted against the shape-1 walk. There is no result root
    /// to borrow from, so <see cref="KafkaException.FromHandle"/> — which frees — is the
    /// only correct reader; a <see cref="KafkaException.FromBorrowedHandle"/> here leaks
    /// the error on every failing key, which no managed assertion can see.
    /// </remarks>
    [Fact]
    public void PerKeyError_IsOwned_AndArrivesWithANullValue()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        // A topic that does not exist: the mock reports UNKNOWN_TOPIC_OR_PARTITION for it.
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "cfg-absent-topic");
        CaptureState capture = SubmitAndCapture(admin, missing);

        Assert.Equal(IntPtr.Zero, capture.Value);
        Assert.NotEqual(IntPtr.Zero, capture.Error);

        // FromHandle consumes the error, so this is its one and only free.
        KafkaException failure = Assert.IsType<KafkaException>(KafkaException.FromHandle(capture.Error));

        // The mock's exact message (definition-of-done.md §3), rendered by the Rust
        // ConfigResource's own Display (config_resource.rs:106).
        Assert.Equal(
            "Resource ConfigResource(type=Topic, name='cfg-absent-topic') not found.", failure.Message);
    }

    /// <summary>
    /// A failed resource faults <b>only its own</b> awaitable — the header says a
    /// per-resource failure is not a call failure — and the walk reads the error, never the
    /// null value beside it.
    /// </summary>
    [Fact]
    public async Task AFailedResource_FaultsOnlyItsOwnAwaitable()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, options: null).All(), s_deadline);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "cfg-absent-topic");

        DescribeConfigsResult result = admin.DescribeConfigs(new[] { present, missing }, options: null);

        Config config = await TestTimeout.Run(() => result.Values[present], s_deadline);
        Assert.NotNull(config);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing]), s_deadline);
        Assert.Contains("not found", failure.Message, StringComparison.Ordinal);

        // …and All() fails because one resource did.
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// <c>ConfigSource</c> decodes from Java's enum constant <b>NAMES</b>, member by member,
    /// and an unrecognised name degrades to <c>UNKNOWN</c> rather than throwing.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Asserted against the mapping directly, because the mock cannot produce any name
    /// but <c>"UNKNOWN"</c></b> — it builds every entry through Java's two-argument
    /// <c>ConfigEntry</c> constructor, which defaults the source. Degrading rather than
    /// throwing is the Java contract: a broker introducing a new source must not fail the
    /// whole describe (<c>ConfigEntry.java:224</c> is what <c>UNKNOWN</c> is for).
    /// </remarks>
    [Fact]
    public void ConfigSource_DecodesFromJavasConstantNames()
    {
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicTopicConfig, ConfigMarshal.SourceFromName("DYNAMIC_TOPIC_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig,
            ConfigMarshal.SourceFromName("DYNAMIC_BROKER_LOGGER_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicBrokerConfig, ConfigMarshal.SourceFromName("DYNAMIC_BROKER_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig,
            ConfigMarshal.SourceFromName("DYNAMIC_DEFAULT_BROKER_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicClientMetricsConfig,
            ConfigMarshal.SourceFromName("DYNAMIC_CLIENT_METRICS_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.DynamicGroupConfig, ConfigMarshal.SourceFromName("DYNAMIC_GROUP_CONFIG"));
        Assert.Equal(
            ConfigEntry.ConfigSource.StaticBrokerConfig, ConfigMarshal.SourceFromName("STATIC_BROKER_CONFIG"));
        Assert.Equal(ConfigEntry.ConfigSource.DefaultConfig, ConfigMarshal.SourceFromName("DEFAULT_CONFIG"));
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, ConfigMarshal.SourceFromName("UNKNOWN"));

        // Degrade, never throw.
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, ConfigMarshal.SourceFromName("DYNAMIC_FUTURE_CONFIG"));
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, ConfigMarshal.SourceFromName(null));
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, ConfigMarshal.SourceFromName(string.Empty));

        // ⚠ NOT a PascalCase parse: the C# member names are not the wire names.
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, ConfigMarshal.SourceFromName("DefaultConfig"));
    }

    /// <summary>
    /// <c>ConfigType</c> decodes from Java's enum constant <b>NAMES</b>, member by member.
    /// </summary>
    /// <remarks>
    /// <inheritdoc cref="ConfigSource_DecodesFromJavasConstantNames" path="/remarks"/>
    /// </remarks>
    [Fact]
    public void ConfigType_DecodesFromJavasConstantNames()
    {
        Assert.Equal(ConfigEntry.ConfigType.Boolean, ConfigMarshal.TypeFromName("BOOLEAN"));
        Assert.Equal(ConfigEntry.ConfigType.String, ConfigMarshal.TypeFromName("STRING"));
        Assert.Equal(ConfigEntry.ConfigType.Int, ConfigMarshal.TypeFromName("INT"));
        Assert.Equal(ConfigEntry.ConfigType.Short, ConfigMarshal.TypeFromName("SHORT"));
        Assert.Equal(ConfigEntry.ConfigType.Long, ConfigMarshal.TypeFromName("LONG"));
        Assert.Equal(ConfigEntry.ConfigType.Double, ConfigMarshal.TypeFromName("DOUBLE"));
        Assert.Equal(ConfigEntry.ConfigType.List, ConfigMarshal.TypeFromName("LIST"));
        Assert.Equal(ConfigEntry.ConfigType.Class, ConfigMarshal.TypeFromName("CLASS"));
        Assert.Equal(ConfigEntry.ConfigType.Password, ConfigMarshal.TypeFromName("PASSWORD"));
        Assert.Equal(ConfigEntry.ConfigType.Unknown, ConfigMarshal.TypeFromName("UNKNOWN"));

        Assert.Equal(ConfigEntry.ConfigType.Unknown, ConfigMarshal.TypeFromName("DECIMAL"));
        Assert.Equal(ConfigEntry.ConfigType.Unknown, ConfigMarshal.TypeFromName(null));
        Assert.Equal(ConfigEntry.ConfigType.Unknown, ConfigMarshal.TypeFromName("String"));
    }

    /// <summary>
    /// ⚠ <b>Nothing native-backed survives on the copied-out types</b> — no
    /// <see cref="IntPtr"/>, no <see cref="SafeHandle"/>, anywhere in
    /// <see cref="Config"/> / <see cref="ConfigEntry"/> / <c>ConfigEntry.ConfigSynonym</c>.
    /// </summary>
    /// <remarks>
    /// This is the <b>structural</b> half of the copy-out guarantee, and it is what covers
    /// the synonym tree: the Rust mock builds every entry with an empty synonym list, so no
    /// result root reachable today carries one, and the behavioural
    /// copy-out-then-destroy test above cannot read a synonym's fields. A field holding a
    /// borrowed pointer would be caught here regardless of whether any test can reach it.
    /// </remarks>
    [Fact]
    public void TheCopiedOutTypes_HoldNoNativeState()
    {
        foreach (Type type in new[] { typeof(Config), typeof(ConfigEntry), typeof(ConfigEntry.ConfigSynonym) })
        {
            FieldInfo[] fields = type.GetFields(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);

            Assert.DoesNotContain(fields, field => field.FieldType == typeof(IntPtr));
            Assert.DoesNotContain(fields, field => typeof(SafeHandle).IsAssignableFrom(field.FieldType));
            Assert.DoesNotContain(fields, field => field.FieldType == typeof(UIntPtr));
        }
    }

    /// <summary>
    /// A synonym round-trips its three fields, including a <see langword="null"/> value —
    /// the shape <c>ConfigMarshal</c> builds when the ABI reports one.
    /// </summary>
    /// <remarks>
    /// Asserted on the type rather than through a result root because no root reachable
    /// today carries a synonym (see
    /// <see cref="TheCopiedOutTypes_HoldNoNativeState"/>). ⚠ A null synonym value inside
    /// the bound is a <b>genuine null</b>, not an out-of-range marker — the loop is bounded
    /// by <c>synonym_count</c> — so it must not become <c>""</c>.
    /// </remarks>
    [Fact]
    public void ConfigSynonym_CarriesItsThreeFields_AndPreservesANullValue()
    {
        ConfigEntry.ConfigSynonym withValue =
            new ConfigEntry.ConfigSynonym("a", "1", ConfigEntry.ConfigSource.StaticBrokerConfig);
        Assert.Equal("a", withValue.Name);
        Assert.Equal("1", withValue.Value);
        Assert.Equal(ConfigEntry.ConfigSource.StaticBrokerConfig, withValue.Source);
        Assert.Equal("ConfigSynonym(name=a, value=1, source=StaticBrokerConfig)", withValue.ToString());

        ConfigEntry.ConfigSynonym withNull =
            new ConfigEntry.ConfigSynonym("a", null, ConfigEntry.ConfigSource.DefaultConfig);
        Assert.Null(withNull.Value);
        Assert.NotEqual(withValue, withNull);

        // Order is preserved by the marshaller because it appends; the entry exposes the
        // list unchanged, which is what makes Java's precedence order meaningful.
        ConfigEntry entry = new ConfigEntry(
            "k",
            "v",
            ConfigEntry.ConfigSource.DynamicTopicConfig,
            isSensitive: false,
            isReadOnly: false,
            new[] { withValue, withNull },
            ConfigEntry.ConfigType.String,
            "doc");
        Assert.Equal(new[] { withValue, withNull }, entry.Synonyms);
    }

    /// <summary>
    /// ⚠ <b>THE RE-ARMED VALUE-DESTROY GUARD (M15/P9 CP8).</b> Under shape 4 a wrong or
    /// missing per-key <c>Config_destroy</c> only <em>leaks</em>, so the old injection
    /// point had nothing to assert on. This counts production's own destroy seam instead:
    /// exactly one destroy per callback, on the success path <b>and</b> on the failing
    /// key whose value is NULL.
    /// </summary>
    [Fact]
    public async Task PerKeyValue_IsDestroyedExactlyOncePerCallback()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, options: null).All(),
            s_deadline);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "cfg-absent-topic");

        Drive drive = DrivePerKey(admin, present, missing);

        await TestTimeout.Run(() => drive.Operation.Tasks[present], s_deadline);
        Assert.True(drive.Operation.Tasks[missing].IsFaulted);

        Assert.Equal(2, drive.DestroyCalls);
        Assert.Equal(1, drive.NonNullValues);
    }

    /// <summary>
    /// Submits <c>describe_configs_async</c> with a callback that runs the
    /// <em>production</em> per-key marshaller, wrapping only <c>destroyValue</c> so it can
    /// be counted (<c>definition-of-done.md</c> §12), and returns once every key fired.
    /// </summary>
    private static Drive DrivePerKey(NativeAdminClient admin, params ConfigResource[] resources)
    {
        Drive drive = new Drive(resources);
        GCHandle gcHandle = GCHandle.Alloc(drive, GCHandleType.Normal);
        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(resources.Length);
        try
        {
            int[] types = new int[resources.Length];
            IntPtr[] names = new IntPtr[resources.Length];
            for (int index = 0; index < resources.Length; index++)
            {
                types[index] = (int)resources[index].Type;
                Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(resources[index].Name);
                pinned.Add(name);
                names[index] = name.Pointer;
            }

            NativeMethods.AdminClientDescribeConfigsAsync(
                admin.Handle.DangerousGetHandle(),
                types,
                names,
                resources.Length,
                -1,
                includeSynonyms: true,
                includeDocumentation: true,
                s_perKey,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(
                drive.Done.Wait(s_deadline),
                $"only {drive.Done.InitialCount - drive.Done.CurrentCount} of "
                    + $"{drive.Done.InitialCount} per-key callbacks fired");
        }
        finally
        {
            foreach (Utf8Marshal.PinnedUtf8String name in pinned)
            {
                name.Dispose();
            }

            gcHandle.Free();
        }

        return drive;
    }

    private static void OnPerKey(
        int resourceType, IntPtr resourceName, IntPtr value, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        Drive? drive = null;
        try
        {
            drive = (Drive)GCHandle.FromIntPtr(userData).Target!;
            if (value != IntPtr.Zero)
            {
                Interlocked.Increment(ref drive.NonNullValues);
            }

            KeyedResultMarshal.CompleteKey(
                drive.Operation,
                new ConfigResource(
                    ConfigResourceMarshal.TypeFromId(resourceType),
                    KeyedResultMarshal.ReadStringKey(resourceName)),
                value,
                error,
                AdminCallbacks.ConfigPerKeyValue,
                drive.DestroyValue);
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
        finally
        {
            drive?.Done.Signal();
        }
    }

    /// <summary>
    /// Submits <c>describe_configs_async</c> directly for <b>one</b> resource and hands the
    /// caller that key's <b>owned</b> value and error, which the capturing callback
    /// deliberately does not destroy — the callback owns both, and here that owner is the
    /// test.
    /// </summary>
    private static CaptureState SubmitAndCapture(NativeAdminClient admin, params ConfigResource[] resources)
    {
        CaptureState capture = new CaptureState();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        List<Utf8Marshal.PinnedUtf8String> pinned = new List<Utf8Marshal.PinnedUtf8String>(resources.Length);
        try
        {
            int[] types = new int[resources.Length];
            IntPtr[] names = new IntPtr[resources.Length];
            for (int index = 0; index < resources.Length; index++)
            {
                types[index] = (int)resources[index].Type;
                Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(resources[index].Name);
                pinned.Add(name);
                names[index] = name.Pointer;
            }

            NativeMethods.AdminClientDescribeConfigsAsync(
                admin.Handle.DangerousGetHandle(),
                types,
                names,
                resources.Length,
                -1,
                includeSynonyms: true,
                includeDocumentation: true,
                s_capture,
                GCHandle.ToIntPtr(gcHandle));

            Assert.True(capture.Done.Wait(s_deadline), "the describeConfigs callback never fired");
        }
        finally
        {
            foreach (Utf8Marshal.PinnedUtf8String name in pinned)
            {
                name.Dispose();
            }

            gcHandle.Free();
        }

        return capture;
    }

    private static void OnCapture(
        int resourceType, IntPtr resourceName, IntPtr value, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            CaptureState capture = (CaptureState)GCHandle.FromIntPtr(userData).Target!;
            capture.ResourceType = resourceType;

            // ⚠ Copied HERE: the key is a borrowed const char*, valid only for the call.
            capture.ResourceName = Utf8Marshal.PtrToString(resourceName);
            capture.Value = value;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    private sealed class Drive
    {
        internal int DestroyCalls;

        internal int NonNullValues;

        internal Drive(ConfigResource[] keys)
        {
            Operation = new KeyedAdminOperation<ConfigResource, Config>(
                "describeConfigs", keys, EqualityComparer<ConfigResource>.Default);
            Done = new CountdownEvent(keys.Length);
            DestroyValue = handle =>
            {
                Interlocked.Increment(ref DestroyCalls);
                NativeMethods.ConfigDestroy(handle);
            };
        }

        internal KeyedAdminOperation<ConfigResource, Config> Operation { get; }

        internal CountdownEvent Done { get; }

        /// <summary>
        /// Production's destroy, counted. Allocated once so the count is not confused by a
        /// fresh delegate per callback.
        /// </summary>
        internal Action<IntPtr> DestroyValue { get; }
    }

    private sealed class CaptureState
    {
        internal int ResourceType = int.MinValue;

        internal string? ResourceName;

        internal IntPtr Value;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }
}
