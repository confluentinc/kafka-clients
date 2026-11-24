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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The asynchronous built-in serializers and deserializers against their synchronous classes: Java's bytes out and
/// values back from both, the same refusal of malformed bytes, thrown by the call itself, a
/// <see cref="ValueTask{TResult}"/> that is already completed whatever the token, the memory the
/// <c>ReadOnlyMemory</c> deserializer returns as given and the <c>byte[]</c> one copies, the synchronous class each
/// hands to a client, and the <c>Configure</c> extension of the two asynchronous interfaces.
/// </summary>
public sealed class AsyncSerializationTests
{
    private const string Topic = "testTopic";

    private const string SerializationNamespace = "Confluent.Kafka.Common.Serialization";

    private static readonly byte[] s_myString = { 0x6d, 0x79, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67 };

    private static readonly CancellationToken[] s_tokens = { CancellationToken.None, new CancellationToken(canceled: true) };

    public static TheoryData<Type, Type> Pairs => new TheoryData<Type, Type>
    {
        { typeof(AsyncBooleanSerializer), typeof(BooleanSerializer) },
        { typeof(AsyncBooleanDeserializer), typeof(BooleanDeserializer) },
        { typeof(AsyncByteArraySerializer), typeof(ByteArraySerializer) },
        { typeof(AsyncByteArrayDeserializer), typeof(ByteArrayDeserializer) },
        { typeof(AsyncDoubleSerializer), typeof(DoubleSerializer) },
        { typeof(AsyncDoubleDeserializer), typeof(DoubleDeserializer) },
        { typeof(AsyncFloatSerializer), typeof(FloatSerializer) },
        { typeof(AsyncFloatDeserializer), typeof(FloatDeserializer) },
        { typeof(AsyncIntegerSerializer), typeof(IntegerSerializer) },
        { typeof(AsyncIntegerDeserializer), typeof(IntegerDeserializer) },
        { typeof(AsyncLongSerializer), typeof(LongSerializer) },
        { typeof(AsyncLongDeserializer), typeof(LongDeserializer) },
        { typeof(AsyncReadOnlyMemorySerializer), typeof(ReadOnlyMemorySerializer) },
        { typeof(AsyncReadOnlyMemoryDeserializer), typeof(ReadOnlyMemoryDeserializer) },
        { typeof(AsyncShortSerializer), typeof(ShortSerializer) },
        { typeof(AsyncShortDeserializer), typeof(ShortDeserializer) },
        { typeof(AsyncStringSerializer), typeof(StringSerializer) },
        { typeof(AsyncStringDeserializer), typeof(StringDeserializer) },
        { typeof(AsyncUUIDSerializer), typeof(UUIDSerializer) },
        { typeof(AsyncUUIDDeserializer), typeof(UUIDDeserializer) },
        { typeof(AsyncVoidSerializer), typeof(VoidSerializer) },
        { typeof(AsyncVoidDeserializer), typeof(VoidDeserializer) },
    };

    [Fact]
    public void EachPair_WritesJavasBytes_AndReadsTheValueBack_FromTheSyncAndTheAsyncClass()
    {
        AssertVector(
            new BooleanSerializer(), new AsyncBooleanSerializer(), new BooleanDeserializer(), new AsyncBooleanDeserializer(),
            true, new byte[] { 0x01 });
        AssertVector(
            new BooleanSerializer(), new AsyncBooleanSerializer(), new BooleanDeserializer(), new AsyncBooleanDeserializer(),
            false, new byte[] { 0x00 });
        AssertVector(
            new ByteArraySerializer(), new AsyncByteArraySerializer(), new ByteArrayDeserializer(), new AsyncByteArrayDeserializer(),
            (byte[])s_myString.Clone(), s_myString);
        AssertVector(
            new DoubleSerializer(), new AsyncDoubleSerializer(), new DoubleDeserializer(), new AsyncDoubleDeserializer(),
            5678567.12312d, new byte[] { 0x41, 0x55, 0xa9, 0x79, 0xc7, 0xe1, 0x32, 0xb5 });
        AssertVector(
            new FloatSerializer(), new AsyncFloatSerializer(), new FloatDeserializer(), new AsyncFloatDeserializer(),
            5678567.12312f, new byte[] { 0x4a, 0xad, 0x4b, 0xce });
        AssertVector(
            new IntegerSerializer(), new AsyncIntegerSerializer(), new IntegerDeserializer(), new AsyncIntegerDeserializer(),
            423412424, new byte[] { 0x19, 0x3c, 0xc2, 0xc8 });
        AssertVector(
            new LongSerializer(), new AsyncLongSerializer(), new LongDeserializer(), new AsyncLongDeserializer(),
            922337203685477580L, new byte[] { 0x0c, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc });
        AssertVector<ReadOnlyMemory<byte>?>(
            new ReadOnlyMemorySerializer(), new AsyncReadOnlyMemorySerializer(),
            new ReadOnlyMemoryDeserializer(), new AsyncReadOnlyMemoryDeserializer(),
            new ReadOnlyMemory<byte>((byte[])s_myString.Clone()), s_myString, memory => memory?.ToArray());
        AssertVector(
            new ShortSerializer(), new AsyncShortSerializer(), new ShortDeserializer(), new AsyncShortDeserializer(),
            (short)32767, new byte[] { 0x7f, 0xff });
        AssertVector(
            new StringSerializer(), new AsyncStringSerializer(), new StringDeserializer(), new AsyncStringDeserializer(),
            "my string", s_myString);
        AssertVector(
            new UUIDSerializer(), new AsyncUUIDSerializer(), new UUIDDeserializer(), new AsyncUUIDDeserializer(),
            Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"), Encoding.ASCII.GetBytes("01234567-89ab-cdef-0123-456789abcdef"));
    }

    [Fact]
    public void EachSerializer_WritesNullForNull_FromTheSyncAndTheAsyncClass_TheVoidOneForAnyValue()
    {
        AssertNull<bool?>(new BooleanSerializer(), new AsyncBooleanSerializer(), null);
        AssertNull<byte[]>(new ByteArraySerializer(), new AsyncByteArraySerializer(), null);
        AssertNull<double?>(new DoubleSerializer(), new AsyncDoubleSerializer(), null);
        AssertNull<float?>(new FloatSerializer(), new AsyncFloatSerializer(), null);
        AssertNull<int?>(new IntegerSerializer(), new AsyncIntegerSerializer(), null);
        AssertNull<long?>(new LongSerializer(), new AsyncLongSerializer(), null);
        AssertNull<ReadOnlyMemory<byte>?>(new ReadOnlyMemorySerializer(), new AsyncReadOnlyMemorySerializer(), null);
        AssertNull<short?>(new ShortSerializer(), new AsyncShortSerializer(), null);
        AssertNull<string>(new StringSerializer(), new AsyncStringSerializer(), null);
        AssertNull<Guid?>(new UUIDSerializer(), new AsyncUUIDSerializer(), null);
        AssertNull<object?>(new VoidSerializer(), new AsyncVoidSerializer(), null);
        AssertNull<object?>(new VoidSerializer(), new AsyncVoidSerializer(), "a value");
    }

    [Fact]
    public void EachDeserializer_RefusesMalformedBytes_WithTheSyncClasssException_ThrownByTheCallItself()
    {
        AssertRefused<bool?, SerializationException>(
            new BooleanDeserializer(), new AsyncBooleanDeserializer(), new byte[0],
            "Size of data received by BooleanDeserializer is not 1");
        AssertRefused<bool?, SerializationException>(
            new BooleanDeserializer(), new AsyncBooleanDeserializer(), new byte[] { 0x02 },
            "Unexpected byte received by BooleanDeserializer: 2");
        AssertRefused<double?, SerializationException>(
            new DoubleDeserializer(), new AsyncDoubleDeserializer(), new byte[7],
            "Size of data received by DoubleDeserializer is not 8");
        AssertRefused<float?, SerializationException>(
            new FloatDeserializer(), new AsyncFloatDeserializer(), new byte[3],
            "Size of data received by Deserializer is not 4");
        AssertRefused<int?, SerializationException>(
            new IntegerDeserializer(), new AsyncIntegerDeserializer(), new byte[5],
            "Size of data received by IntegerDeserializer is not 4");
        AssertRefused<long?, SerializationException>(
            new LongDeserializer(), new AsyncLongDeserializer(), new byte[7],
            "Size of data received by LongDeserializer is not 8");
        AssertRefused<short?, SerializationException>(
            new ShortDeserializer(), new AsyncShortDeserializer(), new byte[1],
            "Size of data received by ShortDeserializer is not 2");
        AssertRefused<Guid?, SerializationException>(
            new UUIDDeserializer(), new AsyncUUIDDeserializer(), Encoding.ASCII.GetBytes("not-a-uuid"),
            "Error parsing data into UUID");
        AssertRefused<object?, IllegalArgumentException>(
            new VoidDeserializer(), new AsyncVoidDeserializer(), new byte[0],
            "Data should be null for a VoidDeserializer.");
        AssertRefused<object?, IllegalArgumentException>(
            new VoidDeserializer(), new AsyncVoidDeserializer(), new byte[] { 0x01 },
            "Data should be null for a VoidDeserializer.");
    }

    [Fact]
    public async Task TheStringAndUuidClasses_AreConfiguredAsTheirSyncClasses()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["key.serializer.encoding"] = "UTF-16",
            ["key.deserializer.encoding"] = "UTF-16",
        };
        RecordHeaders headers = new RecordHeaders();

        StringSerializer serializer = new StringSerializer();
        serializer.Configure(configs, true);
        AsyncStringSerializer asyncSerializer = new AsyncStringSerializer();
        await asyncSerializer.Configure(configs, true);
        byte[] utf16 = serializer.Serialize(Topic, "my string")!.Value.ToArray();
        Assert.NotEqual(s_myString, utf16);
        Assert.Equal(utf16, Completed(asyncSerializer.Serialize(Topic, headers, "my string", CancellationToken.None))!.Value.ToArray());

        AsyncStringDeserializer asyncDeserializer = new AsyncStringDeserializer();
        await asyncDeserializer.Configure(configs, true, CancellationToken.None);
        Assert.Equal("my string", Completed(asyncDeserializer.Deserialize(Topic, headers, utf16, CancellationToken.None)));

        Guid uuid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");
        UUIDSerializer uuidSerializer = new UUIDSerializer();
        uuidSerializer.Configure(configs, true);
        AsyncUUIDSerializer asyncUuidSerializer = new AsyncUUIDSerializer();
        await asyncUuidSerializer.Configure(configs, true);
        byte[] uuidUtf16 = uuidSerializer.Serialize(Topic, uuid)!.Value.ToArray();
        Assert.Equal(uuidUtf16, Completed(asyncUuidSerializer.Serialize(Topic, headers, uuid, CancellationToken.None))!.Value.ToArray());

        AsyncUUIDDeserializer asyncUuidDeserializer = new AsyncUUIDDeserializer();
        await asyncUuidDeserializer.Configure(configs, true, CancellationToken.None);
        Assert.Equal(uuid, Completed(asyncUuidDeserializer.Deserialize(Topic, headers, uuidUtf16, CancellationToken.None)));
    }

    [Fact]
    public void TheStringClasses_RefuseAnUnknownEncoding_AsTheirSyncClasses()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["key.serializer.encoding"] = "encoding-does-not-exist",
            ["key.deserializer.encoding"] = "encoding-does-not-exist",
        };

        SerializationException[] errors =
        {
            Assert.Throws<SerializationException>(() => new StringSerializer().Configure(configs, true)),
            Assert.Throws<SerializationException>(() => Completed(new AsyncStringSerializer().Configure(configs, true))),
            Assert.Throws<SerializationException>(() => new StringDeserializer().Configure(configs, true)),
            Assert.Throws<SerializationException>(() => Completed(new AsyncStringDeserializer().Configure(configs, true))),
        };

        foreach (SerializationException e in errors)
        {
            Assert.Equal("Unsupported encoding encoding-does-not-exist", e.Message);
            Assert.Equal("encoding-does-not-exist", Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
        }
    }

    [Fact]
    public void TheReadOnlyMemoryDeserializer_ReturnsTheMemoryItIsGiven_AndIsTheOnlyBuiltInThatMayKeepIt()
    {
        byte[] batch = { 9, 9, 1, 2, 3, 9 };
        ReadOnlyMemory<byte> data = new ReadOnlyMemory<byte>(batch, 2, 3);

        ValueTask<ReadOnlyMemory<byte>?> task =
            new AsyncReadOnlyMemoryDeserializer().Deserialize(Topic, new RecordHeaders(), data, CancellationToken.None);

        ReadOnlyMemory<byte> returned = Completed(task)!.Value;
        Assert.True(returned.Equals(data));
        Assert.True(MemoryMarshal.TryGetArray(returned, out ArraySegment<byte> segment));
        Assert.Same(batch, segment.Array);
        Assert.Equal(2, segment.Offset);
        Assert.Equal(3, segment.Count);

        Type[] retaining = typeof(IAsyncDeserializer<>).Assembly.GetExportedTypes()
            .Where(type => type.IsClass && type.GetInterfaces().Any(
                contract => contract.IsGenericType
                    && contract.GetGenericTypeDefinition() == typeof(IAsyncDeserializerWithRetention<>)))
            .ToArray();
        Assert.Equal(new[] { typeof(AsyncReadOnlyMemoryDeserializer) }, retaining);
    }

    [Fact]
    public void TheReadOnlyMemoryDeserializer_ReturnsAnEmptyMemory_NotNull_ForNoBytes()
    {
        ValueTask<ReadOnlyMemory<byte>?> task = new AsyncReadOnlyMemoryDeserializer().Deserialize(
            Topic, new RecordHeaders(), ReadOnlyMemory<byte>.Empty, CancellationToken.None);

        ReadOnlyMemory<byte>? returned = Completed(task);
        Assert.True(returned.HasValue);
        Assert.Equal(0, returned!.Value.Length);
    }

    [Fact]
    public void TheByteArrayDeserializer_ReturnsACopy()
    {
        byte[] batch = { 9, 9, 1, 2, 3, 9 };
        ReadOnlyMemory<byte> data = new ReadOnlyMemory<byte>(batch, 2, 3);

        ValueTask<byte[]?> task = new AsyncByteArrayDeserializer().Deserialize(Topic, new RecordHeaders(), data, CancellationToken.None);

        byte[] returned = Completed(task)!;
        Assert.NotSame(batch, returned);
        batch[2] = 7;
        Assert.Equal(new byte[] { 1, 2, 3 }, returned);
    }

    [Theory]
    [MemberData(nameof(Pairs))]
    public void AnAsyncBuiltIn_HandsOutItsSyncClass_ThroughTheInternalInterface(Type asyncType, Type syncType)
    {
        object instance = Activator.CreateInstance(asyncType)!;
        bool serializer = asyncType.Name.EndsWith("Serializer", StringComparison.Ordinal);
        Type definition = serializer ? typeof(IAsyncBuiltInSerializer<>) : typeof(IAsyncBuiltInDeserializer<>);
        Type unwrap = Assert.Single(
            asyncType.GetInterfaces(),
            contract => contract.IsGenericType && contract.GetGenericTypeDefinition() == definition);
        PropertyInfo inner = unwrap.GetProperty("Inner")!;
        Type argument = unwrap.GetGenericArguments()[0];

        object? first = inner.GetValue(instance);

        Assert.IsType(syncType, first);
        Assert.Same(first, inner.GetValue(instance));
        Assert.Equal((serializer ? typeof(ISerializer<>) : typeof(IDeserializer<>)).MakeGenericType(argument), inner.PropertyType);
        Assert.Contains(
            (serializer ? typeof(IAsyncSerializer<>) : typeof(IAsyncDeserializer<>)).MakeGenericType(argument),
            asyncType.GetInterfaces());
        Assert.False(unwrap.IsVisible);
    }

    [Fact]
    public void TheInternalInterface_HandsOutTheVeryClassTheAsyncOneCalls()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?> { ["key.serializer.encoding"] = "UTF-16" };
        AsyncStringSerializer asyncSerializer = new AsyncStringSerializer();
        Completed(asyncSerializer.Configure(configs, true));

        ISerializer<string> inner = ((IAsyncBuiltInSerializer<string>)asyncSerializer).Inner;
        IDeserializer<int?> innerDeserializer = ((IAsyncBuiltInDeserializer<int?>)new AsyncIntegerDeserializer()).Inner;

        Assert.Equal(Encoding.BigEndianUnicode.GetBytes("a"), inner.Serialize(Topic, "a")!.Value.ToArray().Skip(2).ToArray());
        Assert.Equal(423412424, innerDeserializer.Deserialize(Topic, new byte[] { 0x19, 0x3c, 0xc2, 0xc8 }));
    }

    [Theory]
    [MemberData(nameof(Pairs))]
    public void AnAsyncBuiltIn_HasTheShapeOfItsSyncClass(Type asyncType, Type syncType)
    {
        Assert.True(asyncType.IsSealed);
        Assert.Equal(SerializationNamespace, asyncType.Namespace);
        Assert.Equal("Async" + syncType.Name, asyncType.Name);
        Assert.Equal(Implements(syncType, "WithConfigure"), Implements(asyncType, "WithConfigure"));
        Assert.False(typeof(IDisposable).IsAssignableFrom(syncType));
        Assert.False(typeof(IDisposable).IsAssignableFrom(asyncType));
        Assert.False(typeof(IAsyncDisposable).IsAssignableFrom(asyncType));
        Assert.Equal(syncType.GetMethod("Close") is null, asyncType.GetMethod("Close") is null);
        Assert.Equal(Constructors(syncType), Constructors(asyncType));
    }

    [Fact]
    public void ThePairs_AreEveryAsyncSerializerAndDeserializerClassExported()
    {
        string[] exported = typeof(IAsyncSerializer<>).Assembly.GetExportedTypes()
            .Where(type => type.IsClass && !type.IsAbstract && type.GetInterfaces().Any(
                contract => contract.IsGenericType
                    && (contract.GetGenericTypeDefinition() == typeof(IAsyncSerializer<>)
                        || contract.GetGenericTypeDefinition() == typeof(IAsyncDeserializer<>))))
            .Select(type => type.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        string[] pairs = Pairs.Select(row => ((Type)row[0]).Name).OrderBy(name => name, StringComparer.Ordinal).ToArray();

        Assert.Equal(22, pairs.Length);
        Assert.Equal(pairs, exported);
    }

    [Fact]
    public async Task Configure_OnAnAsyncSerializerOrDeserializer_CallsTheConfigureInterface_AndOtherwiseNothing()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?> { ["k"] = "v" };
        ConfiguredSerde configured = new ConfiguredSerde();
        StrayConfigureSerde stray = new StrayConfigureSerde();
        using CancellationTokenSource source = new CancellationTokenSource();

        await ((IAsyncSerializer<string>)configured).Configure(configs, true, source.Token);
        await ((IAsyncDeserializer<string>)configured).Configure(configs, false);
        Completed(((IAsyncSerializer<string>)stray).Configure(configs, true));
        Completed(((IAsyncDeserializer<string>)stray).Configure(configs, true));

        Assert.Equal(2, configured.Seen.Count);
        Assert.Same(configs, configured.Seen[0].Configs);
        Assert.True(configured.Seen[0].IsKey);
        Assert.Equal(source.Token, configured.Seen[0].Token);
        Assert.Same(configs, configured.Seen[1].Configs);
        Assert.False(configured.Seen[1].IsKey);
        Assert.Equal(CancellationToken.None, configured.Seen[1].Token);
        Assert.Equal(0, stray.Calls);
    }

    [Fact]
    public async Task Configure_OnAnAsyncSerializerOrDeserializer_HandsBackTheTaskOfTheConfigureInterface()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>();
        ConfiguredSerde configured = new ConfiguredSerde
        {
            Pending = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
        };

        ValueTask serializer = ((IAsyncSerializer<string>)configured).Configure(configs, true);
        ValueTask deserializer = ((IAsyncDeserializer<string>)configured).Configure(configs, false);
        Assert.False(serializer.IsCompleted);
        Assert.False(deserializer.IsCompleted);

        configured.Pending.SetException(new InvalidOperationException("the registry is away"));

        Assert.Equal("the registry is away", (await Assert.ThrowsAsync<InvalidOperationException>(() => serializer.AsTask())).Message);
        Assert.Equal("the registry is away", (await Assert.ThrowsAsync<InvalidOperationException>(() => deserializer.AsTask())).Message);
    }

    [Fact]
    public void Configure_OnTheBuiltInAsyncClasses_IsCompletedWhenItReturns_AndDoesNotReadTheToken()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>();
        CancellationToken cancelled = new CancellationToken(canceled: true);

        Completed(new AsyncStringSerializer().Configure(configs, true, cancelled));
        Completed(new AsyncStringDeserializer().Configure(configs, true, cancelled));
        Completed(new AsyncUUIDSerializer().Configure(configs, false, cancelled));
        Completed(new AsyncUUIDDeserializer().Configure(configs, false, cancelled));
    }

    [Fact]
    public void Configure_OnANullAsyncSerializerOrDeserializer_Throws()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>();

        ArgumentNullException serializer = Assert.Throws<ArgumentNullException>(
            () => Completed(((IAsyncSerializer<string>)null!).Configure(configs, true)));
        ArgumentNullException deserializer = Assert.Throws<ArgumentNullException>(
            () => Completed(((IAsyncDeserializer<string>)null!).Configure(configs, true)));

        Assert.Equal("serializer", serializer.ParamName);
        Assert.Equal("deserializer", deserializer.ParamName);
    }

    private static void AssertVector<T>(
        ISerializer<T> serializer,
        IAsyncSerializer<T> asyncSerializer,
        IDeserializer<T> deserializer,
        IAsyncDeserializer<T> asyncDeserializer,
        T value,
        byte[] javaBytes,
        Func<T?, object?>? comparable = null)
    {
        comparable ??= given => given;
        RecordHeaders headers = new RecordHeaders();
        ReadOnlyMemory<byte> data = javaBytes;

        foreach (CancellationToken token in s_tokens)
        {
            Assert.Equal(javaBytes, serializer.Serialize(Topic, headers, value)!.Value.ToArray());
            Assert.Equal(javaBytes, Completed(asyncSerializer.Serialize(Topic, headers, value, token))!.Value.ToArray());

            Assert.Equal(comparable(value), comparable(deserializer.Deserialize(Topic, headers, data.Span)));
            Assert.Equal(comparable(value), comparable(Completed(asyncDeserializer.Deserialize(Topic, headers, data, token))));
        }
    }

    private static void AssertNull<T>(ISerializer<T> serializer, IAsyncSerializer<T> asyncSerializer, T? value)
    {
        RecordHeaders headers = new RecordHeaders();

        foreach (CancellationToken token in s_tokens)
        {
            Assert.Null(serializer.Serialize(Topic, headers, value));
            Assert.Null(Completed(asyncSerializer.Serialize(Topic, headers, value, token)));
        }
    }

    private static void AssertRefused<T, TException>(
        IDeserializer<T> deserializer, IAsyncDeserializer<T> asyncDeserializer, byte[] bytes, string message)
        where TException : Exception
    {
        RecordHeaders headers = new RecordHeaders();
        ReadOnlyMemory<byte> data = bytes;

        TException sync = Assert.Throws<TException>(() => { _ = deserializer.Deserialize(Topic, headers, data.Span); });
        Assert.Equal(message, sync.Message);

        foreach (CancellationToken token in s_tokens)
        {
            TException async = Assert.Throws<TException>(
                () => { _ = asyncDeserializer.Deserialize(Topic, headers, data, token); });
            Assert.Equal(message, async.Message);
            Assert.Equal(sync.InnerException?.GetType(), async.InnerException?.GetType());
            Assert.Equal(sync.InnerException?.Message, async.InnerException?.Message);
        }
    }

    // The value of a task that must already be completed: no built-in waits.
    private static T Completed<T>(ValueTask<T> task)
    {
        Assert.True(task.IsCompletedSuccessfully);
        return task.Result;
    }

    private static void Completed(ValueTask task) => Assert.True(task.IsCompletedSuccessfully);

    private static bool Implements(Type type, string suffix) =>
        type.GetInterfaces().Any(contract => contract.Name.Contains(suffix));

    private static string[] Constructors(Type type) =>
        type.GetConstructors()
            .Select(constructor => string.Join(", ", constructor.GetParameters().Select(parameter => parameter.ParameterType.FullName)))
            .OrderBy(text => text, StringComparer.Ordinal)
            .ToArray();

    /// <summary>An asynchronous serializer and deserializer with the configure interfaces, recording each call.</summary>
    private sealed class ConfiguredSerde : IAsyncSerializerWithConfigure<string>, IAsyncDeserializerWithConfigure<string>
    {
        internal List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey, CancellationToken Token)> Seen { get; } =
            new List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey, CancellationToken Token)>();

        /// <summary>When set, what each Configure hands back, still pending.</summary>
        internal TaskCompletionSource<bool>? Pending { get; set; }

        public ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken)
        {
            Seen.Add((configs, isKey, cancellationToken));
            return Pending is null ? default : new ValueTask(Pending.Task);
        }

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, string? data, CancellationToken cancellationToken) => default;

        public ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;
    }

    /// <summary>An asynchronous serializer and deserializer with a public Configure but not the interfaces.</summary>
    private sealed class StrayConfigureSerde : IAsyncSerializer<string>, IAsyncDeserializer<string>
    {
        internal int Calls { get; private set; }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => Calls++;

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, string? data, CancellationToken cancellationToken) => default;

        public ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;
    }
}
