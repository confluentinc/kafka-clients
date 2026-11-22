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
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using Confluent.Kafka.Common.Header.Internals;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The members Java's <c>RecordHeader</c> and <c>RecordHeaders</c> have beyond the interfaces: the buffer constructor,
/// which decodes the key and copies the value on first read and then drops the buffer, and
/// <see cref="RecordHeaders.SetReadOnly()"/> and <see cref="RecordHeaders.IsReadOnly()"/>, with which the producers seal a
/// record's headers. Both classes are internal, as Java's package <c>common.header.internals</c> is not public API.
/// </summary>
public sealed class RecordHeaderClassMembersTests
{
    private const string ClosedMessage = "RecordHeaders has been closed.";

    private static readonly TimeSpan s_joinBound = TimeSpan.FromSeconds(30);

    [Fact]
    public void RecordHeaderAndRecordHeaders_AreNotPublic()
    {
        Assert.False(typeof(RecordHeader).IsVisible);
        Assert.False(typeof(RecordHeaders).IsVisible);
        Assert.DoesNotContain(typeof(RecordHeader).Assembly.GetExportedTypes(), type => type.Namespace == typeof(RecordHeader).Namespace);
    }

    [Fact]
    public void BufferConstructor_DecodesTheKeyAtTheFirstRead_ThenKeepsIt()
    {
        byte[] key = Encoding.UTF8.GetBytes("abc");
        RecordHeader header = new RecordHeader(key, null);

        key[0] = (byte)'x';
        string first = header.Key();
        key[1] = (byte)'y';

        Assert.Equal("xbc", first);
        Assert.Same(first, header.Key());
    }

    [Fact]
    public void BufferConstructor_CopiesTheValueAtTheFirstRead_IndependentOfTheBufferAfterIt()
    {
        byte[] value = { 1, 2, 3 };
        RecordHeader header = new RecordHeader(Encoding.UTF8.GetBytes("k"), value);

        value[0] = 7;
        byte[] first = header.Value()!;
        value[1] = 8;

        Assert.Equal(new byte[] { 7, 2, 3 }, first);
        Assert.NotSame(value, first);
        Assert.Same(first, header.Value());
    }

    [Fact]
    public void BufferConstructor_ReadsOnlyTheBuffersSlices()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("--trace-é--");
        int keyLength = Encoding.UTF8.GetByteCount("trace-é");
        RecordHeader header = new RecordHeader(
            new ReadOnlyMemory<byte>(bytes, 2, keyLength), new ReadOnlyMemory<byte>(bytes, 0, 2));

        Assert.Equal("trace-é", header.Key());
        Assert.Equal(new byte[] { (byte)'-', (byte)'-' }, header.Value());
    }

    [Fact]
    public void BufferConstructor_ANullValueBufferIsANullValue_AndAnEmptyOneAnEmptyValue()
    {
        Assert.Null(new RecordHeader(Encoding.UTF8.GetBytes("n"), null).Value());

        byte[]? empty = new RecordHeader(Encoding.UTF8.GetBytes("e"), ReadOnlyMemory<byte>.Empty).Value();
        Assert.NotNull(empty);
        Assert.Empty(empty);

        Assert.Equal(string.Empty, new RecordHeader(ReadOnlyMemory<byte>.Empty, null).Key());
    }

    [Theory]
    [InlineData(new byte[] { 0x61, 0xFF, 0x62 })]
    [InlineData(new byte[] { 0x61, 0xED, 0xA0, 0x80, 0x62 })]
    [InlineData(new byte[] { 0x61, 0xE2, 0x82, 0x62 })]
    public void BufferConstructor_DecodesAMalformedKeyAsJavaDoes_OneReplacementPerMalformedSequence(byte[] key)
    {
        string replacement = char.ConvertFromUtf32(0xFFFD);

        Assert.Equal("a" + replacement + "b", new RecordHeader(key, null).Key());
    }

    [Fact]
    public void BufferConstructor_EqualityHashAndString_AreTheStringForms()
    {
        RecordHeader fromString = new RecordHeader("k", new byte[] { 0x01, 0xff });

        Assert.True(FromBuffers().Equals(fromString));
        Assert.True(fromString.Equals(FromBuffers()));
        Assert.Equal(fromString.GetHashCode(), FromBuffers().GetHashCode());
        Assert.Equal("RecordHeader(key = k, value = [1, -1])", FromBuffers().ToString());
        Assert.False(FromBuffers().Equals(new RecordHeader("k", new byte[] { 0x01 })));

        RecordHeader nullValue = new RecordHeader("n", null);
        Assert.True(new RecordHeader(Encoding.UTF8.GetBytes("n"), null).Equals(nullValue));
        Assert.Equal(nullValue.GetHashCode(), new RecordHeader(Encoding.UTF8.GetBytes("n"), null).GetHashCode());
        Assert.Equal("RecordHeader(key = n, value = null)", new RecordHeader(Encoding.UTF8.GetBytes("n"), null).ToString());

        static RecordHeader FromBuffers() => new RecordHeader(Encoding.UTF8.GetBytes("k"), new byte[] { 0x01, 0xff });
    }

    [Fact]
    public void BufferConstructor_ConcurrentFirstReads_DecodeEachBufferOnce()
    {
        const int threadCount = 16;
        for (int pass = 0; pass < 100; pass++)
        {
            RecordHeader header = new RecordHeader(Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("value"));
            string?[] keys = new string?[threadCount];
            byte[]?[] values = new byte[]?[threadCount];
            using ManualResetEventSlim start = new ManualResetEventSlim(false);
            int failures = 0;
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                int slot = i;
                threads[i] = new Thread(() =>
                {
                    try
                    {
                        start.Wait();
                        keys[slot] = header.Key();
                        values[slot] = header.Value();
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref failures);
                    }
                });
                threads[i].Start();
            }

            start.Set();
            foreach (Thread thread in threads)
            {
                Assert.True(thread.Join(s_joinBound));
            }

            Assert.Equal(0, Volatile.Read(ref failures));
            Assert.Equal("key", keys[0]);
            Assert.Equal(Encoding.UTF8.GetBytes("value"), values[0]);
            Assert.All(keys, read => Assert.Same(keys[0], read));
            Assert.All(values, read => Assert.Same(values[0], read));
        }
    }

    [Fact]
    public void BufferConstructor_DropsEachBufferAtItsFirstRead()
    {
        RecordHeader header = OverThrowawayBuffers(out WeakReference keyBuffer, out WeakReference valueBuffer);

        Collect();
        Assert.True(keyBuffer.IsAlive, "an unread key must keep its buffer");
        Assert.True(valueBuffer.IsAlive, "an unread value must keep its buffer");

        Assert.Equal("key", header.Key());
        Collect();
        Assert.False(keyBuffer.IsAlive, "the first read of the key must drop its buffer");
        Assert.True(valueBuffer.IsAlive, "reading the key must not drop the value's buffer");

        Assert.Equal(Encoding.UTF8.GetBytes("value"), header.Value());
        Collect();
        Assert.False(valueBuffer.IsAlive, "the first read of the value must drop its buffer");

        GC.KeepAlive(header);
    }

    [Fact]
    public void RecordHeader_HoldsTwoReferenceFields_SoAHeaderBuiltFromAStringHasNoBufferField()
    {
        FieldInfo[] fields = typeof(RecordHeader).GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        Assert.Equal(2, fields.Length);
        Assert.All(fields, field => Assert.Equal(typeof(object), field.FieldType));
    }

    [Fact]
    public void SetReadOnlyAndIsReadOnly_AreMethods_NotAProperty()
    {
        MethodInfo set = PublicInstanceMethod(nameof(RecordHeaders.SetReadOnly));
        MethodInfo isReadOnly = PublicInstanceMethod(nameof(RecordHeaders.IsReadOnly));

        Assert.Equal(typeof(void), set.ReturnType);
        Assert.Equal(typeof(bool), isReadOnly.ReturnType);
        Assert.Empty(typeof(RecordHeaders).GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Null(typeof(RecordHeaders).GetProperty(
            nameof(RecordHeaders.IsReadOnly), BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance));
    }

    [Fact]
    public void SetReadOnly_ThenAddAndRemoveThrowJavasIllegalStateException()
    {
        RecordHeaders headers = new RecordHeaders();
        headers.Add("k", new byte[] { 1 });
        Assert.False(headers.IsReadOnly());

        headers.SetReadOnly();

        Assert.True(headers.IsReadOnly());
        Assert.Equal(ClosedMessage, Assert.Throws<IllegalStateException>(() => headers.Add("k2", null)).Message);
        Assert.Equal(
            ClosedMessage,
            Assert.Throws<IllegalStateException>(() => headers.Add(new RecordHeader("k3", null))).Message);
        Assert.Equal(ClosedMessage, Assert.Throws<IllegalStateException>(() => headers.Remove("k")).Message);
        Assert.Equal("RecordHeaders(headers = [RecordHeader(key = k, value = [1])], isReadOnly = true)", headers.ToString());

        headers.SetReadOnly();
        Assert.True(headers.IsReadOnly());
        Assert.False(new RecordHeaders(headers).IsReadOnly());
    }

    // Not inlined, so no local of the test roots the two arrays.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static RecordHeader OverThrowawayBuffers(out WeakReference keyBuffer, out WeakReference valueBuffer)
    {
        byte[] key = Encoding.UTF8.GetBytes("key");
        byte[] value = Encoding.UTF8.GetBytes("value");
        keyBuffer = new WeakReference(key);
        valueBuffer = new WeakReference(value);
        return new RecordHeader(key, value);
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static MethodInfo PublicInstanceMethod(string name)
    {
        MethodInfo[] methods = typeof(RecordHeaders).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(method => method.Name == name)
            .ToArray();
        MethodInfo method = Assert.Single(methods);
        Assert.Empty(method.GetParameters());
        return method;
    }
}
