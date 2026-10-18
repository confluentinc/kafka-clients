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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests that <see cref="Exception.InnerException"/> carries the core error's cause chain, Java's <c>getCause()</c>, on
/// a message-source error and on an admin client created with a bad <c>bootstrap.servers</c>.
/// </summary>
public sealed class KafkaExceptionCauseTests
{
    private const int UnknownServerErrorCode = -1;

    private const int ConfigErrorCode = -10;

    private const string BadBootstrap = "not-a-host-port";

    private const int LocalIllegalArgumentCode = -3;

    private const string OuterMessage = "the operation failed";

    private const string CauseMessage = "its cause: café";

    private static KafkaException CreateWithBadBootstrap() =>
        Assert.Throws<KafkaException>(
            () => new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = BadBootstrap }));

    [Fact]
    public void AdminClientCreate_BadBootstrap_CarriesTheCoreCauseAsInnerException()
    {
        KafkaException outer = CreateWithBadBootstrap();

        Assert.Equal(UnknownServerErrorCode, outer.Code);
        Assert.Equal("Failed to create new KafkaAdminClient", outer.Message);
        Assert.False(outer.IsRetriable);

        KafkaException inner = Assert.IsType<KafkaException>(outer.InnerException);
        Assert.Equal(ConfigErrorCode, inner.Code);
        Assert.Equal("Invalid url in bootstrap.servers: " + BadBootstrap, inner.Message);
        Assert.False(inner.IsRetriable);

        Assert.Null(inner.InnerException);
    }

    [Fact]
    public void MessageSourceError_CarriesTheCoreCauseAsInnerException()
    {
        KafkaException outer = KafkaException.FromHandle(MessageSourceError())!;

        Assert.Equal(UnknownServerErrorCode, outer.Code);
        Assert.Equal(OuterMessage, outer.Message);
        Assert.False(outer.IsRetriable);

        KafkaException inner = Assert.IsType<KafkaException>(outer.InnerException);
        Assert.Equal(LocalIllegalArgumentCode, inner.Code);
        Assert.Equal(CauseMessage, inner.Message);
        Assert.False(inner.IsRetriable);

        Assert.Null(inner.InnerException);
    }

    [Fact]
    public void MessageSourceError_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        for (int i = 0; i < 200; i++)
        {
            Exception? read = KafkaException.FromFfiError(MessageSourceError(), owned: true);
            KafkaException outer = Assert.IsType<KafkaException>(read);
            Assert.NotNull(outer.InnerException);
        }
    }

    private static IntPtr MessageSourceError()
    {
        using Utf8Marshal.PinnedUtf8String causeMessage = Utf8Marshal.Pin(CauseMessage);
        using Utf8Marshal.PinnedUtf8String outerMessage = Utf8Marshal.Pin(OuterMessage);
        IntPtr outErr = IntPtr.Zero;
        IntPtr source = NativeMethods.ErrorLocalIllegalArgument(causeMessage.Pointer, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, source);
        IntPtr outer = NativeMethods.ErrorKafkaMessageSource(outerMessage.Pointer, source, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, outer);
        return outer;
    }
}
