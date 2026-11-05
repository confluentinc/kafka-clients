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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Java.Lang;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests <see cref="KafkaException"/>: its constructors, its id, its wraps, and a core error read as its class with its
/// handle freed exactly once.
/// </summary>
public sealed class KafkaExceptionTests
{
    private const string ClassicUnsupported =
        "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).";

    private static Dictionary<string, string> BaseConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void ClassicGroupProtocol_ThrowsUnsupportedOperationException()
    {
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "classic";

        UnsupportedOperationException ex =
            Assert.Throws<UnsupportedOperationException>(() => NativeConsumer.Create(config));

        Assert.Equal(ClassicUnsupported, ex.Message);
    }

    [Fact]
    public void DefaultGroupProtocol_IsClassic_ThrowsUnsupportedOperationException()
    {
        UnsupportedOperationException ex =
            Assert.Throws<UnsupportedOperationException>(() => NativeConsumer.Create(BaseConfig()));

        Assert.Equal(ClassicUnsupported, ex.Message);
    }

    [Fact]
    public void NonAsciiInvalidConfigValue_ErrorMessageEchoesValue()
    {
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "café";

        ConfigException ex = Assert.Throws<ConfigException>(() => NativeConsumer.Create(config));

        Assert.Contains("café", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PublicConstructors_CodeIsTheIdOfABareKafkaException()
    {
        Exception cause = new InvalidOperationException("c");
        KafkaException[] built =
        {
            new KafkaException(),
            new KafkaException("m"),
            new KafkaException("m", cause),
            new KafkaException(cause),
        };

        Assert.All(built, exception => Assert.Equal(-1, exception.Code));
        Assert.All(built, exception => Assert.Equal(-1, exception.FfiId));
        Assert.All(built, exception => Assert.False(exception is Common.Errors.RetriableException));
    }

    [Fact]
    public void NullMessage_MessageIsEmpty_JavaToStringIsTheTypeName()
    {
        KafkaException withoutMessage = new KafkaException((string?)null);
        KafkaException withoutMessageWithCause = new KafkaException(null, new InvalidOperationException("c"));
        KafkaException emptyMessage = new KafkaException(string.Empty);

        Assert.Equal(string.Empty, withoutMessage.Message);
        Assert.Equal("Confluent.Kafka.Common.KafkaException", JavaText.ToString(withoutMessage));
        Assert.Equal(string.Empty, withoutMessageWithCause.Message);
        Assert.Equal("Confluent.Kafka.Common.KafkaException", JavaText.ToString(withoutMessageWithCause));
        Assert.Equal(string.Empty, emptyMessage.Message);
        Assert.Equal("Confluent.Kafka.Common.KafkaException: ", JavaText.ToString(emptyMessage));
    }

    [Fact]
    public void CoreErrorConstructor_KeepsTheMessageAndCause_CodeIsTheIdOfABareKafkaException()
    {
        Exception cause = new InvalidOperationException("c");

        KafkaException exception = new KafkaException(default(CoreError), "core message: café", cause);

        Assert.Equal("core message: café", exception.Message);
        Assert.Same(cause, exception.InnerException);
        Assert.Equal(-1, exception.Code);
        Assert.False(exception is Common.Errors.RetriableException);
        Assert.Equal("Confluent.Kafka.Common.KafkaException: core message: café", JavaText.ToString(exception));
    }

    [Fact]
    public void CoreErrorConstructor_NullMessage_MessageIsEmpty()
    {
        KafkaException exception = new KafkaException(default(CoreError), null, null);

        Assert.Equal(string.Empty, exception.Message);
        Assert.Null(exception.InnerException);
        Assert.Equal("Confluent.Kafka.Common.KafkaException", JavaText.ToString(exception));
    }

    [Fact]
    public void AClassConstructor_CodeIsTheIdOfTheClass()
    {
        KafkaException exception = new Common.Errors.TopicExistsException("m");
        KafkaException retriable = new Common.Errors.NotCoordinatorException("m");

        Assert.Equal(36, exception.Code);
        Assert.Equal(36, exception.FfiId);
        Assert.False(exception is Common.Errors.RetriableException);
        Assert.Equal(16, retriable.Code);
        Assert.IsAssignableFrom<Common.Errors.RetriableException>(retriable);
    }

    [Fact]
    public void WrapAtPoll_WrapsAnotherExceptionAsJavasNewKafkaException_AndKeepsAKafkaException()
    {
        Exception cause = new InvalidOperationException("listener failed");
        KafkaException kafka = new Common.Errors.TopicExistsException("m");

        KafkaException wrapped = KafkaException.WrapAtPoll(cause);

        Assert.Equal("System.InvalidOperationException: listener failed", wrapped.Message);
        Assert.Same(cause, wrapped.InnerException);
        Assert.Equal(-1, wrapped.Code);
        Assert.False(wrapped is Common.Errors.RetriableException);
        Assert.Same(kafka, KafkaException.WrapAtPoll(kafka));
    }

    [Fact]
    public void FromFfiError_NullHandle_ReturnsNull()
    {
        Assert.Null(KafkaException.FromFfiError(IntPtr.Zero, owned: true));
    }

    [Fact]
    public void ErrorPath_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        for (int i = 0; i < 200; i++)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr error = NativeMethods.ErrorKafka(ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
            Assert.IsType<KafkaException>(KafkaException.FromFfiError(error, owned: true));
        }
    }
}
