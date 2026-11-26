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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="ConsumerGroupMetadataValues"/> registers a user's metadata with the core, which reads it back through
/// its own C getters as the values were read: UTF-8 with an unpaired surrogate as <c>'?'</c>, and a
/// <see langword="null"/> group instance id as none. A <see langword="null"/> group id or member id never reaches it: the
/// producer refuses it, as Java's class does (<c>PublicConsumerGroupMetadataTests</c>).
/// </summary>
public sealed class ConsumerGroupMetadataValuesTests
{
    [Theory]
    [InlineData("grüppe-✓", 7, "member-✓", "instance-✓", "grüppe-✓", "member-✓", "instance-✓")]
    [InlineData("group", 0, "", null, "group", "", null)]
    public void TheCore_ReadsTheValuesBack(
        string groupId,
        int generationId,
        string memberId,
        string? groupInstanceId,
        string expectedGroupId,
        string expectedMemberId,
        string? expectedGroupInstanceId)
    {
        Assert.Equal(
            (expectedGroupId, generationId, expectedMemberId, expectedGroupInstanceId),
            ReadBack(new TestGroupMetadata(groupId, generationId, memberId, groupInstanceId)));
    }

    // Built in code: an attribute argument is stored as UTF-8, which cannot hold an unpaired surrogate.
    [Fact]
    public void TheCore_ReadsAnUnpairedSurrogateAsAQuestionMark()
    {
        Assert.Equal(
            ("a?b", -1, "c?d", "e?f"),
            ReadBack(new TestGroupMetadata("a\uD800b", -1, "c\uDC00d", "e\uD800f")));
    }

    [Fact]
    public void Dispose_FreesTheValuesOnce()
    {
        ConsumerGroupMetadataValues values = ConsumerGroupMetadataValues.Of(new TestGroupMetadata("group", 1, "member"));

        values.Dispose();
        values.Dispose();

        Assert.True(values.IsClosed);
    }

    private static (string?, int, string?, string?) ReadBack(TestGroupMetadata userMetadata)
    {
        using ConsumerGroupMetadataValues values = ConsumerGroupMetadataValues.Of(userMetadata);
        using SafeConsumerGroupMetadataHandle handle = values.Register();

        bool added = false;
        try
        {
            handle.DangerousAddRef(ref added);
            IntPtr metadata = handle.DangerousGetHandle();
            IntPtr outErr = IntPtr.Zero;
            string? groupId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(metadata, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int generationId = NativeMethods.ConsumerGroupMetadataGenerationId(metadata, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? memberId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(metadata, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? groupInstanceId =
                Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupInstanceId(metadata, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (groupId, generationId, memberId, groupInstanceId);
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }
    }
}
