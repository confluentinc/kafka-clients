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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads the native <c>DeletedRecords</c> value through production's reader and keys a mock
/// <c>deleteRecords</c> by its composite key.
/// </summary>
public sealed class AdminP2bResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    [Theory]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(123L)]
    public void DeletedRecords_LowWatermark_IsCopiedOut(long lowWatermark)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = Native.DeletedRecordsNew(lowWatermark, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        DeletedRecords copied;
        try
        {
            copied = NativeAdminClient.CopyOutDeletedRecords(native);
        }
        finally
        {
            Native.DeletedRecordsDestroy(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        Assert.Equal(lowWatermark, copied.LowWatermark);
    }

    [Fact]
    public void CompositeKey_DistinguishesPartitionsWithinATopic_AndTopicsAtOnePartition()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        TopicPartition a0 = new TopicPartition("p2b-composite-a", 0);
        TopicPartition a1 = new TopicPartition("p2b-composite-a", 1);
        TopicPartition b0 = new TopicPartition("p2b-composite-b", 0);

        var deleted = admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete>
            {
                [a0] = RecordsToDelete.BeforeOffset(1),
                [a1] = RecordsToDelete.BeforeOffset(2),
                [b0] = RecordsToDelete.BeforeOffset(3),
            },
            null);

        Assert.Equal(new HashSet<TopicPartition> { a0, a1, b0 }, new HashSet<TopicPartition>(deleted.LowWatermarks.Keys));
        Assert.Equal(3, deleted.LowWatermarks.Values.Distinct().Count());
        foreach (TopicPartition key in new[] { a0, a1, b0 })
        {
            KafkaException failure = Assert.Throws<KafkaException>(() => deleted.LowWatermarks[key].Get(s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DeletedRecords_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DeletedRecordsNew(long lowWatermark, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeletedRecords_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void DeletedRecordsDestroy(IntPtr self, ref IntPtr outErr);
    }
}
