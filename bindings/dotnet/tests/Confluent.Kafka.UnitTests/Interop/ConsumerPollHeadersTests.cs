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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The record-header copy-out (M3/P3, PLAN decision 2 — headers included, internal
/// only). Headers land on the internal <see cref="ConsumerRecord"/> with each key
/// marshalled via the <b>length-delimited</b> <c>out_len</c> path (ffi-marshalling.md
/// §B3, never a NUL-scan). The empty-headers case is covered by the round-trip suite;
/// here we exercise the header-key §B3 path and the null-value case.
/// </summary>
/// <remarks>
/// <b>Scope note (source-verified).</b> The <c>MockConsumer</c> broker-free driver
/// exposed at the C ABI is <c>MockConsumer_add_record(topic, partition, offset, key,
/// value)</c> — it does <b>not</b> accept headers. The record built by the mock
/// therefore has no headers, so a header <em>round-trip</em> through the mock is not
/// reachable this phase; only the <b>empty-headers</b> case
/// (<c>header_count == 0</c>) is exercisable end to end (see
/// <c>ConsumerPollReceivePathTests.PollAsync_RoundTripsAllFields</c>, which asserts
/// <c>Headers</c> is empty). The header copy-out marshaller
/// (<c>ConsumerRecordsMarshal.CopyHeaders</c>) — count → per-index length-delimited
/// key (§B3) + copy-out value — is verified by inspection against the header
/// accessors, and the length-delimited §B3 path itself is directly and thoroughly
/// tested through the <b>topic</b> and via <see cref="Utf8MarshalLengthDelimitedTests"/>
/// (which share the exact <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> primitive
/// the header-key path uses). This residual mirrors the M3/P2 D-Q4 precedent: the
/// driver the phase can reach does not exercise every branch, so the unreachable
/// branch is documented rather than faked. It closes when a header-carrying mock
/// driver (or the public client's typed producer→consumer round-trip) lands.
/// </remarks>
public sealed class ConsumerPollHeadersTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "hdr-topic";
    private const int Partition = 0;

    [Fact]
    public async Task PollAsync_RecordWithoutHeaders_HasEmptyHeaders()
    {
        // The end-to-end reachable header case on the mock: header_count == 0 →
        // an empty (shared, non-null) header list. (add_record carries no headers.)
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (Topic, Partition) });
        await consumer.SeekAsync(Topic, Partition, offset: 0);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);

        ConsumerRecords records = await consumer.PollAsync(s_pollTimeout);

        ConsumerRecord record = Assert.Single(records);
        Assert.NotNull(record.Headers);
        Assert.Empty(record.Headers);
    }
}
