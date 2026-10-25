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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives production's per-id <c>ProducerIdAndEpoch</c> combiner over the two futures FenceProducers reads for each
/// id: it waits for both, then fails with the producer id's failure, else the epoch's, else completes with the pair.
/// </summary>
public sealed class AdminFenceProducersCombineTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BothSucceed_CompletesWithThePair_OnlyOnceBothHaveSettled(bool producerIdFirst)
    {
        KafkaFutureImpl<long> producerId = new KafkaFutureImpl<long>();
        KafkaFutureImpl<short> epoch = new KafkaFutureImpl<short>();
        KafkaFutureImpl<ProducerIdAndEpoch> combined = NativeAdminClient.CombineProducerIdAndEpoch(producerId, epoch);

        if (producerIdFirst)
        {
            producerId.Complete(9_001L);
        }
        else
        {
            epoch.Complete(17);
        }

        Assert.False(combined.IsSettled);

        if (producerIdFirst)
        {
            epoch.Complete(17);
        }
        else
        {
            producerId.Complete(9_001L);
        }

        Assert.True(combined.IsSettled);
        Assert.Null(combined.Failure);
        ProducerIdAndEpoch pair = combined.GetNow(default);
        Assert.Equal(9_001L, pair.ProducerId);
        Assert.Equal((short)17, pair.Epoch);
    }

    [Fact]
    public void TheProducerIdFails_FailsWithItsInstance_OnceTheEpochHasSettled()
    {
        KafkaFutureImpl<long> producerId = new KafkaFutureImpl<long>();
        KafkaFutureImpl<short> epoch = new KafkaFutureImpl<short>();
        KafkaFutureImpl<ProducerIdAndEpoch> combined = NativeAdminClient.CombineProducerIdAndEpoch(producerId, epoch);
        KafkaException failure = new KafkaException(47, "the producer id failed", isRetriable: false);

        producerId.CompleteWithError(failure);
        Assert.False(combined.IsSettled);

        epoch.Complete(3);
        Assert.True(combined.IsSettled);
        Assert.Same(failure, combined.Failure);
    }

    [Fact]
    public void BothFail_TheProducerIdsFailureWins_WhicheverFailedFirst()
    {
        KafkaFutureImpl<long> producerId = new KafkaFutureImpl<long>();
        KafkaFutureImpl<short> epoch = new KafkaFutureImpl<short>();
        KafkaFutureImpl<ProducerIdAndEpoch> combined = NativeAdminClient.CombineProducerIdAndEpoch(producerId, epoch);
        KafkaException epochFailure = new KafkaException(48, "the epoch failed", isRetriable: true);
        KafkaException producerIdFailure = new KafkaException(47, "the producer id failed", isRetriable: false);

        epoch.CompleteWithError(epochFailure);
        Assert.False(combined.IsSettled);

        producerId.CompleteWithError(producerIdFailure);
        Assert.Same(producerIdFailure, combined.Failure);
    }

    [Fact]
    public void OnlyTheEpochFails_FailsWithTheEpochsInstance()
    {
        KafkaFutureImpl<long> producerId = new KafkaFutureImpl<long>();
        KafkaFutureImpl<short> epoch = new KafkaFutureImpl<short>();
        KafkaFutureImpl<ProducerIdAndEpoch> combined = NativeAdminClient.CombineProducerIdAndEpoch(producerId, epoch);
        KafkaException failure = new KafkaException(48, "the epoch failed", isRetriable: true);

        epoch.CompleteWithError(failure);
        producerId.Complete(1L);

        Assert.Same(failure, combined.Failure);
    }

    [Fact]
    public void SourcesAlreadySettled_TheCombinedFutureIsSettledAtOnce()
    {
        KafkaFutureImpl<long> producerId = new KafkaFutureImpl<long>();
        KafkaFutureImpl<short> epoch = new KafkaFutureImpl<short>();
        producerId.Complete(5L);
        epoch.Complete(2);

        KafkaFutureImpl<ProducerIdAndEpoch> combined = NativeAdminClient.CombineProducerIdAndEpoch(producerId, epoch);

        Assert.True(combined.IsSettled);
        Assert.Equal(5L, combined.GetNow(default).ProducerId);
        Assert.Equal((short)2, combined.GetNow(default).Epoch);
    }
}
