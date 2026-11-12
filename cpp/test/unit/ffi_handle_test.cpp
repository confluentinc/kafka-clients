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

#include <confluent/kafka/detail/ffi.hpp>
#include <confluent/kafka/version.hpp>

#include <gtest/gtest.h>

#include <string_view>
#include <utility>

namespace {

using confluent::kafka::detail::ErrorHandle;
using confluent::kafka::detail::ErrorSlot;
using TopicPartitionHandle =
    confluent::kafka::detail::UniqueHandle<kafka_common_TopicPartition_t, kafka_common_TopicPartition_destroy_err>;

TEST(FfiHandle, ConstructReadAndDestroy) {
    ErrorSlot err;
    const TopicPartitionHandle tp{kafka_common_TopicPartition_new_err("topic", 3, err.slot())};
    ASSERT_FALSE(err.has_error());
    ASSERT_TRUE(tp);

    const char* topic = kafka_common_TopicPartition_topic_err(tp.get(), err.slot());
    ASSERT_FALSE(err.has_error());
    EXPECT_EQ(std::string_view{topic}, "topic");
    EXPECT_EQ(kafka_common_TopicPartition_partition_err(tp.get(), err.slot()), 3);
    EXPECT_FALSE(err.has_error());
}

TEST(FfiHandle, MoveTransfersOwnership) {
    ErrorSlot err;
    TopicPartitionHandle first{kafka_common_TopicPartition_new_err("topic", 0, err.slot())};
    ASSERT_FALSE(err.has_error());
    kafka_common_TopicPartition_t* raw = first.get();

    TopicPartitionHandle second{std::move(first)};
    EXPECT_FALSE(first);  // NOLINT(bugprone-use-after-move): the moved-from state is what is tested
    EXPECT_EQ(second.get(), raw);

    TopicPartitionHandle third;
    third = std::move(second);
    EXPECT_FALSE(second);  // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(third.get(), raw);
    EXPECT_EQ(third.release(), raw);
    EXPECT_FALSE(third);
    TopicPartitionHandle{raw}.reset();
}

TEST(FfiHandle, NullMandatoryArgumentIsReportedAsJavaDoes) {
    ErrorSlot err;
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new_err(nullptr, 0, err.slot());
    EXPECT_EQ(tp, nullptr);
    ASSERT_TRUE(err.has_error());

    const ErrorHandle error = err.release();
    EXPECT_FALSE(err.has_error());
    ASSERT_TRUE(error);
    EXPECT_EQ(error.code(), kafka_common_Error_LOCAL_NULL_POINTER);
    EXPECT_EQ(error.message(), "topic must not be null");
}

TEST(FfiHandle, DefaultErrorHandleIsEmpty) {
    const ErrorHandle none;
    EXPECT_FALSE(none);
    EXPECT_EQ(none.get(), nullptr);
}

TEST(Version, MatchesTheThreeNumbers) {
    using namespace confluent::kafka;
    EXPECT_EQ(VERSION, std::to_string(VERSION_MAJOR) + "." + std::to_string(VERSION_MINOR) + "." +
                           std::to_string(VERSION_PATCH));
}

}  // namespace
