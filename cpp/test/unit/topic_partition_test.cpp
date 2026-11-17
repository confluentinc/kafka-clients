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

// TopicPartition (org.apache.kafka.common.TopicPartition).
//
// Java's TopicPartitionTest has two tests, both of Java serialization
// (testSerializationRoundtrip, testTopiPartitionSerializationCompatibility):
// cpp/CLAUDE.md §5 makes Serializable nothing in C++, so they are not
// translated beyond the values they check. The rest are the Rust core's tests
// of its translation (rust/src/common/topic_partition.rs), as §11.1 says.

#include <confluent/kafka/common.hpp>

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <unordered_set>

namespace {

using confluent::kafka::common::TopicPartition;

// TopicPartitionTest.checkValues, without the serialization around it.
TEST(TopicPartitionTest, KeepsTheTopicAndPartition) {
    const std::string topic_name = "mytopic";
    const int32_t part_num = 5;
    const TopicPartition tp(topic_name, part_num);
    EXPECT_EQ(tp.partition(), part_num) << "partition number should be " << part_num << " but got " << tp.partition();
    EXPECT_EQ(tp.topic(), topic_name) << "topic should be " << topic_name << " but got " << tp.topic();
}

// test_topic_partition_equality
TEST(TopicPartitionTest, IsEqualWhenTopicAndPartitionAre) {
    const TopicPartition tp1("test", 0);
    const TopicPartition tp2("test", 0);
    const TopicPartition tp3("test", 1);
    const TopicPartition tp4("other", 0);
    EXPECT_EQ(tp1, tp2);
    EXPECT_NE(tp1, tp3);
    EXPECT_NE(tp1, tp4);
    const TopicPartition copy = tp1;  // copies share the value
    EXPECT_EQ(copy, tp1);
}

// test_topic_partition_hash
TEST(TopicPartitionTest, HashesEqualValuesAlike) {
    std::unordered_set<TopicPartition> set;
    set.insert(TopicPartition("test", 0));
    set.insert(TopicPartition("test", 0));
    EXPECT_EQ(set.size(), 1U);
    set.insert(TopicPartition("test", 1));
    EXPECT_EQ(set.size(), 2U);
}

// test_topic_partition_display
TEST(TopicPartitionTest, PrintsTopicDashPartition) {
    const TopicPartition tp("mytopic", 5);
    EXPECT_EQ(to_string(tp), "mytopic-5");
    std::ostringstream out;
    out << tp;
    EXPECT_EQ(out.str(), "mytopic-5");
}

}  // namespace
