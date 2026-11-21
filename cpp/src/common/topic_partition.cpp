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

#include <confluent/kafka/common/topic_partition.hpp>

#include <ostream>
#include <string>

namespace confluent::kafka::common {

// The value is immutable, so copies share it.
struct TopicPartition::Impl {
    std::string topic;
    int32_t partition;
};

TopicPartition::TopicPartition(std::string_view topic, int32_t partition)
    : impl_(std::make_shared<const Impl>(Impl{.topic = std::string(topic), .partition = partition})) {}

TopicPartition::TopicPartition(const TopicPartition& other) = default;

TopicPartition::TopicPartition(TopicPartition&& other) noexcept = default;

TopicPartition& TopicPartition::operator=(const TopicPartition& other) = default;

TopicPartition& TopicPartition::operator=(TopicPartition&& other) noexcept = default;

TopicPartition::~TopicPartition() = default;

int32_t TopicPartition::partition() const {
    return impl_->partition;
}

const std::string& TopicPartition::topic() const {
    return impl_->topic;
}

bool operator==(const TopicPartition& a, const TopicPartition& b) {
    return a.impl_ == b.impl_ || (a.partition() == b.partition() && a.topic() == b.topic());
}

std::string to_string(const TopicPartition& tp) {
    return tp.topic() + "-" + std::to_string(tp.partition());
}

std::ostream& operator<<(std::ostream& out, const TopicPartition& tp) {
    return out << to_string(tp);
}

}  // namespace confluent::kafka::common

std::size_t std::hash<confluent::kafka::common::TopicPartition>::operator()(
    const confluent::kafka::common::TopicPartition& tp) const noexcept {
    const std::size_t topic = std::hash<std::string>{}(tp.topic());
    return (31U * (31U + static_cast<std::size_t>(static_cast<uint32_t>(tp.partition())))) + topic;
}
