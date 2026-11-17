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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace confluent::kafka::common {

/// A topic name and partition number
class TopicPartition {
public:
    /// Creates the topic partition `partition` of `topic`.
    /// @param topic the topic name
    /// @param partition the partition number
    TopicPartition(std::string_view topic, int32_t partition)
        : impl_(std::make_shared<const Impl>(Impl{std::string(topic), partition})) {}

    /// Returns the partition number.
    /// @return the partition number
    [[nodiscard]] int32_t partition() const { return impl_->partition; }

    /// Returns the topic name.
    /// @return the topic name
    [[nodiscard]] const std::string& topic() const { return impl_->topic; }

    /// Whether `a` and `b` name the same partition of the same topic.
    /// @param a a topic partition
    /// @param b another topic partition
    /// @return true when the topic and the partition are equal
    friend bool operator==(const TopicPartition& a, const TopicPartition& b) {
        return a.impl_ == b.impl_ || (a.partition() == b.partition() && a.topic() == b.topic());
    }

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout. The value is
    // immutable, so copies share it.
    struct Impl {
        std::string topic;
        int32_t partition;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns `<topic>-<partition>`, as Java's toString.
/// @param tp a topic partition
/// @return the topic, a dash and the partition
inline std::string to_string(const TopicPartition& tp) {
    return tp.topic() + "-" + std::to_string(tp.partition());
}

/// Writes `to_string(tp)` to `out`.
/// @param out the stream
/// @param tp a topic partition
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const TopicPartition& tp) {
    return out << to_string(tp);
}

}  // namespace confluent::kafka::common

/// Hashes a topic partition from its topic and partition, as Java's hashCode reads them.
template <>
struct std::hash<confluent::kafka::common::TopicPartition> {
    std::size_t operator()(const confluent::kafka::common::TopicPartition& tp) const noexcept {
        const std::size_t topic = std::hash<std::string>{}(tp.topic());
        return 31U * (31U + static_cast<std::size_t>(static_cast<uint32_t>(tp.partition()))) + topic;
    }
};
