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

#include <confluent/kafka/export.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// A topic name and partition number
class CONFLUENT_KAFKA_CPP_API TopicPartition {
public:
    /// Creates the topic partition `partition` of `topic`.
    /// @param topic the topic name
    /// @param partition the partition number
    TopicPartition(std::string_view topic, int32_t partition);

    /// Copies `other`, sharing its immutable value.
    /// @param other the topic partition to copy
    TopicPartition(const TopicPartition& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the topic partition to move
    TopicPartition(TopicPartition&& other) noexcept;

    /// Copies `other` into this topic partition.
    /// @param other the topic partition to copy
    /// @return this topic partition
    TopicPartition& operator=(const TopicPartition& other);

    /// Moves `other` into this topic partition; `other` is left empty.
    /// @param other the topic partition to move
    /// @return this topic partition
    TopicPartition& operator=(TopicPartition&& other) noexcept;

    ~TopicPartition();

    /// Returns the partition number.
    /// @return the partition number
    [[nodiscard]] int32_t partition() const;

    /// Returns the topic name.
    /// @return the topic name
    [[nodiscard]] const std::string& topic() const;

    /// Whether `a` and `b` name the same partition of the same topic.
    /// @param a a topic partition
    /// @param b another topic partition
    /// @return true when the topic and the partition are equal
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const TopicPartition& a, const TopicPartition& b);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns `<topic>-<partition>`, as Java's toString.
/// @param tp a topic partition
/// @return the topic, a dash and the partition
CONFLUENT_KAFKA_CPP_API std::string to_string(const TopicPartition& tp);

/// Writes `to_string(tp)` to `out`.
/// @param out the stream
/// @param tp a topic partition
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const TopicPartition& tp);

}  // namespace confluent::kafka::common

/// Hashes a topic partition from its topic and partition, as Java's hashCode reads them.
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::common::TopicPartition> {
    /// Returns the hash of `tp`.
    /// @param tp a topic partition
    /// @return its hash
    std::size_t operator()(const confluent::kafka::common::TopicPartition& tp) const noexcept;
};
