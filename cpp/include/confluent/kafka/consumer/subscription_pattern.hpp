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
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

namespace confluent::kafka::consumer {

/// Represents a regular expression compatible with Google RE2/J, used to subscribe to topics.
/// This just keeps the String representation of the pattern, and all validations to ensure
/// it is RE2/J compatible are delegated to the broker.
class CONFLUENT_KAFKA_CPP_API SubscriptionPattern {
public:
    /// Constructs a new subscription pattern.
    /// @param pattern The regular expression pattern compatible with RE2/J
    explicit SubscriptionPattern(std::string_view pattern);

    /// Copies `other`, sharing its immutable value.
    /// @param other the subscription pattern to copy
    SubscriptionPattern(const SubscriptionPattern& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the subscription pattern to move
    SubscriptionPattern(SubscriptionPattern&& other) noexcept;

    /// Copies `other` into this subscription pattern.
    /// @param other the subscription pattern to copy
    /// @return this subscription pattern
    SubscriptionPattern& operator=(const SubscriptionPattern& other);

    /// Moves `other` into this subscription pattern; `other` is left empty.
    /// @param other the subscription pattern to move
    /// @return this subscription pattern
    SubscriptionPattern& operator=(SubscriptionPattern&& other) noexcept;

    ~SubscriptionPattern();

    /// Returns the pattern.
    /// @return Regular expression pattern compatible with RE2/J.
    [[nodiscard]] const std::string& pattern() const;

    /// Whether `a` and `b` hold the same pattern.
    /// @param a a subscription pattern
    /// @param b another subscription pattern
    /// @return true when the patterns are equal
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const SubscriptionPattern& a, const SubscriptionPattern& b);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns the pattern, as Java's toString.
/// @param pattern a subscription pattern
/// @return the pattern
CONFLUENT_KAFKA_CPP_API std::string to_string(const SubscriptionPattern& pattern);

/// Writes `to_string(pattern)` to `out`.
/// @param out the stream
/// @param pattern a subscription pattern
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const SubscriptionPattern& pattern);

}  // namespace confluent::kafka::consumer

/// Hashes a subscription pattern by its pattern, as Java's hashCode.
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::consumer::SubscriptionPattern> {
    /// Returns the hash of `p`.
    /// @param p a subscription pattern
    /// @return its hash
    std::size_t operator()(const confluent::kafka::consumer::SubscriptionPattern& p) const noexcept;
};
