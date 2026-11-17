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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::consumer {

/// Represents a regular expression compatible with Google RE2/J, used to subscribe to topics.
/// This just keeps the String representation of the pattern, and all validations to ensure
/// it is RE2/J compatible are delegated to the broker.
class SubscriptionPattern {
public:
    /// Constructs a new subscription pattern.
    /// @param pattern The regular expression pattern compatible with RE2/J
    explicit SubscriptionPattern(std::string_view pattern)
        : impl_(std::make_shared<const Impl>(Impl{std::string(pattern)})) {}

    /// Returns the pattern.
    /// @return Regular expression pattern compatible with RE2/J.
    [[nodiscard]] const std::string& pattern() const { return impl_->pattern; }

    /// Whether `a` and `b` hold the same pattern.
    /// @param a a subscription pattern
    /// @param b another subscription pattern
    /// @return true when the patterns are equal
    friend bool operator==(const SubscriptionPattern& a, const SubscriptionPattern& b) {
        return a.impl_ == b.impl_ || a.pattern() == b.pattern();
    }

private:
    // cpp/CLAUDE.md §12.3: one pointer; the value is immutable, so copies share it.
    struct Impl {
        std::string pattern;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns the pattern, as Java's toString.
/// @param pattern a subscription pattern
/// @return the pattern
inline std::string to_string(const SubscriptionPattern& pattern) {
    return pattern.pattern();
}

/// Writes `to_string(pattern)` to `out`.
/// @param out the stream
/// @param pattern a subscription pattern
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const SubscriptionPattern& pattern) {
    return out << pattern.pattern();
}

}  // namespace confluent::kafka::consumer

/// Hashes a subscription pattern by its pattern, as Java's hashCode.
template <>
struct std::hash<confluent::kafka::consumer::SubscriptionPattern> {
    std::size_t operator()(const confluent::kafka::consumer::SubscriptionPattern& p) const noexcept {
        return std::hash<std::string>{}(p.pattern());
    }
};
