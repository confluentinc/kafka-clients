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

#include <confluent/kafka/consumer/subscription_pattern.hpp>

#include <ostream>
#include <string>

namespace confluent::kafka::consumer {

// The value is immutable, so copies share it.
struct SubscriptionPattern::Impl {
    std::string pattern;
};

SubscriptionPattern::SubscriptionPattern(std::string_view pattern)
    : impl_(std::make_shared<const Impl>(Impl{std::string(pattern)})) {}

SubscriptionPattern::SubscriptionPattern(const SubscriptionPattern& other) = default;

SubscriptionPattern::SubscriptionPattern(SubscriptionPattern&& other) noexcept = default;

SubscriptionPattern& SubscriptionPattern::operator=(const SubscriptionPattern& other) = default;

SubscriptionPattern& SubscriptionPattern::operator=(SubscriptionPattern&& other) noexcept = default;

SubscriptionPattern::~SubscriptionPattern() = default;

const std::string& SubscriptionPattern::pattern() const {
    return impl_->pattern;
}

bool operator==(const SubscriptionPattern& a, const SubscriptionPattern& b) {
    return a.impl_ == b.impl_ || a.pattern() == b.pattern();
}

std::string to_string(const SubscriptionPattern& pattern) {
    return pattern.pattern();
}

std::ostream& operator<<(std::ostream& out, const SubscriptionPattern& pattern) {
    return out << pattern.pattern();
}

}  // namespace confluent::kafka::consumer

std::size_t std::hash<confluent::kafka::consumer::SubscriptionPattern>::operator()(
    const confluent::kafka::consumer::SubscriptionPattern& p) const noexcept {
    return std::hash<std::string>{}(p.pattern());
}
