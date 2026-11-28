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

// SubscriptionPattern (org.apache.kafka.clients.consumer.SubscriptionPattern).
// Java ships no test of it; these are the Rust core's tests of its
// translation (rust/src/consumer/subscription_pattern.rs), as cpp/CLAUDE.md
// §11.1 says.

#include <confluent/kafka/consumer.hpp>

#include <gtest/gtest.h>

#include <sstream>
#include <unordered_set>

namespace {

using confluent::kafka::consumer::SubscriptionPattern;

// test_pattern_accessor
TEST(SubscriptionPatternTest, KeepsThePattern) {
    const SubscriptionPattern p("orders-.*");
    EXPECT_EQ(p.pattern(), "orders-.*");
}

// test_display_is_pattern
TEST(SubscriptionPatternTest, PrintsThePattern) {
    const SubscriptionPattern p("orders-.*");
    EXPECT_EQ(to_string(p), "orders-.*");
    std::ostringstream out;
    out << p;
    EXPECT_EQ(out.str(), "orders-.*");
}

// test_equality_and_hash
TEST(SubscriptionPatternTest, IsEqualAndHashedByItsPattern) {
    const SubscriptionPattern a("a.*");
    const SubscriptionPattern b("a.*");
    const SubscriptionPattern c("b.*");
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
    const std::unordered_set<SubscriptionPattern> set{a, b, c};
    EXPECT_EQ(set.size(), 2U);
}

}  // namespace
