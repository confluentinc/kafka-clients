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

// PatternType (org.apache.kafka.common.resource.PatternType).
//
// Java has no PatternTypeTest; these are the Rust core's tests of its
// translation (rust/src/common/resource/pattern_type.rs), as cpp/CLAUDE.md
// §11.1 says.

#include <confluent/kafka/common/resource.hpp>

#include <gtest/gtest.h>

#include <array>

namespace {

using confluent::kafka::common::resource::PatternType;
namespace pattern_type = confluent::kafka::common::resource::pattern_type;

constexpr std::array<PatternType, 5> kValues{PatternType::UNKNOWN, PatternType::ANY, PatternType::MATCH,
                                             PatternType::LITERAL, PatternType::PREFIXED};

TEST(PatternTypeTest, CodeRoundTripsForAllVariants) {
    for (const auto pt : kValues) {
        EXPECT_EQ(pattern_type::from_code(code(pt)), pt);
    }
}

TEST(PatternTypeTest, CodeValuesMatchJavaWireValues) {
    EXPECT_EQ(code(PatternType::UNKNOWN), 0);
    EXPECT_EQ(code(PatternType::ANY), 1);
    EXPECT_EQ(code(PatternType::MATCH), 2);
    EXPECT_EQ(code(PatternType::LITERAL), 3);
    EXPECT_EQ(code(PatternType::PREFIXED), 4);
}

TEST(PatternTypeTest, IsSpecificOnlyForConcreteTypes) {
    EXPECT_FALSE(is_specific(PatternType::UNKNOWN));
    EXPECT_FALSE(is_specific(PatternType::ANY));
    EXPECT_FALSE(is_specific(PatternType::MATCH));
    EXPECT_TRUE(is_specific(PatternType::LITERAL));
    EXPECT_TRUE(is_specific(PatternType::PREFIXED));
}

TEST(PatternTypeTest, IsUnknownOnlyForUnknown) {
    for (const auto pt : kValues) {
        EXPECT_EQ(is_unknown(pt), pt == PatternType::UNKNOWN);
    }
}

TEST(PatternTypeTest, FromStringIsCaseSensitiveAndDefaultsToUnknown) {
    EXPECT_EQ(pattern_type::from_string("LITERAL"), PatternType::LITERAL);
    EXPECT_EQ(pattern_type::from_string("literal"), PatternType::UNKNOWN);
    EXPECT_EQ(pattern_type::from_string("bogus"), PatternType::UNKNOWN);
}

TEST(PatternTypeTest, FromCodeUnknownDefaultsToUnknown) {
    EXPECT_EQ(pattern_type::from_code(120), PatternType::UNKNOWN);
}

}  // namespace
