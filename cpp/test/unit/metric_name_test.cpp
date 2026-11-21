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

// MetricName (org.apache.kafka.common.MetricName).
//
// Java has no MetricNameTest; these are the Rust core's tests of its
// translation (rust/src/common/metric_name.rs), as cpp/CLAUDE.md §11.1 says,
// plus the getters, toString and the hash.

#include <confluent/kafka/common.hpp>

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace {

using confluent::kafka::common::MetricName;
using Tags = std::unordered_map<std::string, std::string>;

TEST(MetricNameTest, KeepsItsFields) {
    const MetricName n("message-size-avg", "producer-metrics", "average message size", Tags{{"client-id", "p1"}});
    EXPECT_EQ(n.name(), "message-size-avg");
    EXPECT_EQ(n.group(), "producer-metrics");
    EXPECT_EQ(n.description(), "average message size");
    EXPECT_EQ(n.tags(), (Tags{{"client-id", "p1"}}));
}

TEST(MetricNameTest, EqualityIgnoresDescription) {
    const MetricName n1("n", "g", "desc-a", Tags{{"k", "v"}});
    const MetricName n2("n", "g", "desc-b", Tags{{"k", "v"}});
    EXPECT_EQ(n1, n2);
    EXPECT_EQ(std::hash<MetricName>{}(n1), std::hash<MetricName>{}(n2));
}

TEST(MetricNameTest, EqualityIsTagOrderIndependent) {
    Tags t1;
    t1.emplace("a", "1");
    t1.emplace("b", "2");
    Tags t2;
    t2.emplace("b", "2");
    t2.emplace("a", "1");
    EXPECT_EQ(MetricName("n", "g", "", t1), MetricName("n", "g", "", t2));
    EXPECT_EQ(std::hash<MetricName>{}(MetricName("n", "g", "", t1)),
              std::hash<MetricName>{}(MetricName("n", "g", "", t2)));
}

TEST(MetricNameTest, InequalityOnNameGroupTags) {
    const MetricName base("n", "g", "", Tags{{"k", "v"}});
    EXPECT_NE(base, MetricName("n2", "g", "", Tags{{"k", "v"}}));
    EXPECT_NE(base, MetricName("n", "g2", "", Tags{{"k", "v"}}));
    EXPECT_NE(base, MetricName("n", "g", "", Tags{{"k", "v2"}}));
}

TEST(MetricNameTest, ToString) {
    EXPECT_EQ(to_string(MetricName("n", "g", "d", Tags{{"b", "2"}, {"a", "1"}})),
              "MetricName [name=n, group=g, description=d, tags={a=1, b=2}]");
    std::ostringstream out;
    out << MetricName("n", "g", "", Tags{});
    EXPECT_EQ(out.str(), "MetricName [name=n, group=g, description=, tags={}]");
}

TEST(MetricNameTest, HashSet) {
    const std::unordered_set<MetricName> set{MetricName("n", "g", "a", Tags{}), MetricName("n", "g", "b", Tags{}),
                                             MetricName("m", "g", "a", Tags{})};
    EXPECT_EQ(set.size(), 2U);
}

}  // namespace
