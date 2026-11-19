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

// ConfigResource (org.apache.kafka.common.config.ConfigResource), from Java's
// ConfigResourceTest, plus the class's own members.

#include <confluent/kafka/common/config.hpp>

#include <gtest/gtest.h>

#include <array>
#include <sstream>

namespace {

using confluent::kafka::common::config::ConfigResource;
namespace config_resource_type = confluent::kafka::common::config::config_resource::type;
using Type = ConfigResource::Type;

TEST(ConfigResourceTest, shouldGetTypeFromId) {
    EXPECT_EQ(Type::TOPIC, config_resource_type::for_id(2));
    EXPECT_EQ(Type::BROKER, config_resource_type::for_id(4));
    EXPECT_EQ(Type::CLIENT_METRICS, config_resource_type::for_id(16));
    EXPECT_EQ(Type::GROUP, config_resource_type::for_id(32));
}

TEST(ConfigResourceTest, shouldReturnUnknownForUnknownCode) {
    EXPECT_EQ(Type::UNKNOWN, config_resource_type::for_id(-1));
    EXPECT_EQ(Type::UNKNOWN, config_resource_type::for_id(0));
    EXPECT_EQ(Type::UNKNOWN, config_resource_type::for_id(1));
}

// Java iterates ConfigResource.Type.values(); the compiler's values() is not
// translated (cpp/CLAUDE.md §3.3), so the constants are listed.
TEST(ConfigResourceTest, shouldRoundTripEveryType) {
    for (const auto type :
         {Type::GROUP, Type::CLIENT_METRICS, Type::BROKER_LOGGER, Type::BROKER, Type::TOPIC, Type::UNKNOWN}) {
        EXPECT_EQ(type, config_resource_type::for_id(id(type)));
    }
}

TEST(ConfigResourceTest, KeepsItsTypeAndNameAndPrintsThem) {
    const ConfigResource topic(Type::TOPIC, "orders");
    EXPECT_EQ(topic.type(), Type::TOPIC);
    EXPECT_EQ(topic.name(), "orders");
    EXPECT_FALSE(topic.is_default());
    EXPECT_TRUE(ConfigResource(Type::BROKER, "").is_default());
    EXPECT_EQ(topic, ConfigResource(Type::TOPIC, "orders"));
    EXPECT_NE(topic, ConfigResource(Type::BROKER, "orders"));
    EXPECT_EQ(to_string(topic), "ConfigResource(type=TOPIC, name='orders')");
    std::ostringstream out;
    out << topic;
    EXPECT_EQ(out.str(), "ConfigResource(type=TOPIC, name='orders')");
}

}  // namespace
