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

// SslClientAuth (org.apache.kafka.common.config.SslClientAuth).
//
// Java has no SslClientAuthTest; these are the Rust core's tests of its
// translation (rust/src/common/config/ssl_client_auth.rs), as cpp/CLAUDE.md
// §11.1 says. Its test_default, test_from_str_roundtrip and
// test_from_str_case_insensitive test Rust's Default and FromStr, which have
// no Java member and so no C++ translation.

#include <confluent/kafka/common/config.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <sstream>

namespace {

using confluent::kafka::common::config::SslClientAuth;
namespace ssl_client_auth = confluent::kafka::common::config::ssl_client_auth;

TEST(SslClientAuthTest, ForConfigNoneKey) {
    EXPECT_EQ(ssl_client_auth::for_config(std::nullopt), SslClientAuth::NONE);
}

TEST(SslClientAuthTest, ForConfigRequired) {
    EXPECT_EQ(ssl_client_auth::for_config("REQUIRED"), SslClientAuth::REQUIRED);
}

TEST(SslClientAuthTest, ForConfigRequested) {
    EXPECT_EQ(ssl_client_auth::for_config("REQUESTED"), SslClientAuth::REQUESTED);
}

TEST(SslClientAuthTest, ForConfigNoneVariant) {
    EXPECT_EQ(ssl_client_auth::for_config("NONE"), SslClientAuth::NONE);
}

TEST(SslClientAuthTest, ForConfigCaseInsensitive) {
    EXPECT_EQ(ssl_client_auth::for_config("required"), SslClientAuth::REQUIRED);
    EXPECT_EQ(ssl_client_auth::for_config("Requested"), SslClientAuth::REQUESTED);
    EXPECT_EQ(ssl_client_auth::for_config("none"), SslClientAuth::NONE);
}

TEST(SslClientAuthTest, ForConfigInvalid) {
    EXPECT_EQ(ssl_client_auth::for_config("INVALID"), std::nullopt);
    EXPECT_EQ(ssl_client_auth::for_config(""), std::nullopt);
}

TEST(SslClientAuthTest, Display) {
    EXPECT_EQ(to_string(SslClientAuth::REQUIRED), "required");
    EXPECT_EQ(to_string(SslClientAuth::REQUESTED), "requested");
    std::ostringstream out;
    out << SslClientAuth::NONE;
    EXPECT_EQ(out.str(), "none");
}

TEST(SslClientAuthTest, ValuesInJavaOrder) {
    ASSERT_EQ(ssl_client_auth::VALUES.size(), 3U);
    EXPECT_EQ(ssl_client_auth::VALUES[0], SslClientAuth::REQUIRED);
    EXPECT_EQ(ssl_client_auth::VALUES[1], SslClientAuth::REQUESTED);
    EXPECT_EQ(ssl_client_auth::VALUES[2], SslClientAuth::NONE);
}

}  // namespace
