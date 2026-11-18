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

#include <confluent/kafka/common/config/ssl_client_auth.hpp>

#include <algorithm>
#include <cctype>
#include <ostream>
#include <string>
#include <utility>

#include "detail/ffi.hpp"

namespace confluent::kafka::common::config {

// cpp/CLAUDE.md §3.3: the header spells the C header's values, which it cannot include.
static_assert(static_cast<int32_t>(SslClientAuth::REQUIRED) == kafka_common_config_SslClientAuth_REQUIRED);
static_assert(static_cast<int32_t>(SslClientAuth::REQUESTED) == kafka_common_config_SslClientAuth_REQUESTED);
static_assert(static_cast<int32_t>(SslClientAuth::NONE) == kafka_common_config_SslClientAuth_NONE);

std::string to_string(SslClientAuth ssl_client_auth) {
    switch (ssl_client_auth) {
        case SslClientAuth::REQUIRED:
            return "required";
        case SslClientAuth::REQUESTED:
            return "requested";
        case SslClientAuth::NONE:
            return "none";
    }
    return {};
}

std::ostream& operator<<(std::ostream& out, SslClientAuth ssl_client_auth) {
    return out << to_string(ssl_client_auth);
}

namespace ssl_client_auth {

std::optional<SslClientAuth> for_config(std::optional<std::string_view> key) {
    if (!key) {
        return SslClientAuth::NONE;
    }
    std::string upper_case_key(*key);
    std::ranges::transform(upper_case_key, upper_case_key.begin(),
                           [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    static constexpr std::array<std::pair<std::string_view, SslClientAuth>, 3> NAMES{{
        {"REQUIRED", SslClientAuth::REQUIRED},
        {"REQUESTED", SslClientAuth::REQUESTED},
        {"NONE", SslClientAuth::NONE},
    }};
    for (const auto& [name, auth] : NAMES) {
        if (upper_case_key == name) {
            return auth;
        }
    }
    return std::nullopt;
}

}  // namespace ssl_client_auth

}  // namespace confluent::kafka::common::config
