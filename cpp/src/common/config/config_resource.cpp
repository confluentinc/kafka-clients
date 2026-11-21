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

#include <confluent/kafka/common/config/config_resource.hpp>

#include <ostream>
#include <string>

namespace confluent::kafka::common::config {

namespace {

// Java's Enum.name() of a Type, for toString; Type does not override toString, so cpp/CLAUDE.md §3.3
// gives it no public to_string.
std::string_view type_name(ConfigResource::Type type) {
    switch (type) {
        case ConfigResource::Type::GROUP:
            return "GROUP";
        case ConfigResource::Type::CLIENT_METRICS:
            return "CLIENT_METRICS";
        case ConfigResource::Type::BROKER_LOGGER:
            return "BROKER_LOGGER";
        case ConfigResource::Type::BROKER:
            return "BROKER";
        case ConfigResource::Type::TOPIC:
            return "TOPIC";
        case ConfigResource::Type::UNKNOWN:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

}  // namespace

// The value is immutable, so copies share it.
struct ConfigResource::Impl {
    Type type;
    std::string name;
};

ConfigResource::ConfigResource(Type type, std::string_view name)
    : impl_(std::make_shared<const Impl>(Impl{.type = type, .name = std::string(name)})) {}

ConfigResource::ConfigResource(const ConfigResource& other) = default;

ConfigResource::ConfigResource(ConfigResource&& other) noexcept = default;

ConfigResource& ConfigResource::operator=(const ConfigResource& other) = default;

ConfigResource& ConfigResource::operator=(ConfigResource&& other) noexcept = default;

ConfigResource::~ConfigResource() = default;

ConfigResource::Type ConfigResource::type() const {
    return impl_->type;
}

const std::string& ConfigResource::name() const {
    return impl_->name;
}

bool ConfigResource::is_default() const {
    return impl_->name.empty();
}

bool operator==(const ConfigResource& a, const ConfigResource& b) {
    return a.impl_ == b.impl_ || (a.type() == b.type() && a.name() == b.name());
}

std::string to_string(const ConfigResource& resource) {
    return "ConfigResource(type=" + std::string(type_name(resource.type())) + ", name='" + resource.name() + "')";
}

int8_t id(ConfigResource::Type type) {
    return static_cast<int8_t>(type);
}

std::ostream& operator<<(std::ostream& out, const ConfigResource& resource) {
    return out << to_string(resource);
}

namespace config_resource::type {

ConfigResource::Type for_id(int8_t id) {
    switch (id) {
        case 32:
            return ConfigResource::Type::GROUP;
        case 16:
            return ConfigResource::Type::CLIENT_METRICS;
        case 8:
            return ConfigResource::Type::BROKER_LOGGER;
        case 4:
            return ConfigResource::Type::BROKER;
        case 2:
            return ConfigResource::Type::TOPIC;
        default:
            return ConfigResource::Type::UNKNOWN;
    }
}

}  // namespace config_resource::type
}  // namespace confluent::kafka::common::config

std::size_t std::hash<confluent::kafka::common::config::ConfigResource>::operator()(
    const confluent::kafka::common::config::ConfigResource& r) const noexcept {
    return (31U * std::hash<confluent::kafka::common::config::ConfigResource::Type>{}(r.type())) +
           std::hash<std::string>{}(r.name());
}
