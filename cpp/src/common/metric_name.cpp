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

#include <confluent/kafka/common/metric_name.hpp>

#include <map>
#include <ostream>
#include <string>

namespace confluent::kafka::common {

// The value is immutable, so copies share it.
struct MetricName::Impl {
    std::string name;
    std::string group;
    std::string description;
    std::unordered_map<std::string, std::string> tags;
};

MetricName::MetricName(std::string_view name, std::string_view group, std::string_view description,
                       const std::unordered_map<std::string, std::string>& tags)
    : impl_(std::make_shared<const Impl>(Impl{.name = std::string(name),
                                              .group = std::string(group),
                                              .description = std::string(description),
                                              .tags = tags})) {}

MetricName::MetricName(const MetricName& other) = default;

MetricName::MetricName(MetricName&& other) noexcept = default;

MetricName& MetricName::operator=(const MetricName& other) = default;

MetricName& MetricName::operator=(MetricName&& other) noexcept = default;

MetricName::~MetricName() = default;

const std::string& MetricName::name() const {
    return impl_->name;
}

const std::string& MetricName::group() const {
    return impl_->group;
}

const std::unordered_map<std::string, std::string>& MetricName::tags() const {
    return impl_->tags;
}

const std::string& MetricName::description() const {
    return impl_->description;
}

bool operator==(const MetricName& a, const MetricName& b) {
    return a.impl_ == b.impl_ || (a.group() == b.group() && a.name() == b.name() && a.tags() == b.tags());
}

std::string to_string(const MetricName& metric_name) {
    std::string tags;
    for (const auto& [key, value] :
         std::map<std::string, std::string>(metric_name.tags().begin(), metric_name.tags().end())) {
        tags.append(tags.empty() ? "" : ", ").append(key).append("=").append(value);
    }
    return "MetricName [name=" + metric_name.name() + ", group=" + metric_name.group() +
           ", description=" + metric_name.description() + ", tags={" + tags + "}]";
}

std::ostream& operator<<(std::ostream& out, const MetricName& metric_name) {
    return out << to_string(metric_name);
}

}  // namespace confluent::kafka::common

std::size_t std::hash<confluent::kafka::common::MetricName>::operator()(
    const confluent::kafka::common::MetricName& metric_name) const noexcept {
    const std::hash<std::string> text;
    std::size_t tags = 0;
    for (const auto& [key, value] : metric_name.tags()) {
        tags += text(key) ^ text(value);
    }
    std::size_t h = 31U + text(metric_name.group());
    h = (31U * h) + text(metric_name.name());
    return (31U * h) + tags;
}
