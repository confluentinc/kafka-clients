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

#include <confluent/kafka/common/cluster_resource.hpp>

#include <ostream>
#include <string>

namespace confluent::kafka::common {

// The value is immutable, so copies share it.
struct ClusterResource::Impl {
    std::optional<std::string> cluster_id;
};

ClusterResource::ClusterResource(std::optional<std::string_view> cluster_id)
    : impl_(std::make_shared<const Impl>(
          Impl{cluster_id ? std::optional<std::string>(std::string(*cluster_id)) : std::nullopt})) {}

ClusterResource::ClusterResource(const ClusterResource& other) = default;

ClusterResource::ClusterResource(ClusterResource&& other) noexcept = default;

ClusterResource& ClusterResource::operator=(const ClusterResource& other) = default;

ClusterResource& ClusterResource::operator=(ClusterResource&& other) noexcept = default;

ClusterResource::~ClusterResource() = default;

const std::optional<std::string>& ClusterResource::cluster_id() const {
    return impl_->cluster_id;
}

bool operator==(const ClusterResource& a, const ClusterResource& b) {
    return a.impl_ == b.impl_ || a.cluster_id() == b.cluster_id();
}

std::string to_string(const ClusterResource& resource) {
    return "ClusterResource(clusterId=" + resource.cluster_id().value_or("null") + ")";
}

std::ostream& operator<<(std::ostream& out, const ClusterResource& resource) {
    return out << to_string(resource);
}

}  // namespace confluent::kafka::common

std::size_t std::hash<confluent::kafka::common::ClusterResource>::operator()(
    const confluent::kafka::common::ClusterResource& resource) const noexcept {
    const auto& id = resource.cluster_id();
    return 31U + (id ? std::hash<std::string>{}(*id) : 0U);
}
