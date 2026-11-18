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

#include <confluent/kafka/common/node.hpp>

#include <ostream>
#include <string>

namespace confluent::kafka::common {

namespace {

std::optional<std::string> optional_string(std::optional<std::string_view> s) {
    return s ? std::optional<std::string>(std::string(*s)) : std::nullopt;
}

}  // namespace

// The value is immutable, so copies share it.
struct Node::Impl {
    int32_t id;
    std::string id_string;
    std::optional<std::string> host;
    int32_t port;
    std::optional<std::string> rack;
    bool is_fenced;
};

Node::Node(int32_t id, std::optional<std::string_view> host, int32_t port)
    : Node(id, host, port, std::nullopt, false) {}

Node::Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack)
    : Node(id, host, port, rack, false) {}

Node::Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack,
           bool is_fenced)
    : impl_(std::make_shared<const Impl>(Impl{.id = id,
                                              .id_string = std::to_string(id),
                                              .host = optional_string(host),
                                              .port = port,
                                              .rack = optional_string(rack),
                                              .is_fenced = is_fenced})) {}

Node::Node(const Node& other) = default;

Node::Node(Node&& other) noexcept = default;

Node& Node::operator=(const Node& other) = default;

Node& Node::operator=(Node&& other) noexcept = default;

Node::~Node() = default;

const Node& Node::no_node() {
    static const Node node(-1, "", -1);
    return node;
}

bool Node::is_empty() const {
    return !impl_->host || impl_->host->empty() || impl_->port < 0;
}

int32_t Node::id() const {
    return impl_->id;
}

const std::string& Node::id_string() const {
    return impl_->id_string;
}

const std::optional<std::string>& Node::host() const {
    return impl_->host;
}

int32_t Node::port() const {
    return impl_->port;
}

bool Node::has_rack() const {
    return impl_->rack.has_value();
}

const std::optional<std::string>& Node::rack() const {
    return impl_->rack;
}

bool Node::is_fenced() const {
    return impl_->is_fenced;
}

bool operator==(const Node& a, const Node& b) {
    return a.impl_ == b.impl_ || (a.id() == b.id() && a.port() == b.port() && a.host() == b.host() &&
                                  a.rack() == b.rack() && a.is_fenced() == b.is_fenced());
}

std::string to_string(const Node& node) {
    return node.host().value_or("null") + ":" + std::to_string(node.port()) + " (id: " + node.id_string() +
           " rack: " + node.rack().value_or("null") + " isFenced: " + (node.is_fenced() ? "true" : "false") + ")";
}

std::ostream& operator<<(std::ostream& out, const Node& node) {
    return out << to_string(node);
}

}  // namespace confluent::kafka::common

std::size_t std::hash<confluent::kafka::common::Node>::operator()(
    const confluent::kafka::common::Node& node) const noexcept {
    const std::hash<std::string> text;
    std::size_t h = 31U + (node.host() ? text(*node.host()) : 0U);
    h = (31U * h) + static_cast<std::size_t>(static_cast<uint32_t>(node.id()));
    h = (31U * h) + static_cast<std::size_t>(static_cast<uint32_t>(node.port()));
    h = (31U * h) + (node.rack() ? text(*node.rack()) : 0U);
    return (31U * h) + (node.is_fenced() ? 1231U : 1237U);
}
