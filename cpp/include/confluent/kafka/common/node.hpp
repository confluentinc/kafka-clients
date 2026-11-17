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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// Information about a Kafka node
class Node {
public:
    /// Creates the node `id` at `host`:`port`, with no rack and not fenced.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    Node(int32_t id, std::optional<std::string_view> host, int32_t port) : Node(id, host, port, std::nullopt, false) {}

    /// Creates the node `id` at `host`:`port` in `rack`, not fenced.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    /// @param rack the rack, or `std::nullopt` when it has none
    Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack)
        : Node(id, host, port, rack, false) {}

    /// Creates the node `id` at `host`:`port` in `rack`.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    /// @param rack the rack, or `std::nullopt` when it has none
    /// @param is_fenced whether the broker is fenced
    Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack,
         bool is_fenced)
        : impl_(std::make_shared<const Impl>(
              Impl{id, std::to_string(id), optional_string(host), port, optional_string(rack), is_fenced})) {}

    /// Returns the placeholder node: id -1, empty host, port -1.
    /// @return the placeholder node
    static const Node& no_node() {
        static const Node node(-1, "", -1);
        return node;
    }

    /// Check whether this node is empty, which may be the case if no_node() is used as a placeholder
    /// in a response payload with an error.
    /// @return true if it is, false otherwise
    [[nodiscard]] bool is_empty() const { return !impl_->host || impl_->host->empty() || impl_->port < 0; }

    /// The node id of this node
    /// @return the node id
    [[nodiscard]] int32_t id() const { return impl_->id; }

    /// String representation of the node id.
    /// Typically, the integer id is used to serialize over the wire, the string representation is used as an
    /// identifier with NetworkClient code
    /// @return the node id as a string
    [[nodiscard]] const std::string& id_string() const { return impl_->id_string; }

    /// The host name for this node
    /// @return the host name, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& host() const { return impl_->host; }

    /// The port for this node
    /// @return the port
    [[nodiscard]] int32_t port() const { return impl_->port; }

    /// True if this node has a defined rack
    /// @return whether the rack is defined
    [[nodiscard]] bool has_rack() const { return impl_->rack.has_value(); }

    /// The rack for this node
    /// @return the rack, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& rack() const { return impl_->rack; }

    /// Returns whether this node is fenced.
    ///
    /// This applies to broker nodes only. For controller quorum nodes, this field
    /// is not relevant and is defined to be `false`.
    /// @return whether this node is fenced
    [[nodiscard]] bool is_fenced() const { return impl_->is_fenced; }

    /// Whether `a` and `b` have the same id, host, port, rack and fencing.
    /// @param a a node
    /// @param b another node
    /// @return true when every field but the id string is equal, as Java's equals
    friend bool operator==(const Node& a, const Node& b) {
        return a.impl_ == b.impl_ || (a.id() == b.id() && a.port() == b.port() && a.host() == b.host() &&
                                      a.rack() == b.rack() && a.is_fenced() == b.is_fenced());
    }

private:
    static std::optional<std::string> optional_string(std::optional<std::string_view> s) {
        return s ? std::optional<std::string>(std::string(*s)) : std::nullopt;
    }

    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout. The value is
    // immutable, so copies share it.
    struct Impl {
        int32_t id;
        std::string id_string;
        std::optional<std::string> host;
        int32_t port;
        std::optional<std::string> rack;
        bool is_fenced;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns `<host>:<port> (id: <id> rack: <rack> isFenced: <is_fenced>)`, as Java's toString,
/// with `null` for an absent host or rack.
/// @param node a node
/// @return the node's fields
inline std::string to_string(const Node& node) {
    return node.host().value_or("null") + ":" + std::to_string(node.port()) + " (id: " + node.id_string() +
           " rack: " + node.rack().value_or("null") + " isFenced: " + (node.is_fenced() ? "true" : "false") + ")";
}

/// Writes `to_string(node)` to `out`.
/// @param out the stream
/// @param node a node
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const Node& node) {
    return out << to_string(node);
}

}  // namespace confluent::kafka::common

/// Hashes a node from the fields Java's hashCode reads, in its order.
template <>
struct std::hash<confluent::kafka::common::Node> {
    std::size_t operator()(const confluent::kafka::common::Node& node) const noexcept {
        const std::hash<std::string> text;
        std::size_t h = 31U + (node.host() ? text(*node.host()) : 0U);
        h = 31U * h + static_cast<std::size_t>(static_cast<uint32_t>(node.id()));
        h = 31U * h + static_cast<std::size_t>(static_cast<uint32_t>(node.port()));
        h = 31U * h + (node.rack() ? text(*node.rack()) : 0U);
        return 31U * h + (node.is_fenced() ? 1231U : 1237U);
    }
};
