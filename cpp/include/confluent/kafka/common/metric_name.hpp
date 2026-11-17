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
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

namespace confluent::kafka::common {

/// The `MetricName` class encapsulates a metric's name, logical group and its related attributes. It should be
/// constructed using metrics.metric_name(...).
///
/// This class captures the following parameters:
///
///      **name** The name of the metric
///      **group** logical group name of the metrics to which this metric belongs.
///      **description** A human-readable description to include in the metric. This is optional.
///      **tags** additional key/value attributes of the metric. This is optional.
///
/// group, tags parameters can be used to create unique metric names while reporting in JMX or any custom reporting.
///
/// Ex: standard JMX MBean can be constructed like **domainName:type=group,key1=val1,key2=val2**
class MetricName {
public:
    /// Please create MetricName by method `Metrics::metric_name(name, group, description, tags)`
    ///
    /// @param name        The name of the metric
    /// @param group       logical group name of the metrics to which this metric belongs
    /// @param description A human-readable description to include in the metric
    /// @param tags        additional key/value attributes of the metric
    MetricName(std::string_view name, std::string_view group, std::string_view description,
               const std::unordered_map<std::string, std::string>& tags)
        : impl_(std::make_shared<const Impl>(
              Impl{std::string(name), std::string(group), std::string(description), tags})) {}

    /// Returns the name of the metric.
    /// @return the name
    [[nodiscard]] const std::string& name() const { return impl_->name; }

    /// Returns the logical group name of the metrics to which this metric belongs.
    /// @return the group
    [[nodiscard]] const std::string& group() const { return impl_->group; }

    /// Returns the additional key/value attributes of the metric.
    /// @return the tags
    [[nodiscard]] const std::unordered_map<std::string, std::string>& tags() const { return impl_->tags; }

    /// Returns the human-readable description of the metric.
    /// @return the description
    [[nodiscard]] const std::string& description() const { return impl_->description; }

    /// Whether `a` and `b` have the same name, group and tags; the description is not compared, as in Java.
    /// @param a a metric name
    /// @param b another metric name
    /// @return true when the group, the name and the tags are equal
    friend bool operator==(const MetricName& a, const MetricName& b) {
        return a.impl_ == b.impl_ || (a.group() == b.group() && a.name() == b.name() && a.tags() == b.tags());
    }

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout. The value is
    // immutable, so copies share it.
    struct Impl {
        std::string name;
        std::string group;
        std::string description;
        std::unordered_map<std::string, std::string> tags;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns `MetricName [name=<name>, group=<group>, description=<description>, tags={<k>=<v>, ...}]`, as Java's
/// toString; the tags are printed in key order, where Java prints its map's iteration order.
/// @param metric_name a metric name
/// @return the metric name's fields
inline std::string to_string(const MetricName& metric_name) {
    std::string tags;
    for (const auto& [key, value] :
         std::map<std::string, std::string>(metric_name.tags().begin(), metric_name.tags().end())) {
        tags += (tags.empty() ? "" : ", ") + key + "=" + value;
    }
    return "MetricName [name=" + metric_name.name() + ", group=" + metric_name.group() +
           ", description=" + metric_name.description() + ", tags={" + tags + "}]";
}

/// Writes `to_string(metric_name)` to `out`.
/// @param out the stream
/// @param metric_name a metric name
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const MetricName& metric_name) {
    return out << to_string(metric_name);
}

}  // namespace confluent::kafka::common

/// Hashes a metric name from its group, name and tags, as Java's hashCode reads them; the tags' hash is the
/// order-independent sum of Java's Map.hashCode.
template <>
struct std::hash<confluent::kafka::common::MetricName> {
    std::size_t operator()(const confluent::kafka::common::MetricName& metric_name) const noexcept {
        const std::hash<std::string> text;
        std::size_t tags = 0;
        for (const auto& [key, value] : metric_name.tags()) {
            tags += text(key) ^ text(value);
        }
        std::size_t h = 31U + text(metric_name.group());
        h = 31U * h + text(metric_name.name());
        return 31U * h + tags;
    }
};
