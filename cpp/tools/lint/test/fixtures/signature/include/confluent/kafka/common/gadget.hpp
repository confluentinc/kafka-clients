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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace confluent::kafka::common {
class Gadget {
public:
    Gadget(std::string_view name, int size);  // EXPECT: confluent-kafka-java-signature parameter 'size' of 'Gadget' is int
    int64_t total(int32_t b, std::string_view label);  // EXPECT: confluent-kafka-java-signature is named a in Java
    [[nodiscard]] int32_t length() const;  // EXPECT: confluent-kafka-java-signature returns int32_t
    [[nodiscard]] std::string describe() const;
    void rename(std::string_view name = "x");  // EXPECT: confluent-kafka-java-signature has a default argument
    Gadget create();  // EXPECT: confluent-kafka-java-signature is not static
    [[nodiscard]] std::optional<std::string> label(std::string_view key) const;
    [[nodiscard]] std::optional<std::string> tag(std::string_view key) const;  // EXPECT: confluent-kafka-java-signature org.apache.kafka.common.Gadget#tag(String) return nullable
};
}  // namespace confluent::kafka::common
