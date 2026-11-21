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

#define CK_API __attribute__((visibility("default")))

namespace confluent::kafka::common {

class CK_API Exported {
public:
    Exported();
    Exported(const Exported& other);
    ~Exported();
    [[nodiscard]] int32_t size() const;
    [[nodiscard]] int32_t inline_size() const { return 1; }  // EXPECT: confluent-kafka-library is defined in a public header
    [[nodiscard]] static constexpr int32_t limit() { return 2; }
    void removed() = delete;
    friend CK_API bool operator==(const Exported& a, const Exported& b);
    friend bool operator<(const Exported& a, const Exported& b);  // EXPECT: confluent-kafka-library is not marked CONFLUENT_KAFKA_CPP_API

private:
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

class Hidden {  // EXPECT: confluent-kafka-library is not marked CONFLUENT_KAFKA_CPP_API
public:
    Hidden();
};

class CK_API Defaulted {
public:
    Defaulted();
    ~Defaulted() = default;  // EXPECT: confluent-kafka-library is defined in a public header

private:
    std::shared_ptr<int> impl_;
};

// An interface's constructor stays in the header (§12.3); its destructor does not.
class CK_API Listener {
public:
    virtual ~Listener() {}  // EXPECT: confluent-kafka-library is defined in a public header
    virtual void on_event() = 0;

protected:
    Listener() {}
};

template <class T>
class Box {
public:
    [[nodiscard]] T get() const { return value_; }

private:
    T value_{};
};

template <class T>
T identity(T t) {
    return t;
}

CK_API int32_t exported_function(int32_t x);
int32_t hidden_function(int32_t x);  // EXPECT: confluent-kafka-library is not marked CONFLUENT_KAFKA_CPP_API
CK_API inline int32_t inline_function(int32_t x) { return x; }  // EXPECT: confluent-kafka-library is defined in a public header
constexpr int32_t constexpr_function(int32_t x) { return x; }
inline int32_t uses_box() { return Box<int32_t>().get() + identity(1); }  // EXPECT: confluent-kafka-library is defined in a public header
// EXPECT@86: confluent-kafka-library is not marked CONFLUENT_KAFKA_CPP_API

}  // namespace confluent::kafka::common

template <>
struct CK_API std::hash<confluent::kafka::common::Exported> {
    std::size_t operator()(const confluent::kafka::common::Exported& e) const noexcept;
};

template <>
struct std::hash<confluent::kafka::common::Hidden> {  // EXPECT: confluent-kafka-library is not marked CONFLUENT_KAFKA_CPP_API
    std::size_t operator()(const confluent::kafka::common::Hidden&) const noexcept { return 0; }  // EXPECT: confluent-kafka-library is defined in a public header
};
