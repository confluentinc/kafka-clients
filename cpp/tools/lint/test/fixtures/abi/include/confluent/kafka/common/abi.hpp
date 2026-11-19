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
#include <memory>

namespace confluent::kafka::common {

class TwoFields {  // EXPECT: confluent-kafka-abi holds 2 data member(s)
public:
    TwoFields();
    ~TwoFields();

private:
    std::shared_ptr<int> a_;
    std::shared_ptr<int> b_;
};

class Raw {  // EXPECT: confluent-kafka-abi is passed in registers
private:
    int* handle_ = nullptr;
};

struct Agg {  // EXPECT: confluent-kafka-abi is an aggregate
    std::shared_ptr<int> impl;
};

class Virt {
public:
    virtual ~Virt() = default;
    virtual void run();  // EXPECT: confluent-kafka-abi is virtual, but 'Virt' translates no Java interface
};

class Bits {  // EXPECT: confluent-kafka-abi holds 1 data member(s), flags not a pointer
public:
    ~Bits();

private:
    unsigned flags : 3;  // EXPECT: confluent-kafka-abi is a bit-field
};

class Pimpl {
public:
    void run();

private:
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

class Face {
public:
    virtual ~Face() = default;
    virtual void run() = 0;
    virtual void run(int32_t x) = 0;  // EXPECT: confluent-kafka-abi overloads a virtual name
    virtual void stop() = 0;
};

class Base {};
class Diamond : public virtual Base {};  // EXPECT: confluent-kafka-abi inherits virtually

enum Loose : int32_t { A, B };  // EXPECT: confluent-kafka-abi is an unscoped enum

enum class Untyped { A, B };  // EXPECT: confluent-kafka-abi does not write its underlying type

}  // namespace confluent::kafka::common
