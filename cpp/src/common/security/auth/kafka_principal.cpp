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

#include <confluent/kafka/common/security/auth/kafka_principal.hpp>

#include <atomic>
#include <ostream>
#include <string>
#include <utility>

namespace confluent::kafka::common::security::auth {

// Java's volatile flag is atomic.
struct KafkaPrincipal::Impl {
    Impl(std::string principal_type, std::string name, bool token_authenticated)
        : principal_type(std::move(principal_type)), name(std::move(name)), token_authenticated(token_authenticated) {}
    std::string principal_type;
    std::string name;
    std::atomic<bool> token_authenticated;
};

const KafkaPrincipal& KafkaPrincipal::ANONYMOUS() {
    static const KafkaPrincipal principal(USER_TYPE, "ANONYMOUS");
    return principal;
}

KafkaPrincipal::KafkaPrincipal(std::string_view principal_type, std::string_view name)
    : KafkaPrincipal(principal_type, name, false) {}

KafkaPrincipal::KafkaPrincipal(std::string_view principal_type, std::string_view name, bool token_authenticated)
    : impl_(std::make_unique<Impl>(std::string(principal_type), std::string(name), token_authenticated)) {}

KafkaPrincipal::KafkaPrincipal(const KafkaPrincipal& other)
    : KafkaPrincipal(other.principal_type(), other.name(), other.token_authenticated()) {}

KafkaPrincipal::KafkaPrincipal(KafkaPrincipal&& other) noexcept = default;

KafkaPrincipal& KafkaPrincipal::operator=(const KafkaPrincipal& other) {
    if (this != &other) {
        impl_ = KafkaPrincipal(other).impl_;
    }
    return *this;
}

KafkaPrincipal& KafkaPrincipal::operator=(KafkaPrincipal&& other) noexcept = default;

KafkaPrincipal::~KafkaPrincipal() = default;

const std::string& KafkaPrincipal::name() const {
    return impl_->name;
}

const std::string& KafkaPrincipal::principal_type() const {
    return impl_->principal_type;
}

void KafkaPrincipal::token_authenticated(bool token_authenticated) {
    impl_->token_authenticated.store(token_authenticated);
}

bool KafkaPrincipal::token_authenticated() const {
    return impl_->token_authenticated.load();
}

bool operator==(const KafkaPrincipal& a, const KafkaPrincipal& b) {
    return &a == &b || (a.principal_type() == b.principal_type() && a.name() == b.name());
}

std::string to_string(const KafkaPrincipal& principal) {
    return principal.principal_type() + ":" + principal.name();
}

std::ostream& operator<<(std::ostream& out, const KafkaPrincipal& principal) {
    return out << to_string(principal);
}

}  // namespace confluent::kafka::common::security::auth

std::size_t std::hash<confluent::kafka::common::security::auth::KafkaPrincipal>::operator()(
    const confluent::kafka::common::security::auth::KafkaPrincipal& principal) const noexcept {
    const std::hash<std::string> text;
    return (31U * text(principal.principal_type())) + text(principal.name());
}
