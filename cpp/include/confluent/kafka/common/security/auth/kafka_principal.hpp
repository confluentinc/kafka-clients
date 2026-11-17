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

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::common::security::auth {

/// Principals in Kafka are defined by a type and a name. The principal type will always be `"User"`
/// for the simple authorizer that is enabled by default, but custom authorizers can leverage different
/// principal types (such as to enable group or role-based ACLs). The KafkaPrincipalBuilder interface
/// is used when you need to derive a different principal type from the authentication context, or when
/// you need to represent relations between different principals. For example, you could extend
/// KafkaPrincipal in order to link a user principal to one or more role principals.
///
/// For custom extensions of KafkaPrincipal, there two key points to keep in mind:
/// 1. To be compatible with the ACL APIs provided by Kafka (including the command line tool), each ACL
///    can only represent a permission granted to a single principal (consisting of a principal type and name).
///    It is possible to use richer ACL semantics, but you must implement your own mechanisms for adding
///    and removing ACLs.
/// 2. In general, KafkaPrincipal extensions are only useful when the corresponding Authorizer
///    is also aware of the extension. If you have a KafkaPrincipalBuilder which derives user groups
///    from the authentication context (e.g. from an SSL client certificate), then you need a custom
///    authorizer which is capable of using the additional group information.
///
/// @par Translation notes
/// Java's object is shared by reference and its token flag changes in place; a C++ copy is a new
/// principal with its own flag.
class KafkaPrincipal {
public:
    /// The principal type of the simple authorizer.
    static constexpr std::string_view USER_TYPE = "User";

    /// Returns the anonymous user principal, `User:ANONYMOUS`.
    /// @return the anonymous principal
    static const KafkaPrincipal& ANONYMOUS() {
        static const KafkaPrincipal principal(USER_TYPE, "ANONYMOUS");
        return principal;
    }

    /// Creates a principal that is not token authenticated.
    /// @param principal_type the principal type
    /// @param name the principal name
    KafkaPrincipal(std::string_view principal_type, std::string_view name)
        : KafkaPrincipal(principal_type, name, false) {}

    /// Creates a principal.
    /// @param principal_type the principal type
    /// @param name the principal name
    /// @param token_authenticated whether the principal authenticated with a delegation token
    KafkaPrincipal(std::string_view principal_type, std::string_view name, bool token_authenticated)
        : impl_(std::make_unique<Impl>(std::string(principal_type), std::string(name), token_authenticated)) {}

    /// Copies `other`: the copy has its own token flag.
    /// @param other the principal to copy
    KafkaPrincipal(const KafkaPrincipal& other)
        : KafkaPrincipal(other.principal_type(), other.name(), other.token_authenticated()) {}

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the principal to move
    KafkaPrincipal(KafkaPrincipal&& other) noexcept = default;

    /// Copies `other` into this principal.
    /// @param other the principal to copy
    /// @return this principal
    KafkaPrincipal& operator=(const KafkaPrincipal& other) {
        if (this != &other) {
            impl_ = KafkaPrincipal(other).impl_;
        }
        return *this;
    }

    /// Moves `other` into this principal; `other` is left empty.
    /// @param other the principal to move
    /// @return this principal
    KafkaPrincipal& operator=(KafkaPrincipal&& other) noexcept = default;

    ~KafkaPrincipal() = default;

    /// Returns the principal name.
    /// @return the name
    [[nodiscard]] const std::string& name() const { return impl_->name; }

    /// Returns the principal type.
    /// @return the principal type
    [[nodiscard]] const std::string& principal_type() const { return impl_->principal_type; }

    /// Sets whether the principal authenticated with a delegation token.
    /// @param token_authenticated whether it did
    void token_authenticated(bool token_authenticated) { impl_->token_authenticated.store(token_authenticated); }

    /// Returns whether the principal authenticated with a delegation token.
    /// @return whether it did
    [[nodiscard]] bool token_authenticated() const { return impl_->token_authenticated.load(); }

    /// Whether `a` and `b` have the same type and name; the token flag is not compared, as in Java.
    /// @param a a principal
    /// @param b another principal
    /// @return true when the principal types and the names are equal
    friend bool operator==(const KafkaPrincipal& a, const KafkaPrincipal& b) {
        return &a == &b || (a.principal_type() == b.principal_type() && a.name() == b.name());
    }

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout. Java's
    // volatile flag is atomic.
    struct Impl {
        Impl(std::string principal_type, std::string name, bool token_authenticated)
            : principal_type(std::move(principal_type)),
              name(std::move(name)),
              token_authenticated(token_authenticated) {}
        std::string principal_type;
        std::string name;
        std::atomic<bool> token_authenticated;
    };
    std::unique_ptr<Impl> impl_;
};

/// Returns `<principal type>:<name>`, as Java's toString.
/// @param principal a principal
/// @return the type, a colon and the name
inline std::string to_string(const KafkaPrincipal& principal) {
    return principal.principal_type() + ":" + principal.name();
}

/// Writes `to_string(principal)` to `out`.
/// @param out the stream
/// @param principal a principal
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const KafkaPrincipal& principal) {
    return out << to_string(principal);
}

}  // namespace confluent::kafka::common::security::auth

/// Hashes a principal from its type and name, as Java's hashCode reads them.
template <>
struct std::hash<confluent::kafka::common::security::auth::KafkaPrincipal> {
    std::size_t operator()(const confluent::kafka::common::security::auth::KafkaPrincipal& principal) const noexcept {
        const std::hash<std::string> text;
        return 31U * text(principal.principal_type()) + text(principal.name());
    }
};
