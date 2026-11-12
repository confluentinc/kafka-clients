// Copyright 2025 Confluent Inc.
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

// The one place the binding includes the C API. Every other header of the
// binding includes this file, never `confluent_kafka_bindings.h` itself
// (cpp/CLAUDE.md §1): the C header declares no `extern "C"` block of its own.
extern "C" {
#include <confluent_kafka_bindings.h>
}

#include <iostream>
#include <string_view>
#include <utility>

namespace confluent::kafka::detail {

/// The `kafka_common_Error_t **out_err` argument of one C call: NULL on entry,
/// as the C API requires, and freed on destruction when the call stored an
/// error that nobody took with `release()`.
class ErrorSlot;

/// Owner of a C handle, freed with the `_destroy_err` function given as the
/// template argument. Move-only, as the Rust type behind the handle is
/// (cpp/CLAUDE.md §5). A failure of `_destroy_err` is a caught Rust panic:
/// a destructor cannot throw, so it is logged and freed.
template <class T, auto Destroy>
class UniqueHandle {
public:
    using element_type = T;

    UniqueHandle() noexcept = default;
    explicit UniqueHandle(T* handle) noexcept : handle_(handle) {}
    UniqueHandle(UniqueHandle&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
    UniqueHandle& operator=(UniqueHandle&& other) noexcept {
        if (this != &other) {
            reset();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }
    UniqueHandle(const UniqueHandle&) = delete;
    UniqueHandle& operator=(const UniqueHandle&) = delete;
    ~UniqueHandle() { reset(); }

    /// Returns the handle, or `nullptr` after a move or `release()`.
    [[nodiscard]] T* get() const noexcept { return handle_; }
    /// Returns `true` while this object owns a handle.
    [[nodiscard]] explicit operator bool() const noexcept { return handle_ != nullptr; }
    /// Gives up ownership and returns the handle; the caller frees it.
    [[nodiscard]] T* release() noexcept { return std::exchange(handle_, nullptr); }
    /// Frees the owned handle, if any, then owns `handle`.
    void reset(T* handle = nullptr) noexcept;

private:
    T* handle_ = nullptr;
};

/// An owned `kafka_common_Error_t`, as a fallible C function stores it in
/// `*out_err` or as `kafka_common_Error_source_err` hands one out (borrowed
/// there: see `ErrorHandle::borrowed`).
class ErrorHandle {
public:
    ErrorHandle() noexcept = default;
    /// Takes ownership of `error`, which `kafka_common_Error_destroy_err` frees.
    explicit ErrorHandle(kafka_common_Error_t* error) noexcept : owned_(error) {}

    /// Returns the Java class of the error as the C enumerator: the FFI id.
    [[nodiscard]] kafka_common_Error_e code() const noexcept;
    /// Returns Java's `getMessage()`, a view valid while this error lives;
    /// empty when the message is `null`.
    [[nodiscard]] std::string_view message() const noexcept;
    /// Returns the C handle; `nullptr` for a default-constructed object.
    [[nodiscard]] const kafka_common_Error_t* get() const noexcept { return owned_.get(); }
    [[nodiscard]] explicit operator bool() const noexcept { return static_cast<bool>(owned_); }

private:
    UniqueHandle<kafka_common_Error_t, kafka_common_Error_destroy_err> owned_;
};

class ErrorSlot {
public:
    ErrorSlot() noexcept = default;
    ErrorSlot(const ErrorSlot&) = delete;
    ErrorSlot(ErrorSlot&&) = delete;
    ErrorSlot& operator=(const ErrorSlot&) = delete;
    ErrorSlot& operator=(ErrorSlot&&) = delete;
    ~ErrorSlot() { const ErrorHandle discard{std::exchange(error_, nullptr)}; }

    /// The argument to pass as `out_err`; `*slot()` is NULL until a call fails.
    [[nodiscard]] kafka_common_Error_t** slot() noexcept { return &error_; }
    /// Returns `true` once a call stored an error that was not yet taken.
    [[nodiscard]] bool has_error() const noexcept { return error_ != nullptr; }
    /// Takes the stored error, leaving the slot NULL for the next call.
    [[nodiscard]] ErrorHandle release() noexcept { return ErrorHandle{std::exchange(error_, nullptr)}; }

private:
    kafka_common_Error_t* error_ = nullptr;
};

template <class T, auto Destroy>
void UniqueHandle<T, Destroy>::reset(T* handle) noexcept {
    if (handle_ != nullptr) {
        kafka_common_Error_t* failure = nullptr;
        Destroy(handle_, &failure);
        if (failure != nullptr) {
            const ErrorHandle error{failure};
            std::clog << "confluent.kafka: freeing a handle failed: " << error.message() << '\n';
        }
    }
    handle_ = handle;
}

inline kafka_common_Error_e ErrorHandle::code() const noexcept {
    ErrorSlot failure;
    const kafka_common_Error_e code = kafka_common_Error__enum_err(owned_.get(), failure.slot());
    if (!failure.has_error()) {
        return code;
    }
    // The accessor fails only on a NULL handle or a caught panic, both bugs.
    // The C header gives a NULL mandatory argument the first code and every
    // other caught panic the second.
    return owned_ ? kafka_common_Error_LOCAL_ILLEGAL_STATE : kafka_common_Error_LOCAL_NULL_POINTER;
}

inline std::string_view ErrorHandle::message() const noexcept {
    ErrorSlot failure;
    const char* message = kafka_common_Error_message_err(owned_.get(), failure.slot());
    if (failure.has_error() || message == nullptr) {
        return {};
    }
    return message;
}

}  // namespace confluent::kafka::detail
