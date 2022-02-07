#include "broker/endpoint_id.hh"

#include <new>

#include <caf/node_id.hpp>

#include "broker/internal/native.hh"

namespace broker {

namespace {

using internal::native;
using native_t = caf::node_id;

} // namespace

static_assert(sizeof(endpoint_id::impl*) == sizeof(caf::node_id));

endpoint_id::endpoint_id() noexcept {
  new (obj_) native_t();
}

endpoint_id::endpoint_id(endpoint_id&& other) noexcept {
  new (obj_) native_t(std::move(native(other)));
}

endpoint_id::endpoint_id(const endpoint_id& other) noexcept {
  new (obj_) native_t(native(other));
}

endpoint_id::endpoint_id(const impl* ptr) noexcept  {
  new (obj_) native_t(native(ptr));
}

endpoint_id& endpoint_id::operator=(endpoint_id&& other) noexcept {
  native(*this) = std::move(native(other));
  return *this;
}

endpoint_id& endpoint_id::operator=(const endpoint_id& other) noexcept {
  native(*this) = native(other);
  return *this;
}

endpoint_id::~endpoint_id() {
  native(*this).~native_t();
}

bool endpoint_id::valid() const noexcept {
  return static_cast<bool>(native(*this));
}

void endpoint_id::swap(endpoint_id& other) noexcept {
  native(*this).swap(native(other));
}

int endpoint_id::compare(const endpoint_id& other) const noexcept {
  return native(*this).compare(native(other));
}

size_t endpoint_id::hash() const noexcept {
  std::hash<caf::node_id> f;
  return f(native(*this));
}

endpoint_id::impl* endpoint_id::native_ptr() noexcept {
  return reinterpret_cast<impl*>(obj_);
}

const endpoint_id::impl* endpoint_id::native_ptr() const noexcept {
  return reinterpret_cast<const impl*>(obj_);
}

std::string to_string(const endpoint_id& x) {
  return to_string(native(x));
}

} // namespace broker
