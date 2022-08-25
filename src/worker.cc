#include "broker/worker.hh"

#include <new>

#include <caf/actor.hpp>
#include <caf/send.hpp>

#include "broker/internal/native.hh"

namespace broker {

namespace {

using internal::native;
using native_t = caf::actor;

} // namespace

static_assert(worker::obj_size >= sizeof(caf::actor));

worker::worker() noexcept {
  new (obj_) native_t();
}

worker::worker(worker&& other) noexcept {
  new (obj_) native_t(std::move(native(other)));
}

worker::worker(const worker& other) noexcept {
  new (obj_) native_t(native(other));
}

worker::worker(const impl* ptr) noexcept {
  if (ptr)
    new (obj_) native_t(native(ptr));
  else
    new (obj_) native_t();
}

worker& worker::operator=(worker&& other) noexcept {
  native(*this) = std::move(native(other));
  return *this;
}

worker& worker::operator=(const worker& other) noexcept {
  if (this != &other)
    native(*this) = native(other);
  return *this;
}

worker& worker::operator=(std::nullptr_t) noexcept {
  native(*this) = nullptr;
  return *this;
}

worker::~worker() {
  native(*this).~native_t();
}

bool worker::valid() const noexcept {
  return static_cast<bool>(native(*this));
}

void worker::swap(worker& other) noexcept {
  native(*this).swap(native(other));
}

intptr_t worker::compare(const worker& other) const noexcept {
  return native(*this).compare(native(other));
}

size_t worker::hash() const noexcept {
  std::hash<caf::actor> f;
  return f(native(*this));
}

worker::impl* worker::native_ptr() noexcept {
  return reinterpret_cast<impl*>(obj_);
}

const worker::impl* worker::native_ptr() const noexcept {
  return reinterpret_cast<const impl*>(obj_);
}

std::string to_string(const worker& x) {
  return to_string(native(x));
}

} // namespace broker
