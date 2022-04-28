#pragma once

#include "broker/endpoint_id.hh"
#include "broker/error.hh"
#include "broker/status.hh"
#include "broker/worker.hh"

#include <caf/actor.hpp>
#include <caf/error.hpp>
#include <caf/ip_address.hpp>

// Generates the necessary boilerplate code for `native` to work.
#define BROKER_MAP_CAF_TYPE(Native, Facade)                                    \
  template <>                                                                  \
  struct conversion_oracle<Facade> {                                           \
    using native_type = Native;                                                \
    using facade_type = Facade;                                                \
  };                                                                           \
  template <>                                                                  \
  struct conversion_oracle<Facade::impl> : conversion_oracle<Facade> {};       \
  template <>                                                                  \
  struct conversion_oracle<Native> : conversion_oracle<Facade> {};

namespace broker::internal {

/// Metaprogramming utility for implementing `native`.
template <class Facade>
struct conversion_oracle;

BROKER_MAP_CAF_TYPE(caf::error, error)
BROKER_MAP_CAF_TYPE(caf::actor, worker)

/// STL-style alias to safe us some typing.
template <class Facade>
using native_type_t = typename conversion_oracle<Facade>::native_type;

/// STL-style alias to safe us some typing.
template <class Native>
using facade_type_t = typename conversion_oracle<Native>::facade_type;

/// Returns a reference to the native CAF type from an opaque `impl` pointer.
template <class Opaque>
auto& native(Opaque* x) {
  return *reinterpret_cast<native_type_t<Opaque>*>(x);
}

/// Returns a reference to the native CAF type from an opaque `impl` pointer.
template <class Opaque>
auto& native(const Opaque* x) {
  return *reinterpret_cast<const native_type_t<Opaque>*>(x);
}

/// Returns a reference to the native CAF type from a facade object.
template <class Facade>
auto& native(Facade& x) {
  return native(x.native_ptr());
}

/// Returns a reference to the native CAF type from a facade object.
template <class Facade>
auto& native(const Facade& x) {
  return native(x.native_ptr());
}

/// Converts a native CAF object to a facade object.
template <class Native>
auto facade(const Native& x) {
  using res_t = facade_type_t<Native>;
  return res_t{reinterpret_cast<const typename res_t::impl*>(&x)};
}

} // namespace broker::internal

namespace broker {

/// @relates worker
template <class Inspector>
bool inspect(Inspector& f, worker& x) {
  return inspect(f, internal::native(x));
}

/// @relates error
template <class Inspector>
bool inspect(Inspector& f, error& x) {
  return inspect(f, internal::native(x));
}

template <class Inspector>
bool inspect(Inspector& f, status& x) {
  auto verify = [&x] { return internal::native(x.verify()); };
  return f.object(x).on_load(verify).fields(f.field("code", x.code_),
                                            f.field("context", x.context_),
                                            f.field("message", x.message_));
}

} // namespace broker

#undef BROKER_MAP_CAF_TYPE
