#include "broker/detail/opaque_type.hh"

#include <caf/ref_counted.hpp>

namespace broker::detail {

void intrusive_ptr_add_ref(const opaque_type* ptr) noexcept {
  reinterpret_cast<const caf::ref_counted*>(ptr)->ref();
}

void intrusive_ptr_release(const opaque_type* ptr) noexcept {
  reinterpret_cast<const caf::ref_counted*>(ptr)->deref();
}

} // namespace broker::detail
