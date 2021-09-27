#pragma once

#include <caf/intrusive_ptr.hpp>

namespace broker::detail {

struct opaque_type;

void intrusive_ptr_add_ref(const opaque_type*);

void intrusive_ptr_release(const opaque_type*);

using opaque_ptr = caf::intrusive_ptr<opaque_type>;

} // namespace broker::detail
