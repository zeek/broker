#pragma once

#include <cstdint>

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/response_promise.hpp>

#include "broker/network_info.hh"

namespace broker::detail {

struct retry_state {
  network_info addr;
  caf::response_promise rp;
  uint32_t count;
};

} // namespace broker::detail

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::retry_state)
