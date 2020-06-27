#pragma once

#include <cstdint>

#include <caf/fwd.hpp>

#include "broker/detail/meta_data_writer.hh"
#include "broker/fwd.hh"

namespace broker::detail {

/// Writes meta information of Broker commands to a serializer.
class meta_command_writer {
public:
  meta_command_writer(caf::binary_serializer& sink);

  caf::error operator()(const internal_command& x);

private:
  caf::error apply_tag(uint8_t tag);

  detail::meta_data_writer writer_;
};

} // namespace broker::detail
