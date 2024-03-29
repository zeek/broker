#pragma once

#include "broker/fwd.hh"
#include "broker/internal/meta_data_writer.hh"

#include <caf/fwd.hpp>

#include <cstdint>

namespace broker::internal {

/// Writes meta information of Broker commands to a serializer.
class meta_command_writer {
public:
  meta_command_writer(caf::binary_serializer& sink);

  caf::error operator()(const internal_command& x);

  caf::error operator()(const none& x);

  caf::error operator()(const put_command& x);

  caf::error operator()(const put_unique_command& x);

  caf::error operator()(const erase_command& x);

  caf::error operator()(const expire_command& x);

  caf::error operator()(const add_command& x);

  caf::error operator()(const subtract_command& x);

  caf::error operator()(const snapshot_command& x);

  caf::error operator()(const snapshot_sync_command& x);

  caf::error operator()(const set_command& x);

  caf::error operator()(const clear_command& x);

private:
  caf::error apply_tag(uint8_t tag);

  meta_data_writer writer_;
};

} // namespace broker::internal
