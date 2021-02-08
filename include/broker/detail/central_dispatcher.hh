#pragma once

#include <cstdint>
#include <vector>

#include <caf/fwd.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/unipath_manager.hh"
#include "broker/fwd.hh"

namespace broker::detail {

/// Central point for all `unipath_manager` instances to enqueue items.
class central_dispatcher {
public:
  explicit central_dispatcher(caf::scheduled_actor* self);

  void enqueue(const unipath_manager* source, item_scope scope,
               caf::span<const node_message> messages);

  /// Adds a new output path to the dispatcher.
  void add(unipath_manager_ptr sink);

  auto self() const noexcept {
    return self_;
  }

  const auto& managers() const noexcept {
    return sinks_;
  }

private:
  caf::scheduled_actor* self_;
  std::vector<unipath_manager_ptr> sinks_;
};

} // namespace broker::detail
