#pragma once

#include <caf/fwd.hpp>
#include <caf/stream_manager.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

/// A stream manager with at most one inbound and at most one outbound path.
class unipath_manager : public caf::stream_manager {
public:
  using super = caf::stream_manager;

  explicit unipath_manager(central_dispatcher* dispatcher);

  ~unipath_manager() override;

  using super::handle;

  virtual ptrdiff_t enqueue(caf::span<const item_ptr> ptrs) = 0;

  virtual filter_type filter() = 0;

  virtual void filter(filter_type) = 0;

protected:
  central_dispatcher* dispatcher_;
};

using unipath_manager_ptr = caf::intrusive_ptr<unipath_manager>;

unipath_manager_ptr make_data_source(central_dispatcher* dispatcher);

unipath_manager_ptr make_command_source(central_dispatcher* dispatcher);

unipath_manager_ptr make_data_sink(central_dispatcher* dispatcher,
                                   filter_type filter);

unipath_manager_ptr make_command_sink(central_dispatcher* dispatcher,
                                      filter_type filter);

// Peer managers always have one inbound and one outbound path.
unipath_manager_ptr make_peer_manager(central_dispatcher* dispatcher);

} // namespace broker::detail
