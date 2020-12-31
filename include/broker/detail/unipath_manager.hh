#pragma once

#include <caf/fwd.hpp>
#include <caf/stream_manager.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

/// A stream manager with at most one inbound and at most one outbound path.
///
/// Unlike CAF's regular stream managers, this manager does *not* forward data
/// from its inbound paths to its outbound paths. In this design, all inbound
/// paths feed into the central dispatcher. The dispatcher then pushes data into
/// the outbound paths of *all* unipath managers. Further, manager implicitly
/// filter out all items created by themselves. This is because our only use
/// case for managers with in- and outbound paths is modeling Broker peers.
/// We keep both paths to a single peer in one stream manager to model a
/// bidirectional connection. Hence, forwarding from the in- to the outbound
/// path would in our case send messages received from a peer back to itself.
class unipath_manager : public caf::stream_manager {
public:
  using super = caf::stream_manager;

  explicit unipath_manager(central_dispatcher* dispatcher);

  ~unipath_manager() override;

  using super::handle;

  /// Pushes items downstream.
  virtual ptrdiff_t enqueue(caf::span<const item_ptr> ptrs) = 0;

  /// Returns the filter that this manager applies to enqueued items.
  virtual filter_type filter() = 0;

  /// Sets the filter that this manager applies to enqueued items.
  virtual void filter(filter_type) = 0;

  /// Causes the manager to cache incoming batches until `unblock_inputs()` gets
  /// called.
  virtual void block_inputs();

  /// Release all currently blocked batches and allow processing of batches
  /// again.
  virtual void unblock_inputs();

  /// Returns whether this manager currently blocks incoming batches.
  virtual bool blocks_inputs();

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

/// Peer managers always have one inbound and one outbound path.
/// @note the returned manager returns `true` for `blocks_inputs()`.
unipath_manager_ptr make_peer_manager(central_dispatcher* dispatcher);

} // namespace broker::detail
