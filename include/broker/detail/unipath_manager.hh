#pragma once

#include <functional>

#include <caf/fwd.hpp>
#include <caf/stream_manager.hpp>

#include "broker/detail/item_scope.hh"
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

  struct observer {
    virtual ~observer();
    virtual void closing(unipath_manager*, bool, const caf::error&) = 0;
    virtual void downstream_connected(unipath_manager*, const caf::actor&) = 0;
  };

  explicit unipath_manager(central_dispatcher*, observer*);

  ~unipath_manager() override;

  using super::handle;

  virtual bool enqueue(const unipath_manager* source, item_scope scope,
                       caf::span<const node_message> xs)
    = 0;

  /// Returns the filter that this manager applies to enqueued items.
  virtual filter_type filter() = 0;

  /// Sets the filter that this manager applies to enqueued items.
  virtual void filter(filter_type) = 0;

  /// Checks whether the filter of this manager accepts messages for the given
  /// topic.
  [[nodiscard]] virtual bool accepts(const topic&) const noexcept = 0;

  /// Causes the manager to cache incoming batches until `unblock_inputs()` gets
  /// called.
  virtual void block_inputs();

  /// Release all currently blocked batches and allow processing of batches
  /// again.
  virtual void unblock_inputs();

  /// Returns whether this manager currently blocks incoming batches.
  virtual bool blocks_inputs();

  /// Returns the type ID of the message type accepted by this manager.
  virtual caf::type_id_t message_type() const noexcept = 0;

  /// Returns whether this manager has exactly one inbound path.
  bool has_inbound_path() const noexcept;

  /// Returns whether this manager has exactly one outbound path.
  bool has_outbound_path() const noexcept;

  /// Returns the slot for the inbound path or `caf::invalid_stream_slot` if
  /// none exists.
  caf::stream_slot inbound_path_slot() const noexcept;

  /// Returns the slot for the outbound path or `caf::invalid_stream_slot` if
  /// none exists.
  caf::stream_slot outbound_path_slot() const noexcept;

  /// Returns the connected actor.
  caf::actor hdl() const noexcept;

  /// Returns whether this manager has exactly one inbound and exactly one
  /// outbound path.
  bool fully_connected() const noexcept {
    return has_inbound_path() && has_outbound_path();
  }

  /// Removes the observer, thereby discarding all future events.
  void unobserve() {
    observer_ = nullptr;
  }

  // -- overrides --------------------------------------------------------------

  bool congested(const caf::inbound_path&) const noexcept override;

  void handle(caf::inbound_path*, caf::downstream_msg::close&) override;

  void handle(caf::inbound_path*, caf::downstream_msg::forced_close&) override;

  void handle(caf::stream_slots, caf::upstream_msg::drop&) override;

  void handle(caf::stream_slots, caf::upstream_msg::forced_drop&) override;

protected:
  void closing(bool graceful, const caf::error& reason);

  central_dispatcher* dispatcher_;
  observer* observer_ = nullptr;
};

using unipath_manager_ptr = caf::intrusive_ptr<unipath_manager>;

unipath_manager_ptr make_data_source(central_dispatcher* dispatcher);

unipath_manager_ptr make_command_source(central_dispatcher* dispatcher);

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<data_message> in);

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<command_message> in);

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<node_message_content> in);

unipath_manager_ptr make_data_sink(central_dispatcher* dispatcher,
                                   filter_type filter);

unipath_manager_ptr make_command_sink(central_dispatcher* dispatcher,
                                      filter_type filter);

/// Peer managers always have one inbound and one outbound path.
/// @note the returned manager returns `true` for `blocks_inputs()` and Broker
///       does *not* automatically add the manager to `dispatcher`.
unipath_manager_ptr make_peer_manager(central_dispatcher* dispatcher,
                                      unipath_manager::observer* observer);

} // namespace broker::detail

namespace std {

template <>
struct hash<broker::detail::unipath_manager_ptr> {
  using argument_type = broker::detail::unipath_manager_ptr;
  size_t operator()(const argument_type& x) const noexcept {
    hash<broker::detail::unipath_manager*> f;
    return f(x.get());
  }
};

} // namespace std
