#ifndef BROKER_STORE_CLONE_HH
#define BROKER_STORE_CLONE_HH

#include <caf/actor_system.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/atoms.hh"
#include "broker/report.hh"
#include "broker/store/backend.hh"
#include "broker/store/clone.hh"
#include "broker/store/frontend.hh"
#include "broker/store/memory_backend.hh"
#include "broker/store/sqlite_backend.hh"

namespace broker {
namespace store {

namespace detail {

class clone_actor : public caf::event_based_actor {

public:
  clone_actor(caf::actor_config& cfg, const caf::actor& endpoint, identifier
              master_name, std::chrono::microseconds resync_interval,
              std::unique_ptr<backend> b);

private:
  caf::behavior make_behavior() override;

  void error(std::string master_name, std::string method_name,
             std::string err_msg, bool fatal = false);

  void fatal_error(std::string master_name, std::string method_name,
                   std::string err_msg);

  void get_snapshot(const std::chrono::microseconds& resync_interval);

  void sequence_error(const identifier& master_name,
                      const std::chrono::microseconds& resync_interval);

  bool pending_getsnap_ = false;
  std::unique_ptr<backend> datastore_;
  caf::actor master;
  caf::behavior bootstrap_;
  caf::behavior synchronizing_;
  caf::behavior active_;
  caf::behavior dead_;
};

} // namespace detail

/// A clone of a master data store.  The clone automatically synchronizes to
/// the master version by receiving updates made to the master and applying them
/// locally.  Queries to a cloned store may be quicker than queries to a
/// non-local master store.
class clone : public frontend {
public:
  /// Construct a data store clone.
  /// @param e the broker endpoint to attach the clone.
  /// @param master_name the exact name that the master data store is using.
  /// The master store must be attached either directly to the same endpoint
  /// or to one of its peers.  If attached to a peer, the endpoint must
  /// allow advertising interest in this name.
  /// @param resync_interval the interval at which to re-attempt synchronizing
  /// with the master store should the connection be lost.  If the
  /// clone has not yet synchronized for the first time, updates and queries
  /// queue up until the synchronization completes.  Afterwards, if the
  /// connection to the master store is lost, queries continue to use the
  /// clone's version of the store, but updates will be lost until the master
  /// is once again available.
  /// @param b a backend storage implementation for the clone to use.
  clone(const endpoint& e, identifier master_name,
        std::chrono::duration<double> resync_interval = std::chrono::seconds(1),
        std::unique_ptr<backend> b
        = std::unique_ptr<backend>(new memory_backend));

private:
  void* handle() const override;

  caf::actor actor_;
  caf::scoped_actor self_;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_CLONE_HH
