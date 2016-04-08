#ifndef BROKER_STORE_MASTER_HH
#define BROKER_STORE_MASTER_HH

#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/store/backend.hh"
#include "broker/store/identifier.hh"
#include "broker/store/frontend.hh"
#include "broker/store/memory_backend.hh"

namespace broker {
namespace store {

namespace detail {

class master_actor : public caf::event_based_actor {

public:
  master_actor(caf::actor_config& cfg, std::unique_ptr<backend> s,
               identifier name);
private:
  caf::behavior make_behavior() override;

  void expiry_reminder(const identifier& name, data key,
                       expiration_time expiry);

  void publish(caf::message msg);

  void error(std::string master_name, std::string method_name,
             std::string err_msg);

  std::unique_ptr<backend> datastore;
  std::unordered_map<caf::actor_addr, caf::actor> clones;
  caf::behavior serving;
  caf::behavior init_existing_expiry_reminders;
};

} // namespace detail

/// A master data store.  This type of store is "authoritative" over all its
/// contents meaning that if a clone makes an update, it sends it to the master
/// so that it can make all updates and rebroadcast them to all other clones
/// in a canonical order.
class master : public frontend {
public:
  /// Construct a master data store.
  /// @param e the broker endpoint to attach the master.
  /// @param name a unique name associated with the master store.
  /// A frontend/clone of the master must also use this name and connect via
  /// the same endpoint or via one of its peers.
  /// @param s the storage backend implementation to use.
  master(const endpoint& e, identifier name,
         std::unique_ptr<backend> s
         = std::unique_ptr<backend>(new memory_backend));

private:
  void* handle() const override;

  caf::actor actor_;
  caf::scoped_actor self_;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MASTER_HH
