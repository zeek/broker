#ifndef BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH
#define BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH

#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_storage.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/blocking_actor.hpp>
#include <caf/scoped_execution_unit.hpp>

namespace broker {
namespace detail {

class scoped_flare_actor {
public:
  template <class, class, int>
  friend class caf::actor_cast_access;

  static constexpr bool has_weak_ptr_semantics = false;

  scoped_flare_actor(caf::actor_system& sys);

  scoped_flare_actor(const scoped_flare_actor&) = delete;
  scoped_flare_actor(scoped_flare_actor&&) = default;

  ~scoped_flare_actor();

  scoped_flare_actor& operator=(const scoped_flare_actor&) = delete;
  scoped_flare_actor& operator=(scoped_flare_actor&&) = default;

  caf::blocking_actor* operator->() const;

  caf::blocking_actor& operator*() const;

  caf::actor_addr address() const;

  caf::blocking_actor* ptr() const;

  // Because blocking_actor::dequeue is non-virtual, we need to haul the
  // receive functionality through this actor.
  void receive(caf::behavior& bhvr);

  int descriptor() const;

private:
  caf::actor_control_block* get() const;

  caf::scoped_execution_unit context_;
  caf::strong_actor_ptr self_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH
