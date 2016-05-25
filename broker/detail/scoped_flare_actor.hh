#ifndef BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH
#define BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH

#include <chrono>
#include <limits>

#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_storage.hpp>
#include <caf/blocking_actor.hpp>
#include <caf/extend.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/mixin/requester.hpp>
#include <caf/mixin/sender.hpp>
#include <caf/scoped_execution_unit.hpp>

#include "broker/detail/flare.hh"

namespace broker {
namespace detail {

class flare_actor;

} // namespace detail
} // namespace broker

namespace caf {
namespace mixin {

template <>
struct is_blocking_requester<broker::detail::flare_actor> : std::true_type { };

} // namespace mixin
} // namespace caf

namespace broker {
namespace detail {

class flare_actor
  : public caf::extend<caf::local_actor, flare_actor>::
           with<caf::mixin::requester, caf::mixin::sender> {
public:
  using super = caf::extend<caf::local_actor, flare_actor>::
                with<caf::mixin::requester, caf::mixin::sender>;

  flare_actor(caf::actor_config& sys);

  void initialize() override;

  void act();

  void enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) override;

  void dequeue(caf::behavior& bhvr, caf::message_id mid);

  caf::message dequeue();

  void await_flare(std::chrono::milliseconds timeout =
                   std::chrono::milliseconds(1000));

  const char* name() const override;

  int descriptor() const;

private:
  detail::flare flare_;
};

class scoped_flare_actor;

// An internal proxy object that represents the mailbox of a blocking endpoint.
struct mailbox {
  friend scoped_flare_actor; // construction

public:
  // Retrieves a descriptor that indicates whether a message can be received
  // without blocking.
  int descriptor();

  /// Checks whether the mailbox is empty.
  bool empty();

  // Counts the number of messages in the mailbox, up to given maximum
  // Warning: This is not a constant-time operations, hence the name `count`
  //          as opposed to `size`. The function takes time *O(n)* where *n*
  //          is the size of the mailbox.
  size_t count(size_t max = std::numeric_limits<size_t>::max());

private:
  explicit mailbox(flare_actor* actor);

  flare_actor* actor_;
};

// A caf::scoped_actor equipped with a descriptor suitable for poll/select
// loops that signals whether the actor's mailbox is emppty, i.e., whether the
// actor can receive messages without blocking.
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

  flare_actor* operator->() const;

  flare_actor& operator*() const;

  caf::actor_addr address() const;

  flare_actor* ptr() const;

  caf::message dequeue();

  void dequeue(caf::behavior& bhvr);

  detail::mailbox mailbox();

private:
  caf::actor_control_block* get() const;

  caf::scoped_execution_unit context_;
  caf::strong_actor_ptr self_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH
