#ifndef BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH
#define BROKER_DETAIL_SCOPED_FLARE_ACTOR_HH

#include <limits>

#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_storage.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/blocking_actor.hpp>
#include <caf/scoped_execution_unit.hpp>

namespace broker {
namespace detail {

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
  explicit mailbox(caf::blocking_actor* actor);

  caf::blocking_actor* actor_;
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

  caf::blocking_actor* operator->() const;

  caf::blocking_actor& operator*() const;

  caf::actor_addr address() const;

  caf::blocking_actor* ptr() const;

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
