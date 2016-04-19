#ifndef BROKER_QUEUE_HH
#define BROKER_QUEUE_HH

#include <deque>
#include <memory>

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/atoms.hh"
#include "broker/detail/flare.hh"

namespace broker {

extern std::unique_ptr<caf::actor_system> broker_system;

namespace detail {

// Provides a generic queuing mechanism implemented as an actor combined with a
// flare which signals when the queue is non-empty.  This makes it simple to
// integrate in to traditional event loops.
template <typename Message>
class queue_actor : public caf::event_based_actor {
public:
  queue_actor(caf::actor_config& cfg, flare f)
    : caf::event_based_actor{cfg}, ready_flare(std::move(f)) {
    using namespace caf;
    message_handler common{
      [=](want_atom) { 
        return pop(); 
      },
      [=](Message& msg) {
        q.push_back(std::move(msg));
        this->become(filled);
        ready_flare.fire();
      }
    };
    empty = common.or_else([=](need_atom) { return skip_message(); });
    filled = common.or_else([=](need_atom) { return pop(); });
  }

private:
  caf::behavior make_behavior() override {
    return empty;
  }

  std::deque<Message> pop() {
    auto rval = std::move(q);
    q = {};
    this->become(empty);
    ready_flare.extinguish();
    return rval;
  }

  flare ready_flare;
  caf::behavior empty;
  caf::behavior filled;
  std::deque<Message> q;
};

} // namespace detail

/// Stores items of type T that are awaiting retrieval/processing.
template <class T>
class queue {
  queue(const queue&) = delete;
  queue& operator=(const queue&) = delete;

public:
  queue() 
    : self_{*broker_system} {
    detail::flare f;
    fd_ = f.fd();
    actor_ = broker_system->spawn<detail::queue_actor<T>, caf::priority_aware>(
      std::move(f));
    // FIXME: do not rely on private API.
    self_->planned_exit_reason(caf::exit_reason::unknown);
    actor_->link_to(self_);
  }

  /// @return a file descriptor that is ready for reading when the queue is
  ///         non-empty, suitable for use with poll, select, etc.
  int fd() const {
    return fd_;
  }

  /// @return The contents of the queue at time of call, which may be empty.
  std::deque<T> want_pop() const {
    return pop<want_atom::value>();
  }

  /// @return The contents of the queue, which must contain at least one item.
  /// If it must, the call blocks until at least one item is available.
  std::deque<T> need_pop() const {
    return pop<need_atom::value>();
  }

  void* handle() const {
    return const_cast<caf::actor*>(&actor_);
  }

private:
  template <caf::atom_value Atom>
  std::deque<T> pop() const {
    std::deque<T> rval;
    self_->send(caf::message_priority::high, actor_, Atom);
    self_->receive(
      [&rval](std::deque<T>& msgs) { rval = std::move(msgs); }
    );
    return rval;
  }

  int fd_;
  caf::scoped_actor self_;
  caf::actor actor_;
};

} // namespace broker

#endif // BROKER_QUEUE_HH
