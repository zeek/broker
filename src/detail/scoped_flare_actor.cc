#include "broker/detail/scoped_flare_actor.hh"
#include "broker/detail/flare.hh"

namespace broker {
namespace detail {
namespace {

// A blocking actor which signals its mailbox status via a file descriptor. The
// file descriptor is ready iff the mailbox is not empty, i.e., when a call to
// receive will not block.
class flare_actor : public caf::blocking_actor {
public:
  flare_actor(caf::actor_config& cfg) : blocking_actor(cfg) {
    // FIXME: Dominik mentioned that one should not call is_detached directly,
    // but the CAF code doesn't show how to do it properly via actor_config.
    is_detached(true);
  }

  void enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit* eu) final {
    auto empty = mailbox().empty();
    blocking_actor::enqueue(std::move(ptr), eu);
    if (empty)
      flare_.fire();
  }

  void dequeue(caf::behavior& bhvr,
               caf::message_id mid = caf::invalid_message_id) {
    blocking_actor::dequeue(bhvr, mid);
    if (mailbox().empty())
      flare_.extinguish();
  }

  void act() final {
    CAF_LOG_ERROR("act() of flare_actor called");
  }

  const char* name() const final {
    return "flare_actor";
  }

  int descriptor() const {
    return flare_.fd();
  }

private:
  detail::flare flare_;
};

} // namespace <anonymous>


scoped_flare_actor::scoped_flare_actor(caf::actor_system& sys)
  : context_{&sys} {
  caf::actor_config cfg{&context_};
  self_ = caf::make_actor<flare_actor, caf::strong_actor_ptr>(
    sys.next_actor_id(), sys.node(), &sys, cfg);
  ptr()->is_registered(true);
}

scoped_flare_actor::~scoped_flare_actor() {
  if (! self_)
    return;
  if (! ptr()->is_terminated())
    ptr()->cleanup(caf::exit_reason::normal, &context_);
}

caf::blocking_actor* scoped_flare_actor::operator->() const {
  return ptr();
}

caf::blocking_actor& scoped_flare_actor::operator*() const {
  return *ptr();
}

caf::actor_addr scoped_flare_actor::address() const {
  return ptr()->address();
}

caf::blocking_actor* scoped_flare_actor::ptr() const {
  auto a = caf::actor_cast<caf::abstract_actor*>(self_);
  return static_cast<caf::blocking_actor*>(a);
}

int scoped_flare_actor::descriptor() const {
  return static_cast<flare_actor*>(ptr())->descriptor();
}

void scoped_flare_actor::receive(caf::behavior& bhvr) {
  return static_cast<flare_actor*>(ptr())->dequeue(bhvr);
}

caf::actor_control_block* scoped_flare_actor::get() const {
  return self_.get();
}

} // namespace detail
} // namespace broker
