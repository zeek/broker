#include <caf/blocking_actor.hpp>

#include "broker/blocking_endpoint.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/core_actor.hh"
#include "broker/detail/flare_actor.hh"

namespace broker {

void blocking_endpoint::subscribe(topic t) {
  if (!core_)
    return;
  std::vector<topic> ts{t};
  caf::anon_send(core(), atom::subscribe::value, std::move(ts), subscriber_);
}

void blocking_endpoint::unsubscribe(topic t) {
  if (!core_)
    return;
  caf::anon_send(core(), atom::unsubscribe::value,
                 std::vector<topic>{std::move(t)}, subscriber_);
}

message blocking_endpoint::receive() {
  if (!core_) {
    auto s = make_status<sc::unspecified>("endpoint not initialized");
    return message{caf::make_message(std::move(s))};
  }
  auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
  subscriber->await_data();
  auto msg = subscriber->dequeue()->move_content_to_message();
  return message{std::move(msg)};
}

mailbox blocking_endpoint::mailbox() {
  BROKER_ASSERT(subscriber_);
  auto subscriber = caf::actor_cast<detail::flare_actor*>(subscriber_);
  return detail::make_mailbox(subscriber);
}

blocking_endpoint::blocking_endpoint(caf::actor_system& sys) {
  subscriber_ = sys.spawn<detail::flare_actor>();
  init_core(sys.spawn(detail::core_actor, subscriber_));
}

} // namespace broker
