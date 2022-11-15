#pragma once

#include <mutex>
#include <chrono>
#include <limits>

#include <caf/blocking_actor.hpp>

#include "broker/detail/flare.hh"

namespace broker::internal {

class flare_actor;

} // namespace broker::internal

namespace caf {
namespace mixin {

template <>
struct is_blocking_requester<broker::internal::flare_actor> : std::true_type { };

} // namespace mixin
} // namespace caf

namespace broker::internal {

class flare_actor : public caf::blocking_actor {
public:
  flare_actor(caf::actor_config& sys);

  void launch(caf::execution_unit*, bool, bool) override;

  void act() override;

  void await_data() override;

  bool await_data(timeout_type timeout) override;

  bool enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) override;

  caf::mailbox_element_ptr dequeue() override;

  const char* name() const override;

  void extinguish_one();

  auto descriptor() const noexcept {
    return flare_.fd();
  }

private:
  detail::flare flare_;
  int flare_count_ = 0;
  std::mutex flare_mtx_;
};

} // namespace broker::internal
