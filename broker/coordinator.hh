#ifndef BROKER_COORDINATOR_HH
#define BROKER_COORDINATOR_HH

#include <caf/scheduler/coordinator.hpp>
#include <caf/actor_system.hpp>

#include "broker/custom_clock.hh"

namespace broker {

template <class Policy>
class coordinator : public caf::scheduler::coordinator<Policy> {
public:
  using super = caf::scheduler::coordinator<Policy>;

  coordinator(caf::actor_system& sys) : super(sys) {
  }

  static caf::actor_system::module* make(caf::actor_system& sys,
                                         caf::detail::type_list<>) {
    return new coordinator(sys);
  }

  custom_clock& clock() noexcept override {
    return clock_;
  }

private:

  custom_clock clock_;
};

} // namespace broker

#endif // BROKER_COORDINATOR_HH
