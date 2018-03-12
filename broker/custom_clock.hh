#ifndef BROKER_CUSTOM_CLOCK_HH
#define BROKER_CUSTOM_CLOCK_HH

#include <caf/detail/thread_safe_actor_clock.hpp>
#include <cassert>
#include <mutex>

namespace broker {

class custom_clock : public caf::detail::thread_safe_actor_clock {
public:

  inline void advance(time_point time) {
    std::unique_lock<mutex_type> lock(mutex);
    if ( time > current_time )
      current_time = time;
  }

  inline time_point now() const noexcept override {
    std::unique_lock<mutex_type> lock(mutex);
    return current_time;
  }

private:

  using mutex_type = std::mutex;

  // TODO: C++17 would allow a shared_mutex (use shared_lock for readers)
  mutable mutex_type mutex;
  time_point current_time;
};

} // namespace broker

#endif // BROKER_CUSTOM_CLOCK_HH
