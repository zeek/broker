#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>

namespace broker::detail {

// Drop-in replacement for std::latch.
class latch {
public:
  explicit latch(size_t count) : count_(count) {}

  void count_down() {
    std::unique_lock guard{mtx_};
    if (count_ == 0) {
      throw std::logic_error("latch::count_down: latch is already at 0");
    }
    if (--count_ == 0) {
      cv_.notify_all();
    }
  }

  void arrive_and_wait() {
    std::unique_lock guard{mtx_};
    if (count_ == 0) {
      throw std::logic_error("latch::arrive_and_wait: latch is already at 0");
    }
    if (--count_ == 0) {
      cv_.notify_all();
    } else {
      cv_.wait(guard, [this] { return count_ == 0; });
    }
  }

private:
  std::mutex mtx_;
  std::condition_variable cv_;
  size_t count_;
};

} // namespace broker::detail
