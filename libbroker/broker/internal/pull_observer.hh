#pragma once

#include <caf/error.hpp>

#include <vector>

namespace broker::internal {

/// Helper class for pulling messages from a CAF SPSC buffer.
template <class T>
struct pull_observer {
  explicit pull_observer(std::vector<T>& storage) : buf(&storage) {
    // nop
  }

  void on_next(const T& item) {
    buf->emplace_back(item);
  }

  void on_complete() {
    completed = true;
  }

  void on_error(const caf::error&) {
    failed = true;
  }

  std::vector<T>* buf;

  bool completed = false;

  bool failed = false;
};

} // namespace broker::internal
