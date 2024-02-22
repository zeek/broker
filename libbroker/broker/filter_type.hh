#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "broker/lamport_timestamp.hh"
#include "broker/topic.hh"

namespace broker {

/// A set of topics that represents a subscription or peer filter.
using filter_type = std::vector<topic>;

/// A set of topics with synchronized access. Enables the core actor to share
/// its current filter with the connector.
class shared_filter_type {
public:
  shared_filter_type() = default;

  shared_filter_type(filter_type filter) : filter_(std::move(filter)) {
    // nop
  }

  shared_filter_type(const shared_filter_type&) = delete;

  shared_filter_type& operator=(const shared_filter_type&) = delete;

  /// Reads the current value with `f`.
  template <class F>
  auto read(F&& f) const {
    std::unique_lock guard{mtx_};
    return f(version_, filter_);
  }

  filter_type read() const {
    filter_type result;
    {
      std::unique_lock guard{mtx_};
      result = filter_;
    }
    return result;
  }

  /// Updates the current value with `f`.
  template <class F>
  auto update(F&& f) {
    std::unique_lock guard{mtx_};
    return f(version_, filter_);
  }

  /// Override the current value.
  void set(lamport_timestamp version, filter_type filter) {
    using std::swap;
    std::unique_lock guard{mtx_};
    version_ = version;
    swap(filter, filter_);
  }

private:
  mutable std::mutex mtx_;
  lamport_timestamp version_;
  filter_type filter_;
};

/// @relates shared_filter_type
using shared_filter_ptr = std::shared_ptr<shared_filter_type>;

/// Extends the filter `f` with `x` such that the filter contains the minimal
/// set of subscriptions. For example, subscribing to `/foo/bar` does not change
/// the filter when it already contains a subscription to `/foo`. Further,
/// subscribing to `/foo` if the filter already contains `/foo/bar` replaces the
/// existing entry with the less specific '/foo'.
/// @return `true` if the filter changed, `false` otherwise.
bool filter_extend(filter_type& f, const topic& x);

/// Convenience function for calling `filter_extend` with each topic in `other`.
bool filter_extend(filter_type& f, const filter_type& other);

/// Convenience function for calling `filter_extend` with each topic in `other`
/// that matches `predicate`.
template <class Predicate>
bool filter_extend(filter_type& f, const filter_type& other,
                   Predicate predicate) {
  size_t count = 0;
  for (auto& x : other)
    if (predicate(x) && filter_extend(f, x))
      ++count;
  return count > 0;
}

} // namespace broker
