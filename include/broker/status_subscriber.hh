#pragma once

#include <vector>

#include <caf/actor.hpp>
#include <caf/variant.hpp>

#include "broker/bad_variant_access.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"

#include "broker/detail/shared_subscriber_queue.hh"

namespace broker {

using status_variant = caf::variant<none, error, status>;

/// Provides blocking access to a stream of endpoint events.
class status_subscriber {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- member types ----------------------------------------------------------

  using value_type = status_variant;

  // --- constructors and destructors ------------------------------------------

  status_subscriber(status_subscriber&&) = default;

  status_subscriber& operator=(status_subscriber&&) = default;

  // --- access to values ------------------------------------------------------

  /// @copydoc subscriber::get
  value_type get();

  /// @copydoc subscriber::get
  caf::optional<value_type> get(caf::timestamp timeout);

  /// @copydoc subscriber::get
  caf::optional<value_type> get(duration relative_timeout);

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num, caf::timestamp timeout);

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num,
                              duration relative_timeout = infinite);

  /// @copydoc subscriber::poll
  std::vector<value_type> poll();

  // --- properties ------------------------------------------------------------

  /// @copydoc subscriber::set_rate_calculation
  void set_rate_calculation(bool x) {
    impl_.set_rate_calculation(x);
  }

  size_t rate() const {
    return impl_.rate();
  }

  const caf::actor& worker() const {
    return impl_.worker();
  }

  size_t available() const {
    return impl_.available();
  }

  auto fd() const {
    return impl_.fd();
  }

private:
  // -- force users to use `endpoint::make_status_subscriber` ------------------
  status_subscriber(endpoint& ep, bool receive_statuses = false);

  subscriber impl_;
};

// --- compatibility/wrapper functionality (may be removed later) --------------

template <class T>
inline bool is(const status_variant& v) {
  return caf::holds_alternative<T>(v);
}

template <class T>
inline T* get_if(status_variant& d) {
  return caf::get_if<T>(&d);
}

template <class T>
inline const T* get_if(const status_variant& d) {
  return caf::get_if<T>(&d);
}

template <class T>
inline T& get(status_variant& d) {
  if ( auto rval = caf::get_if<T>(&d) )
    return *rval;
  throw bad_variant_access{};
}

template <class T>
inline const T& get(const status_variant& d) {
  if ( auto rval = caf::get_if<T>(&d) )
    return *rval;
  throw bad_variant_access{};
}

} // namespace broker
