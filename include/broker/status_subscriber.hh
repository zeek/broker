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
  value_type get(caf::timestamp timeout);

  /// @copydoc subscriber::get
  template <class Duration>
  value_type get(Duration relative_timeout) {
    value_type result;
    do {
      if (auto maybe_msg = impl_.get(relative_timeout))
        result = convert(*maybe_msg);
    } while (caf::holds_alternative<none>(result)
             && caf::is_infinite(relative_timeout));
    return result;
  }

  /// @copydoc subscriber::get
  value_type get() {
    return get(caf::infinite);
  }

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num, caf::timestamp timeout);

  /// @copydoc subscriber::get
  template <class Duration>
  std::vector<value_type> get(size_t num, Duration relative_timeout) {
    std::vector<value_type> result;
    do {
      auto msgs = impl_.get(num, relative_timeout);
      for (auto& msg : msgs)
        append_converted(result, msg);
    } while (result.empty() && caf::is_infinite(relative_timeout));
    return result;
  }

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num) {
    return get(num, caf::infinite);
  }

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

  // --- miscellaneous ---------------------------------------------------------

  /// Release any state held by the object, rendering it invalid.
  /// @warning Performing *any* action on this object afterwards invokes
  ///          undefined behavior, except:
  ///          - Destroying the object by calling the destructor.
  ///          - Using copy- or move-assign from a valid `store` to "revive"
  ///            this object.
  ///          - Calling `reset` again (multiple invocations are no-ops).
  /// @note This member function specifically targets the Python bindings. When
  ///       writing Broker applications using the native C++ API, there's no
  ///       point in calling this member function.
  void reset();

private:
  // -- force users to use `endpoint::make_status_subscriber` ------------------
  status_subscriber(endpoint& ep, bool receive_statuses = false);

  value_type convert(const data_message& msg);

  void append_converted(std::vector<value_type>& result,
                        const data_message& msg);

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
