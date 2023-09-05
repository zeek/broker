#pragma once

#include <variant>
#include <vector>

#include "broker/bad_variant_access.hh"
#include "broker/defaults.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/worker.hh"

namespace broker {

using status_variant = std::variant<none, error, status>;

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

  // --- factories -------------------------------------------------------------

  static status_subscriber
  make(endpoint& ep, bool receive_statuses,
       size_t queue_size = defaults::subscriber::queue_size);

  // --- access to values ------------------------------------------------------

  /// @copydoc subscriber::get
  value_type get(timestamp timeout);

  /// @copydoc subscriber::get
  template <class Duration>
  value_type get(Duration relative_timeout) {
    value_type result;
    do {
      if (auto maybe_msg = impl_.get(relative_timeout))
        result = convert(*maybe_msg);
    } while (std::holds_alternative<none>(result)
             && relative_timeout == infinite);
    return result;
  }

  /// @copydoc subscriber::get
  value_type get() {
    return get(infinite);
  }

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num, timestamp timeout);

  /// @copydoc subscriber::get
  template <class Duration>
  std::vector<value_type> get(size_t num, Duration relative_timeout) {
    std::vector<value_type> result;
    do {
      auto msgs = impl_.get(num, relative_timeout);
      for (auto& msg : msgs)
        append_converted(result, msg);
    } while (result.empty() && relative_timeout == infinite);
    return result;
  }

  /// @copydoc subscriber::get
  std::vector<value_type> get(size_t num) {
    return get(num, infinite);
  }

  /// @copydoc subscriber::poll
  std::vector<value_type> poll();

  // --- properties ------------------------------------------------------------

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
  //  status_subscriber(endpoint& ep, bool receive_statuses = false);
  explicit status_subscriber(subscriber impl);

  value_type convert(const data_message& msg);

  void append_converted(std::vector<value_type>& result,
                        const data_message& msg);

  subscriber impl_;
};

// --- compatibility/wrapper functionality (may be removed later) --------------

template <class T>
inline bool is(const status_variant& v) {
  return holds_alternative<T>(v);
}

template <class T>
inline T* get_if(status_variant& d) {
  return get_if<T>(&d);
}

template <class T>
inline const T* get_if(const status_variant& d) {
  return get_if<T>(&d);
}

template <class T>
inline T& get(status_variant& d) {
  if (auto rval = get_if<T>(&d))
    return *rval;
  else
    throw bad_variant_access{};
}

template <class T>
inline const T& get(const status_variant& d) {
  if (auto rval = get_if<T>(&d))
    return *rval;
  else
    throw bad_variant_access{};
}

} // namespace broker
