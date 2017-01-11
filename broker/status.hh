#ifndef BROKER_STATUS_HH
#define BROKER_STATUS_HH

// ATTENTION
// ---------
// When updating this file, make sure to update doc/comm.rst as well because it
// copies parts of this file verbatim.
//
// Included lines: 27-60

#include <string>
#include <tuple>

#include <caf/error.hpp>
#include <caf/make_message.hpp>

#include "broker/endpoint_info.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/operators.hh"
#include "broker/detail/type_traits.hh"

namespace broker {

/// Broker's status codes.
/// @relates status
enum class sc : uint8_t {
  /// The unspecified default error code.
  unspecified = 1,
  /// Successfully added a new peer.
  peer_added,
  /// Successfully removed a peer.
  peer_removed,
  /// Version incompatibility.
  peer_incompatible,
  /// Referenced peer does not exist.
  peer_invalid,
  /// Remote peer not listening.
  peer_unavailable,
  /// An peering request timed out.
  peer_timeout,
  /// Lost connection to peer.
  peer_lost,
  /// Re-gained connection to peer.
  peer_recovered,
  /// Master with given name already exist.
  master_exists,
  /// Master with given name does not exist.
  no_such_master,
  /// The given data store key does not exist.
  no_such_key,
  /// The store operation timed out.
  request_timeout,
  /// The operation expected a different type than provided
  type_clash,
  /// The data value cannot be used to carry out the desired operation.
  invalid_data,
  /// The storage backend failed to execute the operation.
  backend_failure,
};

template <class... Ts>
caf::error make_error(sc x, Ts&&... xs) {
  return {static_cast<uint8_t>(x), caf::atom("broker"),
          caf::make_message(std::forward<Ts>(xs)...)};
}

/// @relates sc
const char* to_string(sc code);

/// Diagnostic status information.
class status : detail::equality_comparable<status, sc>,
               detail::equality_comparable<sc, status> {
public:
  /// Default-constructs a status, indicating a successful operation.
  status() = default;

  /// Checks whether the status represents an error or a notification.
  /// @returns `true` if the status represents an error.
  /// @note A default-constructed status does not constitute an error.
  bool error() const;

  /// Retrieves additional contextual information, if available.
  /// The [status code][::sc] determines the type of information that's
  /// available.
  /// @tparam T The type of the attached context information.
  template <class T>
  const T* context() const {
    if (error_ == caf::none)
      return nullptr;
    BROKER_ASSERT(error_.category() == caf::atom("broker"));
    switch (static_cast<sc>(error_.code())) {
      default:
        return nullptr;
      case sc::peer_added:
      case sc::peer_removed:
      case sc::peer_incompatible:
      case sc::peer_invalid:
      case sc::peer_unavailable:
      case sc::peer_timeout:
      case sc::peer_lost:
      case sc::peer_recovered:
        return &error_.context().get_as<endpoint_info>(0);
    }
  }

  /// Retrieves an optional details about the status, if available.
  /// @returns A textual description of status details.
  const std::string* message() const;

  friend bool operator==(const status& s, sc x);

  friend bool operator==(sc x, const status& s);

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, status& s) {
    return f(s.error_);
  }

private:
  template <sc S, class... Ts>
  friend status make_status(Ts&&...);

  friend status make_status(caf::error);

  template <sc S>
  static detail::enable_if_t<S == sc::unspecified, status>
  make() {
    return status{S};
  }

  template <sc S>
  static detail::enable_if_t<
    S == sc::unspecified || S == sc::request_timeout,
    status
  >
  make(std::string msg) {
    return status{S, std::move(msg)};
  }

  template <sc S>
  static detail::enable_if_t<
    S == sc::peer_added
    || S == sc::peer_removed
    || S == sc::peer_incompatible
    || S == sc::peer_invalid
    || S == sc::peer_unavailable
    || S == sc::peer_timeout
    || S == sc::peer_lost
    || S == sc::peer_recovered,
    status
  >
  make(endpoint_info ei, std::string msg) {
    return status{S, std::move(ei), std::move(msg)};
  }

  static status make(caf::error e);

  explicit status(caf::error e);

  template <class... Ts>
  explicit status(sc code, Ts&&... xs)
    : error_{make_error(code, std::forward<Ts>(xs)...)} {
  }

  caf::error error_;
};

  /// Factory to construct a status instance based on a specific status code.
template <sc S, class... Ts>
status make_status(Ts&&... xs) {
  return status::make<S>(std::forward<Ts>(xs)...);
}

status make_status(caf::error e);

} // namespace broker

#endif // BROKER_STATUS_HH
