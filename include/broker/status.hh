#pragma once

#include <string>
#include <type_traits>
#include <utility>

#include <caf/meta/load_callback.hpp>

#include "broker/convert.hh"
#include "broker/detail/operators.hh"
#include "broker/detail/type_traits.hh"
#include "broker/endpoint_info.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/optional.hh"

namespace broker {

// --sc-enum-start
/// Broker's status codes.
/// @relates status
enum class sc : uint8_t {
  /// Indicates a default-constructed ::status.
  unspecified,
  /// Successfully added a direct connection to a peer.
  peer_added,
  /// Successfully removed a direct connection to a peer.
  peer_removed,
  /// Lost direct connection to a peer.
  peer_lost,
  /// Discovered a new Broker endpoint in the network.
  endpoint_discovered,
  /// Lost all paths to a Broker endpoint.
  endpoint_unreachable,
};
// --sc-enum-end

/// @relates sc
template <sc S>
using sc_constant = std::integral_constant<sc, S>;

/// @relates sc
const char* to_string(sc code) noexcept;

/// @relates sc
bool convert(const std::string& str, sc& code) noexcept;

/// @relates sc
bool convert(const data& str, sc& code) noexcept;

/// @relates sc
bool convertible_to_sc(const data& src) noexcept;

/// @relates sc
template <class Inspector>
bool inspect(Inspector& f, sc& x) {
  auto get = [&] { return static_cast<uint8_t>(x); };
  auto set = [&](uint8_t val) {
    if (val <= static_cast<uint8_t>(sc::endpoint_unreachable)) {
      x = static_cast<sc>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

/// Evaluates to `true` if a ::status with code `S` requires an `endpoint_info`
/// context.
/// @relates sc
template <sc S>
constexpr bool sc_has_endpoint_info_v = S != sc::unspecified;

/// Evaluates to `true` if a ::status with code `S` can contain a `network_info`
/// in its context.
/// @relates sc
template <sc S>
constexpr bool sc_has_network_info_v
  = S == sc::peer_added || S == sc::peer_removed || S == sc::peer_lost;

template <>
struct can_convert_predicate<sc> {
  static bool check(const data& src) noexcept {
    return convertible_to_sc(src);
  }
};

/// Diagnostic status information.
class status : detail::equality_comparable<status, status>,
               detail::equality_comparable<status, sc>,
               detail::equality_comparable<sc, status> {

public:
  template <sc S>
  static status make(endpoint_info ei, std::string msg) {
    static_assert(sc_has_endpoint_info_v<S>);
    return {S, std::move(ei), std::move(msg)};
  }

  template <sc S>
  static status make(node_id node, std::string msg) {
    static_assert(sc_has_endpoint_info_v<S>);
    return {S, endpoint_info{std::move(node), nil}, std::move(msg)};
  }

  /// Default-constructs an unspecified status.
  status() : code_(sc::unspecified) {
    // nop
  }

  /// @returns The code of this status.
  sc code() const;

  /// Retrieves additional contextual information, if available.
  /// The [status code][::sc] determines the type of information that's
  /// available.
  /// @tparam T The type of the attached context information. Only
  ///         `endpoint_info` is supported at the moment.
  template <class T = endpoint_info>
  const T* context() const {
    // TODO: should not be a template.
    static_assert(std::is_same<T, endpoint_info>::value);
    return code_ != sc::unspecified ? &context_ : nullptr;
  }

  /// Retrieves an optional details about the status, if available.
  /// @returns A textual description of status details.
  const std::string* message() const{
    return &message_;
  }

  friend bool operator==(const status& x, const status& y);

  friend bool operator==(const status& x, sc y);

  friend bool operator==(sc x, const status& y);

  friend std::string to_string(const status& s);

  template <class Inspector>
  friend bool inspect(Inspector& f, status& x) {
    auto verify = [&x] { return x.verify(); };
    return f.object(x).on_load(verify).fields(f.field("code", x.code_),
                                              f.field("context", x.context_),
                                              f.field("message", x.message_));
  }

  /// Maps `src` to `["status", code, context, message]`, whereas:
  /// - `code` is ::code encoded as an ::enum_value
  /// - `context` is *context() (if available)
  /// - `message` is *message()
  friend bool convert(const status& src, data& dst);

  /// Converts data in the format `["status", code, context, message]` back to a
  /// status.
  friend bool convert(const data& src, status& dst);

private:
  caf::error verify() const;

  template <class T>
  status(sc code, T&& context, std::string msg)
    : code_(code),
      context_(std::forward<T>(context)),
      message_(std::move(msg)) {
    // nop
  }

  sc code_;
  endpoint_info context_;
  std::string message_;
};

/// @relates status
template <sc S, class... Ts>
status make_status(Ts&&... xs) {
  return status::make<S>(std::forward<Ts>(xs)...);
}

/// @relates status
bool convertible_to_status(const data& src) noexcept;

/// @relates status
bool convertible_to_status(const vector& xs) noexcept;

template <>
struct can_convert_predicate<status> {
  static bool check(const data& src) noexcept {
    return convertible_to_status(src);
  }

  static bool check(const vector& src) noexcept {
    return convertible_to_status(src);
  }
};

/// Creates a view into a ::data object that is convertible to ::status.
class status_view {
public:
  status_view(const status_view&) noexcept = default;

  status_view& operator=(const status_view&) noexcept = default;

  bool valid() const noexcept {
    return xs_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  /// @copydoc status::code
  /// @pre `valid()`
  sc code() const noexcept;

  /// @copydoc status::code
  const std::string* message() const noexcept;

  /// Retrieves additional contextual information, if available.
  optional<endpoint_info> context() const;

  /// Creates a view for given data.
  /// @returns A ::valid view on success, an invalid view otherwise.
  static status_view make(const data& src);

private:
  explicit status_view(const vector* ptr) noexcept : xs_(ptr) {
    // nop
  }

  const vector* xs_;
};

/// @relates status_view
std::string to_string(status_view s);

/// @relates status_view
inline status_view make_status_view(const data& src) {
  return status_view::make(src);
}

} // namespace broker
