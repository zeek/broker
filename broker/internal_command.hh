#ifndef BROKER_INTERNAL_COMMAND_HH
#define BROKER_INTERNAL_COMMAND_HH

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>
#include <caf/variant.hpp>

#include "broker/data.hh"
#include "broker/time.hh"

namespace broker {

/// Sets a value in the key-value store.
struct put_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, put_command& x) {
  return f(caf::meta::type_name("put"), x.key, x.value, x.expiry);
}

/// Removes a value in the key-value store.
struct erase_command {
  data key;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, erase_command& x) {
  return f(caf::meta::type_name("erase"), x.key);
}

/// Adds a value to the existing value.
struct add_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, add_command& x) {
  return f(caf::meta::type_name("add"), x.key, x.value, x.expiry);
}

/// Subtracts a value to the existing value.
struct subtract_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, subtract_command& x) {
  return f(caf::meta::type_name("subtract"), x.key, x.value, x.expiry);
}

/// Forces the master to create and broadcast a new snapshot of its state.
struct snapshot_command {
  caf::actor clone;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, snapshot_command& x) {
  return f(caf::meta::type_name("snapshot"), x.clone);
}

/// Sets the full state of all receiving replicates to the included snapshot.
struct set_command {
  std::unordered_map<data, data> state;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, set_command& x) {
  return f(caf::meta::type_name("set"), x.state);
}

class internal_command {
public:
  using variant_type
    = caf::variant<none, put_command, erase_command, add_command,
                   subtract_command, snapshot_command, set_command>;

  variant_type content;

  internal_command(variant_type value);

  internal_command() = default;
  internal_command(internal_command&&) = default;
  internal_command(const internal_command&) = default;
  internal_command& operator=(internal_command&&) = default;
  internal_command& operator=(const internal_command&) = default;
};


template <class T, class... Ts>
internal_command make_internal_command(Ts&&... xs) {
  return internal_command{T{std::forward<Ts>(xs)...}};
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, internal_command& x) {
  return f(caf::meta::type_name("internal_command"), x.content);
}

} // namespace broker

#endif // BROKER_INTERNAL_COMMAND_HH
