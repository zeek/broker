#ifndef BROKER_INTERNAL_COMMAND_HH
#define BROKER_INTERNAL_COMMAND_HH

#include <utility>
#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/variant.hpp>
#include <caf/optional.hpp>
#include <caf/meta/type_name.hpp>

#include "broker/fwd.hh"
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

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
  caf::actor who;
  request_id req_id;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, put_unique_command& x) {
  return f(caf::meta::type_name("put_unique"), x.key, x.value, x.expiry,
           x.who, x.req_id);
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
  data::type init_type;
  caf::optional<timespan> expiry;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, add_command& x) {
  return f(caf::meta::type_name("add"), x.key, x.value, x.init_type, x.expiry);
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

/// Causes the master to reply with a snapshot of its state.
struct snapshot_command {
  caf::actor remote_core;
  caf::actor remote_clone;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, snapshot_command& x) {
  return f(caf::meta::type_name("snapshot"), x.remote_core, x.remote_clone);
}

/// Since snapshots are sent to clones on a different channel, this allows
/// clones to coordinate the reception of snapshots with the stream of
/// updates that the master may have independently made to it.
struct snapshot_sync_command {
  caf::actor remote_clone;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, snapshot_sync_command& x) {
  return f(caf::meta::type_name("snapshot_sync"), x.remote_clone);
}

/// Sets the full state of all receiving replicates to the included snapshot.
struct set_command {
  std::unordered_map<data, data> state;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, set_command& x) {
  return f(caf::meta::type_name("set"), x.state);
}

/// Drops all values.
struct clear_command {
  // tag type
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, clear_command&) {
  return f(caf::meta::type_name("clear"));
}

class internal_command {
public:
  using variant_type
    = caf::variant<none, put_command, put_unique_command, erase_command,
                   add_command, subtract_command, snapshot_command,
                   snapshot_sync_command, set_command, clear_command>;

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
