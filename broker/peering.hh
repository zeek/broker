#ifndef BROKER_PEERING_HH
#define BROKER_PEERING_HH

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <caf/actor.hpp>

#include "broker/detail/operators.hh"

namespace broker {
class peering;
}

namespace std {
template <>
struct std::hash<broker::peering>;
}

namespace broker {

class endpoint;

// FIXME: this class will go away after PIMPL migration.
/// Contains information about a peering between two endpoints.
class peering : detail::equality_comparable<peering> {
  friend class endpoint;

  template <class Processor>
  friend void serialize(Processor& proc, peering& p, const unsigned) {
    proc & p.endpoint_actor_;
    proc & p.peer_actor_;
    proc & p.remote_;
    proc & p.remote_tuple_;
  }

  friend bool operator==(const peering& lhs, const peering& rhs);

public:
  peering() = default;

  peering(caf::actor endpoint_actor, caf::actor peer_actor, bool remote = false,
          std::pair<std::string, uint16_t> remote_tuple
            = std::make_pair("", 0));

  /// False if the peering is not yet initialized, else true.
  explicit operator bool() const;

  /// @return whether the peering is between a local and remote endpoint.
  bool remote() const;

  const caf::actor& endpoint_actor() const;

  const caf::actor& peer_actor() const;

  /// @return the host and port of a remote endpoint.
  const std::pair<std::string, uint16_t>& remote_tuple() const;

private:
  caf::actor endpoint_actor_;
  caf::actor peer_actor_;
  bool remote_;
  std::pair<std::string, uint16_t> remote_tuple_;
};

} // namespace broker

namespace std {
template <>
struct hash<broker::peering> {
  size_t operator()(const broker::peering& p) const;
};
}

#endif // BROKER_PEERING_HH
