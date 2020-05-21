#pragma once

#include <memory>

#include <caf/fwd.hpp>

#include "broker/domain_options.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/fwd.hh"
#include "broker/timeout.hh"

namespace broker {

/// Partitions the global publish/subscribe layer into external and internal
/// domain. The gateway acts on behalf of all internal peers by channeling all
/// communication through itself. Peers in the internal domain are hidden in the
/// external domain and vice versa.
///
/// The gateway forwards all messages published in one domain to the other
/// domain, but hides the original sender. The gateway appears as the source of
/// all messages it forwards from one domain to another.
///
/// @warning The gateway assumes that peers from the external domain have no
/// peering relations with peers in the internal domain. Putting a gateway into
/// a network with alternative routing paths that bypass the gateway is going to
/// cause undefined behavior.
class gateway {
public:
  // -- member types -----------------------------------------------------------

  // -- constructors, destructors, and assignment operators --------------------

  ~gateway();

  gateway(gateway&&) = default;

  /// Tries to instantiate a new gateway with the default configuration.
  static expected<gateway> make();

  /// Tries to instantiate a new gateway with the given configuration.
  /// @param cfg Base configuration. Users can override parameters by providing
  ///            a `broker.conf`.
  /// @param internal_adaptation Additional settings that effect only the
  ///                            internal domain.
  /// @param external_adaptation Additional settings that effect only the
  ///                            external domain.
  static expected<gateway> make(configuration cfg,
                                domain_options internal_adaptation,
                                domain_options external_adaptation);
  // -- setup and teardown -----------------------------------------------------

  /// @cond PRIVATE

  /// Configures a pair of core actors in disjointed domains to forward
  /// published events to each other.
  static void setup(const caf::actor& internal, const caf::actor& external);

  /// @endcond

  /// Shuts down all background activity and blocks until all local subscribers
  /// and publishers have terminated.
  void shutdown();

  // -- properties -------------------------------------------------------------

  const caf::actor& internal_core() const noexcept;

  const caf::actor& external_core() const noexcept;

  const configuration& config() const noexcept;

  // -- peer management --------------------------------------------------------

  /// Listens at a specific port to accept remote peers in the internal domain.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t listen_internal(const std::string& address = {}, uint16_t port = 0);

  /// Listens at a specific port to accept remote peers in the external domain.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t listen_external(const std::string& address = {}, uint16_t port = 0);

  /// Initiates peerings with a remote endpoints.
  /// @param internal_peers List of endpoints in the internal domain.
  /// @param external_peers List of endpoints in the external domain.
  /// @param retry If non-zero, seconds after which to retry if connection
  ///              cannot be established, or breaks.
  /// @returns A `map` with all failed connection attempts.
  std::map<caf::uri, error> peer(const std::vector<caf::uri>& internal_peers,
                                 const std::vector<caf::uri>& external_peers,
                                 timeout::seconds retry = timeout::peer);

private:
  // -- member types -----------------------------------------------------------

  /// Opaque PIMPL type.
  struct impl;

  // -- utility and helper functions -------------------------------------------

  uint16_t listen_impl(const caf::actor& core, const std::string& address,
                       uint16_t port);

  // -- constructors, destructors, and assignment operators --------------------

  gateway(std::unique_ptr<impl>&&);

  /// Pointer-to-implementation.
  std::unique_ptr<impl> ptr_;
};

} // namespace broker
