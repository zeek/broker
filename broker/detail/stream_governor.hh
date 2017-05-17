#ifndef BROKER_DETAIL_STREAM_GOVERNOR_HH
#define BROKER_DETAIL_STREAM_GOVERNOR_HH

#include <memory>
#include <unordered_map>

#include <caf/filtering_downstream.hpp>
#include <caf/ref_counted.hpp>
#include <caf/stream_handler.hpp>
#include <caf/stream_source.hpp>
#include <caf/upstream.hpp>

#include "broker/atoms.hh"

#include "broker/detail/filter_type.hh"
#include "broker/detail/stream_relay.hh"
#include "broker/detail/stream_type.hh"

namespace broker {
namespace detail {

// -- Forward declarations -----------------------------------------------------

struct core_state;

/// A stream governor dispatches incoming data from all publishers to local
/// subscribers as well as peers. Its primary job is to avoid routing loops by
/// not forwarding data from a peer back to itself.
class stream_governor : public caf::ref_counted {
public:
  // -- nested types -----------------------------------------------------------

  /// The type of each individual stream item.
  using element_type = std::pair<topic, data>;

  /// Stores state for both stream directions as well as a filter.
  class peer_data : public caf::ref_counted {
  public:
    // --- constructors and destructors ----------------------------------------

    peer_data(stream_governor* parent, filter_type y,
              const caf::stream_id& downstream_sid,
              caf::abstract_downstream::policy_ptr pp);

    ~peer_data();

    void send_stream_handshake();

    /// Returns the downstream handle for this peer.
    const caf::strong_actor_ptr& hdl() const;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f, peer_data& x) {
      return f(x.filter, x.incoming_sid, x.out);
    }

    /// Active filter on this peer.
    filter_type filter;

    /// ID of the input stream.
    caf::stream_id incoming_sid;

    /// Output stream.
    caf::downstream<element_type> out;

    /// Relay for this peer.
    stream_relay_ptr relay;

    /// Handle to the actual remote core.
    caf::actor remote_core;
  };

  using peer_data_ptr = caf::intrusive_ptr<peer_data>;

  /// Maps peer handles to peer data.
  using peer_map = std::unordered_map<caf::actor, peer_data_ptr>;

  /// Maps input stream IDs to peer data.
  using input_to_peer_map = std::unordered_map<caf::stream_id, peer_data_ptr>;

  using local_downstream = caf::filtering_downstream<element_type, topic>;

  // --- constructors and destructors ------------------------------------------

  stream_governor(core_state* state);

  ~stream_governor();

  // -- Accessors --------------------------------------------------------------

  inline const peer_map& peers() const {
    return peers_;
  }

  inline bool has_peer(const caf::actor& hdl) const {
    return peers_.count(hdl) > 0;
  }

  inline local_downstream& local_subscribers() {
    return local_subscribers_;
  }

  peer_data* peer(const caf::actor& remote_core);

  // -- Mutators ---------------------------------------------------------------

  /// Adds a new peer.
  peer_data* add_peer(caf::strong_actor_ptr downstream_handle,
                      caf::actor remote_core, const caf::stream_id& sid,
                      filter_type filter);

  /// Updates the filter of an existing peer.
  bool update_peer(const caf::actor& hdl, filter_type filter);

  /// Pushes data into the stream.
  void push(topic&& t, data&& x);

  // -- Overridden member functions of `stream_handler` ------------------------

  caf::error add_downstream(const caf::stream_id& sid,
                            caf::strong_actor_ptr& hdl);

  caf::error confirm_downstream(const caf::stream_id& sid,
                                const caf::strong_actor_ptr& rebind_from,
                                caf::strong_actor_ptr& hdl, long initial_demand,
                                bool redeployable);

  caf::error downstream_demand(const caf::stream_id& sid,
                               caf::strong_actor_ptr& hdl, long new_demand);

  caf::error push();

  caf::expected<long> add_upstream(const caf::stream_id& sid,
                                   caf::strong_actor_ptr& hdl,
                                   const caf::stream_id& up_sid,
                                   caf::stream_priority prio);

  caf::error upstream_batch(const caf::stream_id& sid,
                            caf::strong_actor_ptr& hdl, long, caf::message& xs);

  caf::error close_upstream(const caf::stream_id& sid,
                            caf::strong_actor_ptr& hdl);

  void abort(const caf::stream_id& sid, caf::strong_actor_ptr& cause,
             const caf::error& reason);

  long total_downstream_net_credit() const;

  inline core_state* state() {
    return state_;
  }

private:
  core_state* state_;
  caf::upstream<element_type> in_;
  local_downstream local_subscribers_;
  peer_map peers_;
  input_to_peer_map input_to_peers_;
};

/// @relates stream_governor
void intrusive_ptr_add_ref(stream_governor*);

/// @relates stream_governor
void intrusive_ptr_release(stream_governor* p);

using stream_governor_ptr = caf::intrusive_ptr<stream_governor>;

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_STREAM_GOVERNOR_HH
