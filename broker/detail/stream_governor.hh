#ifndef BROKER_DETAIL_STREAM_GOVERNOR_HH
#define BROKER_DETAIL_STREAM_GOVERNOR_HH

#include <memory>
#include <unordered_map>

#include "caf/filtering_downstream.hpp"
#include "caf/stream_handler.hpp"
#include "caf/stream_source.hpp"
#include "caf/upstream.hpp"

#include "broker/atoms.hh"

#include "broker/detail/aliases.hh"

namespace broker {
namespace detail {

// -- Forward declarations -----------------------------------------------------

struct core_state;

/// A stream governor dispatches incoming data from all publishers to local
/// subscribers as well as peers. Its primary job is to avoid routing loops by
/// not forwarding data from a peer back to itself.
class stream_governor : public caf::stream_handler {
public:
  // -- Nested types -----------------------------------------------------------

  struct peer_data {
    filter_type filter;
    caf::downstream<element_type> out;
    caf::stream_id incoming_sid;

    peer_data(filter_type y, caf::local_actor* self, const caf::stream_id& sid,
              caf::abstract_downstream::policy_ptr pp)
        : filter(std::move(y)),
          out(self, sid, std::move(pp)) {
      // nop
    }

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f, peer_data& x) {
      return f(x.filter, x.out, x.incoming_sid);
    }
  };

  using peer_data_ptr = std::unique_ptr<peer_data>;

  using peer_map = std::unordered_map<caf::strong_actor_ptr, peer_data_ptr>;

  using local_downstream = caf::filtering_downstream<element_type, key_type>;

  // -- Constructors and destructors -------------------------------------------

  stream_governor(core_state* state);

  // -- Accessors --------------------------------------------------------------

  inline const peer_map& peers() const {
    return peers_;
  }

  inline bool has_peer(const caf::strong_actor_ptr& hdl) const {
    return peers_.count(hdl) > 0;
  }

  inline local_downstream& local_subscribers() {
    return local_subscribers_;
  }

  // -- Mutators ---------------------------------------------------------------
  
  template <class... Ts>
  void new_stream(const caf::strong_actor_ptr& hdl, const caf::stream_id& sid,
                  std::tuple<Ts...> xs) {
    CAF_ASSERT(hdl != nullptr);
    stream_type token{sid};
    auto ys = std::tuple_cat(std::make_tuple(token), std::move(xs));
    new_stream(hdl, token, make_message_from_tuple(std::move(ys)));
  }

  peer_data* add_peer(caf::strong_actor_ptr ptr, filter_type filter);

  /// Pushes data into the stream.
  void push(topic&& t, data&& x);

  // -- Overridden member functions of `stream_handler` ------------------------

  caf::error add_downstream(caf::strong_actor_ptr& hdl) override;

  caf::error confirm_downstream(const caf::strong_actor_ptr& rebind_from,
                                caf::strong_actor_ptr& hdl, long initial_demand,
                                bool redeployable) override;

  caf::error downstream_demand(caf::strong_actor_ptr& hdl,
                               long new_demand) override;

  caf::error push(long* hint) override;

  caf::expected<long> add_upstream(caf::strong_actor_ptr& hdl,
                                   const caf::stream_id& sid,
                                   caf::stream_priority prio) override;

  caf::error upstream_batch(caf::strong_actor_ptr& hdl, long,
                            caf::message& xs) override;

  caf::error close_upstream(caf::strong_actor_ptr& hdl) override;

  void abort(caf::strong_actor_ptr& cause, const caf::error& reason) override;

  bool done() const override;

  caf::message make_output_token(const caf::stream_id&) const override;

  long total_downstream_net_credit() const;

private:
  void new_stream(const caf::strong_actor_ptr& hdl,
                  const stream_type& token, caf::message msg);

  core_state* state_;
  caf::upstream<element_type> in_;
  local_downstream local_subscribers_;
  peer_map peers_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_STREAM_GOVERNOR_HH
