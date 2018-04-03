#include "broker/logger.hh" // Must come before any CAF include.

#include "broker/detail/core_actor.hh"

#include <caf/all.hpp>

#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/convert.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/peer_status.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/clone_actor.hh"
#include "broker/detail/die.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/make_unique.hh"
#include "broker/detail/master_actor.hh"
#include "broker/detail/master_resolver.hh"

using namespace caf;

namespace broker {
namespace detail {

const char* core_state::name = "core";

core_state::core_state(caf::event_based_actor* ptr)
  : self(ptr),
    cache(ptr),
    shutting_down(false),
    ep(nullptr) {
  errors_ = self->system().groups().get_local("broker/errors");
  statuses_ = self->system().groups().get_local("broker/statuses");
}

void core_state::init(filter_type initial_filter, broker_options opts,
                      endpoint* e) {
  ep = e; 
  options = std::move(opts);
  filter = std::move(initial_filter);
  cache.set_use_ssl(! options.disable_ssl);
  governor = caf::make_counted<governor_type>(self, this, filter);
}

caf::strong_actor_ptr core_state::prev_peer_from_handshake() {
  auto& xs = self->current_mailbox_element()->content();
  CAF_ASSERT(xs.match_elements<caf::stream_msg>());
  auto& x = xs.get_as<caf::stream_msg>(0);
  if (caf::holds_alternative<caf::stream_msg::open>(x.content))
    return get<caf::stream_msg::open>(x.content).prev_stage;
  return nullptr;
}

void core_state::update_filter_on_peers() {
  CAF_LOG_TRACE("");
  policy().for_each_peer([&](const actor& hdl) {
    self->send(hdl, atom::update::value, filter);
  });
}

void core_state::add_to_filter(filter_type xs) {
  CAF_LOG_TRACE(CAF_ARG(xs));
  // Get initial size of our filter.
  auto s0 = filter.size();
  // Insert new elements then remove duplicates with sort and unique.
  filter.insert(filter.end(), std::make_move_iterator(xs.begin()),
                std::make_move_iterator(xs.end()));
  std::sort(filter.begin(), filter.end());
  auto e = std::unique(filter.begin(), filter.end());
  if (e != filter.end())
    filter.erase(e, filter.end());
  // Update our peers if we have actually changed our filter.
  if (s0 != filter.size()) {
    CAF_LOG_DEBUG("Changed filter to " << filter);
    update_filter_on_peers();
  }
}

bool core_state::has_peer(const caf::actor& x) {
  return pending_peers.count(x) > 0 || policy().has_peer(x) > 0;
}

bool core_state::has_remote_master(const std::string& name) {
  // If we don't have a master recorded locally, we could still have a
  // propagated subscription to a remote core hosting a master.
  auto t = name / topics::reserved / topics::master;
  for (auto& kvp : policy().peers().lanes()) {
    auto& filter = kvp.first.second;
    auto e = filter.end();
    if (std::find(filter.begin(), e, t) != e)
      return true;
  }
  return false;
}

core_policy& core_state::policy() {
  return governor->policy();
}

result<void> init_peering(caf::stateful_actor<core_state>* self,
                          actor remote_core, response_promise rp) {
  CAF_LOG_TRACE(CAF_ARG(remote_core));
  auto& st = self->state;
  // Sanity checking.
  if (remote_core == nullptr) {
    rp.deliver(sec::invalid_argument);
    return rp;
  }
  // Ignore repeated peering requests without error.
  if (st.pending_peers.count(remote_core) > 0 || st.has_peer(remote_core)) {
    rp.deliver(caf::unit);
    return rp;
  }
  // Create necessary state and send message to remote core.
  st.pending_peers.emplace(remote_core,
                           core_state::pending_peer_state{stream_id{}, rp});
  self->send(self * remote_core, atom::peer::value, st.filter, self);
  self->monitor(remote_core);
  return rp;
};

struct retry_state {
  network_info addr;
  response_promise rp;

  void try_once(caf::stateful_actor<core_state>* self);
};

} // namespace detail
} // namespace broker

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::retry_state)

namespace broker {
namespace detail {

void retry_state::try_once(caf::stateful_actor<core_state>* self) {
  auto cpy = std::move(*this);
  self->state.cache.fetch(cpy.addr,
                          [self, cpy](actor x) mutable {
                            init_peering(self, std::move(x), std::move(cpy.rp));
                          },
                          [self, cpy](error) mutable {
                            auto desc = "remote endpoint unavailable";
                            BROKER_ERROR(desc);
                            self->state.emit_error<ec::peer_unavailable>(
                              cpy.addr, desc);
                            if (cpy.addr.retry.count() > 0) {
                              BROKER_INFO("retrying"
                                          << cpy.addr << "in"
                                          << to_string(cpy.addr.retry));
                              self->delayed_send(self, cpy.addr.retry, cpy);
                            } else
                              cpy.rp.deliver(sec::cannot_connect_to_node);
                          });
}

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         filter_type initial_filter, broker_options options,
                         endpoint* ep) {
  self->state.init(std::move(initial_filter), std::move(options), ep);
  // We monitor remote inbound peerings and local outbound peerings.
  self->set_down_handler(
    [=](const caf::down_msg& down) {
      // Only required because the initial `peer` message can get lost.
      auto& st = self->state;
      auto hdl = caf::actor_cast<caf::actor>(down.source);
      auto i = st.pending_peers.find(hdl);
      if (i != st.pending_peers.end()) {
        st.emit_error<ec::peer_unavailable>(hdl, "remote endpoint unavailable");
        i->second.rp.deliver(down.reason);
        st.pending_peers.erase(i);
      }
      /* TODO: still needed? Already tracked by governor.
      BROKER_INFO("got DOWN from peer" << to_string(down.source));
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) {
        return p.actor && p.actor->address() == down.source;
      };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != self->state.peers.end());
      const char* desc;
      if (is_outbound(i->info.flags)) {
        BROKER_ASSERT(is_local(i->info.flags));
        desc = "lost local outbound peer";
      } else {
        BROKER_ASSERT(is_inbound(i->info.flags));
        BROKER_ASSERT(is_remote(i->info.flags));
        desc = "lost remote inbound peer";
      }
      BROKER_INFO(desc);
      self->send(subscriber, make_status<sc::peer_removed>(i->info.peer, desc));
      peers->erase(i);
      */
    }
  );
  return {
    // --- filter manipulation -------------------------------------------------
    [=](atom::subscribe, filter_type& f) {
      CAF_LOG_TRACE(CAF_ARG(f));
      self->state.add_to_filter(std::move(f));
    },
    // --- peering requests from local actors, i.e., "step 0" ------------------
    [=](atom::peer, actor remote_core) -> result<void> {
      return init_peering(self, std::move(remote_core),
                          self->make_response_promise());
    },
    [=](atom::peer, network_info& addr) -> result<void> {
      auto rp = self->make_response_promise();
      retry_state rt{std::move(addr), rp};
      rt.try_once(self);
      return rp;
    },
    [=](atom::peer, atom::retry, network_info& addr) {
      retry_state rt{std::move(addr), {}};
      rt.try_once(self);
    },
    [=](retry_state& rt) { rt.try_once(self); },
    // --- 3-way handshake for establishing peering streams between A and B ----
    // --- A (this node) performs steps #1 and #3; B performs #2 and #4 --------
    // Step #1: - A demands B shall establish a stream back to A
    //          - A has subscribers to the topics `ts`
    [=](atom::peer, filter_type& peer_ts,
        caf::actor& remote_core) -> stream<message> {
      CAF_LOG_TRACE(CAF_ARG(peer_ts) << CAF_ARG(remote_core));
      auto& st = self->state;
      // Reject anonymous peering requests.
      auto p = self->current_sender();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Drop anonymous peering request.");
        return invalid_stream;
      }
      CAF_LOG_DEBUG("received handshake step #1 from" << remote_core
                    << "via" << p << CAF_ARG(actor{self}));
      // Ignore unexpected handshakes as well as handshakes that collide
      // with an already pending handshake.
      auto i = st.pending_peers.find(remote_core);
      if (i != st.pending_peers.end() && i->second.sid.valid()) {
        CAF_LOG_DEBUG("Drop repeated peering request.");
        return invalid_stream;
      }
      // Especially ignore handshakes from already connected peers.
      if (st.policy().has_peer(remote_core)) {
        CAF_LOG_WARNING("Drop peering request from already connected peer.");
        return invalid_stream;
      }
      // Start streaming.
      auto sid = self->make_stream_id();
      auto remote_core_ptr = actor_cast<caf::strong_actor_ptr>(remote_core);
      auto handshake_data = std::make_tuple(st.filter, caf::actor{self});
      if (!self->add_sink<message>(st.governor, sid, nullptr, remote_core_ptr,
                                   caf::no_stages, message_id::make(),
                                   stream_priority::normal,
                                   std::move(handshake_data))) {
        CAF_LOG_WARNING("Unable to add sink state for streaming to peer.");
        return invalid_stream;
      }
      if (!st.policy().add_peer(sid, p, remote_core, std::move(peer_ts))) {
        CAF_LOG_WARNING("Unable to add peer state.");
        st.policy().peers().remove_path(sid, p, sec::invalid_stream_state,
                                        false);
        return invalid_stream;
      }
      core_state::pending_peer_state pps{sid, caf::none};
      st.pending_peers.emplace(remote_core, std::move(pps));
      self->streams().emplace(sid, st.governor);
      return {sid, st.governor};
    },
    // Step #2: B establishes a stream to A and sends its own filter
    [=](const stream<message>& in, filter_type& filter,
        caf::actor& remote_core) {
      CAF_LOG_TRACE(CAF_ARG(in) << CAF_ARG(filter) << remote_core);
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Drop anonymous peering request.");
        return;
      }
      CAF_LOG_DEBUG("received handshake step #2 from" << remote_core
                    << "via" << p << CAF_ARG(actor{self}));
      // Ignore duplicates.
      if (st.policy().has_peer(remote_core)) {
        CAF_LOG_DEBUG("Drop repeated handshake phase #2.");
        return;
      }
      // Start sending data on our own.
      auto sid = self->make_stream_id();
      auto remote_core_ptr = actor_cast<caf::strong_actor_ptr>(remote_core);
      auto handshake_data = std::make_tuple(ok_atom::value, caf::actor{self});
      if (!self->add_sink<message>(st.governor, sid, nullptr, remote_core_ptr,
                                   caf::no_stages, message_id::make(),
                                   stream_priority::normal,
                                   std::move(handshake_data))) {
        CAF_LOG_WARNING("Unable to add sink state for streaming to peer.");
        return;
      }
      if (!st.policy().add_peer(sid, p, remote_core, std::move(filter))) {
        CAF_LOG_WARNING("Unable to initialize peer state.");
        st.policy().peers().remove_path(sid, p, sec::invalid_stream_state,
                                        false);
        return;
      }
      st.policy().init_peer(in.id(), p, remote_core);
      self->streams().emplace(sid, st.governor);
      // Start streaming in opposite direction.
      st.emit_status<sc::peer_added>(remote_core,
                                     "received handshake from remote core");
      auto i = st.pending_peers.find(remote_core);
      if (i != st.pending_peers.end()) {
        i->second.rp.deliver(remote_core);
        st.pending_peers.erase(i);
      }
      // Acknowledge incoming data.
      if (!self->add_source(st.governor, in.id(), caf::none)) {
        CAF_LOG_WARNING("Unable to add source state for a peer.");
        return;
      }
      self->streams().emplace(in.id(), st.governor);
    },
    // Step #3: - A establishes a stream to B
    //          - B has a stream to A and vice versa now
    [=](const stream<message>& in, ok_atom, caf::actor& remote_core) {
      CAF_LOG_TRACE(CAF_ARG(in) << CAF_ARG(remote_core));
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Ignored anonymous peering request.");
        return;
      }
      CAF_LOG_DEBUG("received handshake step #3 from" << remote_core
                    << "via" << p << CAF_ARG(actor{self}));
      // Reject step #3 handshake if this actor didn't receive a step #1
      // handshake previously.
      auto i = st.pending_peers.find(remote_core);
      if (i == st.pending_peers.end() || !i->second.sid.valid()) {
        CAF_LOG_WARNING("Received a step #3 handshake, but no #1 previously.");
        return;
      }
      // Acknowledge incoming data.
      if (!self->add_source(st.governor, in.id(), caf::none)) {
        CAF_LOG_WARNING("Unable to add source state for a peer.");
        return;
      }
      self->streams().emplace(in.id(), st.governor);
      // Get peer data and install stream handler.
      self->demonitor(remote_core); // watched by the stream_aborter now
      st.policy().init_peer(in.id(), p, remote_core);
      i->second.rp.deliver(remote_core);
      st.emit_status<sc::peer_added>(remote_core, "handshake successful");
      st.pending_peers.erase(i);
    },
    // --- asynchronous communication to peers ---------------------------------
    [=](atom::update, filter_type f) {
      CAF_LOG_TRACE(CAF_ARG(f));
      auto& st = self->state;
      auto p = caf::actor_cast<caf::actor>(self->current_sender());
      if (p == nullptr) {
        CAF_LOG_DEBUG("Received anonymous filter update.");
        return;
      }
      if (!st.policy().update_peer(p, std::move(f)))
        CAF_LOG_DEBUG("Cannot update filter of unknown peer:" << to_string(p));
    },
    // --- communication to local actors: incoming streams and subscriptions ---
    [=](atom::join, filter_type& filter) -> expected<endpoint::stream_type> {
      CAF_LOG_TRACE(CAF_ARG(filter));
      // Check if the message is not anonymous and does contain a next stage.
      auto& st = self->state;
      auto cs = self->current_sender();
      if (cs == nullptr)
        return sec::cannot_add_downstream;
      auto& stages = self->current_mailbox_element()->stages;
      if (stages.empty()) {
        CAF_LOG_ERROR("Cannot join a data stream without downstream.");
        auto rp = self->make_response_promise();
        rp.deliver(sec::no_downstream_stages_defined);
        return endpoint::stream_type{stream_id{nullptr, 0}, nullptr};
      }
      auto next = stages.back();
      CAF_ASSERT(next != nullptr);
      // Initiate stream handshake and add subscriber to the governor.
      auto sid = self->make_stream_id();
      std::tuple<> token;
      if (!self->add_sink<endpoint::value_type>(
            st.governor, sid, nullptr, cs, std::move(stages),
            message_id::make(), stream_priority::normal, token)) {
        CAF_LOG_ERROR("Join failed: self->add_sink returned false.");
        return caf::sec::cannot_add_downstream;
      }
      st.add_to_filter(filter);
      st.policy().workers().set_filter(sid, cs, std::move(filter));
      self->streams().emplace(sid, st.governor);
      return endpoint::stream_type{sid, st.governor};
    },
    [=](atom::join, atom::update, stream_id sid, filter_type& filter) {
      auto cs = self->current_sender();
      if (cs == nullptr)
        return;
      auto& st = self->state;
      st.add_to_filter(filter);
      st.policy().workers().set_filter(sid, cs, std::move(filter));
    },
    [=](const endpoint::stream_type& in) {
      CAF_LOG_TRACE(CAF_ARG(in));
      auto& st = self->state;
      auto& cs = self->current_sender();
      if (cs == nullptr)
        return;
      st.local_sources.emplace(in.id(), cs);
      self->add_source(st.governor, in.id(), caf::none);
      self->streams().emplace(in.id(), st.governor);
    },
    [=](atom::publish, topic& t, data& x) {
      CAF_LOG_TRACE(CAF_ARG(t) << CAF_ARG(x));
      self->state.policy().push(std::move(t), std::move(x));
    },
    [=](atom::publish, topic& t, internal_command& x) {
      CAF_LOG_TRACE(CAF_ARG(t) << CAF_ARG(x));
      self->state.policy().push(std::move(t), std::move(x));
    },
    // --- communication to local actors only, i.e., never forward to peers ----
    [=](atom::publish, atom::local, topic& t, data& x) {
      CAF_LOG_TRACE(CAF_ARG(t) << CAF_ARG(x));
      self->state.policy().local_push(std::move(t), std::move(x));
    },
    [=](atom::publish, atom::local, topic& t, internal_command& x) {
      CAF_LOG_TRACE(CAF_ARG(t) << CAF_ARG(x));
      self->state.policy().local_push(std::move(t), std::move(x));
    },
    // --- "one-to-one" communication that bypasses streaming entirely ---------
    [=](atom::publish, endpoint_info& e, topic& t, data& x) {
      CAF_LOG_TRACE(CAF_ARG(t) << CAF_ARG(x));
      auto& st = self->state;
      actor hdl;
      if (e.network) {
        auto tmp = st.cache.find(*e.network);
        if (tmp)
          hdl = std::move(*tmp);
      }
      if (!hdl) {
        auto predicate = [&](const actor& x) { return x.node() == e.node; };
        hdl = st.policy().find_output_peer_hdl(std::move(predicate));
        if (!hdl) {
          BROKER_ERROR("no node found for endpoint info" << e);
          return;
        }
      }
      self->send(hdl, atom::publish::value, atom::local::value,
                 std::move(t), std::move(x));
    },
    // --- data store management -----------------------------------------------
    [=](atom::store, atom::master, atom::attach, const std::string& name,
        backend backend_type, backend_options& opts) -> caf::result<caf::actor> {
      CAF_LOG_TRACE(CAF_ARG(name) << CAF_ARG(backend_type) << CAF_ARG(opts));
      BROKER_INFO("attaching master:" << name);
      // Sanity check: this message must be a point-to-point message.
      auto& cme = *self->current_mailbox_element();
      if (!cme.stages.empty()) {
        CAF_LOG_WARNING("received a master attach message with stages");
        return ec::unspecified;
      }
      auto& st = self->state;
      auto i = st.masters.find(name);
      if (i != st.masters.end()) {
        BROKER_INFO("found local master");
        return i->second;
      }
      if (st.has_remote_master(name)) {
        BROKER_WARNING("remote master with same name exists already");
        return ec::master_exists;
      }
      BROKER_INFO("instantiating backend");
      auto ptr = make_backend(backend_type, std::move(opts));
      BROKER_ASSERT(ptr);
      BROKER_INFO("spawning new master");
      auto ms = self->spawn<caf::linked + caf::lazy_init>(
              master_actor, self, name, std::move(ptr), self->state.ep);
      st.masters.emplace(name, ms);
      // fwd_stream_handshake expects the next stage in cme.stages
      auto ms_ptr = actor_cast<strong_actor_ptr>(ms);
      cme.stages.emplace_back(ms_ptr);
      // Initiate stream handshake and add subscriber to the governor.
      auto sid = self->make_stream_id();
      std::tuple<> token;
      if (!self->add_sink<store::stream_type::value_type>(
            st.governor, sid, nullptr, ms_ptr, std::move(cme.stages),
            message_id::make(), stream_priority::normal, token)) {
        BROKER_ERROR("attaching master failed: self->add_sink returned false.");
        return caf::sec::cannot_add_downstream;
      }
      // Subscribe to messages directly targeted at the master.
      filter_type filter{name / topics::reserved / topics::master};
      st.add_to_filter(filter);
      st.policy().stores().set_filter(sid, ms_ptr, std::move(filter));
      self->streams().emplace(sid, st.governor);
      return ms;
    },
    [=](atom::store, atom::clone, atom::attach,
        std::string& name, double resync_interval, double stale_interval,
        double mutation_buffer_interval) -> caf::result<caf::actor> {
      BROKER_INFO("attaching clone:" << name);

      auto i = self->state.masters.find(name);

      if ( i != self->state.masters.end() && self->node() == i->second->node() )
        {
        BROKER_WARNING("attempted to run clone & master on the same endpoint");
        return ec::no_such_master;
        }

      // Sanity check: this message must be a point-to-point message.
      auto& cme = *self->current_mailbox_element();

      if (!cme.stages.empty())
        return ec::unspecified;

      auto stages = std::move(cme.stages);
      BROKER_INFO("spawning new clone");
      auto clone = self->spawn<linked + lazy_init>(
              clone_actor, self, name, resync_interval, stale_interval,
              mutation_buffer_interval, self->state.ep);
      auto cptr = actor_cast<strong_actor_ptr>(clone);
      auto& st = self->state;
      st.clones.emplace(name, clone);

      // Subscribe to updates.
      std::tuple<> token;
      auto sid = self->make_stream_id();

      if (!self->add_sink<store::stream_type::value_type>(
            st.governor, sid, nullptr, cptr, std::move(stages),
            message_id::make(), stream_priority::normal, token)) {
        BROKER_ERROR("attaching master failed: self->add_sink returned false.");
        return caf::sec::cannot_add_downstream;
      }

      // Subscribe to messages directly targeted at the clone.
      filter_type filter{name / topics::reserved / topics::clone};
      st.add_to_filter(filter);
      st.policy().stores().set_filter(sid, cptr, std::move(filter));
      self->streams().emplace(sid, st.governor);
      return clone;

      return caf::sec::invalid_stream_state; /* FIXME:
      auto spawn_clone = [=](const caf::actor& master) -> caf::actor {
        BROKER_INFO("spawning new clone");
        auto clone = self->spawn<linked + lazy_init>(clone_actor, self,
                                                     master, name);
        auto& st = self->state;
        st.clones.emplace(name, clone);
        // Subscribe to updates.
        filter_type f{name / topics::reserved / topics::clone};
        std::tuple<> token;
        auto sid = st.governor->stores().sid();
        auto cptr = actor_cast<strong_actor_ptr>(clone);
        self->current_mailbox_element()->stages.emplace_back(cptr);
        st.governor->stores().add_path(cptr);
        st.governor->stores().set_filter(cptr, f);
        self->fwd_stream_handshake<store::stream_type::value_type>(sid, token,
                                                                   true);
        st.add_to_filter(std::move(f));
        // Instruct master to generate a snapshot.
        self->state.governor->push(
          name / topics::reserved / topics::master,
          make_internal_command<snapshot_command>(self));
        return clone;
      };
      auto& peers = self->state.governor->peers();
      auto i = self->state.masters.find(name);
      if (i != self->state.masters.end()) {
        // We don't run clone and master on the same endpoint.
        if (self->node() == i->second.node()) {
          BROKER_WARNING("attempted to run clone & master on the same endpoint");
          return ec::no_such_master;
        }
        BROKER_INFO("found master in map");
        return spawn_clone(i->second);
      } else if (peers.empty()) {
        BROKER_INFO("no peers to ask for the master");
        return ec::no_such_master;
      }
      auto resolv = self->spawn<caf::lazy_init>(master_resolver);
      auto rp = self->make_response_promise<caf::actor>();
      std::vector<caf::actor> tmp;
      for (auto& kvp : peers)
        tmp.emplace_back(kvp.first);
      self->request(resolv, caf::infinite, std::move(tmp), std::move(name))
      .then(
        [=](actor& master) mutable {
          BROKER_INFO("received result from resolver:" << master);
          self->state.masters.emplace(name, master);
          rp.deliver(spawn_clone(std::move(master)));
        },
        [=](caf::error& err) mutable {
          BROKER_INFO("received error from resolver:" << err);
          rp.deliver(std::move(err));
        }
      );
      return rp;
      */
    },
    [=](atom::store, atom::master, atom::snapshot,
        const std::string& name, caf::actor& clone) {
      // Instruct master to generate a snapshot.
      self->state.policy().push(
        name / topics::reserved / topics::master,
        make_internal_command<snapshot_command>(self, std::move(clone)));
    },
    [=](atom::store, atom::master, atom::get,
        const std::string& name) -> result<actor> {
      auto i = self->state.masters.find(name);
      if (i != self->state.masters.end())
        return i->second;
      return ec::no_such_master;
    },
    [=](atom::store, atom::master, atom::resolve,
        std::string& name, actor& who_asked) {
      auto i = self->state.masters.find(name);

      if (i != self->state.masters.end()) {
        BROKER_INFO("found local master, using direct link");
        self->send(who_asked, atom::master::value, i->second);
      }

      auto peers = self->state.policy().get_peer_handles();

      if (peers.empty()) {
        BROKER_INFO("no peers to ask for the master");
        self->send(who_asked, atom::master::value,
                   make_error(ec::no_such_master, "no peers"));
      }

      auto resolv = self->spawn<caf::lazy_init>(master_resolver);
      self->send(resolv, std::move(peers), std::move(name),
                 std::move(who_asked));
    },
    // --- accessors -----------------------------------------------------------
    [=](atom::get, atom::peer) {
      auto& st = self->state;
      std::vector<peer_info> result;
      auto add = [&](actor hdl, peer_status status) {
        peer_info tmp;
        tmp.status = status;
        tmp.flags = peer_flags::remote + peer_flags::inbound
                    + peer_flags::outbound;
        tmp.peer.node = hdl.node();
        auto addrs = st.cache.find(hdl);
        // the peer_info only holds a single address, so ... pick first?
        if (addrs)
          tmp.peer.network = *addrs;
        result.emplace_back(std::move(tmp));
      };
      // collect connected peers
      st.policy().for_each_peer([&](const actor& hdl) {
        add(hdl, peer_status::peered);
      });
      // collect pending peers
      for (auto& kvp : st.pending_peers)
        if (kvp.second.sid.valid())
          add(kvp.first, peer_status::connected);
        else
          add(kvp.first, peer_status::connecting);
      return result;
    },
    [=](atom::get, atom::peer, atom::subscriptions) {
      std::vector<topic> result;
      // Collect filters for all peers.
      self->state.policy().for_each_filter([&](const filter_type& xs) {
        result.insert(result.end(), xs.begin(), xs.end());
      });
      // Sort and drop duplicates.
      std::sort(result.begin(), result.end());
      auto e = std::unique(result.begin(), result.end());
      if (e != result.end())
        result.erase(e, result.end());
      return result;
    },
    // --- destructive state manipulations -------------------------------------
    [=](atom::unpeer, network_info addr) {
      auto& st = self->state;
      auto x = self->state.cache.find(addr);
      if (!x || !st.policy().remove_peer(*x, caf::none, false, true))
        st.emit_error<ec::peer_invalid>(addr, "no such peer when unpeering");
    },
    [=](atom::unpeer, actor x) {
      auto& st = self->state;
      if (!x || !st.policy().remove_peer(x, caf::none, false, true))
        st.emit_error<ec::peer_invalid>(x, "no such peer when unpeering");
    },
    [=](atom::no_events) {
      auto& st = self->state;
      st.errors_ = caf::group{};
      st.statuses_ = caf::group{};
    },
    [=](atom::shutdown) {
      self->quit(exit_reason::user_shutdown);
      /* -- To consider:
         -- Terminating the actor after receiving shutdown unconditionally can
         -- cause already published data to not getting forwarded. The
         -- following code implements a more complicated shutdown procedure
         -- that would make sure data is transmitted before shutting down.
         -- However, this is often undesirable because it can take an arbitrary
         -- long time. Also, the current implementation does not terminate in
         -- all cases, i.e., seems not bug-free.
      auto& st = self->state;
      st.shutting_down = true;
      // Shutdown immediately if no local sink or source is connected.
      if (st.policy().at_end()) {
        CAF_LOG_DEBUG("Terminate core actor after receiving 'shutdown'");
        self->quit(exit_reason::user_shutdown);
        return;
      }
      // Wait until local sinks and sources are done, but no longer respond to
      // any future message.
      CAF_LOG_DEBUG("Delay termination of core actor after receiving "
                    "'shutdown' until local sinks and sources are done; "
                    "workers.size:" << st.policy().workers().num_paths()
                    << ", stores.size:" << st.policy().stores().num_paths());
      self->set_default_handler(caf::drop);
      self->become(
        [] {
          // Dummy behavior to keep the actor alive but unresponsive.
        }
      );
      */
    },
    [=](atom::shutdown, atom::store) {
      strong_actor_ptr dummy;
      auto& st = self->state;
      for (auto& p : st.policy().stores().paths())
        self->send_exit(p->hdl, caf::exit_reason::user_shutdown);
    }};
}

} // namespace detail
} // namespace broker
