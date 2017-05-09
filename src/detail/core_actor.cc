#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/convert.hh"
#include "broker/error.hh"
#include "broker/peer_status.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/clone_actor.hh"
#include "broker/detail/core_actor.hh"
#include "broker/detail/die.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/make_unique.hh"
#include "broker/detail/master_actor.hh"

using namespace caf;

namespace broker {
namespace detail {

const char* core_state::name = "core";

namespace {

// Creates endpoint information from a core actor.
endpoint_info make_info(const caf::actor& a, optional<network_info> net = {}) {
  return {a->node(), a->id(), std::move(net)};
}

endpoint_info make_info(network_info net) {
  return {{}, caf::invalid_actor_id, std::move(net)};
}

std::vector<topic> local_subscriptions(caf::stateful_actor<core_state>* self) {
  std::vector<topic> topics;
  for (auto& subscription : self->state.subscriptions)
    topics.emplace_back(subscription.first);
  return topics;
}

// Peforms the peering handshake between two endpoints. When this endpoint (A)
// attempts to peer with another one (B), the following messages are exchanged:
//
//  A -> B: version_A
//  B -> A: version_B
//  A -> B: subscriptions_A
//  B -> A: subscriptions_B
//
void perform_handshake(caf::stateful_actor<core_state>* self,
                       const caf::actor& subscriber,
                       const caf::actor& other,
                       optional<network_info> net = {}) {
  BROKER_DEBUG("performing peering handshake");
  self->request(other, timeout::peer, atom::peer::value, self,
                local_subscriptions(self)).then(
    [=](std::vector<topic>& topics) {
      BROKER_DEBUG("got" << topics.size() << "peer subscriptions");
      // Record new subscriptions.
      for (auto& t : topics)
        self->state.subscriptions[t.string()].subscribers.insert(other);
      // Propagate subscriptions to peers.
      auto peers = &self->state.peers;
      auto subscription = caf::make_message(atom::subscribe::value,
                                            std::move(topics), self);
      for (auto& p : *peers)
        if (p.actor && *p.actor != other)
          self->send(*p.actor, subscription);
      // Set new status: peered.
      auto pred = [=](const peer_state& p) { return p.actor == other; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != peers->end());
      i->info.status = peer_status::peered;
      // Update the endpoint info.
      i->info.peer = make_info(other, std::move(i->info.peer.network));
      // Inform the subscriber about the successfully established peering.
      auto desc = "outbound peering established";
      BROKER_INFO(desc);
      auto s = make_status<sc::peer_added>(i->info.peer, desc);
      self->send(subscriber, s);
    },
    [=](const caf::error& e) mutable {
      BROKER_INFO("error while performing handshake"
                  << self->system().render(e));
      // Report peering error to subscriber.
      auto info = make_info(other, net);
      if (e == caf::sec::request_timeout) {
        auto desc = "peering request timed out";
        BROKER_ERROR(desc);
        auto err = make_error(ec::peer_timeout, std::move(info), desc);
        self->send(subscriber, std::move(err));
        // Try again.
        perform_handshake(self, subscriber, other, std::move(net));
      } else if (e == caf::sec::request_receiver_down) {
        // The supervisor will automatically attempt to re-establish a
        // connection.
        auto desc = "remote endpoint unavailable";
        BROKER_ERROR(desc);
        auto s = make_error(ec::peer_unavailable, std::move(info), desc);
        self->send(subscriber, std::move(s));
      } else if (e == ec::peer_incompatible) {
        auto desc = "incompatible peer version";
        BROKER_INFO(desc);
        self->send(subscriber, make_error(ec::peer_incompatible, info, desc));
        auto peers = &self->state.peers;
        auto pred = [=](const peer_state& p) { return p.actor == other; };
        auto i = std::find_if(peers->begin(), peers->end(), pred);
        if (net) {
          // TODO: Instead of simply abandoning reconnection attempts,
          // disconnect from the remote peer, and then try again after a
          // certain interval.
          BROKER_ASSERT(is_remote(i->info.flags));
          auto sv = self->state.supervisors.find(*net);
          BROKER_ASSERT(sv != self->state.supervisors.end());
          self->send_exit(sv->second, caf::exit_reason::normal);
          self->state.supervisors.erase(sv);
        }
        peers->erase(i);
        desc = "permanently removed incompatible peer";
        BROKER_INFO(desc);
        auto s = make_status<sc::peer_removed>(std::move(info), desc);
        self->send(subscriber, std::move(s));
      } else {
        auto desc = self->system().render(e);
        BROKER_ERROR(desc);
        self->send(subscriber, make_error(ec::unspecified, std::move(desc)));
        self->quit(caf::exit_reason::user_shutdown);
      }
    }
  );
}

// Supervises the connection to an IP address and TCP port.
=======
// Peform// Supervises the connection to an IP address and TCP port.
>>>>>>> Base new core on proof-of-concept stream governor
caf::behavior supervisor(caf::event_based_actor* self, caf::actor core,
                         network_info net, timeout::seconds retry) {
  self->send(self, atom::connect::value);
  self->set_down_handler(
    [=](const caf::down_msg&) {
      BROKER_DEBUG("lost connection to" << to_string(net));
      self->send(core, atom::peer::value, net, peer_status::disconnected);
      self->send(self, atom::connect::value);
    }
  );
  return {
    [=](atom::connect) {
      BROKER_DEBUG("attempting to connect to" << to_string(net));
      auto& mm = self->home_system().middleman();
      auto other = mm.remote_actor(net.address, net.port);
      if (!other && retry != timeout::seconds(0)) {
        // Try again on failure.
        self->delayed_send(self, retry, atom::connect::value);
      } else {
        self->monitor(*other);
        self->send(core, atom::peer::value, net, peer_status::connected,
                   *other);
      }
    }
  };
}

optional<caf::actor> find_remote_master(caf::stateful_actor<core_state>* self,
                                        const std::string& name) {
  // If we don't have a master recorded locally, we could still have a
  // propagated subscription to a remote core hosting a master.
  auto t = name / topics::reserved / topics::master;
  auto s = self->state.subscriptions.find(t.string());
  if (s != self->state.subscriptions.end()) {
    // Only the master subscribes to its inbound topic, so there can be at most
    // a single subscriber.
    BROKER_ASSERT(s->second.subscribers.size() == 1);
    auto& master = *s->second.subscribers.begin();
    return master;
  }
  return nil;
}

} // namespace <anonymous>

void core_state::init(caf::event_based_actor* s, filter_type initial_filter) {
  self = s;
  filter = std::move(initial_filter);
  sid = caf::stream_id{
    self->ctrl(),
    self->new_request_id(caf::message_priority::normal).integer_value()};
  governor = caf::make_counted<stream_governor>(this);
  self->streams().emplace(sid, governor);
}

caf::strong_actor_ptr core_state::prev_peer_from_handshake() {
  auto& xs = self->current_mailbox_element()->content();
  CAF_ASSERT(xs.match_elements<caf::stream_msg>());
  auto& x = xs.get_as<caf::stream_msg>(0);
  if (caf::holds_alternative<caf::stream_msg::open>(x.content))
    return get<caf::stream_msg::open>(x.content).prev_stage;
  return nullptr;
}

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         filter_type initial_filter) {
  self->state.init(self, std::move(initial_filter));
  self->state.info = make_info(self);
  // We monitor remote inbound peerings and local outbound peerings.
  self->set_down_handler(
    [=](const caf::down_msg& down) {
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
      BROKER_DEBUG(desc);
      self->send(subscriber, make_status<sc::peer_removed>(i->info.peer, desc));
      peers->erase(i);
      */
    }
  );
  return {
    // -- Filter manipulation. -------------------------------------------------
    [=](atom::subscribe, filter_type f) {
      auto& fs = self->state.filter;
      fs.insert(fs.end(), std::make_move_iterator(f.begin()),
                std::make_move_iterator(f.end()));
      std::sort(fs.begin(), fs.end());
      auto e = std::unique(fs.begin(), fs.end());
      if (e != fs.end())
        fs.erase(e, fs.end());
      // TODO: update filter on all paths
    },
    // -- Peering requests from local actors, i.e., "step 0". ------------------
    [=](atom::peer, strong_actor_ptr remote_core) -> result<void> {
      auto& st = self->state;
      // Sanity checking.
      if (remote_core == nullptr)
        return sec::invalid_argument;
      // Create necessary state and send message to remote core if not already
      // peering with B.
      if (!st.governor->has_peer(remote_core))
        self->send(actor{self} * actor_cast<actor>(remote_core),
                   atom::peer::value, st.filter);
      return unit;
    },
    // -- 3-way handshake for establishing peering streams between A and B. ----
    // -- A (this node) performs steps #1 and #3. B performs #2 and #4. --------
    // Step #1: A demands B shall establish a stream back to A. A has
    //          subscribers to the topics `ts`.
    [=](atom::peer, filter_type& peer_ts) -> stream_type {
      auto& st = self->state;
      // Reject anonymous peering requests.
      auto p = self->current_sender();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Drop anonymous peering request.");
        return invalid_stream;
      }
      // Ignore unexpected handshakes as well as handshakes that collide
      // with an already pending handshake.
      if (st.pending_peers.count(p) > 0) {
        CAF_LOG_DEBUG("Drop repeated peering request.");
        return invalid_stream;
      }
      auto peer_ptr = st.governor->add_peer(p, std::move(peer_ts));
      if (peer_ptr == nullptr) {
        CAF_LOG_DEBUG("Drop peering request of already known peer.");
        return invalid_stream;
      }
      st.pending_peers.emplace(std::move(p));
      auto& next = self->current_mailbox_element()->stages.back();
      CAF_ASSERT(next != nullptr);
      auto token = std::make_tuple(st.filter);
      self->fwd_stream_handshake<element_type>(st.sid, token);
      return {st.sid, st.governor};
    },
    // step #2: B establishes a stream to A, sending its own local subscriptions
    [=](const stream_type& in, filter_type& filter) {
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Drop anonymous peering request.");
        return;
      }
      // Ignore duplicates.
      if (st.governor->has_peer(p)) {
        CAF_LOG_DEBUG("Drop repeated handshake phase #2.");
        return;
      }
      // Add state to actor.
      auto peer_ptr = st.governor->add_peer(p, std::move(filter));
      peer_ptr->incoming_sid = in.id();
      self->streams().emplace(in.id(), st.governor);
      // Start streaming in opposite direction.
      st.governor->new_stream(p, st.sid, std::make_tuple(ok_atom::value));
    },
    // step #3: A establishes a stream to B
    // (now B has a stream to A and vice versa)
    [=](const stream_type& in, ok_atom) {
      CAF_LOG_TRACE(CAF_ARG(in));
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (p == nullptr) {
        CAF_LOG_DEBUG("Ignored anonymous peering request.");
        return;
      }
      // Reject step #3 handshake if this actor didn't receive a step #1
      // handshake previously.
      auto i = st.pending_peers.find(p);
      if (i == st.pending_peers.end()) {
        CAF_LOG_WARNING("Received a step #3 handshake, but no #1 previously.");
        return;
      }
      st.pending_peers.erase(i);
      auto res = self->streams().emplace(in.id(), st.governor);
      if (!res.second) {
        CAF_LOG_WARNING("Stream already existed.");
      }
    },
    // -- Communication to local actors: incoming streams and subscriptions. ---
    [=](join_atom, filter_type& filter) -> expected<stream_type> {
      auto& st = self->state;
      auto& cs = self->current_sender();
      if (cs == nullptr)
        return sec::cannot_add_downstream;
      auto& stages = self->current_mailbox_element()->stages;
      if (stages.empty()) {
        CAF_LOG_ERROR("Cannot join a data stream without downstream.");
        auto rp = self->make_response_promise();
        rp.deliver(sec::no_downstream_stages_defined);
        return stream_type{stream_id{nullptr, 0}, nullptr};
      }
      auto next = stages.back();
      CAF_ASSERT(next != nullptr);
      std::tuple<> token;
      self->fwd_stream_handshake<element_type>(st.sid, token);
      st.governor->local_subscribers().add_path(cs);
      st.governor->local_subscribers().set_filter(cs, std::move(filter));
      return stream_type{st.sid, st.governor};
    },
    [=](const stream_type& in) {
      auto& st = self->state;
      auto& cs = self->current_sender();
      if (cs == nullptr) {
        return;
      }
      self->streams().emplace(in.id(), st.governor);
    },
    [=](atom::publish, topic& t, data& x) {
      self->state.governor->push(std::move(t), std::move(x));
    },
    // TODO: add TTL component to this message to detect routing loops.
    /* TODO: dispatching is done in governor, how to integrate master/clone
             semantics into the picture?
    [=](topic& t, caf::message& msg, const caf::actor& source) {
      BROKER_DEBUG("got message:" << t << "->" << to_string(msg));
      BROKER_ASSERT(!t.string().empty());
      // Handle internal messages first.
      auto& str = t.string();
      auto i = str.find(topics::reserved.string());
      if (i != std::string::npos) {
        BROKER_ASSERT(i > 1);
        // Is the message for a master?
        auto m = str.rfind(topics::master.string());
        if (m != std::string::npos && !self->state.masters.empty()) {
          auto name = str.substr(0, i - 1);
          auto j = self->state.masters.find(name);
          if (j != self->state.masters.end()) {
            BROKER_DEBUG("delivering message to local master:" << name);
            self->send(j->second, std::move(t), std::move(msg), source);
            return; // This (unicast) message ends here.
          }
        }
        // Is the message for a clone?
        auto c = str.rfind(topics::clone.string());
        if (c != std::string::npos && !self->state.clones.empty()) {
          auto name = str.substr(0, i - 1);
          auto j = self->state.clones.find(name);
          if (j != self->state.clones.end()) {
            BROKER_DEBUG("delivering message to local clone:" << name);
            self->send(j->second, std::move(t), std::move(msg), source);
          }
        }
      }
      // Identify all subscribers, but send this message *at most* once.
      auto subscriptions = self->state.subscriptions.prefix_of(t.string());
      std::unordered_set<caf::actor> sinks;
      for (auto match : subscriptions) {
        ++match->second.messages;
        for (auto& sub : match->second.subscribers)
          if (sub != source)
            sinks.insert(sub);
      }
      // Relay message to all subscribers.
      auto sub_msg = caf::make_message(std::move(t), std::move(msg), self);
      for (auto& sink : sinks)
        if (sink == subscriber) {
          BROKER_DEBUG("dispatching message locally");
          // Local subscribers never see internal messages.
          if (i != std::string::npos)
            BROKER_ERROR("dropping internal message:" << to_string(msg));
          else
            self->send(sink, sub_msg);
        } else {
          BROKER_DEBUG("relaying message to" << to_string(sink));
          self->send(sink, sub_msg);
        }
    },
    */
    [=](atom::store, atom::master, atom::attach, const std::string& name,
        backend backend_type,
        backend_options& opts) -> caf::result<caf::actor> {
      BROKER_DEBUG("attaching master:" << name);
      auto i = self->state.masters.find(name);
      if (i != self->state.masters.end()) {
        BROKER_DEBUG("found local master");
        return i->second;
      }
      if (find_remote_master(self, name)) {
        BROKER_WARNING("remote master with same name exists already");
        return ec::master_exists;
      }
      BROKER_DEBUG("instantiating backend");
      auto ptr = make_backend(backend_type, std::move(opts));
      BROKER_ASSERT(ptr);
      BROKER_DEBUG("spawning new master");
      auto actor = self->spawn<caf::linked>(master_actor, self, name,
                                            std::move(ptr));
      self->state.masters.emplace(name, actor);
      // Subscribe to messages directly targeted at the master.
      auto ts = std::vector<topic>{name / topics::reserved / topics::master};
      self->send(self, atom::subscribe::value, std::move(ts), self);
      return actor;
    },
    [=](atom::store, atom::clone, atom::attach,
        std::string& name) -> caf::result<caf::actor> {
      BROKER_DEBUG("attaching clone:" << name);
      optional<caf::actor> master;
      auto i = self->state.masters.find(name);
      if (i != self->state.masters.end()) {
        BROKER_DEBUG("found local master, using direct link");
        master = i->second;
      } else if (auto remote = find_remote_master(self, name)) {
        BROKER_DEBUG("found remote master");
        master = std::move(*remote);
      } else {
        return ec::no_such_master;
      }
      BROKER_DEBUG("spawning new clone");
      auto clone = self->spawn<caf::linked>(clone_actor, self, *master, name);
      self->state.clones.emplace(name, clone);
      // Instruct clone to download snapshot from master.
      auto msg = caf::make_message(atom::snapshot::value, clone);
      auto t = name / topics::reserved / topics::master;
      self->send(*master, std::move(t), std::move(msg), self);
      // Subscribe to master updates.
      auto ts = std::vector<topic>{name / topics::reserved / topics::clone};
      self->send(self, atom::subscribe::value, std::move(ts), self);
      return clone;
    },
  };
}

} // namespace detail
} // namespace broker
