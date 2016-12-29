#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/convert.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
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

namespace broker {
namespace detail {

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
  auto proto = version::protocol;
  self->request(other, timeout::peer, atom::peer::value, self, proto).then(
    [=](version::type v) {
      BROKER_ASSERT(version::compatible(v));
      BROKER_DEBUG("exchanging subscriptions with peer");
      auto msg = caf::make_message(atom::peer::value, self,
                                   local_subscriptions(self));
      self->request(other, timeout::peer, std::move(msg)).then(
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
          // Inform the subscriber about the successfully established peering.
          status s;
          s.endpoint = make_info(other, net);
          s.info = peer_added;
          s.message = "outbound peering established";
          BROKER_INFO(s.message);
          self->send(subscriber, std::move(s));
        });
    },
    [=](const caf::error& e) mutable {
      // Report peering error to subscriber.
      status s;
      s.endpoint = make_info(other, net);
      if (e == caf::sec::request_timeout) {
        s.message = "peering request timed out";
        BROKER_ERROR(s.message);
        self->send(subscriber, std::move(s));
        // Try again.
        perform_handshake(self, subscriber, other, std::move(net));
      } else if (e == caf::sec::request_receiver_down) {
        // The supervisor will automatically attempt to re-establish a
        // connection.
        s.info = peer_unavailable;
        s.message = "remote endpoint unavailable";
        BROKER_ERROR(s.message);
        self->send(subscriber, std::move(s));
      } else if (e == ec::version_incompatible) {
        s.info = peer_incompatible;
        s.message = "incompatible peer version";
        BROKER_INFO(s.message);
        self->send(subscriber, s);
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
        s.info = peer_removed;
        s.message = "permanently removed incompatible peer";
        BROKER_INFO(s.message);
        self->send(subscriber, std::move(s));
      } else {
        s.message = to_string(e);
        BROKER_ERROR(s.message);
        self->send(subscriber, std::move(s));
        self->quit(caf::exit_reason::user_shutdown);
      }
    }
  );
}

// Supervises the connection to an IP address and TCP port.
caf::behavior supervisor(caf::event_based_actor* self, caf::actor core,
                         network_info net) {
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
      if (!other) {
        // Try again on failure.
        self->delayed_send(self, timeout::reconnect, atom::connect::value);
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

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         caf::actor subscriber, api_flags flags) {
  self->state.forwarding = has_api_flags(flags, routable);
  self->state.info = make_info(self);
  // We monitor remote inbound peerings and local outbound peerings.
  self->set_down_handler(
    [=](const caf::down_msg& down) {
      BROKER_INFO("got DOWN from peer" << to_string(down.source));
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) {
        return p.actor && p.actor->address() == down.source;
      };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != self->state.peers.end());
      auto s = status{peer_removed};
      s.endpoint = i->info.peer;
      if (is_outbound(i->info.flags)) {
        BROKER_ASSERT(is_local(i->info.flags));
        s.message = "lost local outbound peer";
      } else {
        BROKER_ASSERT(is_inbound(i->info.flags));
        BROKER_ASSERT(is_remote(i->info.flags));
        s.message = "lost remote inbound peer";
      }
      self->send(subscriber, std::move(s));
      peers->erase(i);
    }
  );
  return {
    // TODO: add TTL component to this message to detect routing loops.
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
    [=](atom::subscribe, std::vector<topic>& ts, const caf::actor& source) {
      BROKER_DEBUG("got subscribe request for" << to_string(source));
      if (ts.empty()) {
        BROKER_WARNING("ignoring subscription with empty topic list");
        return;
      }
      for (auto& t : ts) {
        BROKER_DEBUG("  -" << t);
        self->state.subscriptions[t.string()].subscribers.insert(source);
      }
      auto msg = caf::make_message(atom::subscribe::value, std::move(ts), self);
      for (auto& p : self->state.peers)
        if (p.actor && *p.actor != source) {
          BROKER_DEBUG("relaying subscriptions to peer" << to_string(*p.actor));
          for (auto& t : msg.get_as<std::vector<topic>>(1))
            self->state.subscriptions[t.string()].subscribers.insert(*p.actor);
          self->send(*p.actor, msg);
        }
    },
    [=](atom::unsubscribe, const topic& t, const caf::actor& source) {
      BROKER_DEBUG("got unsubscribe request for topic" << t);
      // We have erase all matching topics in two phases, because erasing a
      // topic from a radix tree invalidates iterators.
      std::vector<std::string> topics;
      for (auto match : self->state.subscriptions.prefixed_by(t.string())) {
        auto& subscribers = match->second.subscribers;
        BROKER_DEBUG("removing subscriber from topic" << match->first);
        subscribers.erase(source);
        if (subscribers.empty())
          topics.push_back(std::move(match->first));
      }
      for (auto& t : topics) {
        BROKER_DEBUG("removing subscription state of topic" << t);
        self->state.subscriptions.erase(t);
      }
      // Relay unsubscription to peers.
      auto msg = caf::make_message(atom::unsubscribe::value, t, self);
      for (auto& p : self->state.peers)
        if (p.actor && *p.actor != source)
          self->send(*p.actor, msg);
    },
    [=](atom::peer, network_info net) {
      BROKER_DEBUG("peering with remote endpoint" << to_string(net));
      auto pred = [&](const peer_state& p) {
        return p.info.peer.network == net && is_outbound(p.info.flags);
      };
      auto peers = &self->state.peers;
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      if (i != peers->end()) {
        BROKER_WARNING("outbound peering already exists");
        return;
      }
      auto ei = make_info(net);
      auto flags = peer_flags::outbound + peer_flags::remote;
      self->state.peers.push_back({{}, {ei, flags, peer_status::connecting}});
      BROKER_DEBUG("spawning connection supervisor");
      auto sv = self->spawn<caf::linked>(supervisor, self, net);
      self->state.supervisors.emplace(net, sv);
    },
    [=](atom::peer, const caf::actor& other) {
      BROKER_DEBUG("peering with local endpoint");
      auto pred = [&](const peer_state& p) { return p.actor == other; };
      auto peers = &self->state.peers;
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      if (i != peers->end()) {
        BROKER_WARNING("ignoring duplicate peering attempt");
        return;
      }
      auto addr = other->address();
      auto flags = peer_flags::outbound + peer_flags::local;
      peer_info pi{make_info(other), flags, peer_status::initialized};
      self->state.peers.push_back({other, pi});
      self->monitor(other);
      perform_handshake(self, subscriber, other);
    },
    [=](atom::peer, network_info net, peer_status, const caf::actor& other) {
      // Check if this peer is already known.
      auto peers = &self->state.peers;
      auto known = [&](const peer_state& p) {
        return p.actor == other && is_outbound(p.info.flags);
      };
      auto i = std::find_if(peers->begin(), peers->end(), known);
      if (i != peers->end()) {
        BROKER_ERROR("found known peer under different network configuration:"
                     << to_string(net) << "(new)"
                     << to_string(*i->info.peer.network) << "(old)");
        self->quit(caf::exit_reason::user_shutdown);
        return;
      }
      // Locate peer by network info.
      auto pred = [&](const peer_state& p) {
        return p.info.peer.network == net && is_outbound(p.info.flags);
      };
      i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != peers->end());
      BROKER_ASSERT(i->info.status == peer_status::connecting
                    || i->info.status == peer_status::reconnecting);
      i->info.status = peer_status::connected;
      i->actor = other;
      perform_handshake(self, subscriber, other, net);
    },
    [=](atom::peer, network_info net, peer_status stat) {
      if (stat == peer_status::disconnected) {
        BROKER_INFO("lost connection to remote peer" << to_string(net));
        auto pred = [&](const peer_state& p) {
          return p.info.peer.network == net && is_outbound(p.info.flags);
        };
        // Remove peer from subscriptions and switch it "off".
        auto peers = &self->state.peers;
        auto i = std::find_if(peers->begin(), peers->end(), pred);
        BROKER_ASSERT(i != peers->end());
        BROKER_ASSERT(i->actor);
        for (auto& sub : self->state.subscriptions)
          sub.second.subscribers.erase(*i->actor);
        i->actor = {};
        // Notify local subscriber.
        auto s = status{peer_lost};
        s.endpoint = i->info.peer;
        s.message = "lost remote peer";
        self->send(subscriber, std::move(s));
      }
    },
    [=](atom::peer, const caf::actor& other, version::type v) {
      BROKER_DEBUG("got peering request from endpoint" << to_string(other));
      auto rp = self->make_response_promise();
      if (!version::compatible(v)) {
        BROKER_INFO("detected incompatible version" << v);
        rp.deliver(make_error_message(ec::version_incompatible));
        return;
      }
      auto pred = [=](const peer_state& p) { return p.actor == other; };
      auto peers = &self->state.peers;
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      if (i != peers->end()) {
        BROKER_DEBUG("found existing peering");
        i->info.flags = i->info.flags + peer_flags::inbound;
        rp.deliver(caf::make_message(version::protocol));
        return;
      }
      // Gather some information about the other peer. Note that this may not
      // be the endpoint information that the peer has (e.g., due to NAT).
      auto nid = other->address().node();
      auto mm = self->home_system().middleman().actor_handle();
      self->request(mm, timeout::infinite, caf::get_atom::value, nid).then(
        [=](caf::node_id, const std::string& addr, uint16_t port) mutable {
          // Register inbound peer.
          optional<network_info> net;
          if (port > 0)
            net = network_info{addr, port};
          status s;
          s.endpoint = make_info(other, net);
          peer_info pi{s.endpoint, peer_flags::inbound, peer_status::peered};
          peers->push_back({other, std::move(pi)});
          s.info = peer_added;
          s.message = "inbound peering established";
          BROKER_DEBUG(s.message);
          self->monitor(other);
          rp.deliver(caf::make_message(version::protocol));
          self->send(subscriber, std::move(s));
        }
      );
    },
    [=](atom::peer, const caf::actor& other, std::vector<topic>& topics) {
      BROKER_DEBUG("got" << topics.size() << "peer subscriptions");
      for (auto& t : topics)
        self->state.subscriptions[t.string()].subscribers.insert(other);
      // Propagate subscriptions to peers.
      auto subscription = caf::make_message(atom::subscribe::value,
                                            std::move(topics), self);
      for (auto& p : self->state.peers)
        if (p.actor && *p.actor != other)
          self->send(*p.actor, subscription);
      return local_subscriptions(self);
    },
    [=](atom::unpeer, network_info net) {
      BROKER_DEBUG("unpeering with remote endpoint" << to_string(net));
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) {
        return p.info.peer.network == net && is_outbound(p.info.flags);
      };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      status s;
      if (i == peers->end()) {
        s.info = peer_invalid;
        s.endpoint = make_info(net);
        s.message = "no such peer";
      } else {
        if (i->actor) {
          // Tell the other side to stop peering with us.
          self->send(*i->actor, atom::unpeer::value, self, self);
          self->demonitor(*i->actor);
        }
        // Remove the other endpoint from ourselves.
        s.info = peer_removed;
        s.endpoint = make_info(net);
        s.message = "removed peering";
        peers->erase(i);
      }
      self->send(subscriber, std::move(s));
    },
    [=](atom::unpeer, const caf::actor& peer, const caf::actor& source) {
      BROKER_DEBUG("got request to unpeer with endpoint");
      auto peers = &self->state.peers;
      auto handle = peer.address();
      auto pred = [&](const peer_state& p) { return p.actor == peer; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      status s;
      s.endpoint = make_info(peer);
      if (i == peers->end()) {
        s.info = peer_invalid;
        s.message = "no such peer";
        BROKER_DEBUG(s.message);
      } else {
        // Tell the other side to stop peering with us.
        if (peer != source)
          self->send(peer, atom::unpeer::value, self, self);
        self->demonitor(peer);
        // Remove the other endpoint from ourselves.
        s.info = peer_removed;
        s.message = "removed peering";
        BROKER_DEBUG(s.message);
        peers->erase(i);
      }
      self->send(subscriber, std::move(s));
    },
    [=](atom::peer, atom::get) {
      std::vector<peer_info> result;
      std::transform(self->state.peers.begin(),
                     self->state.peers.end(),
                     std::back_inserter(result),
                     [](const peer_state& p) { return p.info; });
      return result;
    },
    [=](atom::network, atom::put, std::string& address, uint16_t port) {
      self->state.info.network = network_info{std::move(address), port};
      return atom::ok::value;
    },
    [=](atom::network, atom::get) {
      auto& net = self->state.info.network;
      if (net)
        return caf::make_message(net->address, net->port);
      else
        return caf::make_message("", uint16_t{0});
    },
    [=](atom::store, atom::master, atom::attach, const std::string& name,
        backend backend_type, backend_options& opts) -> expected<caf::actor> {
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
    [=](atom::store, atom::clone, atom::attach, std::string& name)
    -> expected<caf::actor> {
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
