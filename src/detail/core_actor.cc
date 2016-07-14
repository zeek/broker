#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/error.hh"
#include "broker/message.hh"
#include "broker/peer_status.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/core_actor.hh"

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
          s.local = self->state.info;
          s.peer = make_info(other, net);
          s.info = peer_added;
          s.message = "outbound peering established";
          BROKER_INFO(s.message);
          self->send(subscriber, std::move(s));
        });
    },
    [=](const caf::error& e) mutable {
      // Report peering error to subscriber.
      status s;
      s.local = self->state.info;
      s.peer = make_info(other, net);
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

} // namespace anonymous

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         caf::actor subscriber) {
  self->state.info = make_info(self);
  // The core actor monitors inbound peerings and local outbound peerings.
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
      s.local = self->state.info;
      s.peer = i->info.peer;
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
  auto unsubscribe = [=](const topic& t, const caf::actor& a) {
    // We have to collect all candidate topics to remove first, because
    // erasing a topic from the radix tree invalidates iterators.
    std::vector<topic> topics;
    for (auto match : self->state.subscriptions.prefixed_by(t.string())) {
      auto& subscribers = match->second.subscribers;
      // Remove the subscription state iff we are the only subscriber.
      auto i = subscribers.find(a);
      if (i != subscribers.end()) {
        BROKER_ASSERT(!subscribers.empty());
        if (subscribers.size() == 1) {
          BROKER_DEBUG("removing state for topic" << match->first);
          BROKER_DEBUG("processed" << match->second.messages << "messages");
          subscribers.erase(i);
        } else {
          BROKER_DEBUG("removing subscriber from topic " << match->first);
          BROKER_DEBUG(subscribers.size() - 1 << "subscribers remaining");
          topics.emplace_back(match->first);
        }
      }
    }
    for (auto& t : topics)
      self->state.subscriptions.erase(t.string());
  };
  return {
    [=](topic& t, message& msg, const caf::actor& source) {
      auto subscriptions = self->state.subscriptions.prefix_of(t.string());
      BROKER_DEBUG("got message for" << subscriptions.size()
                   << "subscriptions:" << t << "->" << to_string(msg));
      // Relay message to all subscribers, at most once.
      std::unordered_set<caf::actor> sinks;
      for (auto match : subscriptions) {
        ++match->second.messages;
        for (auto& sub : match->second.subscribers)
          if (sub != source)
            sinks.insert(sub);
      }
      for (auto& sink : sinks)
        if (sink == subscriber) {
          BROKER_DEBUG("dispatching message locally");
          self->send(sink, t, msg);
        } else {
          BROKER_DEBUG("relaying message to" << to_string(sink));
          self->send(sink, t, msg, self);
        }
    },
    [=](atom::subscribe, std::vector<topic>& ts, const caf::actor& source) {
      BROKER_DEBUG("got subscribe request for" << ts.size() << "topics");
      for (auto& t : ts)
        self->state.subscriptions[t.string()].subscribers.insert(source);
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
      unsubscribe(t, source);
      // Relay unsubscription to peers.
      auto msg = caf::make_message(atom::unsubscribe::value, t, self);
      for (auto& p : self->state.peers)
        if (p.actor && *p.actor != source)
          self->send(*p.actor, msg);
    },
    [=](atom::peer, network_info net) {
      BROKER_DEBUG("requesting peering with remote endpoint"
                   << to_string(net));
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
      BROKER_DEBUG("requesting peering with local endpoint");
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
        s.local = self->state.info;
        s.peer = i->info.peer;
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
          s.local = self->state.info;
          s.peer = make_info(other, net);
          peer_info pi{s.peer, peer_flags::inbound, peer_status::peered};
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
      BROKER_DEBUG("requesting unpeering with remote endpoint"
                   << to_string(net));
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) {
        return p.info.peer.network == net && is_outbound(p.info.flags);
      };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      status s;
      s.local = self->state.info;
      if (i == peers->end()) {
        s.info = peer_invalid;
        s.peer = make_info(net);
        s.message = "no such peer";
      } else {
        if (i->actor) {
          // Tell the other side to stop peering with us.
          self->send(*i->actor, atom::unpeer::value, self, false);
          self->demonitor(*i->actor);
        }
        // Remove the other endpoint from ourselves.
        s.info = peer_removed;
        s.peer = make_info(net);
        s.message = "removed peering";
        peers->erase(i);
      }
      self->send(subscriber, std::move(s));
    },
    [=](atom::unpeer, const caf::actor& other, bool propagate) {
      BROKER_DEBUG("got request to unpeer with endpoint");
      auto peers = &self->state.peers;
      auto handle = other.address();
      auto pred = [&](const peer_state& p) { return p.actor == other; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      status s;
      s.local = self->state.info;
      s.peer = make_info(other);
      if (i == peers->end()) {
        s.info = peer_invalid;
        s.message = "no such peer";
        BROKER_DEBUG(s.message);
      } else {
        // Tell the other side to stop peering with us.
        if (propagate)
          self->send(other, atom::unpeer::value, self, false);
        self->demonitor(other);
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
  };
}

} // namespace detail
} // namespace broker
