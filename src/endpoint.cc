#include <unordered_set>

#include "broker/logger.hh" // Before any CAF header.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/radix_tree.hh"

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/version.hh"

namespace broker {
namespace {

struct peer_state {
  optional<caf::actor> actor;
  peer_info info;
};

struct core_state {
  std::vector<peer_state> peers;
  detail::radix_tree<uint64_t> subscriptions;
  std::map<network_info, caf::actor> supervisors;
  endpoint_info info;
  const char* name = "core";
};

// Creates endpoint information from a core actor.
endpoint_info make_info(const caf::actor& a, optional<network_info> net = {}) {
  return {a->node(), a->id(), std::move(net)};
}

endpoint_info make_info(network_info net) {
  return {{}, caf::invalid_actor_id, std::move(net)};
}

// Peforms the peering handshake between two endpoints.
void perform_handshake(caf::stateful_actor<core_state>* self,
                       const caf::actor& subscriber,
                       const caf::actor& other,
                       optional<network_info> net = {}) {
  BROKER_DEBUG("performing peering handshake");
  auto proto = version::protocol;
  self->request(other, timeout::peer, atom::peer::value, self, proto).then(
    [=](version::type v) mutable {
      status s;
      s.local = self->state.info;
      s.remote = make_info(other, net);
      auto peers = &self->state.peers;
      auto pred = [=](const peer_state& p) { return p.actor == other; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != peers->end());
      BROKER_ASSERT(version::compatible(v));
      BROKER_DEBUG("exchanging subscriptions with peer");
      // TODO: implement.
      i->info.status = peer_status::peered;
      s.info = peer_added;
      s.message = "outbound peering established";
      BROKER_INFO(s.message);
      self->send(subscriber, std::move(s));
    },
    [=](const caf::error& e) mutable {
      // Report peering error to subscriber.
      status s;
      s.local = self->state.info;
      s.remote = make_info(other, net);
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
      try {
        BROKER_DEBUG("attempting to connect to" << to_string(net));
        auto& mm = self->home_system().middleman();
        auto other = mm.remote_actor(net.address, net.port);
        self->monitor(other);
        self->send(core, atom::peer::value, net, peer_status::connected, other);
      } catch (const std::runtime_error& e) {
        // Try again on failure.
        self->delayed_send(self, timeout::reconnect, atom::connect::value);
      }
    }
  };
}

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
      s.remote = i->info.peer;
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
    [=](topic& t, message& msg) {
      auto local_matches = self->state.subscriptions.prefix_of(t.string());
      auto current_message = make_message(std::move(t), std::move(msg));
      if (!local_matches.empty()) {
        self->send(subscriber, current_message);
        for (auto match : local_matches)
          ++match->second;
      }
      // TODO: relay message to peers.
    },
    [=](atom::subscribe, const topic& t) {
      BROKER_DEBUG("got subscribe request for topic" << to_string(t));
      self->state.subscriptions.insert({t.string(), 0ull});
      // TODO: relay new subscription to peers.
      return atom::ok::value;
    },
    [=](atom::unsubscribe, const topic& t) {
      BROKER_DEBUG("got unsubscribe request for topic" << to_string(t));
      // We have to collect all topics first, because erasing a topic from the
      // radix tree invalidates iterators.
      std::vector<std::string> topics;
      for (auto match : self->state.subscriptions.prefixed_by(t.string()))
        topics.push_back(match->first);
      for (auto& top : topics)
        self->state.subscriptions.erase(top);
      // TODO: relay deletion of subscription to peers.
      return atom::ok::value;
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
        auto peers = &self->state.peers;
        auto i = std::find_if(peers->begin(), peers->end(), pred);
        BROKER_ASSERT(i != peers->end());
        i->actor = {};
        auto s = status{peer_lost};
        s.local = self->state.info;
        s.remote = i->info.peer;
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
      // Gather some information about the other peer.
      auto nid = other->address().node();
      auto mm = self->home_system().middleman().actor_handle();
      self->request(mm, timeout::infinite, caf::get_atom::value, nid).then(
        [=](caf::node_id, const std::string& addr, uint16_t port) mutable {
          optional<network_info> net;
          if (port > 0)
            net = network_info{addr, port};
          status s;
          s.local = self->state.info;
          s.remote = make_info(other, net);
          peer_info pi{s.remote, peer_flags::inbound, peer_status::peered};
          peers->push_back({other, std::move(pi)});
          s.info = peer_added;
          s.message = "inbound peering established";
          BROKER_DEBUG(s.message);
          self->monitor(other);
          self->send(subscriber, std::move(s));
          rp.deliver(caf::make_message(version::protocol));
        }
      );
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
        s.remote = make_info(net);
        s.message = "no such peer";
      } else {
        if (i->actor) {
          // Tell the other side to stop peering with us.
          self->send(*i->actor, atom::unpeer::value, self, false);
          self->demonitor(*i->actor);
        }
        // Remove the other endpoint from ourselves.
        s.info = peer_removed;
        s.remote = make_info(net);
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
      s.remote = make_info(other);
      if (i == peers->end()) {
        s.info = peer_invalid;
        s.message = "no such peer";
      } else {
        // Tell the other side to stop peering with us.
        if (propagate)
          self->send(other, atom::unpeer::value, self, false);
        self->demonitor(other);
        // Remove the other endpoint from ourselves.
        s.info = peer_removed;
        s.message = "removed peering";
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
        return make_message(net->address, net->port);
      else
        return make_message("", uint16_t{0});
    },
  };
}

} // namespace anonymous

endpoint::endpoint(const blocking_endpoint& other) : core_{other.core_} {
}

endpoint::endpoint(const nonblocking_endpoint& other) : core_{other.core_} {
}

endpoint& endpoint::operator=(const blocking_endpoint& other) {
  core_ = other.core_;
  return *this;
}

endpoint& endpoint::operator=(const nonblocking_endpoint& other) {
  core_ = other.core_;
  return *this;
}

endpoint_info endpoint::info() const {
  auto result = make_info(core());
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](std::string& address, uint16_t port) {
      if (port > 0)
        result.network = network_info{std::move(address), port};
    }
  );
  return result;
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  auto bound = uint16_t{0};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](const std::string&, uint16_t p) {
      bound = p;
    }
  );
  if (bound > 0)
    return 0;
  char const* addr = address.empty() ? nullptr : address.c_str();
  bound = core()->home_system().middleman().publish(core(), port, addr);
  if (bound == 0)
    return 0;
  self->request(core(), timeout::core, atom::network::value, atom::put::value,
                address, bound).receive(
    [](atom::ok) {
      // nop
    }
  );
  return bound;
}

void endpoint::peer(const endpoint& other) {
  caf::anon_send(core(), atom::peer::value, other.core());
}

void endpoint::peer(const std::string& address, uint16_t port) {
  caf::anon_send(core(), atom::peer::value, network_info{address, port});
}

void endpoint::unpeer(const endpoint& other) {
  caf::anon_send(core(), atom::unpeer::value, other.core(), true);
}

void endpoint::unpeer(const std::string& address, uint16_t port) {
  caf::anon_send(core(), atom::unpeer::value, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{core()->home_system()};
  auto msg = make_message(atom::peer::value, atom::get::value);
  self->request(core(), timeout::core, std::move(msg)).receive(
    [&](std::vector<peer_info>& peers) {
      result = std::move(peers);
    }
  );
  return result;
}

void endpoint::publish(topic t, message msg) {
  caf::send_as(core(), core(), std::move(t), std::move(msg));
}

void endpoint::subscribe(topic t) {
  caf::scoped_actor self{core()->home_system()};
  auto msg = make_message(atom::subscribe::value, std::move(t));
  self->request(core(), timeout::subscribe, std::move(msg)).receive(
    [](atom::ok) {
      // nop
    }
  );
}

void endpoint::unsubscribe(topic t) {
  caf::scoped_actor self{core()->home_system()};
  auto msg = make_message(atom::unsubscribe::value, std::move(t));
  self->request(core(), timeout::subscribe, std::move(msg)).receive(
    [](atom::ok) {
      // nop
    }
  );
}

const caf::actor& endpoint::core() const {
  return *core_;
}

message blocking_endpoint::receive() {
  return subscriber_->dequeue();
};

detail::mailbox blocking_endpoint::mailbox() {
  return subscriber_->mailbox();
}

namespace {

auto core_deleter = [](caf::actor* core) {
  caf::anon_send_exit(*core, caf::exit_reason::user_shutdown);
  delete core;
};

} // namespace <anonymous>

blocking_endpoint::blocking_endpoint(caf::actor_system& sys)
  : subscriber_{std::make_shared<detail::scoped_flare_actor>(sys)} {
  auto sub = caf::actor_cast<caf::actor>(*subscriber_);
  auto core = sys.spawn(core_actor, std::move(sub));
  auto ptr = new caf::actor{std::move(core)};
  core_ = std::shared_ptr<caf::actor>(ptr, core_deleter);
}

nonblocking_endpoint::nonblocking_endpoint(caf::actor_system& sys,
                                           caf::behavior bhvr) {
  auto subscriber = [=](caf::event_based_actor* self) {
    self->set_default_handler(caf::drop); // avoids unintended leaks
    return bhvr;
  };
  auto core = sys.spawn(core_actor, sys.spawn(subscriber));
  auto ptr = new caf::actor{std::move(core)};
  core_ = std::shared_ptr<caf::actor>(ptr, core_deleter);
}

} // namespace broker
