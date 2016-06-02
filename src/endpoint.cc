#include <unordered_set>

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/radix_tree.hh"

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/version.hh"

namespace broker {

template <class Processor>
void serialize(Processor& proc, network_info& ni) {
  proc & ni.address;
  proc & ni.port;
}

template <class Processor>
void serialize(Processor& proc, endpoint_uid& euid) {
  proc & euid.node;
  proc & euid.endpoint;
  proc & euid.network;
}

template <class Processor>
void serialize(Processor& proc, peer_info& p) {
  proc & p.uid;
  proc & p.outbound;
  proc & p.inbound;
}

namespace {

struct peer_state {
  caf::actor_addr handle;
  optional<network_info> remote;
  bool outbound;
  bool inbound;
};

struct core_state {
  std::vector<peer_state> peers;
  detail::radix_tree<uint64_t> subscriptions;
  optional<network_info> network;
  const char* name = "core";
};

// Peforms the actual peering between two endpoints.
void peer(caf::stateful_actor<core_state>* self,
          const caf::actor& local_subscriber,
          const caf::actor& other, optional<network_info> remote = {}) {
  auto handle = other->address();
  auto remote_euid = endpoint_uid{handle.node(), handle.id(), remote};
  auto local_euid = endpoint_uid{self->address().node(),
                                 self->address().id(),
                                 self->state.network};
  auto peers = &self->state.peers;
  auto pred = [&](const peer_state& p) {
    return p.remote && remote ? *p.remote == *remote : p.handle == handle;
  };
  // Check whether we track the other endpoint already.
  auto i = std::find_if(peers->begin(), peers->end(), pred);
  if (i != peers->end()) {
    if (i->outbound)
      return; // Peering exists already.
    if (!i->inbound)
      return; // Peering in progress.
    // Enable bi-directional peering.
    i->outbound = true;
    auto s = status{peer_added, local_euid, remote_euid};
    s.message = "additional outbound peering established";
    self->send(local_subscriber, std::move(s));
    return;
  }
  // Nope, we don't track the endpoint. Let's perform the handshake.
  i = peers->insert(i, {other->address(), remote, false, false});
  auto locate = [=](const peer_state& p) { return p.handle == handle; };
  auto proto = version::protocol;
  self->request(other, timeout::peer, atom::peer::value, self, proto).then(
    [=](version::type v) {
      if (!version::compatible(v)) {
        auto s = status{peer_incompatible, local_euid, remote_euid};
        s.message = "incompatible remote version " + std::to_string(v);
        self->send(local_subscriber, std::move(s));
        // TODO: retry peering
      } else {
        auto p = std::find_if(peers->begin(), peers->end(), locate);
        BROKER_ASSERT(p != peers->end());
        BROKER_ASSERT(!p->outbound);
        // TODO: exchange subscriptions.
        // Peering completed successfully.
        p->outbound = true;
        auto s = status{peer_added, local_euid, remote_euid};
        s.message = "outbound peering established";
        self->send(local_subscriber, std::move(s));
      }
    },
    [=](const caf::error& e) {
      // Report peering error to subscriber.
        auto s = status{peer_unavailable, local_euid, remote_euid};
      if (e == caf::sec::request_timeout)
        s.message = "peering request timed out";
      else if (e == caf::sec::request_receiver_down)
        s.message = "remote endpoint unavailable";
      else
        s.message = to_string(e);
      self->send(local_subscriber, std::move(s));
      // TODO: retry peering.
    }
  );
}

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         caf::actor local_subscriber) {
  self->set_down_handler(
    [=](const caf::down_msg& down) {
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) { return p.handle == down.source; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != self->state.peers.end());
      invalidate(i->handle); // Mark peer as down.
      if (i->outbound) {
        // TODO: reconnect
      }
    }
  );
  return {
    [=](topic& t, message& msg) {
      auto local_matches = self->state.subscriptions.prefix_of(t.string());
      auto current_message = make_message(std::move(t), std::move(msg));
      if (!local_matches.empty()) {
        self->send(local_subscriber, current_message);
        for (auto match : local_matches)
          ++match->second;
      }
      // TODO: relay message to peers.
    },
    [=](atom::subscribe, const topic& t) {
      self->state.subscriptions.insert({t.string(), 0ull});
      // TODO: relay subscription change to peers.
      return atom::ok::value;
    },
    [=](atom::unsubscribe, const topic& t) {
      // We have to collect all topics first, because erasing a topic from the
      // radix tree invalidates iterators.
      std::vector<std::string> topics;
      for (auto match : self->state.subscriptions.prefixed_by(t.string()))
        topics.push_back(match->first);
      for (auto& top : topics)
        self->state.subscriptions.erase(top);
      // TODO: relay subscription change to peers.
      return atom::ok::value;
    },
    [=](atom::peer, const std::string& address, uint16_t port) {
      try {
        auto& mm = self->home_system().middleman();
        auto other = mm.remote_actor(address, port);
        peer(self, local_subscriber, other, network_info{address, port});
      } catch (const std::runtime_error& e) {
        auto s = status{peer_unavailable};
        s.local = {self->address().node(), self->address().id(),
                   self->state.network};
        s.remote = {node_id{}, endpoint_id{0}, network_info{address, port}};
        self->send(local_subscriber, std::move(s));
        // TODO: spawn an actor that attempts to reconnect to the address-port
        // pair and then re-initiate the peering process on success.
      }
    },
    [=](atom::peer, const caf::actor& other) {
      // Request peering with in-memory endpoint.
      peer(self, local_subscriber, other);
    },
    [=](atom::peer, const caf::actor& other, version::type v) {
      // Respond to peering request.
      auto rp = self->make_response_promise();
      auto handle = other->address();
      // Get remote IP address and port first.
      auto nid = handle.node();
      auto mm = self->home_system().middleman().actor_handle();
      self->request(mm, timeout::infinite, caf::get_atom::value, nid).then(
        [=](caf::node_id, const std::string& addr, uint16_t port) mutable {
          auto peers = &self->state.peers;
          optional<network_info> remote;
          if (port > 0)
            remote = network_info{addr, port};
          auto pred = [=](const peer_state& p) {
            return p.remote && remote
                ? *p.remote == *remote
                : p.handle == handle;
          };
          auto i = std::find_if(peers->begin(), peers->end(), pred);
          if (i != peers->end()) {
            // We already have an outbound peering relationship (or are trying
            // to create one).
            // TODO: should we send out another status message?
            BROKER_ASSERT(!i->inbound);
            i->inbound = true;
            rp.deliver(caf::make_message(version::protocol));
            return;
          }
          auto remote_euid = endpoint_uid{handle.node(), handle.id(), remote};
          auto local_euid = endpoint_uid{self->address().node(),
                                         self->address().id(),
                                         self->state.network};
          if (!version::compatible(v)) {
            auto s = status{peer_incompatible, local_euid, remote_euid};
            s.message = "received incompatible peering attempt";
            self->send(local_subscriber, std::move(s));
            rp.deliver(caf::make_message(version::protocol));
            return;
          }
          // We have a new inbound peering.
          i = peers->insert(i, {handle, remote, false, true});
          auto s = status{peer_added, local_euid, remote_euid};
          s.message = "inbound peering established";
          self->send(local_subscriber, std::move(s));
          rp.deliver(caf::make_message(version::protocol));
        }
      );
      return rp;
    },
    [=](atom::unpeer, const std::string& address, uint16_t port) {
      auto peers = &self->state.peers;
      auto pred = [&](const peer_state& p) {
        return p.remote
            && p.remote->address == address
            && p.remote->port == port;
      };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      auto s = status{};
      s.local = {self->address().node(),
                 self->address().id(),
                 self->state.network};
      if (i != peers->end()) {
        s.info = peer_removed;
        s.remote = {i->handle.node(), i->handle.id(), i->remote};
        s.message = "removed peering";
        self->demonitor(i->handle);
        peers->erase(i);
      } else {
        s.info = peer_invalid;
        s.remote = {{}, {0}, network_info{address, port}};
        s.message = "no such peer";
      }
      self->send(local_subscriber, std::move(s));
    },
    [=](atom::unpeer, const caf::actor& other) {
      auto peers = &self->state.peers;
      auto handle = other.address();
      auto pred = [&](const peer_state& p) { return p.handle == handle; };
      auto i = std::find_if(peers->begin(), peers->end(), pred);
      BROKER_ASSERT(i != peers->end()); // in-memory endpoints can't go down.
      auto s = status{peer_removed};
      s.local = {self->address().node(),
                 self->address().id(),
                 self->state.network};
      s.remote = {i->handle.node(), i->handle.id(), i->remote};
      s.message = "removed peering";
      self->send(local_subscriber, std::move(s));
      peers->erase(i);
      self->demonitor(other);
    },
    [=](atom::peer, atom::get) {
      std::vector<peer_info> result;
      auto map = [](const peer_state& p) -> peer_info {
        auto euid = endpoint_uid{p.handle.node(), p.handle.id(), p.remote};
        return {euid, p.outbound, p.inbound};
      };
      std::transform(self->state.peers.begin(),
                     self->state.peers.end(),
                     std::back_inserter(result),
                     map);
      return result;
    },
    [=](atom::network, atom::put, std::string& address, uint16_t port) {
      self->state.network = network_info{std::move(address), port};
      return atom::ok::value;
    },
    [=](atom::network, atom::get) {
      auto& net = self->state.network;
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

endpoint_uid endpoint::uid() const {
  auto result = endpoint_uid{core_->address().node(), core_->address().id()};
  caf::scoped_actor self{core_->home_system()};
  self->request(core_, timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](std::string& address, uint16_t port) {
      if (port > 0)
        result.network = network_info{std::move(address), port};
    }
  );
  return result;
}

uint16_t endpoint::listen(uint16_t port, const std::string& address) {
  auto bound = uint16_t{0};
  caf::scoped_actor self{core_->home_system()};
  self->request(core_, timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](const std::string&, uint16_t p) {
      bound = p;
    }
  );
  if (bound > 0)
    return 0;
  char const* addr = address.empty() ? nullptr : address.c_str();
  bound = core_->home_system().middleman().publish(core_, port, addr);
  if (bound == 0)
    return 0;
  self->request(core_, timeout::core, atom::network::value, atom::put::value,
                address, bound).receive(
    [](atom::ok) {
      // nop
    }
  );
  return bound;
}

void endpoint::peer(const endpoint& other) {
  caf::anon_send(core_, atom::peer::value, other.core_);
}

void endpoint::peer(const std::string& address, uint16_t port) {
  caf::anon_send(core_, atom::peer::value, address, port);
}

void endpoint::unpeer(const endpoint& other) {
  caf::anon_send(core_, atom::unpeer::value, other.core_);
}

void endpoint::unpeer(const std::string& address, uint16_t port) {
  caf::anon_send(core_, atom::unpeer::value, address, port);
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{core_->home_system()};
  auto msg = make_message(atom::peer::value, atom::get::value);
  self->request(core_, timeout::core, std::move(msg)).receive(
    [&](std::vector<peer_info>& peers) {
      result = std::move(peers);
    }
  );
  return result;
}

void endpoint::publish(topic t, message msg) {
  caf::send_as(core_, core_, std::move(t), std::move(msg));
}

void endpoint::subscribe(topic t) {
  caf::scoped_actor self{core_->home_system()};
  auto msg = make_message(atom::subscribe::value, std::move(t));
  self->request(core_, timeout::subscribe, std::move(msg)).receive(
    [](atom::ok) {
      // nop
    }
  );
}

void endpoint::unsubscribe(topic t) {
  caf::scoped_actor self{core_->home_system()};
  auto msg = make_message(atom::unsubscribe::value, std::move(t));
  self->request(core_, timeout::subscribe, std::move(msg)).receive(
    [](atom::ok) {
      // nop
    }
  );
}

endpoint::endpoint() : core_{caf::unsafe_actor_handle_init} {
  // Derived classes complete the endpoint initialization.
}

message blocking_endpoint::receive() {
  return subscriber_->dequeue();
};

detail::mailbox blocking_endpoint::mailbox() {
  return subscriber_->mailbox();
}

blocking_endpoint::blocking_endpoint(caf::actor_system& sys)
  : subscriber_{std::make_shared<detail::scoped_flare_actor>(sys)} {
  core_ = sys.spawn(core_actor, caf::actor_cast<caf::actor>(*subscriber_));
}

nonblocking_endpoint::nonblocking_endpoint(caf::actor_system& sys,
                                           caf::actor subscriber) {
  core_ = sys.spawn(core_actor, std::move(subscriber));
}

} // namespace broker
