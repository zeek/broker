#include <unordered_set>

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/radix_tree.hh"

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/timeout.hh"
#include "broker/version.hh"

#include <iostream> // TODO: remove after debugging

namespace broker {
namespace {

struct core_state {
  // TODO: consider "weak peers" to avoid cyclic referencing. Upgrade the weak
  // handle to a strong handle only upon sending messages.
  std::unordered_set<caf::actor> peers;
  detail::radix_tree<uint64_t> subscriptions;
  const char* name = "core";
};

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         caf::actor local_subscriber) {
  BROKER_ASSERT(local_subscriber);
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
      return atom::ok::value;
    },
    [=](atom::peer, const caf::actor& other) -> caf::response_promise {
      // Request peering.
      auto rp = self->make_response_promise();
      auto i = self->state.peers.find(other);
      if (i != self->state.peers.end())
        return rp.deliver(atom::ok::value);
      self->state.peers.insert(other);
      self->request(other, timeout::peer, atom::peer::value, self,
                    version::protocol).then(
        [=](atom::ok, version::type) mutable {
          rp.deliver(atom::ok::value);
        },
        [=](error& e) mutable {
          if (e == ec::version_incompatible) {
            // TODO: report error properly: send a status message.
            std::cerr << "broker protocol version mismatch: " <<
                         to_string(e.context()) << std::endl;
          }
          rp.deliver(std::move(e));
        }
      );
      return rp;
    },
    [=](atom::peer, const caf::actor& other, version::type v) {
      // Respond to peering request.
      if (version::compatible(v))
        return make_message(
          fail<ec::version_incompatible>(v, version::protocol));
      self->state.peers.insert(other);
      return make_message(atom::ok::value, version::protocol);
    },
    [=](atom::peer, atom::get) {
      std::vector<caf::actor_addr> peers;
      std::transform(self->state.peers.begin(),
                     self->state.peers.end(),
                     std::back_inserter(peers),
                     [](const caf::actor& a) { return a->address(); });
      return peers;
    },
  };
}

caf::behavior nonblocking_actor(caf::blocking_actor* self) {
  return {};
}

bool peer_cores(const caf::actor& src, const caf::actor& dst) {
  auto result = true;
  caf::scoped_actor self{src->home_system()};
  self->request(dst, timeout::peer, atom::peer::value, src).receive(
    [](atom::ok) {
      // nop
    },
    [&](const error& c) {
      result = false;
    }
  );
  return result;
}

} // namespace anonymous

peer::peer(caf::actor_addr addr) : addr_{std::move(addr)} {
}

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

bool endpoint::peer(const endpoint& other) {
  return peer_cores(core_, other.core_);
}

bool endpoint::peer(const std::string& address, uint16_t port) {
  auto r = core_->home_system().middleman().remote_actor(address, port);
  return r && peer_cores(core_, r);
}

bool endpoint::unpeer(const endpoint& other) {
  BROKER_ASSERT(! "not yet implemented"); // TODO
  return false;
}

std::vector<peer> endpoint::peers() const {
  std::vector<broker::peer> result;
  caf::scoped_actor self{core_->home_system()};
  auto msg = make_message(atom::peer::value, atom::get::value);
  self->request(core_, timeout::peer, std::move(msg)).receive(
    [&](const std::vector<caf::actor_addr>& peers) {
      std::transform(peers.begin(),
                     peers.end(),
                     std::back_inserter(result),
                     [](const caf::actor_addr& a) { return broker::peer{a}; });
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

blocking_endpoint::blocking_endpoint(caf::actor_system& sys) 
  : subscriber_{std::make_shared<caf::scoped_actor>(sys)} {
  core_ = sys.spawn(core_actor, *subscriber_);
}

message blocking_endpoint::receive() {
  for (;;) {
    (*subscriber_)->await_data();
    auto ptr = (*subscriber_)->next_message();
    if (ptr)
      return ptr->msg;
  }
};

bool blocking_endpoint::empty() const {
  return (*subscriber_)->mailbox().empty();
}


nonblocking_endpoint::nonblocking_endpoint(caf::actor_system& sys,
                                           caf::actor subscriber) {
  core_ = sys.spawn(core_actor, std::move(subscriber));
}

} // namespace broker
