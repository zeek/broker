#include <unordered_set>
#include <sstream>

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/send.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/report.hh"
#include "broker/store/query.hh"

// FIXME: move from implementation to includes.
#include "src/util/radix_tree.hh"

#ifdef DEBUG
// So that we don't have a recursive expansion from sending messages via the
// report::manager endpoint.
#define BROKER_ENDPOINT_DEBUG(endpoint_pointer, subtopic, msg)                 \
  if (endpoint_pointer != broker::report::manager)                             \
    broker::report::send(broker::report::level::debug, subtopic, msg)
#else
#define BROKER_ENDPOINT_DEBUG(endpoint_pointer, subtopic, msg)
#endif

namespace broker {

// TODO: remove after migration to multiple broker systems.
extern std::unique_ptr<caf::actor_system> broker_system;

static inline caf::actor& handle_to_actor(void* h) {
  return *static_cast<caf::actor*>(h);
}

namespace detail {

static std::string to_string(const topic_set& ts) {
  std::string rval{"{"};
  bool first = true;
  for (const auto& e : ts) {
    if (first)
      first = false;
    else
      rval += ", ";
    rval += e.first;
  }
  rval += "}";
  return rval;
}

void ocs_update(const caf::actor& q, peering p,
                outgoing_connection_status::tag t, std::string name = "") {
  caf::anon_send(q, outgoing_connection_status{std::move(p), t,
                                               std::move(name)});
}

void ics_update(const caf::actor& q, std::string name,
                incoming_connection_status::tag t) {
  caf::anon_send(q, incoming_connection_status{t, std::move(name)});
}

endpoint_actor::endpoint_actor(
  caf::actor_config& cfg, const endpoint* ep, std::string arg_name, int flags,
  caf::actor ocs_queue, caf::actor ics_queue)
    : caf::event_based_actor{cfg},
      name(std::move(arg_name)),
      behavior_flags(flags) {
  using namespace caf;
  using namespace std;
  auto ocs_established = outgoing_connection_status::tag::established;
  auto ocs_disconnect = outgoing_connection_status::tag::disconnected;
  auto ocs_incompat = outgoing_connection_status::tag::incompatible;
  active = {
    [=](int version) {
      return make_message(BROKER_PROTOCOL_VERSION == version,
                          BROKER_PROTOCOL_VERSION);
    },
    [=](peer_atom, actor& peer, peering& p) {
      auto it = peers.find(peer.address());
      if (it != peers.end()) {
        ocs_update(ocs_queue, move(p), ocs_established, it->second.name);
        return;
      }
      request(peer, infinite, BROKER_PROTOCOL_VERSION).await(
        [=](bool compat, int their_version) {
          if (!compat)
            ocs_update(ocs_queue, move(p), ocs_incompat);
          else
            request(peer, infinite, peer_atom::value, this, name,
                    advertised_subscriptions).await(
              [=](string& pname, topic_set& ts) {
                add_peer(move(peer), pname, move(ts), false);
                ocs_update(ocs_queue, move(p), ocs_established, move(pname));
              },
              [=](const caf::error&) {
                ocs_update(ocs_queue, move(p), ocs_disconnect);
              }
            );
        },
        [=](const caf::error& e) {
          if (e == sec::unexpected_message)
            ocs_update(ocs_queue, move(p), ocs_incompat);
          else if (e == sec::request_receiver_down)
            ocs_update(ocs_queue, move(p), ocs_disconnect);
          else
            assert(!"unhandled request error");
        }
      );
    },
    [=](peer_atom, actor& peer, string& pname, topic_set& ts) {
      ics_update(ics_queue, pname,
                 incoming_connection_status::tag::established);
      add_peer(move(peer), move(pname), move(ts), true);
      return make_message(name, advertised_subscriptions);
    },
    [=](unpeer_atom, const actor& peer) {
      auto itp = peers.find(peer.address());
      if (itp == peers.end())
        return;
      BROKER_DEBUG("endpoint." + name,
                   "Unpeered with: '" + itp->second.name + "'");
      if (itp->second.incoming)
        ics_update(ics_queue, itp->second.name,
                   incoming_connection_status::tag::disconnected);
      demonitor(peer);
      peers.erase(itp);
      peer_subscriptions.erase(peer.address());
    },
    [=](const down_msg& d) {
      demonitor(d.source);
      auto itp = peers.find(d.source);
      if (itp != peers.end()) {
        BROKER_DEBUG("endpoint." + name,
                     "Peer down: '" + itp->second.name + "'");
        if (itp->second.incoming)
          ics_update(ics_queue, itp->second.name,
                     incoming_connection_status::tag::disconnected);
        peers.erase(itp);
        peer_subscriptions.erase(d.source);
        return;
      }
      auto s = local_subscriptions.erase(d.source);
      if (!s)
        return;
      BROKER_DEBUG("endpoint." + name,
                   "Local subscriber down with subscriptions: "
                     + to_string(s->subscriptions));
      for (auto& sub : s->subscriptions)
        if (!local_subscriptions.have_subscriber_for(sub.first))
          unadvertise_subscription(topic{move(sub.first)});
    },
    [=](unsub_atom, const topic& t, const actor& peer) {
      BROKER_DEBUG("endpoint." + name, "Peer '" + get_peer_name(peer)
                                         + "' unsubscribed to '" + t + "'");
      peer_subscriptions.unregister_topic(t, peer.address());
    },
    [=](sub_atom, topic& t, actor& peer) {
      BROKER_DEBUG("endpoint." + name, "Peer '" + get_peer_name(peer)
                                         + "' subscribed to '" + t + "'");
      peer_subscriptions.register_topic(move(t), move(peer));
    },
    [=](master_atom, store::identifier& id, actor& a) {
      if (local_subscriptions.exact_match(id)) {
        report::error("endpoint." + name + ".store.master." + id,
                    "Failed to register master data store with id '"
                    + id + "' because a master already exists with"
                           " that id.");
        return;
      }

      BROKER_DEBUG("endpoint." + name,
                   "Attached master data store named '" + id + "'");
      add(move(id), move(a));
    },
    [=](local_sub_atom, topic& t, actor& a) {
      BROKER_DEBUG("endpoint." + name,
                   "Attached local queue for topic '" + t + "'");
      add(move(t), move(a));
    },
    [=](const topic& t, broker::message& msg, int flags) {
      bool from_peer = peers.find(current_sender()) != peers.end();
      if (from_peer) {
        BROKER_ENDPOINT_DEBUG(ep, "endpoint." + name,
                              "Got remote message from peer '"
                                + get_peer_name(current_sender())
                                + "', topic '" + t + "': " + to_string(msg));
        publish_locally(t, std::move(msg), flags, from_peer);
        // Don't re-publish messages sent by a peer (they go one hop).
      } else {
        BROKER_ENDPOINT_DEBUG(ep, "endpoint." + name,
                              "Publish local message with topic '" + t + "': "
                                + to_string(msg));
        publish_locally(t, msg, flags, from_peer);
        publish_current_msg_to_peers(t, flags);
      }
    },
    [=](store_actor_atom, const store::identifier& n) {
      return find_master(n);
    },
    [=](const store::identifier& n, const store::query& q,
        const actor& requester) {
      auto master = find_master(n);
      if (master) {
        BROKER_DEBUG("endpoint." + name,
                     "Forwarded data store query: "
                       + caf::to_string(current_message()));
        forward_to(master);
      } else {
        BROKER_DEBUG("endpoint." + name,
                     "Failed to forward data store query: "
                       + caf::to_string(current_message()));
        send(requester, this, store::result(store::result::status::failure));
      }
    },
    on<store::identifier, anything>() >>
      [=](const store::identifier& id) {
        // This message should be a store update operation.
        auto master = find_master(id);
        if (master) {
          BROKER_DEBUG("endpoint." + name,
                       "Forwarded data store update: "
                         + caf::to_string(current_message()));
          forward_to(master);
        } else
          report::warn("endpoint." + name + ".store.master." + id,
                       "Data store update dropped due to nonexistent "
                       " master with id '"
                         + id + "'");
      },
    [=](flags_atom, int flags) {
      bool auto_before = (behavior_flags & AUTO_ADVERTISE);
      behavior_flags = flags;
      bool auto_after = (behavior_flags & AUTO_ADVERTISE);
      if (auto_before == auto_after)
        return;
      if (auto_before) {
        topic_set to_remove;
        for (const auto& t : advertised_subscriptions)
          if (advert_acls.find(t.first) == advert_acls.end())
            to_remove.insert({t.first, true});
        BROKER_DEBUG("endpoint." + name,
                     "Toggled AUTO_ADVERTISE off,"
                     " no longer advertising: "
                       + to_string(to_remove));
        for (const auto& t : to_remove)
          unadvertise_subscription(topic{t.first});
        return;
      }
      BROKER_DEBUG("endpoint." + name, "Toggled AUTO_ADVERTISE on");
      for (const auto& t : local_subscriptions.topics())
        advertise_subscription(topic{t.first});
    },
    [=](acl_pub_atom, topic& t) {
      BROKER_DEBUG("endpoint." + name, "Allow publishing topic: " + t);
      pub_acls.insert({move(t), true});
    },
    [=](acl_unpub_atom, const topic& t) {
      BROKER_DEBUG("endpoint." + name, "Disallow publishing topic: " + t);
      pub_acls.erase(t);
    },
    [=](advert_atom, string& t) {
      BROKER_DEBUG("endpoint." + name,
                   "Allow advertising subscription: " + t);
      if (advert_acls.insert({t, true}).second
          && local_subscriptions.exact_match(t))
        // Now permitted to advertise an existing subscription.
        advertise_subscription(move(t));
    },
    [=](unadvert_atom, string& t) {
      BROKER_DEBUG("endpoint." + name,
                   "Disallow advertising subscription: " + t);
      if (advert_acls.erase(t) && local_subscriptions.exact_match(t))
        // No longer permitted to advertise an existing subscription.
        unadvertise_subscription(move(t));
    },
    others() >>
      [=] {
        report::warn("endpoint." + name,
                     "Got unexpected message: "
                       + caf::to_string(current_message()));
      }};
}

caf::behavior endpoint_actor::make_behavior() {
  return active;
}

std::string endpoint_actor::get_peer_name(const caf::actor_addr& a) const {
  auto it = peers.find(a);
  if (it == peers.end())
    return "<unknown>";
  return it->second.name;
}

std::string endpoint_actor::get_peer_name(const caf::actor& peer) const {
  return get_peer_name(peer.address());
}

void endpoint_actor::add_peer(caf::actor peer, std::string peer_name,
                              topic_set ts, bool incoming) {
  BROKER_DEBUG("endpoint." + name, "Peered with: '" + peer_name
                                     + "', subscriptions: " + to_string(ts));
  demonitor(peer);
  monitor(peer);
  peers[peer.address()] = {peer, std::move(peer_name), incoming};
  peer_subscriptions.insert(subscriber{std::move(peer), std::move(ts)});
}

void endpoint_actor::add(std::string topic_or_id, caf::actor a) {
  demonitor(a);
  monitor(a);
  local_subscriptions.register_topic(topic_or_id, std::move(a));
  if ((behavior_flags & AUTO_ADVERTISE)
      || advert_acls.find(topic_or_id) != advert_acls.end())
    advertise_subscription(std::move(topic_or_id));
}

caf::actor endpoint_actor::find_master(const store::identifier& id) {
  auto m = local_subscriptions.exact_match(id);
  if (!m)
    m = peer_subscriptions.exact_match(id);
  if (!m)
    return caf::invalid_actor;
  return *m->begin();
}

void endpoint_actor::advertise_subscription(topic t) {
  if (advertised_subscriptions.insert({t, true}).second) {
    BROKER_DEBUG("endpoint." + name, "Advertise new subscription: " + t);
    publish_subscription_operation(std::move(t), sub_atom::value);
  }
}

void endpoint_actor::unadvertise_subscription(topic t) {
  if (advertised_subscriptions.erase(t)) {
    BROKER_DEBUG("endpoint." + name, "Unadvertise subscription: " + t);
    publish_subscription_operation(std::move(t), unsub_atom::value);
  }
}

void endpoint_actor::publish_subscription_operation(
  topic t, caf::atom_value op) {
  if (peers.empty())
    return;
  auto msg = caf::make_message(std::move(op), std::move(t), this);
  for (const auto& peer : peers)
    send(peer.second.ep, msg);
}

void endpoint_actor::publish_locally(const topic& t, broker::message msg,
                                     int flags, bool from_peer) {
  if (!from_peer && !(flags & SELF))
    return;
  auto matches = local_subscriptions.prefix_matches(t);
  if (matches.empty())
    return;
  auto caf_msg = caf::make_message(std::move(msg));
  for (const auto& match : matches)
    for (const auto& a : match->second)
      send(a, caf_msg);
}

void endpoint_actor::publish_current_msg_to_peers(const topic& t, int flags) {
  if (!(flags & PEERS))
    return;
  if (!(behavior_flags & AUTO_PUBLISH) && pub_acls.find(t) == pub_acls.end())
    // Not allowed to publish this topic to peers.
    return;
  // send instead of forward_to so peer can use
  // current_sender() to check if msg comes from a peer.
  if ((flags & UNSOLICITED))
    for (const auto& peer : peers)
      send(peer.second.ep, current_message());
  else
    for (const auto& a : peer_subscriptions.unique_prefix_matches(t))
      send(a, current_message());
}

endpoint_proxy_actor::endpoint_proxy_actor(
  caf::actor_config& cfg, caf::actor local, std::string endpoint_name,
  std::string addr, uint16_t port, std::chrono::duration<double> retry_freq,
  caf::actor ocs_queue)
  : caf::event_based_actor{cfg} {
  using namespace caf;
  using namespace std;
  peering p{local, this, true, make_pair(addr, port)};
  trap_exit(true);
  bootstrap = {
    after(chrono::seconds(0)) >> [=] {
      try_connect(p, endpoint_name);
    }
  };
  disconnected = {
    [=](peerstat_atom) {
      ocs_update(ocs_queue, p, outgoing_connection_status::tag::disconnected);
    },
    [=](const exit_msg& e) {
      quit();
    },
    [=](quit_atom) {
      quit();
    },
    after(chrono::duration_cast<chrono::microseconds>(retry_freq)) >> [=] {
      try_connect(p, endpoint_name);
    }
  };
  connected = {
    [=](peerstat_atom) {
      send(local, peer_atom::value, remote, p);
    },
    [=](const exit_msg& e) {
      send(remote, unpeer_atom::value, local);
      send(local, unpeer_atom::value, remote);
      quit();
    },
    [=](quit_atom) {
      send(remote, unpeer_atom::value, local);
      send(local, unpeer_atom::value, remote);
      quit();
    },
    [=](const down_msg& d) {
      BROKER_DEBUG(report_subtopic(endpoint_name, addr, port),
                   "Disconnected from peer");
      demonitor(remote);
      remote = invalid_actor;
      become(disconnected);
      ocs_update(ocs_queue, p, outgoing_connection_status::tag::disconnected);
    },
    others() >> [=] {
      report::warn(report_subtopic(endpoint_name, addr, port),
                   "Remote endpoint proxy got unexpected message: "
                     + caf::to_string(current_message()));
    }
  };
}

caf::behavior endpoint_proxy_actor::make_behavior() {
  return bootstrap;
}

std::string endpoint_proxy_actor::report_subtopic(
  const std::string& endpoint_name, const std::string& addr, uint16_t port)
  const {
  std::ostringstream st;
  st << "endpoint." << endpoint_name << ".remote_proxy." << addr << ":"
     << port;
  return st.str();
}

bool endpoint_proxy_actor::try_connect(const peering& p,
                                       const std::string& endpoint_name) {
  using namespace caf;
  using namespace std;
  const std::string& addr = p.remote_tuple().first;
  const uint16_t& port = p.remote_tuple().second;
  const caf::actor& local = p.endpoint_actor();
  try {
    remote = broker_system->middleman().remote_actor(addr, port);
  } catch (const exception& e) {
    report::warn(report_subtopic(endpoint_name, addr, port),
                 string("Failed to connect: ") + e.what());
  }
  if (!remote) {
    become(disconnected);
    return false;
  }
  BROKER_DEBUG(report_subtopic(endpoint_name, addr, port), "Connected");
  monitor(remote);
  become(connected);
  send(local, peer_atom::value, remote, p);
  return true;
}

} // namespace detail

endpoint::endpoint(std::string name, int flags)
  : name_{std::move(name)},
    flags_{flags},
    self_{*broker_system},
    actor_{broker_system->spawn<detail::endpoint_actor>(
          this, name_, flags_,
          handle_to_actor(outgoing_conns_.handle()),
          handle_to_actor(incoming_conns_.handle()))} {
    // FIXME: do not rely on private API
    self_->planned_exit_reason(caf::exit_reason::unknown);
    actor_->link_to(self_);
}

const std::string& endpoint::name() const {
  return name_;
}

int endpoint::flags() const {
  return flags_;
}

void endpoint::set_flags(int flags) {
  flags_ = flags;
  caf::anon_send(actor_, flags_atom::value, flags);
}

int endpoint::last_errno() const {
  return last_errno_;
}

const std::string& endpoint::last_error() const {
  return last_error_;
}

bool endpoint::listen(uint16_t port, const char* addr, bool reuse_addr) {
  try {
    broker_system->middleman().publish(actor_, port, addr, reuse_addr);
  } catch (const std::exception& e) {
    last_errno_ = 0;
    last_error_ = e.what();
    return false;
  }
  return true;
}

peering endpoint::peer(std::string addr, uint16_t port,
                       std::chrono::duration<double> retry) {
  auto port_addr = std::pair<std::string, uint16_t>(addr, port);
  peering rval;
  for (const auto& p : peers_)
    if (p.remote() && port_addr == p.remote_tuple()) {
      rval = p;
      break;
    }
  if (rval)
    caf::anon_send(rval.peer_actor(), peerstat_atom::value);
  else {
    auto h = handle_to_actor(outgoing_conns_.handle());
    auto a = broker_system->spawn<detail::endpoint_proxy_actor>(
      actor_, name_, addr, port, retry, h);
    a->link_to(self_);
    rval = peering{actor_, std::move(a), true, port_addr};
    peers_.insert(rval);
  }
  return rval;
}

peering endpoint::peer(const endpoint& e) {
  if (this == &e)
    return {};
  peering p{actor_, e.actor_};
  peers_.insert(p);
  caf::anon_send(actor_, peer_atom::value, e.actor_, p);
  return p;
}

bool endpoint::unpeer(peering p) {
  if (!p)
    return false;
  auto it = peers_.find(p);
  if (it == peers_.end())
    return false;
  peers_.erase(it);
  if (p.remote())
    // The proxy actor initiates unpeer messages.
    caf::anon_send(p.peer_actor(), quit_atom::value);
  else {
    caf::anon_send(actor_, unpeer_atom::value, p.peer_actor());
    caf::anon_send(p.peer_actor(), unpeer_atom::value, actor_);
  }
  return true;
}

const outgoing_connection_status_queue&
endpoint::outgoing_connection_status() const {
  return outgoing_conns_;
}

const incoming_connection_status_queue&
endpoint::incoming_connection_status() const {
  return incoming_conns_;
}

void endpoint::send(topic t, message msg, int flags) const {
  caf::anon_send(actor_, std::move(t), std::move(msg), flags);
}

void endpoint::publish(topic t) {
  caf::anon_send(actor_, acl_pub_atom::value, t);
}

void endpoint::unpublish(topic t) {
  caf::anon_send(actor_, acl_unpub_atom::value, t);
}

void endpoint::advertise(topic t) {
  caf::anon_send(actor_, advert_atom::value, t);
}

void endpoint::unadvertise(topic t) {
  caf::anon_send(actor_, unadvert_atom::value, t);
}

void* endpoint::handle() const {
  return const_cast<caf::actor*>(&actor_);
}

} // namespace broker

// Begin C API
#include "broker/broker.h"
using std::nothrow;

void broker_deque_of_incoming_connection_status_delete(
  broker_deque_of_incoming_connection_status* d) {
  delete reinterpret_cast<std::deque<broker::incoming_connection_status>*>(d);
}

size_t broker_deque_of_incoming_connection_status_size(
  const broker_deque_of_incoming_connection_status* d) {
  using deque_ptr = const std::deque<broker::incoming_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  return dd->size();
}

broker_incoming_connection_status*
broker_deque_of_incoming_connection_status_at(
  broker_deque_of_incoming_connection_status* d, size_t idx) {
  using deque_ptr = std::deque<broker::incoming_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  return reinterpret_cast<broker_incoming_connection_status*>(&(*dd)[idx]);
}

void broker_deque_of_incoming_connection_status_erase(
  broker_deque_of_incoming_connection_status* d, size_t idx) {
  using deque_ptr = std::deque<broker::incoming_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  dd->erase(dd->begin() + idx);
}

int broker_incoming_connection_status_queue_fd(
  const broker_incoming_connection_status_queue* q) {
  using queue_ptr = const broker::incoming_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  return qq->fd();
}

broker_deque_of_incoming_connection_status*
broker_incoming_connection_status_queue_want_pop(
  const broker_incoming_connection_status_queue* q) {
  auto rval = new (nothrow) std::deque<broker::incoming_connection_status>;
  if (!rval)
    return nullptr;
  using queue_ptr = const broker::incoming_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  *rval = qq->want_pop();
  return reinterpret_cast<broker_deque_of_incoming_connection_status*>(rval);
}

broker_deque_of_incoming_connection_status*
broker_incoming_connection_status_queue_need_pop(
  const broker_incoming_connection_status_queue* q) {
  auto rval = new (nothrow) std::deque<broker::incoming_connection_status>;
  if (!rval)
    return nullptr;
  using queue_ptr = const broker::incoming_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  *rval = qq->need_pop();
  return reinterpret_cast<broker_deque_of_incoming_connection_status*>(rval);
}

void broker_deque_of_outgoing_connection_status_delete(
  broker_deque_of_outgoing_connection_status* d) {
  delete reinterpret_cast<std::deque<broker::outgoing_connection_status>*>(d);
}

size_t broker_deque_of_outgoing_connection_status_size(
  const broker_deque_of_outgoing_connection_status* d) {
  using deque_ptr = const std::deque<broker::outgoing_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  return dd->size();
}

broker_outgoing_connection_status*
broker_deque_of_outgoing_connection_status_at(
  broker_deque_of_outgoing_connection_status* d, size_t idx) {
  using deque_ptr = std::deque<broker::outgoing_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  return reinterpret_cast<broker_outgoing_connection_status*>(&(*dd)[idx]);
}

void broker_deque_of_outgoing_connection_status_erase(
  broker_deque_of_outgoing_connection_status* d, size_t idx) {
  using deque_ptr = std::deque<broker::outgoing_connection_status>*;
  auto dd = reinterpret_cast<deque_ptr>(d);
  dd->erase(dd->begin() + idx);
}

int broker_outgoing_connection_status_queue_fd(
  const broker_outgoing_connection_status_queue* q) {
  using queue_ptr = const broker::outgoing_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  return qq->fd();
}

broker_deque_of_outgoing_connection_status*
broker_outgoing_connection_status_queue_want_pop(
  const broker_outgoing_connection_status_queue* q) {
  auto rval = new (nothrow) std::deque<broker::outgoing_connection_status>;
  if (!rval)
    return nullptr;
  using queue_ptr = const broker::outgoing_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  *rval = qq->want_pop();
  return reinterpret_cast<broker_deque_of_outgoing_connection_status*>(rval);
}

broker_deque_of_outgoing_connection_status*
broker_outgoing_connection_status_queue_need_pop(
  const broker_outgoing_connection_status_queue* q) {
  auto rval = new (nothrow) std::deque<broker::outgoing_connection_status>;
  if (!rval)
    return nullptr;
  using queue_ptr = const broker::outgoing_connection_status_queue*;
  auto qq = reinterpret_cast<queue_ptr>(q);
  *rval = qq->need_pop();
  return reinterpret_cast<broker_deque_of_outgoing_connection_status*>(rval);
}

broker_endpoint* broker_endpoint_create(const char* name) {
  try {
    auto rval = new broker::endpoint(name);
    return reinterpret_cast<broker_endpoint*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

broker_endpoint* broker_endpoint_create_with_flags(const char* name,
                                                   int flags) {
  try {
    auto rval = new broker::endpoint(name, flags);
    return reinterpret_cast<broker_endpoint*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

void broker_endpoint_delete(broker_endpoint* e) {
  delete reinterpret_cast<broker::endpoint*>(e);
}

const char* broker_endpoint_name(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return ee->name().data();
}

int broker_endpoint_flags(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return ee->flags();
}

void broker_endpoint_set_flags(broker_endpoint* e, int flags) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  return ee->set_flags(flags);
}

int broker_endpoint_last_errno(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return ee->last_errno();
}

const char* broker_endpoint_last_error(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return ee->last_error().data();
}

int broker_endpoint_listen(broker_endpoint* e, uint16_t port, const char* addr,
                           int reuse_addr) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  return ee->listen(port, addr, reuse_addr);
}

broker_peering* broker_endpoint_peer_remotely(broker_endpoint* e,
                                              const char* addr, uint16_t port,
                                              double retry_interval) {
  auto rval = new (nothrow) broker::peering;
  if (!rval)
    return nullptr;
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto retry = std::chrono::duration<double>(retry_interval);
  try {
    *rval = ee->peer(addr, port, retry);
  } catch (std::bad_alloc&) {
    delete rval;
    return nullptr;
  }
  return reinterpret_cast<broker_peering*>(rval);
}

broker_peering* broker_endpoint_peer_locally(broker_endpoint* self,
                                             const broker_endpoint* other) {
  auto rval = new (nothrow) broker::peering;
  if (!rval)
    return nullptr;
  auto s = reinterpret_cast<broker::endpoint*>(self);
  auto o = reinterpret_cast<const broker::endpoint*>(other);
  *rval = s->peer(*o);
  return reinterpret_cast<broker_peering*>(rval);
}

int broker_endpoint_unpeer(broker_endpoint* e, const broker_peering* p) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto pp = reinterpret_cast<const broker::peering*>(p);
  return ee->unpeer(*pp);
}

const broker_outgoing_connection_status_queue*
broker_endpoint_outgoing_connection_status(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return reinterpret_cast<const broker_outgoing_connection_status_queue*>(
    &ee->outgoing_connection_status());
}

const broker_incoming_connection_status_queue*
broker_endpoint_incoming_connection_status(const broker_endpoint* e) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  return reinterpret_cast<const broker_incoming_connection_status_queue*>(
    &ee->incoming_connection_status());
}

int broker_endpoint_send(broker_endpoint* e, const broker_string* topic,
                         const broker_message* msg) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  auto mm = reinterpret_cast<const broker::message*>(msg);
  try {
    ee->send(*tt, *mm);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}

int broker_endpoint_send_with_flags(broker_endpoint* e,
                                    const broker_string* topic,
                                    const broker_message* msg, int flags) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  auto mm = reinterpret_cast<const broker::message*>(msg);
  try {
    ee->send(*tt, *mm, flags);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}

int broker_endpoint_publish(broker_endpoint* e, const broker_string* topic) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  try {
    ee->publish(*tt);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}

int broker_endpoint_unpublish(broker_endpoint* e, const broker_string* topic) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  try {
    ee->unpublish(*tt);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}

int broker_endpoint_advertise(broker_endpoint* e, const broker_string* topic) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  try {
    ee->advertise(*tt);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}

int broker_endpoint_unadvertise(broker_endpoint* e,
                                const broker_string* topic) {
  auto ee = reinterpret_cast<broker::endpoint*>(e);
  auto tt = reinterpret_cast<const std::string*>(topic);
  try {
    ee->unadvertise(*tt);
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}
