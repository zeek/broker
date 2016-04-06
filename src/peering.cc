#include <iostream>
#include <caf/scoped_actor.hpp>

#include "broker/peering.hh"
#include "broker/util/hash.hh"

namespace broker {

peering::peering(caf::actor ea, caf::actor pa, bool r,
                 std::pair<std::string, uint16_t> rt)
  : endpoint_actor_{std::move(ea)},
    peer_actor_{std::move(pa)},
    remote_{r},
    remote_tuple_{std::move(rt)} {
}

peering::operator bool() const {
  return peer_actor_;
}

bool peering::remote() const {
  return remote_;
}

const caf::actor& peering::endpoint_actor() const {
  return endpoint_actor_;
}

const caf::actor& peering::peer_actor() const {
  return peer_actor_;
}

const std::pair<std::string, uint16_t>& peering::remote_tuple() const {
  return remote_tuple_;
}

bool operator==(const peering& lhs, const peering& rhs) {
  return lhs.endpoint_actor_ == rhs.endpoint_actor_
      && lhs.peer_actor_ == rhs.peer_actor_
      && lhs.remote_ == rhs.remote_
      && lhs.remote_tuple_ == rhs.remote_tuple_;
}

} // namespace broker

size_t std::hash<broker::peering>::operator()(const broker::peering& p) const {
  size_t rval = 0;
  broker::util::hash_combine(rval, p.endpoint_actor());
  broker::util::hash_combine(rval, p.peer_actor());
  return rval;
}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_peering* broker_peering_create() {
  return reinterpret_cast<broker_peering*>(new (nothrow) broker::peering());
}

void broker_peering_delete(broker_peering* p) {
  delete reinterpret_cast<broker::peering*>(p);
}

broker_peering* broker_peering_copy(const broker_peering* p) {
  auto pp = reinterpret_cast<const broker::peering*>(p);
  auto rval = new (nothrow) broker::peering(*pp);
  return reinterpret_cast<broker_peering*>(rval);
}

int broker_peering_is_remote(const broker_peering* p) {
  auto pp = reinterpret_cast<const broker::peering*>(p);
  return pp->remote();
}

const char* broker_peering_remote_host(const broker_peering* p) {
  auto pp = reinterpret_cast<const broker::peering*>(p);
  return pp->remote_tuple().first.data();
}

uint16_t broker_peering_remote_port(const broker_peering* p) {
  auto pp = reinterpret_cast<const broker::peering*>(p);
  return pp->remote_tuple().second;
}

int broker_peering_is_initialized(const broker_peering* p) {
  auto pp = reinterpret_cast<const broker::peering*>(p);
  bool rval(*pp);
  return rval;
}

int broker_peering_eq(const broker_peering* a, const broker_peering* b) {
  auto aa = reinterpret_cast<const broker::peering*>(a);
  auto bb = reinterpret_cast<const broker::peering*>(b);
  return *aa == *bb;
}

size_t broker_peering_hash(const broker_peering* a) {
  auto aa = reinterpret_cast<const broker::peering*>(a);
  return std::hash<broker::peering>{}(*aa);
}
