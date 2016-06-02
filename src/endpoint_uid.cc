#include "broker/endpoint_uid.hh"

namespace broker {


endpoint_uid::endpoint_uid(node_id nid, endpoint_id eid,
                           optional<network_info> ni)
  : node{nid},
    endpoint{eid},
    network{std::move(ni)} {
}

bool operator==(const endpoint_uid& x, const endpoint_uid& y) {
  return x.node == y.node && x.endpoint == y.endpoint;
}

bool operator!=(const endpoint_uid& x, const endpoint_uid& y) {
  return !(x == y);
}

} // namespace broker
