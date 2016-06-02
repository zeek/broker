#include "broker/status.hh"

namespace broker {

const char* to_string(status_info info) {
  switch (info) {
    case unknown_status:
      return "<unknown>";
    case peer_added:
      return "peer_added";
    case peer_removed:
      return "peer_removed";
    case peer_incompatible:
      return "peer_incompatible";
    case peer_invalid:
      return "peer_invalid";
    case peer_unavailable:
      return "peer_unavailable";
    case peer_lost:
      return "peer_lost";
    case peer_recovered:
      return "peer_recovered";
  }
}

status::status(status_info info) : info{info} {
}

status::status(status_info info, endpoint_uid local, endpoint_uid remote,
               std::string message)
  : info{info},
    local{local},
    remote{remote},
    message{std::move(message)} {
}

bool operator==(const status& x, status_info y) {
  return static_cast<uint8_t>(x.info) == y;
}

bool operator==(status_info x, const status& y) {
  return y == x;
}

} // namespace broker
