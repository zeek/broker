#include "broker/error.hh"

#include "broker/detail/assert.hh"

namespace broker {

const char* to_string(ec code) {
  switch (code) {
    default:
      BROKER_ASSERT(!"missing to_string implementation");
      return "<unknown>";
    case ec::unspecified:
      return "<unknown>";
    case ec::peer_incompatible:
      return "peer_incompatible";
    case ec::peer_invalid:
      return "peer_invalid";
    case ec::peer_unavailable:
      return "peer_unavailable";
    case ec::peer_timeout:
      return "peer_timeout";
    case ec::master_exists:
      return "master_exists";
    case ec::no_such_master:
      return "no_such_master";
    case ec::no_such_key:
      return "no_such_key";
    case ec::request_timeout:
      return "request_timeout";
    case ec::type_clash:
      return "type_clash";
    case ec::invalid_data:
      return "invalid_data";
    case ec::backend_failure:
      return "backend_failure";
    case ec::stale_data:
      return "stale_data";
  }
}

} // namespace broker
