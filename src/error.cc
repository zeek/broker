#include "broker/error.hh"

#include "broker/detail/assert.hh"

namespace broker {

const char* to_string(ec x) {
  switch (x) {
    default:
      BROKER_ASSERT(!"missing to_string implementation");
      return "<unknown>";
    case ec::unspecified:
      return "<unspecified>";
    case ec::version_incompatible:
      return "version_incompatible";
    case ec::master_exists:
      return "master_exists";
    case ec::no_such_master:
      return "no_such_master";
    case ec::no_such_key:
      return "no_such_key";
    case ec::type_clash:
      return "type_clash";
    case ec::invalid_data:
      return "invalid_data";
    case ec::backend_failure:
      return "backend_failure";
  }
};

} // namespace broker
