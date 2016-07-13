#include "broker/error.hh"

namespace broker {

namespace {

const char* descriptions[] = {
  "<unspecified>",
  "version_incompatible",
  "master_exists",
  "no_such_master",
  "no_such_key",
};

} // namespace <anonymous>

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  if (index > static_cast<size_t>(ec::no_such_key))
    return "<unknown>";
  return descriptions[index];
}

} // namespace broker
