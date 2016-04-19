#include "broker/error.hh"

namespace broker {

namespace {

const char* descriptions[] = {
  "<unspecified>",
  "version_incompatible",
};

} // namespace <anonymous>

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  if (index > static_cast<size_t>(ec::version_incompatible))
    return "<unknown>";
  return descriptions[index];
}

} // namespace broker
