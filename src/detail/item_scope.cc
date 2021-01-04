#include "broker/detail/item_scope.hh"

namespace broker::detail {

namespace {

constexpr const char* item_scope_strings[] = {
  "global",
  "local",
  "remote",
};

} // namespace

std::string to_string(item_scope x) {
  return item_scope_strings[static_cast<uint8_t>(x)];
}

} // namespace broker::detail
