#include "broker//shutdown_options.hh"

namespace {

static constexpr const char* shutdown_options_strings[] = {
  "nullopt",
  "await_stores_on_shutdown",
};

void append(std::string& result, broker::shutdown_options::flag flag) {
  if (result.back() != '(')
    result += ", ";
  result += shutdown_options_strings[static_cast<uint8_t>(flag)];
}

} // namespace

namespace broker {

std::string to_string(shutdown_options options) {
  std::string result = "shutdown_options(";
  for (auto flag : {shutdown_options::await_stores_on_shutdown})
    if (options.contains(flag))
      append(result, flag);
  result += ')';
  return result;
}

} // namespace broker
