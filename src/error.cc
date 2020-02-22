#include "broker/error.hh"

#include "broker/detail/assert.hh"

namespace broker {

namespace {

const char* ec_names[] = {
  "none",
  "unspecified",
  "peer_incompatible",
  "peer_invalid",
  "peer_unavailable",
  "peer_timeout",
  "master_exists",
  "no_such_master",
  "no_such_key",
  "request_timeout",
  "type_clash",
  "invalid_data",
  "backend_failure",
  "stale_data",
  "cannot_open_file",
  "cannot_write_file",
  "invalid_topic_key",
  "end_of_file",
  "invalid_tag",
};

} // namespace

const char* to_string(ec code) {
  auto index = static_cast<uint8_t>(code);
  BROKER_ASSERT(index < sizeof(ec_names));
  return ec_names[index];
}

bool convert(const std::string& str, ec& code) {
  auto predicate = [&](const char* cstr) { return cstr == str; };
  auto begin = std::begin(ec_names);
  auto end = std::end(ec_names);
  auto i = std::find_if(begin, end, predicate);
  if (i == begin || i == end)
    return false;
  code = static_cast<ec>(std::distance(begin, i));
  return true;
}

} // namespace broker
