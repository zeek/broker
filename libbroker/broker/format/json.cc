#include "broker/format/json.hh"

#include "broker/defaults.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/internal/json.hh"
#include "broker/variant.hh"

#include <caf/expected.hpp>
#include <caf/json_object.hpp>
#include <caf/json_value.hpp>

#include <cassert>
#include <chrono>
#include <ctime>

namespace broker::format::json::v1 {

size_t encode_to_buf(timestamp value, std::array<char, 32>& buf) {
  namespace sc = std::chrono;
  using clock_type = sc::system_clock;
  using clock_timestamp = typename clock_type::time_point;
  using clock_duration = typename clock_type::duration;
  auto tse = value.time_since_epoch();
  clock_timestamp as_sys_time{sc::duration_cast<clock_duration>(tse)};
  auto secs = clock_type::to_time_t(as_sys_time);
  auto msecs = sc::duration_cast<sc::milliseconds>(tse).count() % 1000;
  // We print in ISO 8601 format, e.g., "2020-09-01T15:58:42.372". 32-Bytes are
  // more than enough space.
  tm time_buf;
#ifdef _MSC_VER
  localtime_s(&time_buf, &secs);
#else
  localtime_r(&secs, &time_buf);
#endif
  buf[0] = '"';
  auto pos = strftime(buf.data() + 1, buf.size() - 1, "%FT%T", &time_buf) + 1;
  buf[pos++] = '.';
  if (msecs > 0) {
    assert(msecs < 1000);
    buf[pos++] = static_cast<char>((msecs / 100) + '0');
    buf[pos++] = static_cast<char>(((msecs % 100) / 10) + '0');
    buf[pos++] = static_cast<char>((msecs % 10) + '0');
  } else {
    for (int i = 0; i < 3; ++i)
      buf[pos++] = '0';
  }
  buf[pos++] = '"';
  buf[pos] = '\0';
  return pos;
}

error decode(std::string_view str, variant& result) {
  // Parse the JSON text into a JSON object.
  auto val = caf::json_value::parse_shallow(str);
  if (!val)
    return error{ec::invalid_json};
  auto obj = val->to_object();

  std::string_view topic = topic::reserved;
  if (auto maybe_topic = obj.value("topic"); maybe_topic.is_string())
    topic = std::string_view{maybe_topic.to_string().data(),
                             maybe_topic.to_string().size()};

  // Try to convert the JSON structure into our binary serialization format.
  std::vector<std::byte> buf;
  buf.reserve(512); // Allocate some memory to avoid small allocations.
  if (auto err = internal::json::data_message_to_binary(obj, buf))
    return err;
  // Turn the binary data into a data envelope. TTL and sender/receiver are
  // not part of the JSON representation, so we use defaults values.
  auto res = data_envelope::deserialize(endpoint_id::nil(), endpoint_id::nil(),
                                        defaults::ttl, topic, buf.data(),
                                        buf.size());
  if (!res)
    return res.error();
  result = (*res)->value();
  return {};
}

} // namespace broker::format::json::v1
