#include "broker/format/json.hh"

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
  auto pos = strftime(buf.data() + 1, buf.size(), "%FT%T", &time_buf) + 1;
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

} // namespace broker::format::json::v1
