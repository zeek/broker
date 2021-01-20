#include "broker/peer_status.hh"

#include <array>

#include "broker/convert.hh"

namespace broker {

namespace {

using namespace std::literals::string_view_literals;

constexpr auto peer_status_strings = std::array{
  "initialized"sv,
  "connecting"sv,
  "connected"sv,
  "peered"sv,
  "disconnected"sv,
  "reconnecting"sv,
};

using peer_status_converter_t
  = detail::enum_converter<peer_status, decltype(peer_status_strings)>;

constexpr peer_status_converter_t peer_status_converter
  = peer_status_converter_t{&peer_status_strings};

} // namespace

bool convert(peer_status src, peer_status_ut& dst) noexcept {
  return peer_status_converter(src, dst);
}

bool convert(peer_status src, std::string& dst) {
  return peer_status_converter(src, dst);
}

bool convert(peer_status_ut src, peer_status& dst) noexcept {
  return peer_status_converter(src, dst);
}

bool convert(const std::string& src, peer_status& dst) noexcept {
  return peer_status_converter(src, dst);
}

std::string to_string(peer_status code) {
  std::string result;
  convert(code, result);
  return result;
}

} // namespace broker
