#include "broker/backend.hh"

#include <array>

#include "broker/convert.hh"

namespace broker {

namespace {

using namespace std::literals::string_view_literals;

constexpr auto backend_strings = std::array{
  "memory"sv,
  "sqlite"sv,
};

using backend_converter_t
  = detail::enum_converter<backend, decltype(backend_strings)>;

constexpr backend_converter_t backend_converter
  = backend_converter_t{&backend_strings};

} // namespace

bool convert(backend src, backend_ut& dst) noexcept {
  return backend_converter(src, dst);
}

bool convert(backend src, std::string& dst) {
  return backend_converter(src, dst);
}

bool convert(backend_ut src, backend& dst) noexcept {
  return backend_converter(src, dst);
}

bool convert(const std::string& src, backend& dst) noexcept {
  return backend_converter(src, dst);
}

std::string to_string(backend code) {
  std::string result;
  convert(code, result);
  return result;
}

} // namespace broker
