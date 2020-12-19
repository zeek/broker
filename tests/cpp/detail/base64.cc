#define SUITE detail.base64

#include "broker/detail/base64.hh"

#include "test.hh"

using namespace broker;

namespace {

template <class... Ts>
auto bytes(Ts... xs) {
  using byte = typename detail::blob_buffer::value_type;
  detail::blob_buffer buf{static_cast<byte>(xs)...};
  return buf;
}

template <class... Ts>
auto encode(Ts... xs) {
  auto buf = bytes(xs...);
  std::string result;
  detail::base64::encode(buf, result);
  return result;
}

auto decode(std::string_view str) {
  detail::blob_buffer result;
  detail::base64::decode(str, result);
  return result;
}

} // namespace

TEST(base64 encoding converts byte sequences to strings) {
  CHECK_EQUAL(encode(0xb3, 0x7a, 0x4f, 0x2c, 0xc0, 0x62, 0x4f, 0x16, 0x90, 0xf6,
                     0x46, 0x06, 0xcf, 0x38, 0x59, 0x45, 0xb2, 0xbe, 0xc4,
                     0xea),
              "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

TEST(base64 decoding converts strings to byte sequences) {
  CHECK_EQUAL(decode("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
              bytes(0xb3, 0x7a, 0x4f, 0x2c, 0xc0, 0x62, 0x4f, 0x16, 0x90, 0xf6,
                    0x46, 0x06, 0xcf, 0x38, 0x59, 0x45, 0xb2, 0xbe, 0xc4,
                    0xea));
}
