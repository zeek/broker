#include "broker/internal/wire_format.hh"

#include "broker/broker-test.test.hh"

#include "broker/command_envelope.hh"
#include "broker/envelope.hh"
#include "broker/ping_envelope.hh"
#include "broker/pong_envelope.hh"
#include "broker/routing_update_envelope.hh"

using namespace broker;

using namespace std::literals;

namespace {

std::vector<std::byte> hex2bytes(const char* hex, size_t len) {
  // Sanity checks.
  if (len == 0)
    return {};
  if (len % 2 != 0)
    throw std::invalid_argument("hex string has odd length");
  std::vector<std::byte> result;
  while (len > 0) {
    char buf[] = {hex[0], hex[1], '\0'};
    result.push_back(static_cast<std::byte>(strtol(buf, nullptr, 16)));
    hex += 2;
    len -= 2;
  }
  return result;
}

std::vector<std::byte> hex2bytes(std::string_view str) {
  return hex2bytes(str.data(), str.size());
}

// - sender:     5746d8ee-3644-5bd2-9f54-d5a44c26c2a1
// - receiver:   c0551e8d-392b-509b-a47b-ae46f75befac
// - type:       1 (data)
// - ttl:        16
// - topic size: 6
// - topic:      "foobar"
// - data:       "hello world"
constexpr std::string_view data_hex =
  "5746d8ee36445bd29f54d5a44c26c2a1" // sender
  "c0551e8d392b509ba47bae46f75befac" // receiver
  "01"                               // type
  "0010"                             // ttl
  "0006"                             // topic size
  "666f6f626172"                     // topic
  "050b68656c6c6f20776f726c64";      // data (tag 5, size 11, string data)

} // namespace

TEST(the wire format can encode and decode a data envelope) {
  envelope_ptr res;
  internal::wire_format::v1::trait uut;
  auto bytes = hex2bytes(data_hex);
  auto caf_bytes = caf::as_bytes(caf::make_span(bytes));
  auto sender = endpoint_id::from_bytes(bytes.data());
  auto receiver = endpoint_id::from_bytes(bytes.data() + 16);
  MESSAGE("deserialization");
  CHECK(uut.convert(caf_bytes, res));
  if (!CHECK(res != nullptr))
    return;
  CHECK_EQUAL(res->sender(), sender);
  CHECK_EQUAL(res->receiver(), receiver);
  CHECK_EQUAL(res->type(), envelope_type::data);
  CHECK_EQUAL(res->ttl(), 16u);
  CHECK_EQUAL(res->topic(), "foobar");
  CHECK_EQUAL(res->as_data()->value().to_string(), "hello world");
  MESSAGE("serialization");
  std::byte_buffer out;
  CHECK(uut.convert(res, out));
  CHECK_EQ(out, std::byte_buffer(caf_bytes.begin(), caf_bytes.end()));
}
