#include "broker/format/bin.hh"

#include "broker/broker-test.test.hh"

#include "broker/internal_command.hh"

using namespace broker;
using namespace std::literals;

namespace {

template <class T>
auto apply_serialize(const T& value) {
  std::vector<std::byte> buf;
  caf::binary_serializer sink{nullptr, buf};
  if (!sink.apply(value))
    FAIL("serialization failed");
  return buf;
}

template <class T>
auto apply_encoder(const T& value) {
  std::vector<std::byte> buf;
  format::bin::v1::encoder sink{std::back_inserter(buf)};
  if (!sink.apply(value))
    FAIL("serialization failed");
  return buf;
}

template <class T>
auto do_encode(const T& value) {
  std::vector<std::byte> buf;
  format::bin::v1::encode(value, std::back_inserter(buf));
  return buf;
}

address addr(const std::string& str) {
  address result;
  if (!convert(str, result))
    FAIL("conversion to address failed for " << str);
  return result;
}

subnet snet(const std::string& str) {
  subnet result;
  if (!convert(str, result))
    FAIL("conversion to subnet failed for " << str);
  return result;
}

} // namespace

#define CHECK_EQ_ENCODE_FUN(stmt)                                              \
  {                                                                            \
    auto val = stmt;                                                           \
    CHECK_EQ(do_encode(val), apply_serialize(val));                            \
  }                                                                            \
  static_cast<void>(0)

TEST(encode emits the same output for data as a binary serializer) {
  CHECK_EQ_ENCODE_FUN(data{});
  CHECK_EQ_ENCODE_FUN(data{nil});
  CHECK_EQ_ENCODE_FUN(data{true});
  CHECK_EQ_ENCODE_FUN(data{false});
  CHECK_EQ_ENCODE_FUN(data{count{0}});
  CHECK_EQ_ENCODE_FUN(data{count{1234567890}});
  CHECK_EQ_ENCODE_FUN(data{integer{0}});
  CHECK_EQ_ENCODE_FUN(data{integer{1234567890}});
  CHECK_EQ_ENCODE_FUN(data{integer{-1234567890}});
  CHECK_EQ_ENCODE_FUN(data{1.0});
  CHECK_EQ_ENCODE_FUN(data{-1.0});
  CHECK_EQ_ENCODE_FUN(data{"hello world"s});
  CHECK_EQ_ENCODE_FUN(data{addr("192.168.9.8")});
  CHECK_EQ_ENCODE_FUN(data{snet("192.168.9.8/24")});
  CHECK_EQ_ENCODE_FUN(data{port(8080, port::protocol::tcp)});
  CHECK_EQ_ENCODE_FUN(data{port(9000, port::protocol::udp)});
  CHECK_EQ_ENCODE_FUN(data{timestamp{timespan{1234567890}}});
  CHECK_EQ_ENCODE_FUN(data{timespan{1234567890}});
  CHECK_EQ_ENCODE_FUN(data{enum_value{"foobar"}});
  CHECK_EQ_ENCODE_FUN(data(set{false, count{1}, "hello world"s}));
  CHECK_EQ_ENCODE_FUN(
    data(table{{count{1}, "hello"s}, {count{2}, {"world"s}}}));
  CHECK_EQ_ENCODE_FUN(data(vector{false, count{1}, "hello world"s}));
}

#define CHECK_EQ_ENCODE_OBJ(stmt)                                              \
  {                                                                            \
    auto val = stmt;                                                           \
    CHECK_EQ(apply_encoder(val), apply_serialize(val));                        \
  }                                                                            \
  static_cast<void>(0)

TEST(the encoder emits the same output for data as a binary serializer) {
  auto eid = endpoint_id::random(0xF00BA2);
  using i32_or_string = std::variant<int32_t, std::string>;
  CHECK_EQ_ENCODE_OBJ(data{});
  CHECK_EQ_ENCODE_OBJ(data{nil});
  CHECK_EQ_ENCODE_OBJ(data{true});
  CHECK_EQ_ENCODE_OBJ(data{false});
  CHECK_EQ_ENCODE_OBJ(data{count{0}});
  CHECK_EQ_ENCODE_OBJ(data{count{1234567890}});
  CHECK_EQ_ENCODE_OBJ(data{integer{0}});
  CHECK_EQ_ENCODE_OBJ(data{integer{1234567890}});
  CHECK_EQ_ENCODE_OBJ(data{integer{-1234567890}});
  CHECK_EQ_ENCODE_OBJ(data{1.0});
  CHECK_EQ_ENCODE_OBJ(data{-1.0});
  CHECK_EQ_ENCODE_OBJ(data{"hello world"s});
  CHECK_EQ_ENCODE_OBJ(data{addr("192.168.9.8")});
  CHECK_EQ_ENCODE_OBJ(data{snet("192.168.9.8/24")});
  CHECK_EQ_ENCODE_OBJ(data{port(8080, port::protocol::tcp)});
  CHECK_EQ_ENCODE_OBJ(data{port(9000, port::protocol::udp)});
  CHECK_EQ_ENCODE_OBJ(data{timestamp{timespan{1234567890}}});
  CHECK_EQ_ENCODE_OBJ(data{timespan{1234567890}});
  CHECK_EQ_ENCODE_OBJ(data{enum_value{"foobar"}});
  CHECK_EQ_ENCODE_OBJ(data(set{false, count{1}, "hello world"s}));
  CHECK_EQ_ENCODE_OBJ(
    data(table{{count{1}, "hello"s}, {count{2}, {"world"s}}}));
  CHECK_EQ_ENCODE_OBJ(data(vector{false, count{1}, "hello world"s}));
  CHECK_EQ_ENCODE_OBJ(i32_or_string{42});
  CHECK_EQ_ENCODE_OBJ(i32_or_string{"hello world"s});
  CHECK_EQ_ENCODE_OBJ(eid);
  CHECK_EQ_ENCODE_OBJ((put_command{"foo", "bar", std::nullopt, {eid, 42}}));
  CHECK_EQ_ENCODE_OBJ((put_unique_command{
    "foo", "bar", timespan{2500}, {eid, 42}, 12345, {eid, 23}}));
  CHECK_EQ_ENCODE_OBJ(
    (put_unique_result_command{true, {eid, 42}, 12345, {eid, 23}}));
  CHECK_EQ_ENCODE_OBJ((erase_command{"foo", {eid, 42}}));
  CHECK_EQ_ENCODE_OBJ((expire_command{"foo", {eid, 42}}));
  CHECK_EQ_ENCODE_OBJ(
    (add_command{"foo", 1, data::type::integer, std::nullopt, {eid, 42}}));
  CHECK_EQ_ENCODE_OBJ((subtract_command{"foo", 1, std::nullopt, {eid, 42}}));
  CHECK_EQ_ENCODE_OBJ((clear_command{{eid, 42}}));
  CHECK_EQ_ENCODE_OBJ((attach_writer_command{111, 222}));
  CHECK_EQ_ENCODE_OBJ((ack_clone_command{111, 222, {}}));
  CHECK_EQ_ENCODE_OBJ((cumulative_ack_command{111}));
  CHECK_EQ_ENCODE_OBJ((nack_command{{111, 222, 333}}));
  CHECK_EQ_ENCODE_OBJ((keepalive_command{42}));
  CHECK_EQ_ENCODE_OBJ((retransmit_failed_command{42}));
  CHECK_EQ_ENCODE_OBJ((internal_command{
    123, {eid, 1}, {eid, 2}, {retransmit_failed_command{42}}}));
  CHECK_EQ_ENCODE_OBJ(network_info("192.168.9.2", 8080, timeout::seconds{1}));
}
