#include "broker/format/bin.hh"

#include "broker/broker-test.test.hh"

#include "broker/internal_command.hh"

using namespace broker;
using namespace std::literals;

namespace {

template <class T>
auto apply_serialize(const T& value) {
  std::vector<caf::byte> buf;
  caf::binary_serializer sink{nullptr, buf};
  if (!sink.apply(value))
    FAIL("serialization failed");
  return buf;
}

template <class Out = caf::byte, class T>
auto apply_encoder(const T& value) {
  std::vector<Out> buf;
  format::bin::v1::encoder sink{std::back_inserter(buf)};
  if (!sink.apply(value))
    FAIL("serialization failed");
  return buf;
}

template <class T>
auto do_encode(const T& value) {
  std::vector<caf::byte> buf;
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

namespace {

struct dummy_decoder_handler {
  int indent = 0;
  std::string log;

  void value(none) {
    log.insert(log.end(), indent, ' ');
    log += "value: none\n";
  }

  void value(bool arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += arg ? "true" : "false";
    log += "\n";
  }

  void value(broker::count arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += std::to_string(arg);
    log += " [count]\n";
  }

  void value(broker::integer arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += std::to_string(arg);
    log += " [integer]\n";
  }

  void value(broker::real arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += std::to_string(arg);
    // Drop trailing zeros.
    log.erase(log.find_last_not_of('0') + 1);
    // Drop trailing dot.
    if (log.back() == '.')
      log.pop_back();
    log += " [real]\n";
  }

  void value(std::string_view arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += arg;
    log += "\n";
  }

  void value(enum_value_view arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += arg.name;
    log += " [enum]\n";
  }

  void value(address arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += broker::to_string(arg);
    log += "\n";
  }

  void value(subnet arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += broker::to_string(arg);
    log += "\n";
  }

  void value(port arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += broker::to_string(arg);
    log += "\n";
  }

  void value(timespan arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += std::to_string(arg.count());
    log += " [timespan]\n";
  }

  void value(timestamp arg) {
    log.insert(log.end(), indent, ' ');
    log += "value: ";
    log += std::to_string(arg.time_since_epoch().count());
    log += " [timestamp]\n";
  }

  dummy_decoder_handler& begin_list() {
    log.insert(log.end(), indent, ' ');
    log += "begin list\n";
    indent += 2;
    return *this;
  }

  void end_list(dummy_decoder_handler&) {
    indent -= 2;
    log.insert(log.end(), indent, ' ');
    log += "end list\n";
  }

  dummy_decoder_handler& begin_set() {
    log.insert(log.end(), indent, ' ');
    log += "begin set\n";
    indent += 2;
    return *this;
  }

  void end_set(dummy_decoder_handler&) {
    indent -= 2;
    log.insert(log.end(), indent, ' ');
    log += "end set\n";
  }

  dummy_decoder_handler& begin_table() {
    log.insert(log.end(), indent, ' ');
    log += "begin table\n";
    indent += 2;
    return *this;
  }

  void end_table(dummy_decoder_handler&) {
    indent -= 2;
    log.insert(log.end(), indent, ' ');
    log += "end table\n";
  }

  void begin_key_value_pair() {
    log.insert(log.end(), indent, ' ');
    log += "begin key-value-pair\n";
    indent += 2;
  }

  void end_key_value_pair() {
    indent -= 2;
    log.insert(log.end(), indent, ' ');
    log += "end key-value-pair\n";
  }
};

template <class T>
std::string do_decode(T&& arg) {
  auto input = data{std::forward<T>(arg)};
  auto buf = apply_encoder<std::byte>(input);
  dummy_decoder_handler handler;
  auto [ok, pos] = format::bin::v1::decode(buf.data(), buf.data() + buf.size(),
                                           handler);
  if (!ok) {
    FAIL("decoding failed at position "s + std::to_string(pos - buf.data()));
  }
  if (pos != buf.data() + buf.size()) {
    FAIL("decoding did not consume the entire input");
  }
  return std::move(handler.log);
}

} // namespace

TEST(decode produces a series of events) {
  CHECK_EQ(do_decode(data{}), "value: none\n");
  CHECK_EQ(do_decode(true), "value: true\n");
  CHECK_EQ(do_decode(count{42}), "value: 42 [count]\n");
  CHECK_EQ(do_decode(integer{42}), "value: 42 [integer]\n");
  CHECK_EQ(do_decode(real{1.2}), "value: 1.2 [real]\n");
  CHECK_EQ(do_decode("FooBar"s), "value: FooBar\n");
  CHECK_EQ(do_decode(addr("192.168.9.8")), "value: 192.168.9.8\n");
  CHECK_EQ(do_decode(snet("192.168.9.0/24")), "value: 192.168.9.0/24\n");
  CHECK_EQ(do_decode(port(8080, port::protocol::tcp)), "value: 8080/tcp\n");
  CHECK_EQ(do_decode(timestamp{timespan{12345}}), "value: 12345 [timestamp]\n");
  CHECK_EQ(do_decode(timespan{12345}), "value: 12345 [timespan]\n");
  CHECK_EQ(do_decode(enum_value{"FooBar"}), "value: FooBar [enum]\n");
  CHECK_EQ(do_decode(set{}), // empty set
           "begin set\n"
           "end set\n");
  CHECK_EQ(do_decode(set{data{count{1}}, data{count{2}}}),
           "begin set\n"
           "  value: 1 [count]\n"
           "  value: 2 [count]\n"
           "end set\n");
  CHECK_EQ(do_decode(vector{}), // empty list
           "begin list\n"
           "end list\n");
  CHECK_EQ(do_decode(vector{data{count{1}}, data{count{2}}}),
           "begin list\n"
           "  value: 1 [count]\n"
           "  value: 2 [count]\n"
           "end list\n");
  CHECK_EQ(do_decode(table{}), // empty table
           "begin table\n"
           "end table\n");
  CHECK_EQ(do_decode(table{
             {data{"a"}, data{count{1}}},
             {data{"b"}, data{count{2}}},
           }),
           "begin table\n"
           "  begin key-value-pair\n"
           "    value: a\n"
           "    value: 1 [count]\n"
           "  end key-value-pair\n"
           "  begin key-value-pair\n"
           "    value: b\n"
           "    value: 2 [count]\n"
           "  end key-value-pair\n"
           "end table\n");
}

namespace {

template <class T>
T apply_decoder(const std::vector<std::byte>& buf) {
  auto first = buf.data();
  auto last = first + buf.size();
  format::bin::v1::decoder decoder{first, last};
  auto result = T{};
  if (!decoder.apply(result)) {
    FAIL("decoding failed");
  }
  return result;
}

template <class T>
T roundtrip(const T& value) {
  return apply_decoder<T>(apply_encoder<std::byte>(value));
}

template <class T, class... Ts>
auto make_array(T value, Ts... values) {
  static_assert((std::is_same_v<T, Ts> && ...), "all types must be the same");
  return std::array<T, sizeof...(Ts) + 1>{value, values...};
}

template <class T, class... Ts>
auto make_vector(T value, Ts... values) {
  static_assert((std::is_same_v<T, Ts> && ...), "all types must be the same");
  return std::vector{value, values...};
}

auto make_dict(
  std::initializer_list<std::pair<std::string, std::string>> pairs) {
  std::map<std::string, std::string> result;
  for (auto& [key, value] : pairs)
    result.emplace(key, value);
  return result;
}

} // namespace

TEST(the decoder can deserialize the output of the encoder) {
  // Check primitive types.
  CHECK_EQ(roundtrip(true), true);
  CHECK_EQ(roundtrip(false), false);
  CHECK_EQ(roundtrip(int8_t{-7}), -7);
  CHECK_EQ(roundtrip(int16_t{-518}), -518);
  CHECK_EQ(roundtrip(int32_t{-84'777}), -84'777);
  CHECK_EQ(roundtrip(int64_t{-4'937'239ll}), -4'937'239ll);
  CHECK_EQ(roundtrip(uint8_t{7}), 7);
  CHECK_EQ(roundtrip(uint16_t{518}), 518u);
  CHECK_EQ(roundtrip(uint32_t{84'777}), 84'777u);
  CHECK_EQ(roundtrip(uint64_t{4'937'239ull}), 4'937'239ull);
  CHECK_EQ(roundtrip(13.2), 13.2);
  CHECK_EQ(roundtrip("hello"s), "hello");
  // Check optional and variant types.
  using int_or_str = std::variant<int, std::string>;
  CHECK_EQ(roundtrip(std::optional<int32_t>{}), std::nullopt);
  CHECK_EQ(roundtrip(std::optional<int32_t>{123}), 123);
  CHECK_EQ(roundtrip(int_or_str{42}), int_or_str{42});
  CHECK_EQ(roundtrip(int_or_str{"foo"s}), int_or_str{"foo"s});
  // Check container types.
  CHECK_EQ(roundtrip(make_array(1, 2, 3)), make_array(1, 2, 3));
  CHECK_EQ(roundtrip(make_vector(1, 2, 3)), make_vector(1, 2, 3));
  CHECK_EQ(roundtrip(make_dict({{"foo", "bar"}})), make_dict({{"foo", "bar"}}));
  // Check types with custom serialization.
  CHECK_EQ(roundtrip(endpoint_id::random(0xF00)), endpoint_id::random(0xF00));
  CHECK_EQ(roundtrip(broker::data{true}), broker::data{true});
  CHECK_EQ(roundtrip(broker::data{false}), broker::data{false});
  CHECK_EQ(roundtrip(broker::data{"hello"s}), broker::data{"hello"s});
  auto cmd = internal_command{123,
                              {endpoint_id::random(1), 2},
                              {endpoint_id::random(3), 4},
                              cumulative_ack_command{42}};
  auto cpy = roundtrip(cmd);
  CHECK_EQ(cpy.seq, cmd.seq);
  CHECK_EQ(cpy.sender, cmd.sender);
  CHECK_EQ(cpy.receiver, cmd.receiver);
  if (CHECK(std::holds_alternative<cumulative_ack_command>(cpy.content))) {
    CHECK_EQ(std::get<cumulative_ack_command>(cpy.content).seq, 42);
  }
}
