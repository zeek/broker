#define SUITE internal.meta_data_writer

#include "broker/internal/meta_data_writer.hh"

#include "test.hh"

#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "broker/internal/read_value.hh"

using namespace broker;

namespace {

struct fixture {
  caf::binary_serializer::container_type buf;
  caf::binary_serializer sink;
  size_t read_pos;

  fixture() : sink(nullptr, buf), read_pos(0) {
    // nop
  }

  template <class T>
  void push(const T& x) {
    internal::meta_data_writer writer{sink};
    CHECK_EQUAL(writer(x), caf::none);
  }

  template <class T>
  T pull() {
    caf::binary_deserializer source{nullptr, buf.data() + read_pos,
                                    buf.size() - read_pos};
    T result{};
    CHECK_EQUAL(detail::read_value(source, result), caf::none);
    read_pos = static_cast<size_t>(source.current() - buf.data());
    return result;
  }

  bool at_end() const {
    return read_pos == buf.size();
  }
};

} // namespace

CAF_TEST_FIXTURE_SCOPE(meta_data_writer_tests, fixture)

CAF_TEST(default constructed data) {
  push(data{});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::none);
  CHECK(at_end());
}

CAF_TEST(boolean data) {
  push(data{true});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::boolean);
  CHECK(at_end());
}

CAF_TEST(count data) {
  push(data{count{42}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::count);
  CHECK(at_end());
}

CAF_TEST(integer data) {
  push(data{integer{42}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK(at_end());
}

CAF_TEST(real data) {
  push(data{4.2});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::real);
  CHECK(at_end());
}

CAF_TEST(string data) {
  push(data{"hello world"});
  CHECK_EQUAL(buf.size(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 11u);
  CHECK(at_end());
}

CAF_TEST(address data) {
  push(data{address{}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::address);
  CHECK(at_end());
}

CAF_TEST(subnet data) {
  push(data{subnet{address{}, 24}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::subnet);
  CHECK(at_end());
}

CAF_TEST(port data) {
  push(data{port{8080, port::protocol::tcp}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::port);
  CHECK(at_end());
}

CAF_TEST(timestamp data) {
  push(data{timestamp{}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::timestamp);
  CHECK(at_end());
}

CAF_TEST(timespan data) {
  push(data{timespan{}});
  CHECK_EQUAL(buf.size(), 1u);
  CHECK_EQUAL(pull<data::type>(), data::type::timespan);
  CHECK(at_end());
}

CAF_TEST(enum_value data) {
  push(data{enum_value{"foobar"}});
  CHECK_EQUAL(buf.size(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::enum_value);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(set data) {
  set xs;
  xs.emplace(integer{1});
  xs.emplace(integer{2});
  xs.emplace(integer{3});
  push(data{xs});
  CHECK_EQUAL(buf.size(), 8u);
  CHECK_EQUAL(pull<data::type>(), data::type::set);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK(at_end());
}

CAF_TEST(table data) {
  table xs;
  xs.emplace(integer{1}, 2.);
  xs.emplace(integer{2}, "hello world");
  xs.emplace(integer{3}, address{});
  push(data{xs});
  CHECK_EQUAL(buf.size(), 15u);
  CHECK_EQUAL(pull<data::type>(), data::type::table);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::real);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 11u);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::address);
  CHECK(at_end());
}

CAF_TEST(vector data) {
  vector xs;
  xs.emplace_back(integer{42});
  xs.emplace_back(std::string{"hello world"});
  xs.emplace_back(12.34);
  push(data{xs});
  CHECK_EQUAL(buf.size(), 12u);
  CHECK_EQUAL(pull<data::type>(), data::type::vector);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::integer);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 11u);
  CHECK_EQUAL(pull<data::type>(), data::type::real);
  CHECK(at_end());
}

CAF_TEST(put_command) {
  auto cmd = internal_command{
    0, {}, put_command{data{"hello"}, data{"broker"}, std::nullopt}};
  push(cmd);
  CHECK_EQUAL(buf.size(), 11u);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::put_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(put_unique_command) {
  auto cmd =
    internal_command{0,
                     {},
                     put_unique_command{data{"hello"}, data{"broker"},
                                        std::nullopt, entity_id::nil(), 0}};
  push(cmd);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::put_unique_command);
  // We expect meta data for `key`, `value`, and `req_id`.
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(erase_command) {
  auto cmd = internal_command{0, {}, erase_command{data{"foobar"}}};
  push(cmd);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::erase_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(add_command) {
  auto cmd =
    internal_command{0,
                     {},
                     add_command{data{"key"}, data{"value"}, data::type::table,
                                 std::nullopt, entity_id::nil()}};
  push(cmd);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::add_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK(at_end());
}

CAF_TEST(subtract_command) {
  auto cmd = internal_command{
    0, {}, subtract_command{data{"key"}, data{"value"}, std::nullopt}};
  push(cmd);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::subtract_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK(at_end());
}

CAF_TEST(clear_command) {
  auto cmd = internal_command{0, {}, clear_command{}};
  push(cmd);
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::clear_command);
  CHECK(at_end());
}

CAF_TEST_FIXTURE_SCOPE_END()
