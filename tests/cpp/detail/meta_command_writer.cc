#define SUITE meta_command_writer

#include "broker/detail/meta_command_writer.hh"

#include "test.hh"

#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "broker/data.hh"

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
  void push(T&& x) {
    internal_command cmd{0, entity_id::nil(), std::forward<T>(x)};
    detail::meta_command_writer writer{sink};
    CHECK_EQUAL(writer(cmd), caf::none);
  }

  template <class T>
  T pull() {
    caf::binary_deserializer source{nullptr, buf.data() + read_pos,
                                    buf.size() - read_pos};
    T result{};
    CHECK_EQUAL(source(result), caf::none);
    read_pos = buf.size() - source.remaining();
    return result;
  }

  bool at_end() const {
    return read_pos == buf.size();
  }
};

} // namespace

CAF_TEST_FIXTURE_SCOPE(meta_command_writer_tests, fixture)

CAF_TEST(put_command) {
  push(put_command{data{"hello"}, data{"broker"}, nil});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::put_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(put_unique_command) {
  push(put_unique_command{data{"hello"}, data{"broker"}, nil, entity_id::nil(),
                          0});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::put_unique_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(erase_command) {
  push(erase_command{data{"foobar"}});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::erase_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 6u);
  CHECK(at_end());
}

CAF_TEST(add_command) {
  push(add_command{data{"key"}, data{"value"}, data::type::table, nil});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::add_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK_EQUAL(pull<data::type>(), data::type::table);
  CHECK(at_end());
}

CAF_TEST(subtract_command) {
  push(subtract_command{data{"key"}, data{"value"}, nil});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::subtract_command);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 3u);
  CHECK_EQUAL(pull<data::type>(), data::type::string);
  CHECK_EQUAL(pull<uint32_t>(), 5u);
  CHECK(at_end());
}

CAF_TEST(clear_command) {
  push(clear_command{});
  CHECK_EQUAL(pull<internal_command::type>(),
              internal_command::type::clear_command);
  CHECK(at_end());
}

CAF_TEST(attach_clone_command) {
}

CAF_TEST(attach_writer_command) {
}

CAF_TEST(keepalive_command) {
}

CAF_TEST(cumulative_ack_command) {
}

CAF_TEST(nack_command) {
}

CAF_TEST(ack_clone_command) {
}

CAF_TEST(retransmit_failed_command) {
}

CAF_TEST_FIXTURE_SCOPE_END()
