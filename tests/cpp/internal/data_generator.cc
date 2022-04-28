#define SUITE internal.data_generator

#include "broker/internal/data_generator.hh"

#include "test.hh"

#include <cctype>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "broker/internal/meta_data_writer.hh"
#include "broker/internal/write_value.hh"

using namespace broker;

namespace {

struct fixture {
  caf::binary_serializer::container_type buf;
  caf::binary_serializer sink;
  size_t read_pos;

  fixture() : sink(nullptr, buf), read_pos(0) {
    // nop
  }

  void add_meta(data::type tag) {
    CHECK_EQUAL(internal::write_value(sink, tag), caf::none);
  }

  void add_meta(data::type tag, uint32_t container_size) {
    CHECK_EQUAL(internal::write_value(sink, tag), caf::none);
    CHECK_EQUAL(internal::write_value(sink, container_size), caf::none);
  }

  data generate() {
    caf::binary_deserializer source{nullptr, buf};
    internal::data_generator generator{source};
    data result;
    CHECK_EQUAL(generator(result), caf::none);
    CHECK_EQUAL(source.remaining(), 0u);
    return result;
  }
};

template <class T>
struct holds {
  bool operator()(const data& x) const noexcept {
    return is<T>(x);
  }
};

} // namespace

FIXTURE_SCOPE(data_generator_tests, fixture)

TEST(no data) {
  add_meta(data::type::none);
  CHECK_EQUAL(generate(), data());
}

TEST(boolean data) {
  add_meta(data::type::boolean);
  auto x = generate();
  CHECK(is<boolean>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(count data) {
  add_meta(data::type::count);
  auto x = generate();
  CHECK(is<count>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(integer data) {
  add_meta(data::type::integer);
  auto x = generate();
  CHECK(is<integer>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(real data) {
  add_meta(data::type::real);
  auto x = generate();
  CHECK(is<real>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(string data) {
  add_meta(data::type::string, 42);
  auto x_data = generate();
  if (!is<std::string>(x_data)) {
    CAF_ERROR("generator did not produce a string");
    return;
  }
  auto& x = get<std::string>(x_data);
  CHECK(std::all_of(x.begin(), x.end(), isprint));
  CHECK_EQUAL(x.size(), 42u);
  CHECK_EQUAL(generate(), x);
}

TEST(address data) {
  add_meta(data::type::address);
  auto x = generate();
  CHECK(is<address>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(subnet data) {
  add_meta(data::type::subnet);
  auto x = generate();
  CHECK(is<subnet>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(port data) {
  add_meta(data::type::port);
  auto x = generate();
  CHECK(is<port>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(timestamp data) {
  add_meta(data::type::timestamp);
  auto x = generate();
  CHECK(is<timestamp>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(timespan data) {
  add_meta(data::type::timespan);
  auto x = generate();
  CHECK(is<timespan>(x));
  CHECK_EQUAL(generate(), x);
}

TEST(enum_value data) {
  add_meta(data::type::enum_value, 42);
  auto x_data = generate();
  if (!is<enum_value>(x_data)) {
    CAF_ERROR("generator did not produce a string");
    return;
  }
  auto& x = get<enum_value>(x_data);
  CHECK(std::all_of(x.name.begin(), x.name.end(), isprint));
  CHECK_EQUAL(x.name.size(), 42u);
  CHECK_EQUAL(generate(), x);
}

TEST(set data) {
  add_meta(data::type::set, 3);
  add_meta(data::type::real);
  add_meta(data::type::string, 8);
  add_meta(data::type::integer);
  auto x_data = generate();
  if (!is<set>(x_data)) {
    CAF_ERROR("generator did not produce a set");
    return;
  }
  auto& x = get<set>(x_data);
  CHECK_EQUAL(x.size(), 3u);
  CHECK(std::any_of(x.begin(), x.end(), holds<integer>{}));
  CHECK(std::any_of(x.begin(), x.end(), holds<real>{}));
  CHECK(std::any_of(x.begin(), x.end(), holds<std::string>{}));
  CHECK(std::none_of(x.begin(), x.end(), holds<set>{}));
  CHECK_EQUAL(generate(), x);
}

TEST(table data) {
  add_meta(data::type::table, 2);
  add_meta(data::type::integer);
  add_meta(data::type::string, 8);
  add_meta(data::type::address);
  add_meta(data::type::real);
  auto x_data = generate();
  if (!is<table>(x_data)) {
    CAF_ERROR("generator did not produce a table");
    return;
  }
  auto& x = get<table>(x_data);
  vector keys;
  vector values;
  for (const auto& kvp : x) {
    keys.emplace_back(kvp.first);
    values.emplace_back(kvp.second);
  }
  CHECK_EQUAL(x.size(), 2u);
  CHECK_EQUAL(keys.size(), 2u);
  CHECK_EQUAL(values.size(), 2u);
  CHECK(std::any_of(keys.begin(), keys.end(), holds<integer>{}));
  CHECK(std::any_of(keys.begin(), keys.end(), holds<address>{}));
  CHECK(std::any_of(values.begin(), values.end(), holds<std::string>{}));
  CHECK(std::any_of(values.begin(), values.end(), holds<real>{}));
  CHECK_EQUAL(generate(), x);
}

TEST(vector data) {
  add_meta(data::type::vector, 3);
  add_meta(data::type::real);
  add_meta(data::type::string, 8);
  add_meta(data::type::integer);
  auto x_data = generate();
  if (!is<vector>(x_data)) {
    CAF_ERROR("generator did not produce a vector");
    return;
  }
  auto& x = get<vector>(x_data);
  if (x.size() != 3) {
    CAF_ERROR("generator did not produce a vector with 3 elements");
    return;
  }
  CHECK(is<real>(x[0]));
  CHECK(is<std::string>(x[1]));
  CHECK(is<integer>(x[2]));
  CHECK_EQUAL(generate(), x);
}

TEST(roundtrip with meta_data_writer) {
  internal::meta_data_writer writer{sink};
  auto x = vector{1, 2, "a", "bc"};
  CHECK_EQUAL(writer(data{x}), caf::none);
  MESSAGE("writer produced " << buf.size() << " Bytes");
  auto y_data = generate();
  if (!is<vector>(y_data)) {
    CAF_ERROR("generator did not produce a vector");
    return;
  }
  auto& y = get<vector>(y_data);
  REQUIRE_EQUAL(x.size(), y.size());
  CHECK(is<integer>(y[0]));
  CHECK(is<integer>(y[1]));
  REQUIRE(is<std::string>(y[2]));
  CHECK_EQUAL(get<std::string>(x[2]).size(), get<std::string>(y[2]).size());
  REQUIRE(is<std::string>(y[3]));
  CHECK_EQUAL(get<std::string>(x[3]).size(), get<std::string>(y[3]).size());
}

FIXTURE_SCOPE_END()
