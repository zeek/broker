#define SUITE internal.generator_file_writer

#include "broker/internal/generator_file_writer.hh"

#include "test.hh"

#include "broker/detail/filesystem.hh"
#include "broker/internal/generator_file_reader.hh"
#include "broker/internal/type_id.hh"

using namespace broker;

namespace {

struct fixture {
  fixture() {
    file_name = detail::make_temp_file_name();
  }

  ~fixture() {
    detail::remove(file_name);
  }

  std::string file_name;
};

} // namespace

CAF_TEST_FIXTURE_SCOPE(generator_file_writer_tests, fixture)

CAF_TEST(data_message roundtrip with generator_file_reader) {
  auto x = vector{1, 2, "a", "bc"};
  auto x_msg = make_data_message("foo/bar", x);
  {
    auto out = internal::make_generator_file_writer(file_name);
    *out << x_msg;
  }
  // Read back from file.
  auto reader = internal::make_generator_file_reader(file_name);
  REQUIRE_NOT_EQUAL(reader, nullptr);
  std::variant<data_message, command_message> y_msg;
  CHECK_EQUAL(reader->read(y_msg), caf::none);
  CHECK_EQUAL(reader->at_end(), true);
  CHECK_EQUAL(get_topic(y_msg), topic{"foo/bar"});
  REQUIRE(is_data_message(y_msg));
  data y_data = get_data(get<data_message>(y_msg));
  REQUIRE(is<vector>(y_data));
  auto& y = get<vector>(y_data);
  REQUIRE_EQUAL(x.size(), y.size());
  CHECK(is<integer>(y[0]));
  CHECK(is<integer>(y[1]));
  REQUIRE(is<std::string>(y[2]));
  CHECK_EQUAL(get<std::string>(x[2]).size(), get<std::string>(y[2]).size());
  REQUIRE(is<std::string>(y[3]));
  CHECK_EQUAL(get<std::string>(x[3]).size(), get<std::string>(y[3]).size());
  CHECK_EQUAL(reader->read(y_msg), ec::end_of_file);
}

CAF_TEST(command_message roundtrip with generator_file_reader) {
  auto x = vector{1, 2, "a", "bc"};
  auto x_msg = make_data_message("foo/bar", x);
  {
    auto out = internal::make_generator_file_writer(file_name);
    *out << x_msg;
  }
  // Read back from file.
  auto reader = internal::make_generator_file_reader(file_name);
  REQUIRE_NOT_EQUAL(reader, nullptr);
  std::variant<data_message, command_message> y_msg;
  CHECK_EQUAL(reader->read(y_msg), caf::none);
  CHECK_EQUAL(reader->at_end(), true);
  CHECK_EQUAL(get_topic(y_msg), topic{"foo/bar"});
  REQUIRE(is_data_message(y_msg));
  data y_data = get_data(get<data_message>(y_msg));
  REQUIRE(is<vector>(y_data));
  auto& y = get<vector>(y_data);
  REQUIRE_EQUAL(x.size(), y.size());
  CHECK(is<integer>(y[0]));
  CHECK(is<integer>(y[1]));
  REQUIRE(is<std::string>(y[2]));
  CHECK_EQUAL(get<std::string>(x[2]).size(), get<std::string>(y[2]).size());
  REQUIRE(is<std::string>(y[3]));
  CHECK_EQUAL(get<std::string>(x[3]).size(), get<std::string>(y[3]).size());
  CHECK_EQUAL(reader->read(y_msg), ec::end_of_file);
}

CAF_TEST_FIXTURE_SCOPE_END()
