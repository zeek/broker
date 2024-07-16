#define CAF_TEST_NO_MAIN

#include "broker/broker-test.test.hh"

#include <random>

#include <caf/test/unit_test_impl.hpp>

#include <caf/defaults.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/test/dsl.hpp>

#include "broker/config.hh"
#include "broker/endpoint_id.hh"
#include "broker/filter_type.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

namespace atom = broker::internal::atom;

using broker::internal::native;

std::vector<std::string>
normalize_status_log(const std::vector<broker::data_message>& xs,
                     bool include_endpoint_id) {
  using namespace broker;
  auto stringify = [](const data_message& msg) {
    auto result = std::string{get_topic(msg)};
    result += ": ";
    result += to_string(get_data(msg));
    return result;
  };
  auto code_of = [](const error& err) {
    if (err.category() != caf::type_id_v<broker::ec>)
      return ec::unspecified;
    return static_cast<ec>(err.code());
  };
  std::vector<std::string> lines;
  lines.reserve(xs.size());
  for (auto& x : xs) {
    if (auto err = to<error>(get_data(x))) {
      lines.emplace_back(to_string(code_of(*err)));
    } else if (auto stat = to<status>(get_data(x))) {
      lines.emplace_back(to_string(stat->code()));
      if (include_endpoint_id) {
        auto& line = lines.back();
        line += ": ";
        if (auto ctx = stat->context()) {
          line += to_string(ctx->node);
        } else {
          line += to_string(endpoint_id::nil());
        }
      }
    } else {
      lines.emplace_back(stringify(x));
    }
  }
  return lines;
}

barrier::barrier(ptrdiff_t num_threads) : num_threads_(num_threads), count_(0) {
  // nop
}

void barrier::arrive_and_wait() {
  std::unique_lock<std::mutex> guard{mx_};
  if (++count_ == num_threads_) {
    cv_.notify_all();
    return;
  }
  cv_.wait(guard, [this] { return count_.load() == num_threads_; });
}

beacon::beacon() : value_(false) {
  // nop
}

void beacon::set_true() {
  std::unique_lock<std::mutex> guard{mx_};
  value_ = true;
  cv_.notify_all();
}

void beacon::wait() {
  std::unique_lock<std::mutex> guard{mx_};
  cv_.wait(guard, [this] { return value_.load(); });
}

using namespace broker;

namespace {

std::string_view id_strings[] = {
  "685a1674-e15c-11eb-ba80-0242ac130004",
  "685a1a2a-e15c-11eb-ba80-0242ac130004",
  "685a1b2e-e15c-11eb-ba80-0242ac130004",
  "685a1bec-e15c-11eb-ba80-0242ac130004",
  "685a1caa-e15c-11eb-ba80-0242ac130004",
  "685a1d5e-e15c-11eb-ba80-0242ac130004",
  "685a1e1c-e15c-11eb-ba80-0242ac130004",
  "685a1ed0-e15c-11eb-ba80-0242ac130004",
  "685a20d8-e15c-11eb-ba80-0242ac130004",
  "685a21a0-e15c-11eb-ba80-0242ac130004",
  "685a2254-e15c-11eb-ba80-0242ac130004",
  "685a2308-e15c-11eb-ba80-0242ac130004",
  "685a23bc-e15c-11eb-ba80-0242ac130004",
  "685a2470-e15c-11eb-ba80-0242ac130004",
  "685a2524-e15c-11eb-ba80-0242ac130004",
  "685a27ae-e15c-11eb-ba80-0242ac130004",
  "685a286c-e15c-11eb-ba80-0242ac130004",
  "685a2920-e15c-11eb-ba80-0242ac130004",
  "685a29d4-e15c-11eb-ba80-0242ac130004",
  "685a2a88-e15c-11eb-ba80-0242ac130004",
  "685a2b3c-e15c-11eb-ba80-0242ac130004",
  "685a2bf0-e15c-11eb-ba80-0242ac130004",
  "685a2e2a-e15c-11eb-ba80-0242ac130004",
  "685a2ef2-e15c-11eb-ba80-0242ac130004",
  "685a2fa6-e15c-11eb-ba80-0242ac130004",
  "685a305a-e15c-11eb-ba80-0242ac130004",
};

} // namespace

ids_fixture::ids_fixture() {
  for (char id = 'A'; id <= 'Z'; ++id) {
    auto index = id - 'A';
    str_ids[id] = id_strings[index];
    convert(std::string{id_strings[index]}, ids[id]);
  }
}

ids_fixture::~ids_fixture() {
  // Only defined because the id_fixture is a base class.
}

char ids_fixture::id_by_value(const broker::endpoint_id& value) {
  for (const auto& [key, val] : ids)
    if (val == value)
      return key;
  FAIL("value not found: " << value);
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::broker_test>();
  endpoint::system_guard sys_guard; // Initialize global state.
  // if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //   return 1;
  return caf::test::main(argc, argv);
}
