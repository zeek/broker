#define CAF_TEST_NO_MAIN

#include "test.hh"

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
    std::string result = get_topic(msg).string();
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

base_fixture::base_fixture()
  : ep(make_config()),
    sys(internal::endpoint_access{&ep}.sys()),
    self(sys),
    sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  for (char id = 'A'; id <= 'Z'; ++id) {
    auto index = id - 'A';
    str_ids[id] = id_strings[index];
    convert(std::string{id_strings[index]}, ids[id]);
  }
}

base_fixture::~base_fixture() {
  run();
  // Our core might do some messaging in its dtor, hence we need to make sure
  // messages are handled when enqueued to avoid blocking.
  sched.inline_all_enqueues();
}

char base_fixture::id_by_value(const broker::endpoint_id& value) {
  for (const auto& [key, val] : ids)
    if (val == value)
      return key;
  FAIL("value not found: " << value);
}

configuration base_fixture::make_config() {
  broker_options options;
  options.disable_ssl = true;
  configuration cfg{options};
  auto& nat_cfg = internal::configuration_access{&cfg}.cfg();
  caf::put(nat_cfg.content, "broker.disable-connector", true);
  test_coordinator_fixture<caf::actor_system_config>::init_config(nat_cfg);
  return cfg;
}

namespace {

struct bridge_state {
  static inline const char* name = "broker.test.bridge";
};

using bridge_actor = caf::stateful_actor<bridge_state>;

} // namespace

base_fixture::endpoint_state base_fixture::ep_state(caf::actor core) {
  auto& st = deref<internal::core_actor>(core).state;
  return endpoint_state{st.id, st.filter->read(), core};
}

caf::actor base_fixture::bridge(const endpoint_state& left,
                                const endpoint_state& right) {
  using caf::async::make_spsc_buffer_resource;
  auto& sys = left.hdl.home_system();
  auto [self, launch] = sys.spawn_inactive<bridge_actor>();
  {
    CAF_PUSH_AID_FROM_PTR(self);
    auto [con1, prod1] = make_spsc_buffer_resource<node_message>();
    auto [con2, prod2] = make_spsc_buffer_resource<node_message>();
    caf::anon_send(left.hdl, atom::peer_v, right.id,
                   network_info{to_string(right.id), 42}, right.filter, con1,
                   prod2);
    auto [con3, prod3] = make_spsc_buffer_resource<node_message>();
    auto [con4, prod4] = make_spsc_buffer_resource<node_message>();
    caf::anon_send(right.hdl, atom::peer_v, left.id,
                   network_info{to_string(left.id), 42}, left.filter, con3,
                   prod4);
    self->make_observable().from_resource(con2).subscribe(prod3);
    self->make_observable().from_resource(con4).subscribe(prod1);
  }
  auto hdl = caf::actor{self};
  launch();
  return hdl;
}

caf::actor base_fixture::bridge(const endpoint& left, const endpoint& right) {
  return bridge(ep_state(native(left.core())), ep_state(native(right.core())));
}

caf::actor base_fixture::bridge(caf::actor left_core, caf::actor right_core) {
  return bridge(ep_state(left_core), ep_state(right_core));
}

void base_fixture::push_data(caf::actor core,
                             std::vector<broker::data_message> xs) {
  for (auto& x : xs)
    caf::anon_send(core, atom::publish_v, std::move(x));
}

namespace {

struct data_collector_state {
  static inline const char* name = "broker.test.data-collector";
};

using data_collector_actor = caf::stateful_actor<data_collector_state>;

void data_collector_impl(data_collector_actor* self,
                         std::shared_ptr<std::vector<data_message>> buf,
                         caf::async::consumer_resource<data_message> res) {
  self->make_observable()
    .from_resource(std::move(res))
    .for_each([buf](const data_message& msg) { buf->emplace_back(msg); });
}

} // namespace

std::shared_ptr<std::vector<data_message>>
base_fixture::collect_data(caf::actor core, filter_type filter) {
  using actor_t = data_collector_actor;
  auto& sys = core.home_system();
  auto [con, prod] = caf::async::make_spsc_buffer_resource<data_message>();
  auto buf = std::make_shared<std::vector<data_message>>();
  sys.spawn(data_collector_impl, buf, std::move(con));
  anon_send(core, std::move(filter), std::move(prod));
  return buf;
}

void base_fixture::run() {
  while (sched.has_job() || sched.has_pending_timeout()) {
    sched.run();
    sched.trigger_timeouts();
  }
}

void base_fixture::consume_message() {
  if (!sched.try_run_once())
    CAF_FAIL("no message to consume");
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::broker_test>();
  endpoint::system_guard sys_guard; // Initialize global state.
  // if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //   return 1;
  return caf::test::main(argc, argv);
}
