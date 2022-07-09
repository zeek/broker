#define SUITE internal.core_actor

#include "broker/internal/core_actor.hh"

#include "test.hh"

#include <caf/scheduled_actor/flow.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/internal/logger.hh"

using namespace broker;

namespace {

struct config : public caf::actor_system_config {
  config() {
    set("caf.logger.file.verbosity", "trace");
    // set("caf.logger.console.verbosity", "trace");
  }
};

struct fixture : test_coordinator_fixture<config> {
  using endpoint_state = base_fixture::endpoint_state;

  endpoint_state ep1;

  endpoint_state ep2;

  endpoint_state ep3;

  std::vector<caf::actor> bridges;

  using data_message_list = std::vector<data_message>;

  data_message_list test_data = data_message_list({
    make_data_message("a", 0),
    make_data_message("b", true),
    make_data_message("a", 1),
    make_data_message("a", 2),
    make_data_message("b", false),
    make_data_message("b", true),
    make_data_message("a", 3),
    make_data_message("b", false),
    make_data_message("a", 4),
    make_data_message("a", 5),
  });

  fixture() {
    // We don't do networking, but our flares use the socket API.
    ep1.id = endpoint_id::random(1);
    ep2.id = endpoint_id::random(2);
    ep3.id = endpoint_id::random(3);
  }

  template <class... Ts>
  void spin_up(endpoint_state& ep, Ts&... xs) {
    ep.hdl = sys.spawn<internal::core_actor>(ep.id, ep.filter);
    MESSAGE(ep.id << " is running at " << ep.hdl);
    if constexpr (sizeof...(Ts) == 0)
      run();
    else
      spin_up(xs...);
  }

  ~fixture() {
    for (auto& hdl : bridges)
      caf::anon_send_exit(hdl, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(ep1.hdl, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(ep2.hdl, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(ep3.hdl, caf::exit_reason::user_shutdown);
  }

  caf::actor bridge(const endpoint_state& left, const endpoint_state& right) {
    auto res = base_fixture::bridge(left, right);
    bridges.emplace_back(res);
    return res;
  }

  std::shared_ptr<std::vector<data_message>>
  collect_data(const endpoint_state& ep, filter_type filter) {
    auto res = base_fixture::collect_data(ep.hdl, std::move(filter));
    run();
    return res;
  }

  void push_data(const endpoint_state& ep, data_message_list xs) {
    base_fixture::push_data(ep.hdl, xs);
  }

  auto& state(caf::actor hdl) {
    return deref<internal::core_actor>(hdl).state;
  }

  auto& state(const endpoint_state& ep) {
    return deref<internal::core_actor>(ep.hdl).state;
  }
  auto peer_ids(const endpoint_state& ep) {
    auto result = state(ep).peer_ids();
    std::sort(result.begin(), result.end());
    return result;
  }
};

std::optional<size_t> operator""_os(unsigned long long x) {
  return std::optional<size_t>{static_cast<size_t>(x)};
}

template <class... Ts>
auto ids(Ts... xs) {
  return std::vector<endpoint_id>{xs...};
}

} // namespace

FIXTURE_SCOPE(local_tests, fixture)

TEST(peers forward local data to direct peers) {
  MESSAGE("spin up two endpoints: ep1 and ep2");
  auto abc = filter_type{"a", "b", "c"};
  ep1.filter = abc;
  ep2.filter = abc;
  spin_up(ep1, ep2);
  bridge(ep1, ep2);
  run();
  CHECK_EQUAL(state(ep1).peer_ids(), ids(ep2.id));
  CHECK_EQUAL(state(ep2).peer_ids(), ids(ep1.id));
  MESSAGE("subscribe to data messages on ep2");
  auto buf = collect_data(ep2, abc);
  MESSAGE("publish data on ep1");
  push_data(ep1, test_data);
  run();
  CHECK_EQUAL(*buf, test_data);
}

TEST(peers forward local data to any peer with forwarding paths) {
  MESSAGE("spin up ep1, ep2 and ep3");
  auto abc = filter_type{"a", "b", "c"};
  ep1.filter = abc;
  ep2.filter = abc;
  ep3.filter = abc;
  spin_up(ep1, ep2, ep3);
  bridge(ep1, ep2);
  bridge(ep2, ep3);
  run();
  CHECK_EQUAL(peer_ids(ep1), ids(ep2.id));
  CHECK_EQUAL(peer_ids(ep2), ids(ep1.id, ep3.id));
  CHECK_EQUAL(peer_ids(ep3), ids(ep2.id));
  MESSAGE("subscribe to data messages on ep3");
  auto buf = collect_data(ep3, abc);
  MESSAGE("publish data on ep1");
  push_data(ep1, test_data);
  run();
  CHECK_EQUAL(*buf, test_data);
}

FIXTURE_SCOPE_END()
