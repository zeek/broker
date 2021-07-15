#define SUITE core_actor

#include "broker/core_actor.hh"

#include "test.hh"

#include <caf/scheduled_actor/flow.hpp>

#include "broker/configuration.hh"
#include "broker/detail/flow_controller_callback.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using namespace broker;

namespace {

struct fixture : test_coordinator_fixture<> {
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
    base_fixture::init_socket_api();
    ep1.id = endpoint_id::random(1);
    ep2.id = endpoint_id::random(2);
    ep3.id = endpoint_id::random(3);
  }

  template <class... Ts>
  void spin_up(endpoint_state& ep, Ts&... xs) {
    ep.hdl = sys.spawn<core_actor_type>(ep.id, ep.filter);
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
    base_fixture::deinit_socket_api();
  }

  caf::actor bridge(const endpoint_state& left, const endpoint_state& right) {
    auto res = base_fixture::bridge(left, right);
    bridges.emplace_back(res);
    return res;
  }

  std::shared_ptr<std::vector<data_message>>
  collect_data(const endpoint_state& ep, filter_type filter) {
    auto buf = std::make_shared<std::vector<data_message>>();
    auto cb = detail::make_flow_controller_callback(
      [=](detail::flow_controller* ctrl) mutable {
        using actor_t = caf::event_based_actor;
        auto& sys = ep.hdl.home_system();
        ctrl->add_filter(filter);
        ctrl->select_local_data(filter).subscribe_with<actor_t>(
          sys, [=](actor_t*, caf::flow::observable<data_message> in) {
            in.for_each(
              [buf](const data_message& msg) { buf->emplace_back(msg); });
          });
      });
    caf::anon_send(ep.hdl, std::move(cb));
    run();
    return buf;
  }

  void push_data(const endpoint_state& ep, const data_message_list& xs) {
    auto cb = detail::make_flow_controller_callback(
      [=](detail::flow_controller* ctrl) mutable {
        auto ctx = ctrl->ctx();
        auto obs = ctx->make_observable().from_container(xs).as_observable();
        ctrl->add_source(std::move(obs));
      });
    caf::anon_send(ep.hdl, std::move(cb));
    run();
  }

  auto& state(caf::actor hdl) {
    return deref<core_actor_type>(hdl).state;
  }

  auto& state(const endpoint_state& ep) {
    return deref<core_actor_type>(ep.hdl).state;
  }

  auto& tbl(caf::actor hdl) {
    return state(hdl).tbl();
  }

  auto& tbl(const endpoint_state& ep) {
    return state(ep).tbl();
  }

  auto distance_from(const endpoint_state& src) {
    struct impl {
      fixture* thisptr;
      caf::actor src_hdl;
      optional<size_t> to(const endpoint_state& dst) {
        return alm::distance_to(thisptr->tbl(src_hdl), dst.id);
      }
    };
    return impl{this, src.hdl};
  }
};

optional<size_t> operator""_os(unsigned long long x) {
  return optional<size_t>{static_cast<size_t>(x)};
}

} // namespace <anonymous>

FIXTURE_SCOPE(local_tests, fixture)

TEST(peers forward local data to direct peers) {
  MESSAGE("spin up two endpoints: ep1 and ep2");
  auto abc = filter_type{"a", "b", "c"};
  ep1.filter = abc;
  ep2.filter = abc;
  spin_up(ep1, ep2);
  bridge(ep1, ep2);
  run();
  CHECK_EQUAL(distance_from(ep1).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep2).to(ep1), 1_os);
  MESSAGE("subscribe to data messages on ep2");
  auto buf = collect_data(ep2, abc);
  MESSAGE("publish data on ep1");
  push_data(ep1, test_data);
  CHECK_EQUAL(*buf, test_data);
}

TEST(peers forward local data to any peer with forwarding paths) {
  MESSAGE("spin up: ep1, ep2 and ep3 and only ep3 subscribes to abc topics");
  auto abc = filter_type{"a", "b", "c"};
  ep1.filter = abc;
  ep3.filter = abc;
  spin_up(ep1, ep2, ep3);
  bridge(ep1, ep2);
  bridge(ep2, ep3);
  run();
  CHECK_EQUAL(distance_from(ep1).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep1).to(ep3), 2_os);
  CHECK_EQUAL(distance_from(ep2).to(ep1), 1_os);
  CHECK_EQUAL(distance_from(ep2).to(ep3), 1_os);
  CHECK_EQUAL(distance_from(ep3).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep3).to(ep1), 2_os);
  MESSAGE("subscribe to data messages on ep3");
  auto buf = collect_data(ep3, abc);
  MESSAGE("publish data on ep1");
  push_data(ep1, test_data);
  CHECK_EQUAL(*buf, test_data);
}

TEST(peers propagate broken paths) {
  MESSAGE("spin up: ep1, ep2 and ep3 and only ep3 subscribes to abc topics");
  auto abc = filter_type{"a", "b", "c"};
  ep1.filter = abc;
  ep3.filter = abc;
  spin_up(ep1, ep2, ep3);
  bridge(ep1, ep2);
  auto br = bridge(ep2, ep3);
  run();
  CHECK_EQUAL(distance_from(ep1).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep1).to(ep3), 2_os);
  CHECK_EQUAL(distance_from(ep2).to(ep1), 1_os);
  CHECK_EQUAL(distance_from(ep2).to(ep3), 1_os);
  CHECK_EQUAL(distance_from(ep3).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep3).to(ep1), 2_os);
  MESSAGE("disconnect ep3");
  auto nil = optional<size_t>();
  anon_send_exit(br, caf::exit_reason::user_shutdown);
  run();
  CHECK_EQUAL(distance_from(ep1).to(ep2), 1_os);
  CHECK_EQUAL(distance_from(ep1).to(ep3), nil);
  CHECK_EQUAL(distance_from(ep2).to(ep1), 1_os);
  CHECK_EQUAL(distance_from(ep2).to(ep3), nil);
  CHECK_EQUAL(distance_from(ep3).to(ep2), nil);
  CHECK_EQUAL(distance_from(ep3).to(ep1), nil);
}

FIXTURE_SCOPE_END()
