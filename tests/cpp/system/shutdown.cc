// Checks whether Broker instances shut down gracefully, i.e., Broker endpoints
// ship queued events before closing remote connections.

#define SUITE system.shutdown

#include "test.hh"

#include "broker/endpoint.hh"

#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <vector>

using namespace broker;

namespace {

using string_list = std::vector<std::string>;

configuration make_config(std::string file_path_template) {
  configuration cfg;
  cfg.set("caf.scheduler.max-threads", 2);
  cfg.set("caf.logger.console.verbosity", "quiet");
  cfg.set("caf.logger.file.path", std::move(file_path_template));
  cfg.set("caf.logger.file.verbosity", "trace");
  cfg.set("caf.logger.file.excluded-components", std::vector<std::string>{});
  return cfg;
}

template <class... Ts>
[[noreturn]] void hard_error(const Ts&... xs) {
  (std::cerr << ... << xs) << '\n';
  abort();
}

auto code_of(const error& err) {
  if (err.category() != caf::type_id_v<broker::ec>)
    return ec::unspecified;
  return static_cast<ec>(err.code());
}

struct fixture {
  fixture() {
    t0 = broker::now();
  }

  std::string log_path_template(const char* test_name,
                                const char* endpoint_name) {
    std::string result;
    result += broker::to_string(t0);
    result += ' ';
    result += test_name;
    result += ' ';
    result += endpoint_name;
    result += ".log";
    return result;
  }

  broker::timestamp t0;
};

} // namespace

FIXTURE_SCOPE(system_shutdown_tests, fixture)

// Spins up two Broker endpoints, attaches subscribers for status and error
// events and shuts both endpoints down immediately after peering. The
// subscribers should receive all events (from discovery to disconnecting) of
// the short-lived endpoints.
TEST(status listeners receive peering events) {
  MESSAGE("status subscribers receive discovery and peering events");
  auto ep1_log = std::make_shared<std::vector<data_message>>();
  auto ep2_log = std::make_shared<std::vector<data_message>>();
  auto port_promise = std::promise<uint16_t>{};
  auto port_future = port_promise.get_future();
  barrier checkpoint{2}; // Makes sure that the endpoint in t2 shuts down first.
  auto t1 = std::thread{[&]() mutable {
    endpoint ep{make_config(log_path_template("peering-events", "ep1"))};
    ep.subscribe(
      {topic::statuses()}, [](caf::unit_t&) {},
      [ep1_log](caf::unit_t&, data_message msg) {
        ep1_log->emplace_back(std::move(msg));
      },
      [](caf::unit_t&, const error&) {});
    auto port = ep.listen("127.0.0.1", 0);
    if (port == 0)
      hard_error("endpoint::listen failed");
    MESSAGE("first endpoint listening on port " << port);
    port_promise.set_value(port);
    checkpoint.arrive_and_wait();
  }};
  auto t2 = std::thread{[&, port{port_future.get()}] {
    /*lifetime scope of ep*/ {
      endpoint ep{make_config(log_path_template("peering-events", "ep2"))};
      ep.subscribe(
        {topic::statuses()}, [](caf::unit_t&) {},
        [ep2_log](caf::unit_t&, data_message msg) {
          ep2_log->emplace_back(std::move(msg));
        },
        [](caf::unit_t&, const error&) {});
      if (!ep.peer("127.0.0.1", port))
        hard_error("endpoint::peer failed");
      MESSAGE("second endpoint peered to 127.0.0.1:" << port);
    }
    checkpoint.arrive_and_wait();
  }};
  t1.join();
  t2.join();
  MESSAGE("both endpoints were shut down");
  // Now, ep2 actively closed the peering (by shutting down first) and should
  // report `peer_removed`, whereas ep1 should report `peer_lost` instead.
  CHECK_EQUAL(normalize_status_log(*ep1_log), string_list({
                                                "endpoint_discovered",
                                                "peer_added",
                                                "peer_lost",
                                                "endpoint_unreachable",
                                              }));
  CHECK_EQUAL(normalize_status_log(*ep2_log), string_list({
                                                "endpoint_discovered",
                                                "peer_added",
                                                "peer_removed",
                                                "endpoint_unreachable",
                                              }));
}

TEST(endpoints send published data before terminating) {
  MESSAGE("status subscribers receive discovery and peering events");
  auto ep1_log = std::make_shared<std::vector<data_message>>();
  auto port_promise = std::promise<uint16_t>{};
  auto port_future = port_promise.get_future();
  auto beacon_ptr = std::make_shared<beacon>(); // Blocks t1 until data arrived.
  auto t1 = std::thread{[&]() mutable {
    endpoint ep{make_config(log_path_template("publish", "ep1"))};
    ep.subscribe(
      {"/foo/bar"}, [](caf::unit_t&) {},
      [ep1_log, beacon_ptr](caf::unit_t&, data_message msg) {
        ep1_log->emplace_back(std::move(msg));
        beacon_ptr->set_true();
      },
      [](caf::unit_t&, const error&) {});
    auto port = ep.listen("127.0.0.1", 0);
    if (port == 0)
      hard_error("endpoint::listen failed");
    MESSAGE("first endpoint listening on port " << port);
    port_promise.set_value(port);
    beacon_ptr->wait();
  }};
  auto t2 = std::thread{[&, port{port_future.get()}] {
    /*lifetime scope of ep*/ {
      endpoint ep{make_config(log_path_template("publish", "ep2"))};
      if (!ep.peer("127.0.0.1", port))
        hard_error("endpoint::listen failed");
      MESSAGE("second endpoint peered to 127.0.0.1:" << port);
      ep.publish("/foo/bar", data{"hello world"});
    }
  }};
  t1.join();
  t2.join();
  MESSAGE("both endpoints were shut down");
  // Now, ep2 actively closed the peering and should report `peer_removed`,
  // whereas ep1 should `peer_lost` instead.
  CHECK_EQUAL(normalize_status_log(*ep1_log), string_list({
                                                "/foo/bar: hello world",
                                              }));
}

FIXTURE_SCOPE_END()
