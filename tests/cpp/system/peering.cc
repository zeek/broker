// Puts some stress on the peering logic by launching many peering request
// simultaneously.

#define SUITE system.peering

#include "test.hh"

#include "broker/endpoint.hh"

#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <vector>

#define SYNC_CHECK(stmt)                                                       \
  ([&] {                                                                       \
    std::unique_lock guard{print_mtx};                                         \
    return CHECK(stmt);                                                        \
  })()

#define SYNC_CHECK_EQUAL(lhs, rhs)                                             \
  ([&] {                                                                       \
    std::unique_lock guard{print_mtx};                                         \
    return CHECK_EQUAL(lhs, rhs);                                              \
  })()

#define SYNC_CHECK_FAILED(...)                                                 \
  ([](auto... xs) {                                                            \
    std::ostringstream str;                                                    \
    (str << ... << xs);                                                        \
    std::unique_lock guard{print_mtx};                                         \
    caf::test::detail::check_un(false, __FILE__, __LINE__, str.str().c_str()); \
  })(__VA_ARGS__)

using namespace broker;
using namespace std::literals;

namespace {

static constexpr size_t num_endpoints = 4;

using data_message_list = std::vector<data_message>;
using string_list = std::vector<std::string>;
using data_list = std::vector<data>;

std::string log_path_template(const char* test_name, size_t endpoint_nr) {
  std::string result;
  result += test_name;
  result += ' ';
  result += "ep";
  result += std::to_string(endpoint_nr);
  result += ".log";
  return result;
}

static constexpr bool disable_ssl = true;

static constexpr bool enable_ssl = false;

configuration make_config(const char* test_name, size_t endpoint_nr,
                          bool disable_ssl) {
  broker_options opts;
  opts.disable_forwarding = true;
  opts.disable_ssl = disable_ssl;
  configuration cfg{opts};
  cfg.set("caf.scheduler.max-threads", 2);
  cfg.set("caf.logger.console.verbosity", "quiet");
  cfg.set("caf.logger.file.path", log_path_template(test_name, endpoint_nr));
  // cfg.set("caf.logger.file.verbosity", "trace");
  // cfg.set("caf.logger.file.excluded-components", std::vector<std::string>{});
  // cfg.set("broker.metrics.export.topic", "my/metrics/node-" +
  // std::to_string(endpoint_nr)); cfg.set("broker.metrics.export.prefixes",
  // std::vector{"caf"s,"broker"s}); if (endpoint_nr == 0) {
  //   cfg.set("broker.metrics.import.topics", std::vector{"my/metrics"s});
  //   cfg.set("broker.metrics.port", 4040);
  // }
  return cfg;
}

std::mutex print_mtx;

template <class... Ts>
[[noreturn]] void hard_error(const Ts&... xs) {
  std::unique_lock guard{print_mtx};
  (std::cerr << ... << xs) << '\n';
  abort();
}

template <class... Ts>
void println(const Ts&... xs) {
  std::unique_lock guard{print_mtx};
  (std::cout << ... << xs) << '\n';
}

struct fixture {
  fixture() {
    for (auto& ptr : ep_logs)
      ptr = std::make_shared<data_message_list>();
    for (auto& ptr : ep_values)
      ptr = std::make_shared<data_list>();
    for (auto& ptr : ep_ids)
      ptr = std::make_shared<endpoint_id>();
  }

  std::array<std::shared_ptr<data_message_list>, num_endpoints> ep_logs;
  std::array<std::shared_ptr<data_list>, num_endpoints> ep_values;
  std::array<std::atomic<uint16_t>, num_endpoints> ports;
  std::array<std::thread, num_endpoints> threads;
  std::array<std::shared_ptr<endpoint_id>, num_endpoints> ep_ids;
};

struct alternative {
  std::string a;
  std::string b;
  explicit alternative(std::string_view str) : a(str), b(str) {
    // nop
  }
  alternative(std::string_view a, std::string_view b) : a(a), b(b) {
    // nop
  }
};

std::string to_string(const alternative& x) {
  auto result = std::string{x.a};
  if (x.a != x.b) {
    result += '|';
    result += x.b;
  }
  return result;
}

bool operator==(const alternative& x, const alternative& y) {
  return x.a == y.a || x.a == y.b || x.b == y.a || x.b == y.b;
}

bool operator==(const alternative& x, std::string_view y) {
  return x.a == y || x.b == y;
}

bool operator==(std::string_view x, const alternative& y) {
  return y == x;
}

bool operator!=(const alternative& x, std::string_view y) {
  return !(x == y);
}

bool operator!=(std::string_view x, const alternative& y) {
  return !(x == y);
}

alternative operator""_a(const char* cstr, size_t len) {
  return alternative{std::string_view{cstr, len}};
}

bool ends_with(std::string_view str, std::string_view suffix) {
  auto n = str.size();
  auto m = suffix.size();
  return n >= m ? str.compare(n - m, m, suffix) == 0 : false;
}

std::vector<alternative> grep_id(std::vector<std::string> xs, endpoint_id id) {
  auto suffix = ": " + to_string(id);
  std::vector<alternative> result;
  for (auto& x : xs)
    if (ends_with(x, suffix))
      result.emplace_back(x.substr(0, x.size() - suffix.size()));
  return result;
}

std::vector<std::string> sort(std::vector<std::string> xs) {
  std::sort(xs.begin(), xs.end());
  return xs;
}

std::vector<std::string> grep_hello(std::vector<data_message> xs) {
  auto str = "hello"s;
  std::vector<std::string> result;
  for (auto& x : xs) {
    auto msg = to_string(get_data(x));
    if (msg.find(str) != std::string::npos)
      result.emplace_back(msg);
  }
  return result;
}

} // namespace

FIXTURE_SCOPE(system_peering_tests, fixture)

// Spins up four Broker endpoints, listens for peering events on all of them and
// then form a full mesh. All endpoints wait on a barrier before calling `peer`
// on the endpoints to maximize conflict potential during handshaking.
TEST(a full mesh emits endpoint_discovered and peer_added for all nodes) {
  MESSAGE("initialize state");
  barrier listening{num_endpoints};
  barrier peered{num_endpoints};
  barrier send_and_received{num_endpoints};
  MESSAGE("spin up threads");
  for (size_t index = 0; index != num_endpoints; ++index) {
    threads[index] = std::thread{[&, index] {
      auto log_ptr = ep_logs[index];
      endpoint ep{make_config("peering-events", index, disable_ssl)};
      *ep_ids[index] = ep.node_id();
      barrier got_hellos{2};
      ep.subscribe(
        {topic::statuses(), "foo/bar"}, [](caf::unit_t&) {},
        [log_ptr, n{0}, &got_hellos](caf::unit_t&, data_message msg) mutable {
          if (get_topic(msg).string() == "foo/bar" && ++n == 3)
            got_hellos.arrive_and_wait();
          log_ptr->emplace_back(std::move(msg));
        },
        [](caf::unit_t&, const error&) {});
      auto port = ep.listen();
      if (port == 0)
        hard_error("endpoint ", to_string(ep.node_id()),
                   " failed to open a port");
      ports[index] = port;
      std::map<endpoint_id, std::future<bool>> peer_results;
      listening.arrive_and_wait();
      for (size_t i = 0; i != num_endpoints; ++i) {
        if (i != index) {
          auto p = ports[i].load();
          peer_results.emplace(*ep_ids[i], ep.peer_async("localhost", p, 1s));
        }
      }
      for (auto& [other_id, res] : peer_results) {
        if (!res.get()) {
          SYNC_CHECK_FAILED("endpoint ", to_string(ep.node_id()),
                            " failed to connect to ", to_string(other_id));
        }
      }
      peered.arrive_and_wait();
      ep.publish("foo/bar", "hello from " + std::to_string(index));
      got_hellos.arrive_and_wait();
      send_and_received.arrive_and_wait();
      // std::this_thread::sleep_for(10s);
    }};
  }
  MESSAGE("wait for all threads to complete");
  for (auto& hdl : threads)
    hdl.join();
  MESSAGE("check results");
  std::vector<std::vector<std::string>> normalized_logs;
  for (auto& ep_log : ep_logs)
    normalized_logs.emplace_back(normalize_status_log(*ep_log, true));
  std::vector<std::vector<std::string>> hellos;
  for (size_t index = 0; index != num_endpoints; ++index) {
    std::vector<std::string> lines;
    for (size_t i = 0; i != num_endpoints; ++i)
      if (i != index)
        lines.emplace_back("hello from " + std::to_string(i));
    hellos.emplace_back(lines);
  }
  auto sequence =
    std::vector<alternative>{"endpoint_discovered"_a, "peer_added"_a,
                             alternative{"peer_lost", "peer_removed"},
                             "endpoint_unreachable"_a};
  // Check all endpoints.
  for (size_t index = 0; index != num_endpoints; ++index) {
    for (size_t i = 0; i != num_endpoints; ++i)
      if (i != index)
        CHECK_EQUAL(grep_id(normalized_logs[index], *ep_ids[i]), sequence);
    CHECK_EQUAL(sort(grep_hello(*ep_logs[index])), hellos[index]);
  }
}

TEST(multiple clones can attach to a single master) {
  MESSAGE("initialize state");
  barrier listening{num_endpoints};
  barrier peered{num_endpoints};
  barrier written_to_store{num_endpoints};
  MESSAGE("spin up threads");
  for (size_t index = 0; index != num_endpoints; ++index) {
    threads[index] = std::thread{[&, index] {
      endpoint ep{make_config("peering-events", index, disable_ssl)};
      *ep_ids[index] = ep.node_id();
      store services;
      if (index == 0) {
        if (auto maybe_services = ep.attach_master("zeek/known/services",
                                                   backend::memory)) {
          services = std::move(*maybe_services);
          services.put("foo-0", "bar");
        } else {
          SYNC_CHECK_FAILED("attach_master failed: ",
                            to_string(maybe_services.error()));
        }
      }
      auto port = ep.listen();
      if (port == 0)
        hard_error("endpoint ", to_string(ep.node_id()),
                   " failed to open a port");
      ports[index] = port;
      std::map<endpoint_id, std::future<bool>> peer_results;
      listening.arrive_and_wait();
      for (size_t i = 0; i != num_endpoints; ++i) {
        if (i != index) {
          auto p = ports[i].load();
          peer_results.emplace(*ep_ids[i], ep.peer_async("localhost", p, 1s));
        }
      }
      for (auto& [other_id, res] : peer_results) {
        if (!res.get()) {
          SYNC_CHECK_FAILED("endpoint ", to_string(ep.node_id()),
                            " failed to connect to ", to_string(other_id));
        }
      }
      peered.arrive_and_wait();
      if (index != 0) {
        if (auto maybe_services = ep.attach_clone("zeek/known/services", 0.5);
            SYNC_CHECK(maybe_services)) {
          services = std::move(*maybe_services);
          services.put("foo-" + std::to_string(index), "bar");
          if (auto val = services.get("foo-0"s);
              SYNC_CHECK(val) && SYNC_CHECK(is<std::string>(*val)))
            SYNC_CHECK_EQUAL(get<std::string>(*val), "bar");
          auto idle_res = services.await_idle();
          SYNC_CHECK(idle_res);
          written_to_store.arrive_and_wait();
        }
      } else {
        written_to_store.arrive_and_wait();
        auto keys_data = services.keys();
        REQUIRE(keys_data);
        REQUIRE(is<set>(*keys_data));
        auto keys = std::move(get<set>(*keys_data));
        CHECK_EQUAL(keys, set({"foo-0", "foo-1"s, "foo-2"s, "foo-3"s}));
      }
    }};
  }
  for (auto& hdl : threads)
    hdl.join();
}

TEST(the master may appear after launching the clones) {
  MESSAGE("initialize state");
  barrier listening{num_endpoints};
  barrier peered{num_endpoints};
  barrier waiting_for_master{num_endpoints};
  barrier written_to_store{num_endpoints};
  MESSAGE("spin up threads");
  for (size_t index = 0; index != num_endpoints; ++index) {
    threads[index] = std::thread{[&, index] {
      endpoint ep{make_config("peering-events", index, disable_ssl)};
      *ep_ids[index] = ep.node_id();
      auto port = ep.listen();
      if (port == 0)
        hard_error("endpoint ", to_string(ep.node_id()),
                   " failed to open a port");
      ports[index] = port;
      std::map<endpoint_id, std::future<bool>> peer_results;
      listening.arrive_and_wait();
      for (size_t i = 0; i != num_endpoints; ++i) {
        if (i != index) {
          auto p = ports[i].load();
          peer_results.emplace(*ep_ids[i], ep.peer_async("localhost", p, 1s));
        }
      }
      for (auto& [other_id, res] : peer_results) {
        if (!res.get()) {
          SYNC_CHECK_FAILED("endpoint ", to_string(ep.node_id()),
                            " failed to connect to ", to_string(other_id));
        }
      }
      peered.arrive_and_wait();
      store services;
      if (index != 0) {
        auto maybe_services = ep.attach_clone("zeek/known/services");
        waiting_for_master.arrive_and_wait();
        if (SYNC_CHECK(maybe_services)) {
          services = std::move(*maybe_services);
          services.put("foo-" + std::to_string(index), "bar");
          auto idle_res = services.await_idle();
          SYNC_CHECK(idle_res);
          if (auto val = services.get("foo-0"s);
              SYNC_CHECK(val) && SYNC_CHECK(is<std::string>(*val)))
            SYNC_CHECK_EQUAL(get<std::string>(*val), "bar");
          written_to_store.arrive_and_wait();
        } else {
          FAIL("failed to connect to master: " << maybe_services.error());
        }
      } else {
        waiting_for_master.arrive_and_wait();
        if (auto maybe_services = ep.attach_master("zeek/known/services",
                                                   backend::memory)) {
          services = std::move(*maybe_services);
          services.put("foo-0", "bar");
        } else {
          SYNC_CHECK_FAILED("attach_master failed: ",
                            to_string(maybe_services.error()));
        }
        written_to_store.arrive_and_wait();
        auto keys_data = services.keys();
        REQUIRE(keys_data);
        REQUIRE(is<set>(*keys_data));
        auto keys = std::move(get<set>(*keys_data));
        CHECK_EQUAL(keys, set({"foo-0", "foo-1"s, "foo-2"s, "foo-3"s}));
      }
    }};
  }
  for (auto& hdl : threads)
    hdl.join();
}

TEST(only one put_unique may pass) {
  MESSAGE("initialize state");
  barrier listening{num_endpoints};
  barrier peered{num_endpoints};
  barrier written_to_store{num_endpoints};
  MESSAGE("spin up threads");
  for (size_t index = 0; index != num_endpoints; ++index) {
    threads[index] = std::thread{[&, index] {
      endpoint ep{make_config("peering-events", index, disable_ssl)};
      *ep_ids[index] = ep.node_id();
      store services;
      auto port = ep.listen();
      if (port == 0)
        hard_error("endpoint ", to_string(ep.node_id()),
                   " failed to open a port");
      ports[index] = port;
      std::map<endpoint_id, std::future<bool>> peer_results;
      listening.arrive_and_wait();
      for (size_t i = 0; i != num_endpoints; ++i) {
        if (i != index) {
          auto p = ports[i].load();
          peer_results.emplace(*ep_ids[i], ep.peer_async("localhost", p, 1s));
        }
      }
      for (auto& [other_id, res] : peer_results) {
        if (!res.get()) {
          SYNC_CHECK_FAILED("endpoint ", to_string(ep.node_id()),
                            " failed to connect to ", to_string(other_id));
        }
      }
      peered.arrive_and_wait();
      if (index != 0) {
        if (auto maybe_services = ep.attach_clone("zeek/known/services", 0.5);
            SYNC_CHECK(maybe_services)) {
          services = std::move(*maybe_services);
          std::vector<store::proxy> proxies;
          for (int i = 0; i < 3; ++i) {
            proxies.emplace_back(services);
            for (size_t req_id = 1; req_id < 4; ++req_id) {
              auto actual_id = proxies.back().put_unique("foo", "bar");
              SYNC_CHECK_EQUAL(req_id, actual_id);
            }
          }
          for (auto& px : proxies) {
            for (size_t req_id = 1; req_id < 4; ++req_id) {
              auto resp = px.receive();
              if (SYNC_CHECK(resp.id == req_id)) {
                if (SYNC_CHECK(resp.answer)) {
                  auto vals_ptr = ep_values[index];
                  vals_ptr->emplace_back(*resp.answer);
                }
                if (auto val = services.get("foo"s);
                    SYNC_CHECK(val) && SYNC_CHECK(is<std::string>(*val)))
                  SYNC_CHECK_EQUAL(get<std::string>(*val), "bar");
              }
            }
            SYNC_CHECK(px.mailbox().empty());
          }
          auto idle_res = services.await_idle();
          SYNC_CHECK(idle_res);
          written_to_store.arrive_and_wait();
        }
      } else {
        if (auto maybe_services = ep.attach_master("zeek/known/services",
                                                   backend::memory)) {
          services = std::move(*maybe_services);
        } else {
          SYNC_CHECK_FAILED("attach_master failed: ",
                            to_string(maybe_services.error()));
        }
        written_to_store.arrive_and_wait();
        auto keys_data = services.keys();
        REQUIRE(keys_data);
        REQUIRE(is<set>(*keys_data));
        auto keys = std::move(get<set>(*keys_data));
        CHECK_EQUAL(keys, set({"foo"}));
      }
    }};
  }
  for (auto& hdl : threads)
    hdl.join();
  std::vector<data> results;
  for (size_t index = 0; index != num_endpoints; ++index) {
    auto vals = ep_values[index];
    results.insert(results.end(), vals->begin(), vals->end());
  }
  CHECK_EQ(results.size(), (num_endpoints - 1) * 3 * 3);
  CHECK_EQ(std::count(results.begin(), results.end(), data{true}), 1);
  CHECK_EQ(std::count(results.begin(), results.end(), data{false}),
           static_cast<int>(results.size() - 1));
}

SCENARIO("handshake fails if only one side enables encryption") {
  GIVEN("a server with SSL enabled and a client with SSL disabled") {
    WHEN("calling endpoint::peer on the client") {
      THEN("the handshake fails") {
        auto port_promise = std::promise<uint16_t>{};
        auto port_future = port_promise.get_future();
        barrier checkpoint{2};
        auto t1 = std::thread{[&]() mutable {
          endpoint ep{make_config("ssl-mismatch-1", 0, enable_ssl)};
          auto port = ep.listen("127.0.0.1", 0);
          if (port == 0)
            hard_error("endpoint::listen failed");
          MESSAGE("first endpoint listening on port " << port);
          port_promise.set_value(port);
          checkpoint.arrive_and_wait();
        }};
        auto t2 = std::thread{[&, port{port_future.get()}] {
          endpoint ep{make_config("ssl-mismatch-1", 1, disable_ssl)};
          auto sub = ep.make_status_subscriber();
          ep.peer_nosync("127.0.0.1", port, 0s);
          auto msg = sub.get(1s);
          if (CHECK(std::holds_alternative<error>(msg))) {
            auto& err = std::get<error>(msg);
            CHECK_EQ(err, ec::peer_unavailable);
          }
          checkpoint.arrive_and_wait();
        }};
        t1.join();
        t2.join();
      }
    }
  }
  GIVEN("a server with SSL disabled and a client with SSL enabled") {
    WHEN("calling endpoint::peer on the client") {
      THEN("the handshake fails") {
        auto port_promise = std::promise<uint16_t>{};
        auto port_future = port_promise.get_future();
        barrier checkpoint{2};
        auto t1 = std::thread{[&]() mutable {
          endpoint ep{make_config("ssl-mismatch-2", 0, disable_ssl)};
          auto port = ep.listen("127.0.0.1", 0);
          if (port == 0)
            hard_error("endpoint::listen failed");
          MESSAGE("first endpoint listening on port " << port);
          port_promise.set_value(port);
          checkpoint.arrive_and_wait();
        }};
        auto t2 = std::thread{[&, port{port_future.get()}] {
          endpoint ep{make_config("ssl-mismatch-2", 1, enable_ssl)};
          auto sub = ep.make_status_subscriber();
          ep.peer_nosync("127.0.0.1", port, 0s);
          auto msg = sub.get(1s);
          if (CHECK(std::holds_alternative<error>(msg))) {
            auto& err = std::get<error>(msg);
            CHECK_EQ(err, ec::peer_unavailable);
          }
          checkpoint.arrive_and_wait();
        }};
        t1.join();
        t2.join();
      }
    }
  }
}

FIXTURE_SCOPE_END()
