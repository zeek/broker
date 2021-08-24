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

#define CHECK_FAILED(...)                                                      \
  ([](auto... xs) {                                                            \
    std::ostringstream str;                                                    \
    (str << ... << xs);                                                        \
    std::unique_lock guard{print_mtx};                                         \
    caf::test::detail::check_un(false, __FILE__, __LINE__, str.str().c_str()); \
  })(__VA_ARGS__)

using namespace broker;
using namespace std::literals;

namespace {

using string_list = std::vector<std::string>;

std::string log_path_template(const char* test_name, size_t endpoint_nr) {
  std::string result;
  result += broker::to_string(test_name);
  result += ' ';
  result += test_name;
  result += ' ';
  result += "ep";
  result += std::to_string(endpoint_nr);
  result += ".log";
  return result;
}

configuration make_config(const char* test_name, size_t endpoint_nr) {
  configuration cfg;
  cfg.set("caf.scheduler.max-threads", 2);
  cfg.set("caf.logger.console.verbosity", "quiet");
  cfg.set("caf.logger.file.path", log_path_template(test_name, endpoint_nr));
  // cfg.set("caf.logger.file.verbosity", "trace");
  // cfg.set("caf.logger.file.excluded-components", std::vector<std::string>{});
  // cfg.set("broker.metrics.export.topic", "my/metrics/node-" + std::to_string(endpoint_nr));
  // cfg.set("broker.metrics.export.prefixes", std::vector{"caf"s,"broker"s});
  // if (endpoint_nr == 0) {
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
    t0 = broker::now();
  }

  broker::timestamp t0;
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
  signal(SIGPIPE, +[](int){__builtin_trap();});
  static constexpr size_t num_endpoints = 4;
  MESSAGE("initialize state");
  using data_message_list = std::vector<data_message>;
  std::array<std::shared_ptr<data_message_list>, num_endpoints> ep_logs;
  for (auto& ptr : ep_logs)
    ptr = std::make_shared<data_message_list>();
  std::array<std::atomic<uint16_t>, num_endpoints> ports;
  barrier listening{num_endpoints};
  barrier peered{num_endpoints};
  barrier send_and_received{num_endpoints};
  std::array<std::thread, num_endpoints> threads;
  std::array<std::atomic<caf::uuid>, num_endpoints> ep_ids;
  MESSAGE("spin up threads");
  for (size_t index = 0; index != num_endpoints; ++index) {
    threads[index] = std::thread{[&, index] {
      auto log_ptr = ep_logs[index];
      endpoint ep{make_config("peering-events", index)};
      ep_ids[index] = ep.node_id();
      barrier got_hellos{2};
      ep.subscribe_nosync(
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
          peer_results.emplace(ep_ids[i].load(),
                               ep.peer_async("localhost", p, 0s));
        }
      }
      for (auto& [other_id, res] : peer_results) {
        if (!res.get()) {
          CHECK_FAILED("endpoint ", to_string(ep.node_id()),
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
  auto sequence
    = std::vector<alternative>{"endpoint_discovered"_a, "peer_added"_a,
                               alternative{"peer_lost", "peer_removed"},
                               "endpoint_unreachable"_a};
  // Check all endpoints. We "unroll the loop" for better error localization.
  for (size_t index = 0; index != num_endpoints; ++index) {
    for (size_t i = 0; i != num_endpoints; ++i)
      if (i != index)
        CHECK_EQUAL(grep_id(normalized_logs[index], ep_ids[i]), sequence);
    CHECK_EQUAL(sort(grep_hello(*ep_logs[index])), hellos[index]);
  }
}

FIXTURE_SCOPE_END()
