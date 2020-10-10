// Checks whether Broker instances shut down gracefully, i.e., Broker endpoints
// ship queued events before closing remote connections.

#define SUITE shutdown

#include "test.hh"

#include "broker/endpoint.hh"

#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <vector>

#include <sys/socket.h>
#include <sys/types.h>

using namespace broker;

namespace {

using string_list = std::vector<std::string>;

configuration make_config() {
  configuration cfg;
#if CAF_VERSION < 1800
  using caf::atom;
  cfg.set("scheduler.max-threads", 2);
  cfg.set("logger.console-verbosity", caf::atom("quiet"));
#else
  cfg.set("caf.scheduler.max-threads", 2);
  cfg.set("caf.logger.console.verbosity", "quiet");
#endif
  return cfg;
}

template <class... Ts>
[[noreturn]] void hard_error(const Ts&... xs) {
  (std::cerr << ... << xs) << '\n';
  abort();
}

// Drop-in replacement for std::barrier (based on the TS API as of 2020).
class barrier {
public:
  explicit barrier(ptrdiff_t num_threads)
    : num_threads_(num_threads), count_(0) {
    // nop
  }

  void arrive_and_wait() {
    std::unique_lock<std::mutex> guard{mx_};
    if (++count_ == num_threads_) {
      cv_.notify_all();
      return;
    }
    cv_.wait(guard, [this] { return count_.load() == num_threads_; });
  }

private:
  size_t num_threads_;
  std::mutex mx_;
  std::atomic<size_t> count_;
  std::condition_variable cv_;
};

auto code_of(const error& err) {
#if CAF_VERSION<1800
  if (err.category() != caf::atom("broker"))
    return ec::unspecified;
#else
  if (err.category() != caf::type_id_v<broker::ec>)
    return ec::unspecified;
#endif
  return static_cast<ec>(err.code());
}

auto normalize_status_log(const std::vector<data_message>& xs) {
  std::vector<std::string> lines;
  lines.reserve(xs.size());
  for (auto& x : xs) {
    if (auto err = to<error>(get_data(x)))
      lines.emplace_back(to_string(code_of(*err)));
    else if (auto stat = to<status>(get_data(x)))
      lines.emplace_back(to_string(stat->code()));
    else
      ERROR("neither a status nor an error: " << x);
  }
  return lines;
}

} // namespace

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
    endpoint ep{make_config()};
    ep.subscribe_nosync(
      {topics::statuses}, [](caf::unit_t&) {},
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
    endpoint ep{make_config()};
      ep.subscribe_nosync(
        {topics::statuses}, [](caf::unit_t&) {},
        [ep2_log](caf::unit_t&, data_message msg) {
          ep2_log->emplace_back(std::move(msg));
        },
        [](caf::unit_t&, const error&) {});
      if (!ep.peer("127.0.0.1", port))
        hard_error("endpoint::listen failed");
      MESSAGE("second endpoint peered to 127.0.0.1:" << port);
    }
    checkpoint.arrive_and_wait();
  }};
  t1.join();
  t2.join();
  MESSAGE("both endpoint were shut down");
  // Now, ep2 actively closed the peering and should report `peer_removed`,
  // whereas ep1 should `peer_lost` instead.
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
