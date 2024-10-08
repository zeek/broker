#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "broker/builder.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/topic.hh"
#include "broker/zeek.hh"

#ifndef BROKER_WINDOWS
#  include <fcntl.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <unistd.h>
#endif // BROKER_WINDOWS

using namespace broker;
using namespace std::literals;

#define VERBOSE_OUT                                                            \
  if (verbose)                                                                 \
  std::cout

namespace {

int64_t event_type = 1;
double batch_rate = 1;
int64_t batch_size = 1;
double rate_increase_interval = 0;
double rate_increase_amount = 0;
uint64_t max_received = 0;
uint64_t max_in_flight = 0;
bool server = false;
bool verbose = false;

// Global state
size_t total_recv;
size_t total_sent;
size_t last_sent;
double last_t;

std::atomic<size_t> num_events;

size_t reset_num_events() {
  auto result = num_events.load();
  if (result == 0)
    return 0;
  for (;;)
    if (num_events.compare_exchange_strong(result, 0))
      return result;
}

double current_time() {
  using namespace std::chrono;
  auto t = system_clock::now();
  auto usec = duration_cast<microseconds>(t.time_since_epoch()).count();
  return usec / 1e6;
}

static std::string random_string(int n) {
  static std::minstd_rand rng{31337};
  std::string_view charset = "0123456789"
                             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                             "abcdefghijklmnopqrstuvwxyz";

  std::string result;
  result.resize(n);
  for (auto& c : result) {
    c = charset[rng() % charset.size()];
  }
  return result;
}

static uint64_t random_count() {
  static uint64_t i = 0;
  return ++i;
}

list_builder createEventArgs() {
  switch (event_type) {
    case 1: {
      return list_builder{}.add(42).add("test"sv);
    }

    case 2: {
      // This resembles a line in conn.log.
      address a1;
      address a2;
      convert("1.2.3.4", a1);
      convert("3.4.5.6", a2);
      return list_builder{}
        .add(now())
        .add(random_string(10))
        .add_list(a1, port{4567, port::protocol::tcp}, a2,
                  port{80, port::protocol::tcp})
        .add(enum_value("tcp"))
        .add(random_string(10))
        .add(std::chrono::duration_cast<timespan>(
          std::chrono::duration<double>(3.14)))
        .add(random_count())
        .add(random_count())
        .add(random_string(5))
        .add(true)
        .add(false)
        .add(random_count())
        .add(random_string(10))
        .add(random_count())
        .add(random_count())
        .add(random_count())
        .add(random_count())
        .add_set(random_string(10), random_string(10));
    }

    case 3: {
      std::vector<std::string> keys;
      std::vector<std::string> vals;
      table_builder tbl;
      // Generate a sorted list of 100 different random keys.
      while (keys.size() < 100) {
        auto str = random_string(15);
        auto i = std::lower_bound(keys.begin(), keys.end(), str);
        if (i == keys.end() || *i != str) {
          keys.insert(i, std::move(str));
        }
      }
      // Generate a set of 10 random values for each key.
      for (auto& key : keys) {
        vals.clear();
        while (vals.size() < 10) {
          auto str = random_string(5);
          auto i = std::lower_bound(vals.begin(), vals.end(), str);
          if (i == vals.end() || *i != str) {
            vals.insert(i, std::move(str));
          }
        }
        set_builder entry;
        for (auto& val : vals)
          entry.add(val);
        tbl.add(key, entry);
      }
      // Return a list with the current time and the table.
      return list_builder{}.add(now()).add(tbl);
    }

    default:
      std::cerr << "invalid event type\n";
      abort();
  }
}

constexpr count ProtocolVersion = 1;

constexpr count MessageTypeEvent = 1;

data_message make_event(std::string_view topic_str, std::string_view name) {
  return list_builder{}
    .add(ProtocolVersion)
    .add(MessageTypeEvent)
    .add_list(name, createEventArgs())
    .build_envelope(topic_str);
}

void send_batch(endpoint& ep, publisher& p) {
  auto name = "event_" + std::to_string(event_type);
  list_builder batch;
  for (int i = 0; i < batch_size; i++) {
    batch.add_list(ProtocolVersion, MessageTypeEvent,
                   list_builder{}.add(name).add(createEventArgs()));
  }
  total_sent += batch_size;
  p.publish(std::move(batch));
}

const vector* inner_vector(const vector& vec) {
  for (auto& x : vec)
    if (auto ptr = get_if<vector>(&x))
      return inner_vector(*ptr);
  return &vec;
}

struct source_state {
  static inline const char* name = "broker.benchmark.source";
};

void client_loop(endpoint& ep, bool verbose, status_subscriber& ss);

void client_mode(endpoint& ep, bool verbose, const std::string& host,
                 int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Publish events to /benchmark/events.
  // Connect to remote peer.
  VERBOSE_OUT << "*** init peering: host = " << host << ", port = " << port
              << '\n';
  auto res = ep.peer(host, port, timeout::seconds(1));
  if (!res) {
    std::cerr << "unable to peer to " << host << " on port " << port << '\n';
    return;
  }
  VERBOSE_OUT << "*** endpoint is now peering to remote\n";
  client_loop(ep, verbose, ss);
}

void client_loop(endpoint& ep, bool verbose, status_subscriber& ss) {
  if (batch_rate == 0) {
    ep.publish_all(
      [] {
        // Init: nop.
      },
      [](std::deque<data_message>& out, size_t hint) {
        // Pull: generate random events.
        for (size_t i = 0; i < hint; ++i) {
          auto name = "event_" + std::to_string(event_type);
          out.emplace_back(make_event("/benchmark/events", name));
          // std::cout<<"OUT: "<<out.back()->value()<<std::endl;
        }
      },
      [] {
        // AtEnd: always false since we can produce random data forever.
        return false;
      });
    for (;;) {
      // Print status events.
      auto ev = ss.get();
      if (verbose)
        std::visit([](auto& x) { std::cout << to_string(x) << std::endl; }, ev);
    }
  }
  // Publish one message per interval.
  using std::chrono::duration_cast;
  using fractional_second = std::chrono::duration<double>;
  auto p = ep.make_publisher("/benchmark/events");
  fractional_second fractional_inc_interval{rate_increase_interval};
  auto inc_interval = duration_cast<timespan>(fractional_inc_interval);
  timestamp timeout = std::chrono::system_clock::now();
  auto interval = duration_cast<timespan>(1s);
  interval /= batch_rate;
  auto interval_timeout = timeout + interval;
  for (;;) {
    // Sleep until next timeout.
    timeout += interval;
    std::this_thread::sleep_until(timeout);
    // Ship some data.
    if (p.free_capacity() > 1) {
      send_batch(ep, p);
    } else {
      std::cout << "*** skip batch: publisher queue full" << '\n';
    }
    // Increase batch size when reaching interval_timeout.
    if (rate_increase_interval > 0 && rate_increase_amount > 0) {
      auto now = std::chrono::system_clock::now();
      if (now >= interval_timeout) {
        batch_size += rate_increase_amount;
        interval_timeout += interval;
      }
    }
    // Print status events.
    auto status_events = ss.poll();
    if (verbose)
      for (auto& ev : status_events)
        std::visit([](auto& x) { std::cout << to_string(x) << std::endl; }, ev);
  }
}

void server_loop(endpoint& ep, bool verbose, status_subscriber& ss,
                 std::atomic<bool>& terminate);

// This mode mimics what benchmark.bro does.
void server_mode(endpoint& ep, bool verbose, const std::string& iface,
                 int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to /benchmark/events.
  ep.subscribe(
    {"/benchmark/events"},
    [] {
      // Init: nop.
    },
    [](const data_message& msg) {
      // std::cout<<"MESSAGE: "<<msg->value()<<std::endl;
      //  OnNext: increase the global num_events counter.
      //  FIXME:
      //  auto msg = move_data(x);
      //  // Count number of events (counts each element in a batch as one
      //  event). if (zeek::Message::type(msg) == zeek::Message::Type::Batch) {
      //    zeek::Batch batch(std::move(msg));
      //    num_events += batch.batch().size();
      //  } else {
      //    ++num_events;
      //  }
      ++num_events;
    },
    [](const error&) {
      // Cleanup: nop.
    });
  // Listen on /benchmark/terminate for stop message.
  std::atomic<bool> terminate{false};
  ep.subscribe(
    {"/benchmark/terminate"},
    [] {
      // Init: nop.
    },
    [&](data_message) {
      // OnNext: any message on this topic triggers termination.
      terminate = true;
    },
    [](const error&) {
      // Cleanup: nop.
    });
  // Start listening for peers.
  auto actual_port = ep.listen(iface, port);
  if (actual_port == 0) {
    std::cerr << "*** failed to listen on port " << port << '\n';
    return;
  } else if (verbose) {
    std::cout << "*** listening on " << actual_port << '\n';
  }
  server_loop(ep, verbose, ss, terminate);
}

void server_loop(endpoint& ep, bool verbose, status_subscriber& ss,
                 std::atomic<bool>& terminate) {
  // Collects stats once per second until receiving stop message.
  using std::chrono::duration_cast;
  timestamp timeout = std::chrono::system_clock::now();
  auto last_time = timeout;
  while (!terminate) {
    // Sleep until next timeout.
    timeout += 1s;
    std::this_thread::sleep_until(timeout);
    // Generate and publish zeek event.
    timestamp now = std::chrono::system_clock::now();
    if (verbose)
      std::cout << "rate: " << reset_num_events() << " events/s" << std::endl;
    // Advance time and print status events.
    last_time = now;
    auto status_events = ss.poll();
    if (verbose)
      for (auto& ev : status_events)
        std::visit([](auto& x) { std::cout << to_string(x) << std::endl; }, ev);
  }
}

void add_options(configuration& cfg) {
  cfg.add_option(&event_type, "event-type,t",
                 "1 (vector, default) | 2 (conn log entry) | 3 (table)");
  cfg.add_option(&batch_rate, "batch-rate,r",
                 "batches/sec (default: 1, set to 0 for infinite)");
  cfg.add_option(&batch_size, "batch-size,s", "events per batch (default: 1)");
  cfg.add_option(&rate_increase_interval, "batch-size-increase-interval,i",
                 "interval for increasing the batch size (in seconds)");
  cfg.add_option(&rate_increase_amount, "batch-size-increase-amount,a",
                 "additional batch size per interval");
  cfg.add_option(&max_received, "max-received,m",
                 "stop benchmark after given count");
  cfg.add_option(&max_in_flight, "max-in-flight,f",
                 "report when exceeding this count");
  cfg.add_option(&server, "server", "run in server mode");
  cfg.add_option(&verbose, "verbose", "enable status output");
}

void usage(const configuration& cfg, const char* cmd_name) {
  std::cerr << "Usage: " << cmd_name
            << " [<options>] <zeek-host>[:<port>] | [--disable-ssl] --server "
               "<interface>:port\n\n"
            << cfg.help_text();
}

} // namespace

int main(int argc, char** argv) {
  endpoint::system_guard sys_guard;
  configuration cfg{skip_init};
  add_options(cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << "\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  if (cfg.remainder().size() != 1) {
    std::cerr << "*** too many arguments\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  // Local variables configurable via CLI.
  auto arg = cfg.remainder().at(0);
  auto separator = arg.find(':');
  if (separator == std::string::npos) {
    std::cerr << "*** invalid argument\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  std::string host = arg.substr(0, separator);
  uint16_t port = 9999;
  try {
    auto str_port = arg.substr(separator + 1);
    if (!str_port.empty()) {
      auto int_port = std::stoi(str_port);
      if (int_port < 0 || int_port > std::numeric_limits<uint16_t>::max())
        throw std::out_of_range("not an uint16_t");
      port = static_cast<uint16_t>(int_port);
    }
  } catch (std::exception& e) {
    std::cerr << "*** invalid port: " << e.what() << "\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  // Run benchmark.
  endpoint ep(std::move(cfg));
  if (server) {
    VERBOSE_OUT << "*** run in server mode\n";
    server_mode(ep, verbose, host, port);
  } else {
    VERBOSE_OUT << "*** run in client mode\n";
    client_mode(ep, verbose, host, port);
  }
  return EXIT_SUCCESS;
}
