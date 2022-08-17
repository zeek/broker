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
std::string store_backend = "memory";

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

thread_local std::minstd_rand rng;

unsigned
random_number(unsigned min_value = 0,
              unsigned upper_bound = std::numeric_limits<unsigned>::max()) {
  // Ideally, we would use std::uniform_int_distribution here. However, some of
  // our use cases have a dynamic upper bound. Creating a new distribution
  // object each time would essentially boil down to the same.
  return (rng() % (upper_bound - min_value)) + min_value;
}

template <class T, size_t N>
const T& choose_from(const T (&xs)[N]) {
  auto index = random_number(0, static_cast<unsigned>(N));
  return xs[index];
}

const data& choose_from(const set& xs) {
  auto index = random_number(0, static_cast<unsigned>(xs.size()));
  auto i = xs.begin();
  std::advance(i, index);
  return *i;
}

template <class T>
const T& choose_from(const std::vector<T>& xs) {
  return xs[random_number(0, static_cast<unsigned>(xs.size()))];
}

std::optional<timespan> random_expiry() {
  std::optional<timespan> result;
  if (auto timeout = random_number(0, 86400); timeout > 0) {
    result.emplace(std::chrono::seconds{timeout});
  }
  return result;
}

std::string random_string(size_t n) {
  const char charset[] = "0123456789"
                         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "abcdefghijklmnopqrstuvwxyz";
  std::string result;
  result.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    result.push_back(choose_from(charset));
  }
  return result;
}

vector createEventArgs() {
  auto random_count = []() -> uint64_t { return random_number(); };
  switch (event_type) {
    case 1: {
      return std::vector<data>{42, "test"};
    }

    case 2: {
      // This resembles a line in conn.log.
      address a1;
      address a2;
      convert("1.2.3.4", a1);
      convert("3.4.5.6", a2);

      return vector{now(),
                    random_string(10),
                    vector{a1, port(4567, port::protocol::tcp), a2,
                           port(80, port::protocol::tcp)},
                    enum_value("tcp"),
                    random_string(10),
                    std::chrono::duration_cast<timespan>(
                      std::chrono::duration<double>(3.14)),
                    random_count(),
                    random_count(),
                    random_string(5),
                    true,
                    false,
                    random_count(),
                    random_string(10),
                    random_count(),
                    random_count(),
                    random_count(),
                    random_count(),
                    set({random_string(10), random_string(10)})};
    }

    case 3: {
      table m;

      for (int i = 0; i < 100; i++) {
        set s;
        for (int j = 0; j < 10; j++)
          s.insert(random_string(5));
        m[random_string(15)] = s;
      }

      return vector{now(), m};
    }

    default:
      std::cerr << "invalid event type\n";
      abort();
  }
}

void send_batch(endpoint& ep, publisher& p) {
  auto name = "event_" + std::to_string(event_type);
  vector batch;
  for (int i = 0; i < batch_size; i++) {
    auto ev = zeek::Event(std::string(name), createEventArgs());
    batch.emplace_back(std::move(ev));
  }
  total_sent += batch.size();
  p.publish(std::move(batch));
}

const vector* inner_vector(const vector& vec) {
  for (auto& x : vec)
    if (auto ptr = get_if<vector>(&x))
      return inner_vector(*ptr);
  return &vec;
}

void receivedStats(endpoint& ep, const data& x) {
  // Example for an x: '[1, 1, [stats_update, [1ns, 1ns, 0]]]'.
  // We are only interested in the '[1ns, 1ns, 0]' part (the inner vector).
  if (!is<vector>(x)) {
    std::cerr << "received invalid stats (not a vector): " << to_string(x)
              << '\n';
    return;
  }
  auto inner = inner_vector(get<vector>(x));
  if (inner->size() != 3) {
    std::cerr << "received invalid stats (most inner vector has size "
              << inner->size() << ", expected 3): " << to_string(x) << '\n';
    return;
  }
  auto& rec = *inner;

  double t;
  convert(get<timestamp>(rec[0]), t);

  double dt_recv;
  convert(get<timespan>(rec[1]), dt_recv);

  auto ev1 = get<count>(rec[2]);
  auto all_recv = ev1;
  total_recv += ev1;

  auto all_sent = (total_sent - last_sent);

  double now;
  convert(broker::now(), now);
  double dt_sent = (now - last_t);

  auto recv_rate = (double(all_recv) / dt_recv);
  auto send_rate = double(total_sent - last_sent) / dt_sent;
  auto in_flight = (total_sent - total_recv);

  std::cerr << to_string(t) << " "
            << "[batch_size=" << batch_size << "] "
            << "in_flight=" << in_flight << " "
            << "d_t=" << dt_recv << " "
            << "d_recv=" << all_recv << " "
            << "d_sent=" << all_sent << " "
            << "total_recv=" << total_recv << " "
            << "total_sent=" << total_sent << " "
            << "[sending at " << send_rate << " ev/s, receiving at "
            << recv_rate << " ev/s\n";

  last_t = now;
  last_sent = total_sent;

  if (max_received && total_recv > max_received) {
    zeek::Event ev("quit_benchmark", std::vector<data>{});
    ep.publish("/benchmark/terminate", ev);
    std::this_thread::sleep_for(2s); // Give clients a bit.
    exit(0);
  }

  static int max_exceeded_counter = 0;
  if (max_in_flight && in_flight > max_in_flight) {
    if (++max_exceeded_counter >= 5) {
      std::cerr << "max-in-flight exceeded for 5 subsequent batches\n";
      exit(1);
    }
  } else
    max_exceeded_counter = 0;
}

struct source_state {
  static inline const char* name = "broker.benchmark.source";
};

void store_writer_loop(endpoint& ep, bool verbose, status_subscriber& ss);

void client_loop(endpoint& ep, bool verbose, status_subscriber& ss);

void client_mode(endpoint& ep, bool verbose, const std::string& host,
                 int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to /benchmark/stats to print server updates.
  ep.subscribe(
    {"/benchmark/stats"},
    [] {
      // Init: nop.
    },
    [&](data_message x) {
      // Print everything we receive.
      receivedStats(ep, move_data(x));
    },
    [](const error&) {
      // Cleanup: nop.
    });
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
  if (event_type == 4) {
    store_writer_loop(ep, verbose, ss);
  } else {
    client_loop(ep, verbose, ss);
  }
}

void store_writer_loop(endpoint& ep, bool verbose, status_subscriber& ss) {
  std::cout << "*** run store-writer loop\n";
  auto st = ep.attach_clone("broker-benchmark");
  if (!st) {
    std::cerr << "unable to attach the data store: " << to_string(st.error())
              << '\n';
    return;
  }
  std::vector<std::string> keys;
  keys.reserve(100);
  for (size_t i = 0; i < 100; ++i) {
    keys.emplace_back("key-");
    keys.back() += std::to_string(i);
  }
  auto do_random_operation = [&st, &keys] {
    // Creates a random integer between 0 and 100.
    auto random_integer_data = [] {
      return data{static_cast<integer>(random_number(0, 100))};
    };
    // Creates a random number, set, vector, table or string.
    auto random_data = [] {
      switch (random_number(0, 5)) {
        case 0: {
          return data{random_string(8)};
        }
        case 1: {
          return data{static_cast<integer>(random_number(0, 100))};
        }
        case 2: {
          vector result;
          auto n = random_number(2, 10);
          for (unsigned i = 0; i < n; ++i) {
            result.emplace_back(random_string(8));
          }
          return data{std::move(result)};
        }
        case 3: {
          table result;
          auto n = random_number(2, 10);
          for (unsigned i = 0; i < n; ++i) {
            result.emplace(data{random_string(8)}, data{random_string(8)});
          }
          return data{std::move(result)};
        }
        default: {
          set result;
          auto n = random_number(2, 10);
          for (unsigned i = 0; i < n; ++i) {
            result.emplace(random_string(8));
          }
          return data{std::move(result)};
        }
      }
    };
    // Try until an operation has been selected.
    for (;;) {
      switch (random_number(0, 11)) {
        case 0:
          st->put(data{choose_from(keys)}, random_data(), random_expiry());
          return;
        case 1:
          st->erase(choose_from(keys));
          return;
          break;
        case 2:
          st->append(data{choose_from(keys)}, data{random_string(8)},
                     random_expiry());
          return;
        case 3:
          st->increment(data{choose_from(keys)}, random_integer_data(),
                        random_expiry());
          return;
        case 4:
          st->decrement(data{choose_from(keys)}, random_integer_data(),
                        random_expiry());
          return;
        case 5:
          st->insert_into(data{choose_from(keys)}, data{random_string(8)},
                          random_expiry());
          return;
        case 6:
          st->insert_into(data{choose_from(keys)}, data{random_string(8)},
                          data{random_string(8)}, random_expiry());
          return;
        case 7:
          st->remove_from(data{choose_from(keys)}, data{random_string(8)},
                          random_expiry());
          return;
        case 8:
          st->push(data{choose_from(keys)}, random_data(), random_expiry());
          return;
        case 9:
          st->pop(data{choose_from(keys)});
          return;
        default:
          st->put_unique(data{choose_from(keys)}, random_data(),
                         random_expiry());
          return;
      }
    }
  };
  auto print_status_events = [&verbose, &ss] {
    auto status_events = ss.poll();
    if (verbose)
      for (auto& ev : status_events)
        std::visit([](auto& x) { std::cout << to_string(x) << std::endl; }, ev);
  };
  if (batch_rate == 0) {
    // Run as fast as possible, but wait once per second for the master to catch
    // up.
    timestamp next_wait = std::chrono::system_clock::now() + 1s;
    for (;;) {
      do_random_operation();
      print_status_events();
      timestamp t = std::chrono::system_clock::now();
      if (next_wait >= t) {
        st->await_idle();
        next_wait = std::chrono::system_clock::now() + 1s;
      }
    }
  } else {
    std::cerr << "NOT IMPLEMENTED\n";
  }
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
          out.emplace_back("/benchmark/events",
                           zeek::Event(std::move(name), createEventArgs()));
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
  // Spin up a store if a backend is configured.
  backend_options store_opts;
  store_opts["path"] = "broker-bench-tmp.sqlite";
  unlink("broker-bench-tmp.sqlite"); // make sure we always start fresh
  auto st = ep.attach_master("broker-benchmark",
                             store_backend == "sqlite" ? backend::sqlite
                                                       : backend::memory,
                             std::move(store_opts));
  if (!st) {
    std::cerr << "*** failed to start store: " << to_string(st.error()) << '\n';
    return;
  }
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to /benchmark/events.
  ep.subscribe(
    {"/benchmark/events"},
    [] {
      // Init: nop.
    },
    [](data_message x) {
      // OnNext: increase the global num_events counter.
      auto msg = move_data(x);
      // Count number of events (counts each element in a batch as one event).
      if (zeek::Message::type(msg) == zeek::Message::Type::Batch) {
        zeek::Batch batch(std::move(msg));
        num_events += batch.batch().size();
      } else {
        ++num_events;
      }
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
    auto stats = vector{now, now - last_time, count{reset_num_events()}};
    if (verbose)
      std::cout << "stats: " << to_string(stats) << std::endl;
    zeek::Event ev("stats_update", std::move(stats));
    ep.publish("/benchmark/stats", std::move(ev));
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
                 "1 (vector, default) | 2 (conn log entry) | 3 (table) "
                 "| 4 (store commands)");
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
  cfg.add_option(&store_backend, "store-backend", "memory (default) or sqlite");
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
    if (store_backend != "sqlite" && store_backend != "memory") {
      std::cerr << "*** invalid store backend, must be 'sqlite' or 'memory'\n";
      return EXIT_FAILURE;
    }
    VERBOSE_OUT << "*** run in server mode\n";
    server_mode(ep, verbose, host, port);
  } else {
    VERBOSE_OUT << "*** run in client mode\n";
    client_mode(ep, verbose, host, port);
  }
  return EXIT_SUCCESS;
}
