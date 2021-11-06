#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <caf/async/bounded_buffer.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>

#include "broker/alm/lamport_timestamp.hh"
#include "broker/config.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/detail/connector.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/topic.hh"
#include "broker/zeek.hh"

#ifndef BROKER_WINDOWS
#  include <sys/types.h>
#  include <sys/socket.h>
#  include <unistd.h>
#  include <fcntl.h>
#endif // BROKER_WINDOWS

using namespace broker;
using namespace std::literals;

namespace {

int event_type = 1;
double batch_rate = 1;
int batch_size = 1;
double rate_increase_interval = 0;
double rate_increase_amount = 0;
uint64_t max_received = 0;
uint64_t max_in_flight = 0;
bool server = false;
bool verbose = false;
bool store_mode = false;

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
    static unsigned int i = 0;
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

     const size_t max_index = (sizeof(charset) - 1);
     char buffer[11];
     for ( unsigned int j = 0; j < sizeof(buffer) - 1; j++ )
    buffer[j] = charset[++i % max_index];
     buffer[sizeof(buffer) - 1] = '\0';

     return buffer;
}

static uint64_t random_count() {
    static uint64_t i = 0;
    return ++i;
}

vector createEventArgs() {
    switch ( event_type ) {
     case 1: {
         return std::vector<data>{42, "test"};
     }

     case 2: {
         // This resembles a line in conn.log.
         address a1;
         address a2;
         convert("1.2.3.4", a1);
         convert("3.4.5.6", a2);

         return vector{
             now(),
             random_string(10),
             vector{
                 a1,
                 port(4567, port::protocol::tcp),
                 a2,
                 port(80, port::protocol::tcp)
             },
             enum_value("tcp"),
             random_string(10),
             std::chrono::duration_cast<timespan>(std::chrono::duration<double>(3.14)),
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
             set({random_string(10), random_string(10)})
        };
     }

     case 3: {
         table m;

         for ( int i = 0; i < 100; i++ ) {
             set s;
             for ( int j = 0; j < 10; j++ )
                 s.insert(random_string(5));
             m[random_string(15)] = s;
         }

         return vector{now(), m};
     }

     default:
       std::cerr << "invalid event type" << std::endl;
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

void receivedStats(endpoint& ep, data x) {
  // Example for an x: '[1, 1, [stats_update, [1ns, 1ns, 0]]]'.
  // We are only interested in the '[1ns, 1ns, 0]' part.
  auto xvec = get<vector>(x);
  auto yvec = get<vector>(xvec[2]);
  auto rec = get<vector>(yvec[1]);

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
            << recv_rate << " ev/s " << std::endl;

  last_t = now;
  last_sent = total_sent;

  if (max_received && total_recv > max_received) {
    zeek::Event ev("quit_benchmark", std::vector<data>{});
    ep.publish("benchmark/terminate", ev);
    std::this_thread::sleep_for(2s); // Give clients a bit.
    exit(0);
  }

  static int max_exceeded_counter = 0;
  if (max_in_flight && in_flight > max_in_flight) {

    if (++max_exceeded_counter >= 5) {
      std::cerr << "max-in-flight exceeded for 5 subsequent batches"
                << std::endl;
      exit(1);
    }
  } else
    max_exceeded_counter = 0;
}

struct source_state {
  static inline const char* name = "broker.benchmark.source";
};

void client_loop(endpoint& ep, status_subscriber& ss, bool verbose_output);

void client_mode(endpoint& ep, const std::string& host, int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to benchmark/stats to print server updates.
  ep.subscribe(
    {"benchmark/stats"},
    [](caf::unit_t&) {
      // nop
    },
    [&](caf::unit_t&, data_message x) {
      // Print everything we receive.
      receivedStats(ep, move_data(x));
    },
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // Publish events to benchmark/events.
  // Connect to remote peer.
  if (verbose)
    std::cout << "*** init peering: host = " << host << ", port = " << port
              << std::endl;
  auto res = ep.peer(host, port, timeout::seconds(1));
  if (!res) {
    std::cerr << "unable to peer to " << host << " on port " << port
              << std::endl;
    return;
  }
  if (verbose)
    std::cout << "*** endpoint is now peering to remote" << std::endl;
  client_loop(ep, ss, verbose);
}

void client_loop(endpoint& ep, status_subscriber& ss, bool verbose_output) {
  if (batch_rate == 0) {
    auto pub = ep.make_publisher("benchmark/events");
    auto producer = std::thread{[pub{std::move(pub)}]() mutable {
      for (;;) {
        auto name = "event_" + std::to_string(event_type);
        pub.publish(zeek::Event{std::move(name), createEventArgs()});
      }
    }};
    for (;;) {
      // Print status events.
      auto ev = ss.get();
      if (verbose_output)
        std::cout << caf::deep_to_string(ev) << std::endl;
    }
  }
  // Publish one message per interval.
  using std::chrono::duration_cast;
  using fractional_second = std::chrono::duration<double>;
  auto p = ep.make_publisher("benchmark/events");
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
      std::cout << "*** skip batch: publisher queue full" << std::endl;
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
    if (verbose_output)
      for (auto& ev : status_events)
        std::cout << caf::deep_to_string(ev) << std::endl;
  }
}

void server_loop(endpoint& ep, status_subscriber& ss,
                 std::atomic<bool>& terminate, bool verbose_output);

// This mode mimics what benchmark.bro does.
void server_mode(endpoint& ep, const std::string& iface, int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to benchmark/events.
  ep.subscribe(
    // Filter.
    {"benchmark/events"},
    // Init.
    [](caf::unit_t&) {
      // nop
    },
    // OnNext.
    [](caf::unit_t&, data_message x) {
      auto msg = move_data(x);
      // Count number of events (counts each element in a batch as one event).
      if (zeek::Message::type(msg) == zeek::Message::Type::Event) {
        ++num_events;
      } else if (zeek::Message::type(msg) == zeek::Message::Type::Batch) {
        zeek::Batch batch(std::move(msg));
        num_events += batch.batch().size();
      } else {
        std::cerr << "unexpected message type" << std::endl;
        exit(1);
      }
    },
    // Cleanup.
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // Listen on benchmark/terminate for stop message.
  std::atomic<bool> terminate{false};
  ep.subscribe(
    {"benchmark/terminate"},
    [](caf::unit_t&) {
      // nop
    },
    [&](caf::unit_t&, data_message) {
      // Any message on this topic triggers termination.
      terminate = true;
    },
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // Start listening for peers.
  auto actual_port = ep.listen(iface, port);
  if (actual_port == 0) {
    std::cerr << "*** failed to listen on port " << port << '\n';
    return;
  } else if (verbose) {
    std::cout << "*** listening on " << actual_port << '\n';
  }
  server_loop(ep, ss, terminate, verbose);
}

void server_loop(endpoint& ep, status_subscriber& ss,
                 std::atomic<bool>& terminate, bool verbose_output) {
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
    if (verbose_output)
      std::cout << "stats: " << caf::deep_to_string(stats) << std::endl;
    zeek::Event ev("stats_update", vector{std::move(stats)});
    ep.publish("benchmark/stats", std::move(ev));
    // Advance time and print status events.
    last_time = now;
    auto status_events = ss.poll();
    if (verbose_output)
      for (auto& ev : status_events)
        std::cout << caf::deep_to_string(ev) << std::endl;
  }
  std::cout << "received stop message on benchmark/terminate" << std::endl;
}

struct peer_setup {
  endpoint_id peer;
  network_info addr;
  filter_type filter;
};

// Server running with the client in the same process, communicating directly
// over bounded buffers rather than communicating over a network.
void bridged_server_mode(endpoint& ep, detail::node_consumer_res con,
                         detail::node_producer_res prod,
                         std::promise<peer_setup>& prom,
                         std::future<peer_setup>& fut) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to benchmark/events.
  ep.subscribe(
    // Filter.
    {"benchmark/events"},
    // Init.
    [](caf::unit_t&) {
      // nop
    },
    // OnNext.
    [](caf::unit_t&, data_message x) {
      auto msg = move_data(x);
      // Count number of events (counts each element in a batch as one event).
      if (zeek::Message::type(msg) == zeek::Message::Type::Event) {
        ++num_events;
      } else if (zeek::Message::type(msg) == zeek::Message::Type::Batch) {
        zeek::Batch batch(std::move(msg));
        num_events += batch.batch().size();
      } else {
        std::cerr << "unexpected message type" << std::endl;
        exit(1);
      }
    },
    // Cleanup.
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // "Handshake" with the client.
  prom.set_value(
    peer_setup{ep.node_id(), network_info{"flow:client", 1234}, ep.filter()});
  auto cl = fut.get();
  caf::anon_send(ep.core(), atom::peer_v, std::move(cl.peer),
                 std::move(cl.addr), alm::lamport_timestamp{},
                 std::move(cl.filter), std::move(con), std::move(prod));
  // Run loop.
  std::atomic<bool> terminate{false};
  server_loop(ep, ss, terminate, true);
}

// Client running with the server in the same process, communicating directly
// over bounded buffers rather than communicating over a network.
void bridged_client_mode(endpoint& ep, detail::node_consumer_res con,
                         detail::node_producer_res prod,
                         std::promise<peer_setup>& prom,
                         std::future<peer_setup>& fut) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // "Handshake" with the server.
  prom.set_value(
    peer_setup{ep.node_id(), network_info{"flow:server", 4321}, ep.filter()});
  auto sv = fut.get();
  caf::anon_send(ep.core(), atom::peer_v, std::move(sv.peer),
                 std::move(sv.addr), alm::lamport_timestamp{},
                 std::move(sv.filter), std::move(con), std::move(prod));
  // Run loop.
  client_loop(ep, ss, false);
}

// Runs server and client in this process (but on separate threads) and connects
// them directly with bounded buffer queues.
void single_process_mode(configuration& cfg) {
  using caf::async::make_bounded_buffer_resource;
  auto [con1, prod1] = make_bounded_buffer_resource<node_message>();
  auto [con2, prod2] = make_bounded_buffer_resource<node_message>();
  std::promise<peer_setup> prom1;
  auto fut1 = prom1.get_future();
  std::promise<peer_setup> prom2;
  auto fut2 = prom2.get_future();
  auto server_fn = [&cfg, &prom1, &fut2, con1{con1}, prod2{prod2}]() mutable {
    endpoint ep{std::move(cfg)};
    bridged_server_mode(ep, std::move(con1), std::move(prod2), prom1, fut2);
  };
  auto server = std::thread{server_fn};
  auto client_fn = [&prom2, &fut1, con2{con2}, prod1{prod1}]() mutable {
    endpoint ep;
    bridged_client_mode(ep, std::move(con2), std::move(prod1), prom2, fut1);
  };
  auto client = std::thread{client_fn};
  server.join();
  client.join();
}

#ifndef BROKER_WINDOWS

// Server running with the client in the same process, communicating over a
// connected socket pair (UNIX stream sockets).
void socketpair_server_mode(endpoint& ep, detail::native_socket fd,
                            std::promise<peer_setup>& prom,
                            std::future<peer_setup>& fut) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Subscribe to benchmark/events.
  ep.subscribe(
    // Filter.
    {"benchmark/events"},
    // Init.
    [](caf::unit_t&) {
      // nop
    },
    // OnNext.
    [](caf::unit_t&, data_message x) {
      auto msg = move_data(x);
      // Count number of events (counts each element in a batch as one event).
      if (zeek::Message::type(msg) == zeek::Message::Type::Event) {
        ++num_events;
      } else if (zeek::Message::type(msg) == zeek::Message::Type::Batch) {
        zeek::Batch batch(std::move(msg));
        num_events += batch.batch().size();
      } else {
        std::cerr << "unexpected message type" << std::endl;
        exit(1);
      }
    },
    // Cleanup.
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // "Handshake" with the client.
  prom.set_value(
    peer_setup{ep.node_id(), network_info{"flow:client", 1234}, ep.filter()});
  auto cl = fut.get();
  caf::anon_send(ep.core(), detail::invalid_connector_event_id,
                 caf::make_message(std::move(cl.peer), std::move(cl.addr),
                                   alm::lamport_timestamp{},
                                   std::move(cl.filter), fd));
  // Run loop.
  std::atomic<bool> terminate{false};
  server_loop(ep, ss, terminate, true);
}

// Client running with the server in the same process, communicating over a
// connected socket pair (UNIX stream sockets).
void socketpair_client_mode(endpoint& ep, detail::native_socket fd,
                            std::promise<peer_setup>& prom,
                            std::future<peer_setup>& fut) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // "Handshake" with the server.
  prom.set_value(
    peer_setup{ep.node_id(), network_info{"flow:server", 4321}, ep.filter()});
  auto sv = fut.get();
  caf::anon_send(ep.core(), detail::invalid_connector_event_id,
                 caf::make_message(std::move(sv.peer), std::move(sv.addr),
                                   alm::lamport_timestamp{},
                                   std::move(sv.filter), fd));
  // Run loop.
  client_loop(ep, ss, false);
}

//
void single_process_with_sockets_mode(configuration& cfg) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
    std::cerr << "socketpair syscall failed\n";
    abort();
  }
  auto make_nonblocking = [](detail::native_socket fd) {
    auto rf = fcntl(fd, F_GETFL, 0);
    if (rf == -1) {
      std::cerr << "fcntl failed\n";
      abort();
    }
    auto wf = rf | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, wf) == -1) {
      std::cerr << "fcntl failed\n";
      abort();
    }
  };
  make_nonblocking(sockets[0]);
  make_nonblocking(sockets[1]);
  std::promise<peer_setup> prom1;
  auto fut1 = prom1.get_future();
  std::promise<peer_setup> prom2;
  auto fut2 = prom2.get_future();
  auto server_fn = [&cfg, &prom1, &fut2, fd{sockets[0]}]() mutable {
    endpoint ep{std::move(cfg)};
    socketpair_server_mode(ep, fd, prom1, fut2);
  };
  auto server = std::thread{server_fn};
  auto client_fn = [&prom2, &fut1, fd{sockets[1]}]() mutable {
    endpoint ep;
    socketpair_client_mode(ep, fd, prom2, fut1);
  };
  auto client = std::thread{client_fn};
  server.join();
  client.join();
}

#endif // BROKER_WINDOWS

void store_master_mode(endpoint& ep, const std::string& iface, int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Spin up the data store.
  auto store = ep.attach_master("benchmark-store", backend::memory);
  // Listen on benchmark/terminate for stop message.
  std::atomic<bool> terminate{false};
  ep.subscribe(
    {"benchmark/terminate"},
    [](caf::unit_t&) {
      // nop
    },
    [&](caf::unit_t&, data_message) {
      // Any message on this topic triggers termination.
      terminate = true;
    },
    [](caf::unit_t&, const caf::error&) {
      // nop
    });
  // Start listening for peers.
  auto actual_port = ep.listen(iface, port);
  if (actual_port == 0) {
    std::cerr << "*** failed to listen on port " << port << '\n';
    return;
  } else if (verbose) {
    std::cout << "*** listening on " << actual_port << '\n';
  }
  // Collects stats once per second until receiving stop message.
  while (!terminate) {
    std::this_thread::sleep_for(1s);
  }
  std::cout << "received stop message on benchmark/terminate" << std::endl;
}

void store_clone_mode(endpoint& ep, const std::string& host, int port) {
  // Make sure to receive status updates.
  auto ss = ep.make_status_subscriber(true);
  // Connect to remote peer.
  if (verbose)
    std::cout << "*** init peering: host = " << host << ", port = " << port
              << std::endl;
  auto res = ep.peer(host, port, timeout::seconds(1));
  if (!res) {
    std::cerr << "*** unable to peer to " << host << " on port " << port
              << std::endl;
    return;
  }
  if (verbose)
    std::cout << "*** endpoint is now peering to remote" << std::endl;
  auto maybe_store = ep.attach_clone("benchmark-store");
  if (!maybe_store) {
    std::cerr << "** unable to attach clone: " << to_string(maybe_store.error())
              << '\n';
    return;
  }
  auto& store = *maybe_store;
  std::atomic<int64_t> writes{0};
  auto stats_printer = std::thread{[&writes] {
    int64_t old_value = 0;
    auto t = std::chrono::system_clock::now();
    std::cout << std::setprecision(4);
    for (;;) {
      t += 1s;
      std::this_thread::sleep_until(t);
      auto value = writes.load();
      if (value == 1)
        return;
      auto n = value - old_value;
      std::cout << "*** " << std::setw(5) << n << " writes/s\n";
      old_value = value;
    }
  }};
  std::vector<broker::data> keys;
  auto key_suffix = to_string(ep.node_id());
  for (size_t index = 0; index < 100; ++index) {
    auto key = std::to_string(index + 1);
    key.insert(0, "k-");
    key += '-';
    key += key_suffix;
    keys.emplace_back(std::move(key));
  }
  auto val = broker::count{1};
  for (;;) {
    // Update all our keys.
    for (auto& key : keys) {
      store.put(key, data{val}, timespan{48h});
      ++writes;
    }
    // Wait (poll) until we get back our last write.
    while (store.get(keys[0]) != val)
      std::this_thread::sleep_for(1ms);
    ++val;
  }
}

struct config : configuration {
  using super = configuration;

  config() : configuration(skip_init) {
    opt_group{custom_options_, "global"}
      .add(event_type, "event-type,t",
           "1 (vector, default) | 2 (conn log entry) | 3 (table)")
      .add(batch_rate, "batch-rate,r",
           "batches/sec (default: 1, set to 0 for infinite)")
      .add(batch_size, "batch-size,s", "events per batch (default: 1)")
      .add(rate_increase_interval, "batch-size-increase-interval,i",
           "interval for increasing the batch size (in seconds)")
      .add(rate_increase_amount, "batch-size-increase-amount,a",
           "additional batch size per interval")
      .add(max_received, "max-received,m", "stop benchmark after given count")
      .add(max_in_flight, "max-in-flight,f", "report when exceeding this count")
      .add(store_mode, "stores", "run the stores benchmark instead")
      .add(server, "server", "run in server mode")
      .add(verbose, "verbose", "enable status output");
  }

  using super::init;

  std::string help_text() const {
    return custom_options_.help_text();
  }
};

void usage(const config& cfg, const char* cmd_name) {
  std::cerr << "Usage: " << cmd_name
            << " [<options>] <zeek-host>[:<port>] | [--disable-ssl] --server "
               "<interface>:port\n\n"
            << cfg.help_text();
}

} // namespace

int main(int argc, char** argv) {
  config cfg;
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << "\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  if (cfg.remainder.size() != 1) {
    std::cerr << "*** expected exactly one argument\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  // Local variables configurable via CLI.
  auto arg = cfg.remainder[0];
  if (arg == "single-process") {
    single_process_mode(cfg);
    return EXIT_SUCCESS;
  }
#ifndef BROKER_WINDOWS
  if (arg == "single-process-socketpair") {
    single_process_with_sockets_mode(cfg);
    return EXIT_SUCCESS;
  }
#endif // BROKER_WINDOWS
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
  if (store_mode) {
    if (server)
      store_master_mode(ep, host, port);
    else
      store_clone_mode(ep, host, port);
  } else {
    if (server)
      server_mode(ep, host, port);
    else
      client_mode(ep, host, port);
  }
  return EXIT_SUCCESS;
}
