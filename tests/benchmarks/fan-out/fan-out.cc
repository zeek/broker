#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <string_view>

#ifndef BROKER_WINDOWS
#  include <cstdio>
#  include <unistd.h>
#endif

using namespace broker;
using namespace std::literals;

// -- constants and utility types ----------------------------------------------

/// Terminal color and font face options.
enum class term_color {
  /// Resets the color to the default color and the font weight to normal.
  reset,
  /// Sets the terminal color to black.
  black,
  /// Sets the terminal color to red.
  red,
  /// Sets the terminal color to green.
  green,
  /// Sets the terminal color to yellow.
  yellow,
  /// Sets the terminal color to blue.
  blue,
  /// Sets the terminal color to magenta.
  magenta,
  /// Sets the terminal color to cyan.
  cyan,
  /// Sets the terminal color to white.
  white,
};

namespace {

constexpr uint64_t default_peer_count = 20;

constexpr uint64_t default_message_count = 10'000'000;

constexpr uint64_t default_payload_size = 1'000;

std::string_view tty_codes[] = {
  "\033[0m",  // reset
  "\033[30m", // black
  "\033[31m", // red
  "\033[32m", // green
  "\033[33m", // yellow
  "\033[34m", // blue
  "\033[35m", // magenta
  "\033[36m", // cyan
  "\033[37m", // white
};

} // namespace

bool is_tty([[maybe_unused]] const std::ostream& out) {
#ifdef BROKER_WINDOWS
  return false;
#else
  if (&out == &std::cout)
    return isatty(STDOUT_FILENO) != 0;
  else if (&out == &std::cerr || &out == &std::clog)
    return isatty(STDERR_FILENO) != 0;
  else
    return false;
#endif
}

std::ostream& operator<<(std::ostream& out, term_color x) {
  if (is_tty(out))
    out << tty_codes[static_cast<int>(x)];
  return out;
}

struct quoted {
  std::string_view str;
};

std::ostream& operator<<(std::ostream& out, const quoted& x) {
  return out << '"' << x.str << '"';
}

struct parameters {
  uint64_t peer_count = default_peer_count;
  uint64_t message_count = default_message_count;
  uint64_t payload_size = default_payload_size;
  uint64_t seed = std::random_device{}();
  bool naive_publish = false;
};

// -- I/O utility --------------------------------------------------------------

namespace {

std::mutex ostream_mtx;

} // namespace

template <class... Ts>
void do_println(std::ostream& out, Ts&&... xs) {
  std::unique_lock<std::mutex> guard{ostream_mtx};
  (out << ... << xs);
  out << term_color::reset << '\n';
}

namespace out {

template <class... Ts>
void println(Ts&&... xs) {
  do_println(std::cout, std::forward<Ts>(xs)...);
}

} // namespace out

namespace err {

template <class... Ts>
void println(Ts&&... xs) {
  do_println(std::cerr, term_color::red, std::forward<Ts>(xs)...,
             term_color::reset);
}

} // namespace err

namespace warn {

template <class... Ts>
void println(Ts&&... xs) {
  do_println(std::cerr, term_color::yellow, std::forward<Ts>(xs)...,
             term_color::reset);
}

} // namespace warn

namespace verbose {

namespace {

bool is_enabled;

} // namespace

bool enabled() {
  return is_enabled;
}

template <class... Ts>
void println(Ts&&... xs) {
  if (is_enabled)
    do_println(std::clog, term_color::blue, std::forward<Ts>(xs)...,
               term_color::reset);
}

} // namespace verbose

// -- other utility ------------------------------------------------------------

template <class RandomEngine>
std::string random_string(RandomEngine& rng, size_t size) {
  std::string_view charset = "0123456789"
                             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                             "abcdefghijklmnopqrstuvwxyz";
  auto dis = std::uniform_int_distribution<size_t>{0, charset.size() - 1};
  std::string result;
  result.reserve(size);
  for (size_t i = 0; i < size; ++i)
    result.push_back(charset[dis(rng)]);
  return result;
}

void add_options(configuration& cfg, parameters& ps) {
  cfg.add_option(&ps.peer_count, "peer-count,p", "number of simulated peers");
  cfg.add_option(&ps.message_count, "message-count,m", "number of messages");
  cfg.add_option(&ps.payload_size, "payload-size,s", "bytes per message");
  cfg.add_option(&ps.seed, "seed", "seed for the random-number generator");
  cfg.add_option(&verbose::is_enabled, "verbose",
                 "enables more console output");
  cfg.add_option(&ps.naive_publish, "naive-publish",
                 "publish data via endpoint::publish instead of publish_all");
  ;
}

struct generator {
  explicit generator(parameters ps)
    : params(ps), rng(static_cast<uint32_t>(ps.seed)) {
    // nop
  }

  parameters params;
  std::minstd_rand rng;

  data_message next() {
    return make_data_message("/benchmark/fan-out"s,
                             data{random_string(rng, params.payload_size)});
  }
};

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

struct padded_id {
  endpoint_id id;
  char padding[BROKER_CONSTRUCTIVE_INTERFERENCE_SIZE - sizeof(endpoint_id)];
};

// -- actual program logic -----------------------------------------------------

void run_subscriber(barrier* sync, padded_id* id_slot, uint16_t port,
                    parameters ps, size_t index) {
  barrier worker_sync{2};
  endpoint ep;
  id_slot->id = ep.node_id();
  verbose::println("started new subscriber endpoint: ", ep.node_id());
  ep.subscribe(
    {"/benchmark"_t},
    [] {
      // Init: nop.
    },
    [&worker_sync, ps, index, n = 0u, &ep](const data_message&) mutable {
      ++n;
      if (index == 0 && n % 1000 == 0)
        verbose::println("subscriber 1 received ", n, " items ...");
      if (n == ps.message_count) {
        verbose::println("Broker endpoint ", ep.node_id(),
                         " is done receiving");
        worker_sync.arrive_and_wait();
      }
    },
    [](const error&) {
      // Cleanup: nop.
    });
  if (!ep.peer("localhost", port)) {
    std::cerr << "ep.peer failed!\n";
    abort();
  }
  sync->arrive_and_wait();
  worker_sync.arrive_and_wait();
}

void run_publisher(endpoint& ep, parameters ps) {
  if (ps.naive_publish) {
    generator gen{ps};
    for (size_t i = 0; i < ps.message_count; ++i) {
      ep.publish(gen.next());
      if (i % 1000 == 0)
        verbose::println("publisher emitted ", i, " items ...");
    }
  } else {
    auto gen = std::make_shared<generator>(ps);
    auto worker = ep.publish_all(
      [](size_t& count) { count = 0; },
      [ps, gen](size_t& count, std::deque<data_message>& out, size_t hint) {
        if (auto n = std::min(static_cast<size_t>(ps.message_count) - count,
                              hint);
            n > 0) {
          for (size_t i = 0; i < n; ++i) {
            out.push_back(gen->next());
            if (++count % 1000 == 0)
              verbose::println("publisher emitted ", count, " items ...");
          }
        }
      },
      [n = ps.message_count](const size_t& count) { return count >= n; });
    ep.wait_for(worker);
  }
}

int main(int argc, char** argv) {
  // Parse CLI / config file.
  configuration cfg{skip_init};
  parameters params;
  add_options(cfg, params);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << "\n\n";
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  if (cfg.remainder().size() > 0) {
    std::cerr << "*** too many arguments (did not expect any)\n\n";
    return EXIT_FAILURE;
  }
  // Spin up the "main" endpoint.
  endpoint ep{std::move(cfg)};
  auto port = ep.listen();
  verbose::println("started publisher endpoint: ", ep.node_id());
  // Spin up N peers, as requested and wait for all of them to connect.
  barrier sync{static_cast<ptrdiff_t>(params.peer_count + 1)};
  std::vector<padded_id> ls;
  ls.resize(params.peer_count);
  std::vector<std::thread> threads;
  threads.resize(params.peer_count);
  for (size_t i = 0; i < params.peer_count; ++i)
    threads[i] = std::thread{run_subscriber, &sync, &ls[i], port, params, i};
  sync.arrive_and_wait();
  verbose::println("started ", params.peer_count, " subscriber endpoints");
  // Wait for all peers to complete their handshake.
  for (auto& x : ls) {
    if (!ep.await_peer(x.id)) {
      std::cerr << "*** peers failed to connect\n";
      return EXIT_FAILURE;
    }
  }
  verbose::println("received all ", params.peer_count, " handshakes -> run!");
  // Light, camera, action!
  run_publisher(ep, params);
  // Tear down.
  verbose::println("tear down -> wait for ", params.peer_count, " threads");
  for (auto& thread : threads)
    thread.join();
  verbose::println("all threads have terminated, bye");
}
