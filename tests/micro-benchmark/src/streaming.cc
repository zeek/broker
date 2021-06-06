#include "main.hh"

#include "broker/message.hh"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/io/middleman.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iomanip>
#include <numeric>

#include <sys/socket.h>
#include <sys/uio.h>

namespace {

// -- print utility ------------------------------------------------------------

struct stats {
  std::string_view benchmark_name;
  std::vector<double> runtime_measurements; // in seconds

  explicit stats(std::string_view name) : benchmark_name(name) {
    // nop
  }

  auto min() const noexcept {
    if (runtime_measurements.empty())
      return 0.0;
    else
      return *std::min_element(runtime_measurements.begin(),
                               runtime_measurements.end());
  }

  auto max() const noexcept {
    if (runtime_measurements.empty())
      return 0.0;
    else
      return *std::max_element(runtime_measurements.begin(),
                               runtime_measurements.end());
  }

  auto avg() const noexcept {
    if (runtime_measurements.empty())
      return 0.0;
    else
      return std::accumulate(runtime_measurements.begin(),
                             runtime_measurements.end(), 0.0)
             / runtime_measurements.size();
  }
};

struct setlw_t {
  int n;
};

auto setlw(int n) {
  return setlw_t{n};
}

std::ostream& operator<<(std::ostream& out, setlw_t token) {
  return out << std::left << std::setw(token.n);
}

struct setrw_t {
  int n;
};

auto setrw(int n) {
  return setrw_t{n};
}

std::ostream& operator<<(std::ostream& out, setrw_t token) {
  return out << std::right << std::setw(token.n);
}

struct padding {
  size_t n;
  char c;
};

std::ostream& operator<<(std::ostream& out, padding x) {
  for (size_t i = 0; i < x.n; ++i)
    out.put(x.c);
  return out;
}

using std::cout;

struct layout {
  static constexpr size_t col_size[] = {45, 15, 15, 15};

  static size_t line_size() noexcept {
    return std::accumulate(std::begin(col_size), std::end(col_size), size_t{0});
  }
};

void print_sep() {
  cout << padding{layout::line_size(), '-'} << '\n';
}

void print_header() {
  print_sep();
  cout << setlw(layout::col_size[0]) << "Benchmark" // Col 1.
       << setrw(layout::col_size[1]) << "Time AVG"  // Col 2.
       << setrw(layout::col_size[2]) << "Time MIN"  // Col 3.
       << setrw(layout::col_size[3]) << "Time MAX"  // Col 4.
       << '\n';
  print_sep();
}

void print_result(const stats& xs) {
  cout << setlw(layout::col_size[0]) << xs.benchmark_name    // Col 1.
       << setrw(layout::col_size[1] - 2) << xs.avg() << " s" // Col 2.
       << setrw(layout::col_size[2] - 2) << xs.min() << " s" // Col 3.
       << setrw(layout::col_size[3] - 2) << xs.max() << " s" // Col 4.
       << '\n';
}

void print_footer() {
  print_sep();
  cout.put('\n');
}

// -- synchronization ----------------------------------------------------------

// Drop-in replacement for std::barrier (based on the TS API as of 2020).
// TODO: tests/cpp/system/shutdown.cc also contains a barrier implementation, we
//       could consolidate the two (or just wait for std::barrier).
class barrier {
public:
  explicit barrier(ptrdiff_t num_threads)
    : num_threads_(num_threads), count_(0) {
    // nop
  }

  void arrive_and_wait() {
    std::unique_lock<std::mutex> guard{mx_};
    auto new_count = ++count_;
    if (new_count == num_threads_) {
      cv_.notify_all();
    } else if (new_count > num_threads_) {
      count_ = 1;
      cv_.wait(guard, [this] { return count_.load() == num_threads_; });
    } else {
      cv_.wait(guard, [this] { return count_.load() == num_threads_; });
    }
  }

private:
  ptrdiff_t num_threads_;
  std::mutex mx_;
  std::atomic<ptrdiff_t> count_;
  std::condition_variable cv_;
};

struct synchronizer {
  using clock_type = std::chrono::steady_clock;
  using time_point = clock_type::time_point;
  using fractional_duration = std::chrono::duration<double>;
  time_point init;
  time_point fin;

  stats& recorder;

  synchronizer(stats& recorder) : recorder(recorder) {
    // nop
  }

  auto runtime() const {
    return std::chrono::duration_cast<fractional_duration>(fin - init);
  }

  void start() {
    init = clock_type::now();
  }

  void stop() {
    fin = clock_type::now();
    recorder.runtime_measurements.emplace_back(runtime().count());
  }
};

// -- worker code --------------------------------------------------------------

template <class ValueType>
struct impl {
  // -- member types -----------------------------------------------------------

  using value_type = ValueType;

  // -- consumer ---------------------------------------------------------------

  struct consumer_state {
    consumer_state(caf::event_based_actor* self, synchronizer* sync, size_t num)
      : self(self), sync(sync), num(num) {
      // nop
    }

    caf::event_based_actor* self;

    synchronizer* sync;

    size_t num;

    static inline const char* name = "broker.consumer";

    caf::behavior make_behavior() {
      return {
        [this](caf::stream<value_type> in) {
          self->unbecome();
          return caf::attach_stream_sink(
            self, in,
            // Initializer.
            [](size_t& received) { received = 0; },
            // Processing step.
            [](size_t& received, value_type) { ++received; },
            // Finalizer.
            [this]([[maybe_unused]] size_t& received, const caf::error&) {
              assert(num == received);
              self->quit();
              sync->stop();
            });
        },
      };
    }
  };

  using consumer_actor = caf::stateful_actor<consumer_state>;

  // -- producer ---------------------------------------------------------------

  struct producer_state {
    producer_state(caf::event_based_actor* self, synchronizer* sync,
                   caf::actor consumer, size_t num, value_type msg)
      : self(self),
        sync(sync),
        consumer(std::move(consumer)),
        num(num),
        msg(std::move(msg)) {
      // nop
    }

    caf::event_based_actor* self;
    synchronizer* sync;
    caf::actor consumer;
    size_t num;
    value_type msg;

    static inline const char* name = "broker.producer";

    caf::behavior make_behavior() {
      sync->start();
      caf::attach_stream_source(
        self, std::move(consumer),
        // Initializer.
        [](size_t& shipped) { shipped = 0; },
        // Generator.
        [this](size_t& shipped, caf::downstream<value_type>& out, size_t hint) {
          if (auto n = std::min(hint, num - shipped); n > 0) {
            for (size_t pushed = 0; pushed < n; ++pushed)
              out.push(msg);
            shipped += n;
          }
        },
        // Done predicate.
        [this](const size_t& shipped) { return shipped >= num; });
      return {};
    }
  };

  using producer_actor = caf::stateful_actor<producer_state>;
};

// -- CAF setup ----------------------------------------------------------------

struct config : caf::actor_system_config {
  config() {
    config_file_path.clear();
    set("caf.scheduler.max-threads", 2);
    set("caf.logger.console.verbosity", "quiet");
    set("caf.logger.file.verbosity", "quiet");
  }
};

// -- benchmarking utility -----------------------------------------------------

template <class ValueType>
void run_single_system(std::string benchmark_name, size_t num, ValueType msg) {
  using impl_type = impl<ValueType>;
  using consumer_actor = typename impl_type::consumer_actor;
  using producer_actor = typename impl_type::producer_actor;
  stats recorder{benchmark_name};
  for (int i = 0; i < 10; ++i) {
    config cfg;
    synchronizer sync{recorder};
    caf::actor_system sys{cfg};
    auto consumer = sys.spawn<consumer_actor>(&sync, num);
    sys.spawn<producer_actor>(&sync, consumer, num, msg);
  }
  print_result(recorder);
}

#if defined(CAF_MACOS) || defined(CAF_IOS) || defined(CAF_BSD)
constexpr int no_sigpipe_io_flag = 0;
#else
constexpr int no_sigpipe_io_flag = MSG_NOSIGNAL;
#endif

std::pair<int, int> make_socket_pair() {
  int sockets[2];
  if (auto res = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets); res != 0) {
    perror("socketpair");
    abort();
  } else {
    return {sockets[0], sockets[1]};
  }
}

// Note: this setup uses some private CAF APIs and is bound to break eventually!
// Unfortunately, there's no way to set this up with the public API at the
// moment. Once there is an officially supported way we'll switch to that API
// instead.
template <class ValueType>
void run_distributed(std::string benchmark_name, size_t num, ValueType msg) {
  using impl_type = impl<ValueType>;
  using consumer_actor = typename impl_type::consumer_actor;
  using producer_actor = typename impl_type::producer_actor;
  stats recorder{benchmark_name};
  for (int i = 0; i < 10; ++i) {
    synchronizer sync{recorder};
    // Note: we use tie instead of `auto [fd0, fd1]` for lambda captures.
    int fd0;
    int fd1;
    std::tie(fd0, fd1) = make_socket_pair();
    // Spin up the "server" with a consumer.
    auto t0 = std::thread{[fd0, sptr{&sync}, num] {
      config cfg;
      cfg.load<caf::io::middleman>();
      cfg.set("caf.middleman.workers", 0);
      caf::actor_system sys{cfg};
      auto consumer = sys.spawn<consumer_actor>(sptr, num);
      std::set<std::string> dummy;
      auto scribe = sys.middleman().backend().new_scribe(fd0);
      auto basp = sys.middleman().get_named_broker("BASP");
      auto cptr = caf::actor_cast<caf::strong_actor_ptr>(consumer);
      caf::anon_send(basp, caf::publish_atom_v, std::move(scribe),
                     uint16_t{1234}, std::move(cptr), std::move(dummy));
    }};
    // Spin up the "client" with the producer.
    auto t1 = std::thread{[fd1, sptr{&sync}, num, msg] {
      config cfg;
      cfg.load<caf::io::middleman>();
      caf::actor_system sys{cfg};
      std::set<std::string> dummy;
      auto scribe = sys.middleman().backend().new_scribe(fd1);
      auto basp = sys.middleman().get_named_broker("BASP");
      caf::actor consumer;
      {
        caf::scoped_actor self{sys};
        self
          ->request(basp, caf::infinite, caf::connect_atom_v, std::move(scribe),
                    uint16_t{1234})
          .receive(
            [&consumer](endpoint_id&, caf::strong_actor_ptr& ptr,
                        std::set<std::string>&) {
              if (ptr) {
                consumer = caf::actor_cast<caf::actor>(ptr);
              } else {
                std::cerr << "*** CAF returned an invalid consumer handle\n";
                abort();
              }
            },
            [](caf::error& err) {
              std::cerr << "*** failed to connect to the consumer: "
                        << caf::to_string(err) << '\n';
              abort();
            });
      }
      sys.spawn<producer_actor>(sptr, consumer, num, msg);
    }};
    t0.join();
    t1.join();
  }
  print_result(recorder);
}

} // namespace

void run_streaming_benchmark() {
  using namespace broker;
  constexpr size_t n = 100'000;
  cout << std::fixed << std::setprecision(6); // Microsecond resolution.
  generator g;
  auto nid = g.next_endpoint_id();
  auto uid = g.next_uuid();
  print_header();
  // TODO: allow users to specify the index range. We currently only enable
  //       message type 1 since it's reasonably fast.
  for (size_t index = 0; index < 1; ++index) {
    auto suffixed = [index](std::string str) {
      str += std::to_string(index);
      return str;
    };
    auto dmsg = make_data_message("/micro/benchmark", g.next_data(index + 1));
    auto nmsg = make_node_message(dmsg, alm::multipath{nid});
    auto lmsg = legacy_node_message{dmsg, 20};
    auto umsg = uuid_node_message{dmsg, uuid_multipath{uid, true}};
    run_single_system(suffixed("/single-system/data-message/"), n, dmsg);
    run_single_system(suffixed("/single-system/node-message/"), n, nmsg);
    run_single_system(suffixed("/single-system/legacy-node-message/"), n, lmsg);
    run_single_system(suffixed("/single-system/uuid-node-message/"), n, umsg);
    run_distributed(suffixed("/distributed/data-message/"), n, dmsg);
    run_distributed(suffixed("/distributed/node-message/"), n, nmsg);
    run_distributed(suffixed("/distributed/legacy-node-message/"), n, lmsg);
    run_distributed(suffixed("/distributed/uuid-node-message/"), n, umsg);
  }
  print_footer();
}
