
#include <getopt.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/time.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>

#include "broker/bro.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/topic.hh"

#include "readerwriterqueue/readerwriterqueue.h"

using EventQueue = moodycamel::ReaderWriterQueue<broker::bro::Event>;

static int event_type = 1;
static double batch_rate = 1;
static int batch_size = 1;
static double rate_increase_interval = 0;
static double rate_increase_amount = 0;
static uint64_t max_received = 0;
static uint64_t max_in_flight = 0;
static int server = 0;
static int disable_ssl = 0;
static int use_bro_batches = 0;
static int use_non_blocking = 0;
static int verbose = 0;

// Global state
static unsigned long total_recv;
static unsigned long total_sent;
static unsigned long last_sent;
static double last_t;
static uint64_t num_events;
static std::mutex lock;

static struct option long_options[] = {
    {"event-type",             required_argument, 0, 't'},
    {"batch-rate",             required_argument, 0, 'r'},
    {"batch-size",             required_argument, 0, 's'},
    {"batch-size-increase-interval", required_argument, 0, 'i'},
    {"batch-size-increase-amount",   required_argument, 0, 'a'},
    {"max-received",           required_argument, 0, 'm'},
    {"max-in-flight",          required_argument, 0, 'f'},
    {"server",                 no_argument, &server, 1},
    {"disable-ssl",            no_argument, &disable_ssl, 1},
    {"use-bro-batches",        no_argument, &use_bro_batches, 1},
    {"use-non-blocking",       no_argument, &use_non_blocking, 1},
    {"verbose",                no_argument, &verbose, 1},
    {0, 0, 0, 0}
};

const char* prog = 0;

static void usage() {
    std::cerr <<
            "Usage: " << prog << " [<options>] <bro-host>[:<port>] | [--disable-ssl] --server <interface>:port\n"
            "\n"
            "   --event-type <1|2|3>                  (default: 1)\n"
            "   --batch-rate <batches/sec>            (default: 1)\n"
            "   --batch-size <num-events>             (default: 1)\n"
            "   --batch-size-increase-interval <secs> (default: 0, off)\n"
            "   --batch-size-increase-amount   <size> (default: 0, off)\n"
            "   --max-received <num-events>           (default: 0, off)\n"
            "   --max-in-flight <num-events>          (default: 0, off)\n"
            "   --disable-ssl                         (default: on)\n"
            "   --verbose                             (default: off)\n"
            "\n";

    exit(1);
}

double current_time() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return  double(tv.tv_sec) + double(tv.tv_usec) / 1e6;
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

struct printer_t {
  using result_type = void;

  template <class T>
  void operator()(const T& x) const {
    std::cout << caf::deep_to_string(x);
  }
};

static constexpr printer_t printer = printer_t{};

broker::vector createEventArgs() {
    switch ( event_type ) {
     case 1: {
         return std::vector<broker::data>{42, "test"};
     }

     case 2: {
         // This resembles a line in conn.log.
         broker::address a1;
         broker::address a2;
         broker::convert("1.2.3.4", a1);
         broker::convert("3.4.5.6", a2);

         return broker::vector{
             broker::now(),
             random_string(10),
             broker::vector{
                 a1,
                 broker::port(4567, broker::port::protocol::tcp),
                 a2,
                 broker::port(80, broker::port::protocol::tcp)
             },
             broker::enum_value("tcp"),
             random_string(10),
             std::chrono::duration_cast<broker::timespan>(std::chrono::duration<double>(3.14)),
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
             broker::set({random_string(10), random_string(10)})
        };
     }

     case 3: {
         broker::table m;

         for ( int i = 0; i < 100; i++ ) {
             broker::set s;
             for ( int j = 0; j < 10; j++ )
                 s.insert(random_string(5));
             m[random_string(15)] = s;
         }

         return broker::vector{broker::now(), m};
     }

     default:
        usage();
        abort();
    }
}

void sendBroBatch(broker::endpoint& ep, broker::publisher& p)
{
    auto name = std::string("event_") + std::to_string(event_type);

    broker::vector batch;
    for ( int i = 0; i < batch_size; i++ ) {
    auto ev = broker::bro::Event(std::string(name), createEventArgs());
    batch.emplace_back(std::move(ev));
    }

    total_sent += batch.size();
    p.publish(broker::bro::Batch(std::move(batch)));
}

void sendBrokerBatchBlocking(broker::endpoint& ep, broker::publisher& p)
{
    auto name = std::string("event_") + std::to_string(event_type);

    std::vector<broker::data> batch;
    for ( int i = 0; i < batch_size; i++ ) {
    auto ev = broker::bro::Event(std::string(name), createEventArgs());
    batch.emplace_back(std::move(ev));
    }

    total_sent += batch.size();
    p.publish(std::move(batch));
}

void sendBrokerBatchNonBlocking(broker::endpoint& ep, EventQueue& q)
{
    auto name = std::string("event_") + std::to_string(event_type);

    for ( int i = 0; i < batch_size; i++ ) {
        auto ev = broker::bro::Event(std::string(name), createEventArgs());
        q.emplace(std::move(ev));
    }

    std::cerr << "buffered now: " << q.size_approx() << std::endl;
}

void receivedStats(broker::endpoint& ep, broker::data x)
{
    // Example for an x: '[1, 1, [stats_update, [1ns, 1ns, 0]]]'.
    // We are only interested in the '[1ns, 1ns, 0]' part.
    auto xvec = caf::get<broker::vector>(x);
    auto yvec = caf::get<broker::vector>(xvec[2]);
    auto rec = caf::get<broker::vector>(yvec[1]);

    double t;
    broker::convert(caf::get<broker::timestamp>(rec[0]), t);

    double dt_recv;
    broker::convert(caf::get<broker::timespan>(rec[1]), dt_recv);

    auto ev1 = caf::get<broker::count>(rec[2]);
    auto all_recv = ev1;
    total_recv += ev1;

    auto all_sent = (total_sent - last_sent);

    double now;
    broker::convert(broker::now(), now);
    double dt_sent = (now - last_t);

    auto recv_rate = (double(all_recv) / dt_recv);
    auto send_rate = double(total_sent - last_sent) / dt_sent;
    auto in_flight = (total_sent - total_recv);

    std::cerr
        << broker::to_string(t) << " "
        << "[batch_size=" << batch_size << "] "
        << "in_flight=" << in_flight << " "
        << "d_t=" << dt_recv << " "
        << "d_recv=" << all_recv << " "
        << "d_sent=" << all_sent << " "
        << "total_recv=" << total_recv << " "
        << "total_sent=" << total_sent << " "
        << "[sending at " << send_rate << " ev/s, receiving at " << recv_rate << " ev/s "
        << std::endl;

    last_t = now;
    last_sent = total_sent;

    if ( max_received && total_recv > max_received ) {
        broker::bro::Event ev("quit_benchmark", std::vector<broker::data>{});
        ep.publish("/benchmark/terminate", ev);
        sleep(2); // Give clients a bit.
        exit(0);
    }

    static int max_exceeded_counter = 0;
    if ( max_in_flight && in_flight > max_in_flight ) {

        if ( ++max_exceeded_counter >= 5 ) {
            std::cerr << "max-in-flight exceeded for 5 subsequent batches" << std::endl;
            exit(1);
        }
    }
    else
        max_exceeded_counter = 0;

}

void clientMode(const char* host, int port) {
    broker::broker_options options;
    options.disable_ssl = disable_ssl;
    broker::configuration cfg{options};
    broker::endpoint ep(std::move(cfg));
    auto ss = ep.make_status_subscriber(true);
    auto p = ep.make_publisher("/benchmark/events");
    EventQueue q;

    ep.subscribe_nosync({"/benchmark/stats"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            receivedStats(ep, std::move(x.second));
                        },
                        [](caf::unit_t&, const caf::error&) {
                            // nop
            });

    if ( use_non_blocking ) {
        using value_type = std::pair<broker::topic, broker::data>;
        ep.publish_all(
                       [](caf::unit_t&) {
                           // nop
                       },
                       [&](caf::unit_t&, caf::downstream<value_type>& out, size_t num) {
                           const broker::bro::Event* ev;
			   std::cerr << "can publish " << num << ", have " << q.size_approx() << std::endl;
                           while ( num-- && (ev = q.peek()) ) {
                               out.push("/benchmark/events", std::move(*ev));
			       q.pop();
                               ++total_sent;
                           }
                       },
                       [](const caf::unit_t&) { return false; });
    }

    if (verbose)
      std::cout << "*** init peering: host = " << host << ", port = " << port
                << std::endl;
    auto res = ep.peer(host, port, broker::timeout::seconds(1));

    if (!res) {
      std::cerr << "unable to peer to " << host << " on port " << port
                << std::endl;
      return;
    } else if (verbose) {
      std::cout << "*** endpoint is now peering to remote" << std::endl;
    }

    double next_increase = current_time() + rate_increase_interval;

    while ( true ) {
        if ( use_bro_batches )
            sendBroBatch(ep, p);
        else if ( use_non_blocking )
            sendBrokerBatchNonBlocking(ep, q);
	else
            sendBrokerBatchBlocking(ep, p);

        usleep(1e6 / batch_rate);

        if ( rate_increase_interval && rate_increase_amount && current_time() > next_increase ) {
            batch_size += rate_increase_amount;
            next_increase = current_time() + rate_increase_interval;
        }
        auto status_events = ss.poll();
        if (verbose) {
          for (auto& ev : status_events) {
            visit(printer, ev);
            std::cout << std::endl;
          }
        }
    }
}

void processEvent(broker::bro::Event& ev) {
    lock.lock();
    num_events++;
    lock.unlock();
    }

void serverMode(const char* iface, int port) {
    // This mimics what benchmark.bro does.
    broker::broker_options options;
    options.disable_ssl = disable_ssl;
    broker::configuration cfg{options};
    broker::endpoint ep(std::move(cfg));
    auto ss = ep.make_status_subscriber(true);
    ep.listen(iface, port);

    bool terminate = false;

    ep.subscribe_nosync({"/benchmark/events"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                auto& msg = x.second;

                if ( broker::bro::Message::type(msg) == broker::bro::Message::Type::Event ) {
                broker::bro::Event ev(std::move(msg));
                    processEvent(ev);
                }

                else if ( broker::bro::Message::type(msg) == broker::bro::Message::Type::Batch ) {
                broker::bro::Batch batch(std::move(msg));
                    for ( auto msg : batch.batch() ) {
                    broker::bro::Event ev(std::move(msg));
                                    processEvent(ev);
                                }
                            }

                            else {
                                std::cerr << "unexpected message type" << std::endl;
                                exit(1);
                            }
                        },

                        [](caf::unit_t&, const caf::error&) {
                            // nop
            });

    ep.subscribe_nosync({"/benchmark/terminate"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            terminate = true;
                        },
                        [](caf::unit_t&, const caf::error&) {
                            // nop
            });

    auto last_t = broker::now();

    while ( ! terminate ) {
        sleep(1);
        auto t = broker::now();
        auto dt = (t - last_t);

        lock.lock();
        auto num = num_events;
        num_events = 0;
        lock.unlock();

        auto stats = broker::vector{t, dt, broker::count{num}};
        if (verbose) {
          std::cout << "stats: " << caf::deep_to_string(stats) << std::endl;
        }
        broker::bro::Event ev("stats_update",
                              std::vector<broker::data>{stats});
        ep.publish("/benchmark/stats", ev);
        last_t = t;
        auto status_events = ss.poll();
        if (verbose) {
          for (auto& ev : status_events) {
            visit(printer, ev);
            std::cout << std::endl;
          }
        }
    }
}

int main(int argc, char** argv) {
    prog = argv[0];

    int option_index = 0;

    while( true ) {
        auto c = getopt_long(argc, argv, "", long_options, &option_index);

        /* Detect the end of the options. */
        if ( c == -1 )
            break;

        switch (c) {
    case 0:
    case 1:
        // Flag
        break;

         case 't':
            event_type = atoi(optarg);
            break;

         case 'r':
            batch_rate = atof(optarg);
            break;

         case 's':
            batch_size = atoi(optarg);
            break;

         case 'i':
            rate_increase_interval = atof(optarg);
            break;

         case 'a':
            rate_increase_amount = atof(optarg);
            break;

         case 'm':
            max_received = atoi(optarg);
            break;

         case 'f':
            max_in_flight = atoi(optarg);
            break;

         default:
            usage();
        }
    }

    if ( optind != argc - 1 )
        usage();

    char* host = argv[optind];
    int port = 9999;

    if ( auto p = strchr(host, ':') ) {
        *p = '\0';
        port = atoi(p + 1);
    }

    if ( server )
        serverMode(host, port);
    else
        clientMode(host, port);

    return 0;
}

