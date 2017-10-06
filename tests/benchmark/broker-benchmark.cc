
#include <getopt.h>
#include <unistd.h>
#include <mutex>
#include <sys/time.h>

#include <broker/broker.hh>
#include <broker/bro.hh>

static int event_type = 1;
static double batch_rate = 1;
static int batch_size = 1;
static double rate_increase_interval = 0;
static double rate_increase_amount = 0;
static uint64_t max_received = 0;
static uint64_t max_in_flight = 0;
static int server = 0;
static int disable_ssl = 0;

// Global state
static unsigned long total_recv;
static unsigned long total_sent;
static unsigned long last_sent;
static double last_t;

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
            "\n";

    exit(1);
}

double current_time() {
    struct timeval tv;
    gettimeofday(&tv, 0) < 0;
    return  double(tv.tv_sec) + double(tv.tv_usec) / 1e6;
}

static std::string random_string(int n) {
    auto randchar = []() -> char {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(n, 0);
    std::generate_n(str.begin(), n, randchar);
    return std::move(str);
}

static uint64_t random_count() {
    return rand() % 100000;
}

broker::bro::Event createEvent() {
    switch ( event_type ) {
     case 1: {
        broker::bro::Event ev("event_1", std::vector<broker::data>{42, "test"});
        return std::move(ev);
     }

     case 2: {
         // This resembles a line in conn.log.
         broker::address a1;
         broker::address a2;
         broker::convert("1.2.3.4", a1);
         broker::convert("3.4.5.6", a2);

         broker::vector args{
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
        broker::bro::Event ev("event_2", args);
        return std::move(ev);
     }

     case 3: {
         broker::table m;

         for ( int i = 0; i < 100; i++ ) {
             broker::set s;
             for ( int j = 0; j < 10; j++ )
                 s.insert(random_string(5));
             m[random_string(15)] = s;
         }

         broker::bro::Event ev("event_3", broker::vector{broker::now(), m});
         return std::move(ev);
     }

     default:
        usage();
        abort();
    }
}

void sendBatch(broker::endpoint& ep, broker::publisher& p) {
    std::vector<broker::data> batch(batch_size);
    std::generate(batch.begin(), batch.end(), []() { return createEvent(); });
    p.publish(batch);
    total_sent += batch.size();
}

void sendEvents(broker::endpoint& ep, broker::publisher& p) {
    while ( true ) {
        sendBatch(ep, p);
        usleep(1e6 / batch_rate);
    }
}

void receivedStats(broker::endpoint& ep, broker::data x)
{
    auto args = broker::bro::Event(x).args()[0];
    auto rec = broker::get<broker::vector>(args);

    double t;
    broker::convert(broker::get<broker::timestamp>(rec[0]), t);

    double dt_recv;
    broker::convert(broker::get<broker::timespan>(rec[1]), dt_recv);

    auto ev1 = broker::get<broker::count>(rec[2]);
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

    ep.subscribe_nosync({"/benchmark/stats"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            receivedStats(ep, std::move(x.second));
                        },
                        [](caf::unit_t&) {
                            // nop
			});

    ep.peer(host, port, broker::timeout::seconds(1));

    double next_increase = current_time() + rate_increase_interval;

    while ( true ) {
        sendBatch(ep, p);
        usleep(1e6 / batch_rate);

        if ( rate_increase_interval && rate_increase_amount && current_time() > next_increase ) {
            batch_size += rate_increase_amount;
            next_increase = current_time() + rate_increase_interval;
        }
    }
}

void serverMode(const char* iface, int port) {
    // This mimics what benchmark.bro does.
    broker::broker_options options;
    options.disable_ssl = disable_ssl;
    broker::configuration cfg{options};
    broker::endpoint ep(std::move(cfg));
    ep.listen(iface, port);

    uint64_t num_events;
    std::mutex lock;

    bool terminate = false;

    ep.subscribe_nosync({"/benchmark/events"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            lock.lock();
			    num_events++;
                            lock.unlock();
                        },
                        [](caf::unit_t&) {
                            // nop
			});

    ep.subscribe_nosync({"/benchmark/terminate"},
                        [](caf::unit_t&) {
                        },
                        [&](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            terminate = true;
                        },
                        [](caf::unit_t&) {
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
        broker::bro::Event ev("stats_update", std::vector<broker::data>{stats});
        ep.publish("/benchmark/stats", ev);

        last_t = t;
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

