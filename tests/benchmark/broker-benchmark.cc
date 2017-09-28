
#include <getopt.h>
#include <unistd.h>
#include <mutex>

#include <broker/broker.hh>
#include <broker/bro.hh>

using namespace broker;

// Options
static int event_type = 1;
static double batch_rate = 1;
static int batch_size = 1;
static double rate_increase_interval = 0;
static double rate_increase_amount = 0;
static int server = 0;

// Global state
static unsigned long total_recv;
static unsigned long total_sent;
static unsigned long last_sent;
static double last_t;

static struct option long_options[] = {
    {"event-type",             required_argument, 0, 't'},
    {"batch-rate",             required_argument, 0, 'r'},
    {"batch-size",             required_argument, 0, 's'},
    {"rate-increase-interval", required_argument, 0, 'i'},
    {"rate-increase-amount",   required_argument, 0, 'a'},
    {"server",                 no_argument, &server, 'S'},
    {0, 0, 0, 0}
};

static void usage(const char* prog) {
    std::cerr <<
            "Usage: " << prog << " [<options>] <bro-host>[:<port>] | --server <interface>:port\n"
            "\n"
            "   --event-type <1|2|3>            (default: 1)\n"
            "   --batch-rate <batches/sec>      (default: 1)\n"
            "   --rate-increase-interval <secs> (default: 0, off)\n"
            "   --rate-increase-amount   <size> (default: 0, off)\n"
            "\n";

    exit(1);
}

broker::bro::Event createEvent() {
    bro::Event ev("event_1", std::vector<broker::data>{42, "test"});
    return std::move(ev);
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

void receivedStats(broker::data x)
{
    auto args = bro::Event(x).args()[0];
    auto rec = get<broker::vector>(args);

    double t;
    broker::convert(get<broker::timestamp>(rec[0]), t);

    double dt_recv;
    broker::convert(get<broker::timespan>(rec[1]), dt_recv);

    auto ev1 = get<broker::count>(rec[2]);
    auto all_recv = ev1;
    total_recv += ev1;

    auto all_sent = (total_sent - last_sent);

    double now;
    broker::convert(broker::now(), now);
    double dt_sent = (now - last_t);

    auto recv_rate = (double(all_recv) / dt_recv);
    auto send_rate = double(total_sent - last_sent) / dt_sent;

    std::cerr
        << broker::to_string(t) << " "
        << "dt=" << dt_recv << " "
        << "recv=" << all_recv << " "
        << "sent=" << all_sent << " "
        << "total_recv=" << total_recv << " "
        << "total_sent=" << total_sent << " "
        << "[sending at " << send_rate << " ev/s, receiving at " << recv_rate << " ev/s "
        << std::endl;

    last_t = now;
    last_sent = total_sent;
}

void clientMode(const char* host, int port) {
    broker_options options;
    configuration cfg{options};
    broker::endpoint ep(std::move(cfg));
    auto ss = ep.make_status_subscriber(true);

    auto p = ep.make_publisher("/benchmark/events");

    ep.subscribe_nosync({"/benchmark/stats"},
                        [](caf::unit_t&) {
                        },
                        [=](caf::unit_t&, std::pair<broker::topic, broker::data> x) {
                            receivedStats(std::move(x.second));
                        },
                        [](caf::unit_t&) {
                            // nop
			});

    ep.peer(host, port, broker::timeout::seconds(1));

    while ( true ) {
        sendBatch(ep, p);
        usleep(1e6 / batch_rate);
    }
}

void serverMode(const char* iface, int port) {
    // This mimics what benchmark.bro does.

    broker_options options;
    configuration cfg{options};
    broker::endpoint ep(std::move(cfg));
    ep.listen(iface, port);

    uint64_t num_events;
    std::mutex lock;

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

    auto last_t = broker::now();

    while ( true ) {
        sleep(1);
        auto t = now();
        auto dt = (t - last_t);

        lock.lock();
        auto num = num_events;
        num_events = 0;
        lock.unlock();

        auto stats = broker::vector{t, dt, broker::count{num}};
        bro::Event ev("stats_update", std::vector<broker::data>{stats});
        ep.publish("/benchmark/stats", ev);

        last_t = t;
    }
}

int main(int argc, char** argv) {

    int option_index = 0;

    while( true ) {
        auto c = getopt_long(argc, argv, "", long_options, &option_index);

        if ( long_options[option_index].flag != 0 )
            break;

        /* Detect the end of the options. */
        if ( c == -1 )
            break;

        switch (c) {
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

         default:
            usage(argv[0]);
        }
    }

    if ( optind != argc - 1 )
        usage(argv[0]);

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

