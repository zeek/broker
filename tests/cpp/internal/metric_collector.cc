#define SUITE metric_collector

#include "broker/internal/metric_collector.hh"

#include "test.hh"

#include "broker/internal/metric_exporter.hh"

namespace atom = broker::internal::atom;

using namespace broker;

using namespace std::literals;

namespace {

// Works around a weird bug in CAF that prevents us from passing the pointer
// directly.
struct collector_ptr {
  internal::metric_collector* value;
};

caf::behavior dummy_core(collector_ptr ptr) {
  return {
    [collector{ptr.value}](atom::publish, data_message msg) {
      CHECK_EQUAL(get_topic(msg), "/all/them/metrics"sv);
      CHECK_GREATER_EQUAL(collector->insert_or_update(get_data(msg)), 6u);
    },
  };
}

struct fixture : base_fixture {
  internal::metric_collector collector;

  caf::actor core;
  caf::actor exporter;

  caf::telemetry::int_gauge* foo_g1;
  caf::telemetry::dbl_gauge* foo_g2;
  caf::telemetry::int_counter* foo_c1;
  caf::telemetry::dbl_counter* foo_c2;
  caf::telemetry::int_histogram* foo_h1;
  caf::telemetry::dbl_histogram* foo_h2;

  fixture() {
    // Initialize metrics.
    auto& reg = sys.metrics();
    foo_g1 = reg.gauge_singleton("foo", "g1", "Int Gauge!");
    foo_g2 = reg.gauge_singleton<double>("foo", "g2", "Dbl Gauge!");
    foo_c1 = reg.counter_singleton("foo", "c1", "Int Counter!");
    foo_c2 = reg.counter_singleton<double>("foo", "c2", "Dbl Counter!");
    std::array<int64_t, 3> int_buckets{{8, 16, 32}};
    auto h1_fam = reg.histogram_family("foo", "h1", {"sys"}, int_buckets,
                                       "Int Histogram!", "seconds");
    foo_h1 = h1_fam->get_or_add({{"sys", "broker"}});
    std::array<double, 3> dbl_buckets{{8.0, 16.0, 32.0}};
    auto h2_fam = reg.histogram_family<double>("foo", "h2", {"sys"},
                                               dbl_buckets, "Dbl Histogram!",
                                               "seconds");
    foo_h2 = h2_fam->get_or_add({{"sys", "broker"}});
    // Spin up actors.
    core = sys.spawn(dummy_core, collector_ptr{&collector});
    exporter = sys.spawn<internal::metric_exporter_actor>(
      core, std::vector<std::string>{}, caf::timespan{2s}, "/all/them/metrics",
      "exporter-1");
    sched.run();
  }

  ~fixture() {
    anon_send_exit(exporter, caf::exit_reason::user_shutdown);
  }
};

bool contains(std::string_view str, std::string_view what) {
  return str.find(what) != std::string_view::npos;
}

#define PROM_CONTAINS(what) CHECK(contains(prom_txt, what))

} // namespace

FIXTURE_SCOPE(telemetry_collector_tests, fixture)

TEST(less predicate) {
  internal::metric_collector::labels_less less;
  using labels_list = std::vector<caf::telemetry::label>;
  { // Single label (equal).
    auto lhs = labels_list{{"type", "native"}};
    auto rhs = labels_list{{"type", "native"}};
    CHECK(!less(lhs, rhs));
    CHECK(!less(rhs, lhs));
  }
  { // Single label (unequal).
    auto lhs = labels_list{{"type", "native"}};
    auto rhs = labels_list{{"type", "web-socket"}};
    CHECK(less(lhs, rhs));
    CHECK(!less(rhs, lhs));
  }
  { // Two equal labels.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    CHECK(!less(lhs, rhs));
    CHECK(!less(rhs, lhs));
  }
  { // Two labels, with the first being smaller.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-2"}, {"type", "native"}};
    CHECK(less(lhs, rhs));
    CHECK(!less(rhs, lhs));
  }
  { // Two labels, with the second being smaller.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-1"}, {"type", "web-socket"}};
    CHECK(less(lhs, rhs));
    CHECK(!less(rhs, lhs));
  }
}

TEST(equal predicate) {
  internal::metric_collector::labels_equal equal;
  using labels_list = std::vector<caf::telemetry::label>;
  { // Single label (equal).
    auto lhs = labels_list{{"type", "native"}};
    auto rhs = labels_list{{"type", "native"}};
    CHECK(equal(lhs, rhs));
  }
  { // Single label (unequal).
    auto lhs = labels_list{{"type", "native"}};
    auto rhs = labels_list{{"type", "web-socket"}};
    CHECK(!equal(lhs, rhs));
  }
  { // Two equal labels.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    CHECK(equal(lhs, rhs));
    CHECK(equal(rhs, lhs));
  }
  { // Two labels, with the first being smaller.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-2"}, {"type", "native"}};
    CHECK(!equal(lhs, rhs));
    CHECK(!equal(rhs, lhs));
  }
  { // Two labels, with the second being smaller.
    auto lhs = labels_list{{"endpoint", "exporter-1"}, {"type", "native"}};
    auto rhs = labels_list{{"endpoint", "exporter-1"}, {"type", "web-socket"}};
    CHECK(!equal(lhs, rhs));
    CHECK(!equal(rhs, lhs));
  }
}

TEST(a collector consumes the output of an exporter) {
  MESSAGE("fill in some data");
  foo_g1->inc(1);
  foo_g2->inc(2.0);
  foo_c1->inc(4);
  foo_c2->inc(8.0);
  foo_h1->observe(16);
  foo_h2->observe(32.0);
  MESSAGE("get the exporter to publish a current snapshot of the metrics");
  sched.advance_time(2s);
  sched.run();
  MESSAGE("read back what the exporter generated in Prometheus text format");
  auto prom_txt = collector.prometheus_text();
  PROM_CONTAINS(R"(foo_g1{endpoint="exporter-1"} 1)");
  PROM_CONTAINS(R"(foo_g2{endpoint="exporter-1"} 2)");
  PROM_CONTAINS(R"(foo_c1{endpoint="exporter-1"} 4)");
  PROM_CONTAINS(R"(foo_c2{endpoint="exporter-1"} 8)");
  PROM_CONTAINS(R"(foo_h1_seconds_bucket)");
  PROM_CONTAINS(R"(foo_h1_seconds_sum{endpoint="exporter-1",sys="broker"} 16)");
  PROM_CONTAINS(
    R"(foo_h1_seconds_count{endpoint="exporter-1",sys="broker"} 1)");
  PROM_CONTAINS(R"(foo_h2_seconds_bucket)");
  PROM_CONTAINS(R"(foo_h2_seconds_sum{endpoint="exporter-1",sys="broker"} 32)");
  PROM_CONTAINS(
    R"(foo_h2_seconds_count{endpoint="exporter-1",sys="broker"} 1)");
}

FIXTURE_SCOPE_END()
