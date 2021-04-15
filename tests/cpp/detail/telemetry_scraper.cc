#define SUITE detail.telemetry_scraper

#include "broker/detail/telemetry_scraper.hh"

#include "test.hh"

using namespace broker;
using namespace std::literals::chrono_literals;

namespace {

caf::behavior dummy_core() {
  return {
    [](atom::publish, data_message) {
      // nop
    },
  };
}

struct metric_row {
  std::string prefix;
  std::string name;
  std::string type;
  table labels;
  data value;
};

template <class Inspector>
bool inspect(Inspector& f, metric_row& row) {
  return f.object(row).fields(
    f.field("prefix", row.prefix), f.field("name", row.name),
    f.field("type", row.type), f.field("labels", row.labels),
    f.field("value", row.value));
}

bool operator==(const metric_row& lhs, const vector& rhs) {
  return rhs.size() == 5
         && lhs.prefix == rhs[0]
         && lhs.name == rhs[1]
         && lhs.type == rhs[2]
         && lhs.labels == rhs[3]
         && lhs.value == rhs[4];
}

bool operator==(const vector& lhs, const metric_row& rhs) {
  return rhs == lhs;
}

bool operator==(const metric_row& lhs, const data& rhs) {
  if (auto vec = get_if<vector>(rhs))
    return lhs == *vec;
  else
    return false;
}

bool operator==(const data& lhs, const metric_row& rhs) {
  return rhs == lhs;
}

struct fixture : base_fixture {
  caf::actor core;
  caf::actor aut;
  caf::telemetry::int_gauge* foo_bar;
  caf::telemetry::int_gauge* bar_foo;

  fixture() {
    foo_bar = sys.metrics().gauge_singleton("foo", "bar", "FooBar!");
    bar_foo = sys.metrics().gauge_singleton("bar", "foo", "BarFoo!");
    std::vector<std::string> selection{"foo"};
    core = sys.spawn(dummy_core);
    aut = sys.spawn<detail::telemetry_scraper_actor>(
      core, std::move(selection), caf::timespan{2s}, "/all/them/metrics");
    sched.run();
  }

  ~fixture() {
    anon_send_exit(aut, caf::exit_reason::user_shutdown);
  }

  auto& state() {
    return deref<detail::telemetry_scraper_actor>(aut).state;
  }
};

} // namespace

FIXTURE_SCOPE(telemetry_scraper_tests, fixture)

TEST(the scraper runs once per interval) {
  CHECK(state().tbl.empty());
  foo_bar->inc();
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(state().tbl.size() == 1))
    CHECK_EQUAL(state().tbl[0],
                (metric_row{"foo", "bar", "gauge", table{}, data{1}}));
  foo_bar->inc();
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(state().tbl.size() == 1))
    CHECK_EQUAL(state().tbl[0],
                (metric_row{"foo", "bar", "gauge", table{}, data{2}}));
}

FIXTURE_SCOPE_END()
