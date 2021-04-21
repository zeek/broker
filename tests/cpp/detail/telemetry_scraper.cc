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
  std::string unit;
  std::string helptext;
  bool is_sum;
  table labels;
  data value;
};

template <class Inspector>
bool inspect(Inspector& f, metric_row& row) {
  return f.object(row).fields(
    f.field("prefix", row.prefix), f.field("name", row.name),
    f.field("type", row.type), f.field("unit", row.unit),
    f.field("helptext", row.helptext), f.field("is_sum", row.is_sum),
    f.field("labels", row.labels), f.field("value", row.value));
}

bool operator==(const metric_row& lhs, const vector& rhs) {
  return rhs.size() == 8
         && lhs.prefix == rhs[0]
         && lhs.name == rhs[1]
         && lhs.type == rhs[2]
         && lhs.unit == rhs[3]
         && lhs.helptext == rhs[4]
         && lhs.is_sum == rhs[5]
         && lhs.labels == rhs[6]
         && lhs.value == rhs[7];
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
  caf::telemetry::int_histogram* foo_hist;
  caf::telemetry::int_gauge* bar_foo;

  fixture() {
    auto& reg = sys.metrics();
    foo_bar = reg.gauge_singleton("foo", "bar", "FooBar!");
    std::array<int64_t, 3> buckets{{8, 16, 32}};
    auto foo_hist_fam = reg.histogram_family("foo", "hist", {"sys"}, buckets,
                                             "FooHist!", "seconds");
    foo_hist = foo_hist_fam->get_or_add({{"sys", "broker"}});
    bar_foo = reg.gauge_singleton("bar", "foo", "BarFoo!");
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

  data foo_hist_buckets(int64_t le_8, int64_t le_16, int64_t le_32,
                        int64_t gt_32, int64_t sum) {
    vector result;
    result.emplace_back(vector{8, le_8});
    result.emplace_back(vector{16, le_16});
    result.emplace_back(vector{32, le_32});
    result.emplace_back(vector{std::numeric_limits<int64_t>::max(), gt_32});
    result.emplace_back(sum);
    return data{std::move(result)};
  }
};

} // namespace

FIXTURE_SCOPE(telemetry_scraper_tests, fixture)

TEST(the scraper runs once per interval) {
  CHECK(state().tbl.empty());
  foo_bar->inc();
  foo_hist->observe(4);
  foo_hist->observe(12);
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(state().tbl.size() == 2)) {
    CHECK_EQUAL(state().tbl[0],
                (metric_row{"foo", "bar", "gauge", "1", "FooBar!", false,
                            table{}, data{1}}));
    CHECK_EQUAL(
      state().tbl[1],
      (metric_row{"foo", "hist", "histogram", "seconds", "FooHist!", false,
                  table{{"sys", "broker"}}, foo_hist_buckets(1, 1, 0, 0, 16)}));
  }
  foo_bar->inc();
  foo_hist->observe(64);
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(state().tbl.size() == 2)) {
    CHECK_EQUAL(state().tbl[0],
                (metric_row{"foo", "bar", "gauge", "1", "FooBar!", false,
                            table{}, data{2}}));
    CHECK_EQUAL(
      state().tbl[1],
      (metric_row{"foo", "hist", "histogram", "seconds", "FooHist!", false,
                  table{{"sys", "broker"}}, foo_hist_buckets(1, 1, 0, 1, 80)}));
  }
}

FIXTURE_SCOPE_END()
