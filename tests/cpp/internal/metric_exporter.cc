#define SUITE metric_exporter

#include "broker/internal/metric_exporter.hh"

#include "test.hh"

namespace atom = broker::internal::atom;

using namespace broker;
using namespace std::literals;

namespace {

struct dummy_core_state {
  data_message last_message;

  caf::behavior make_behavior() {
    return {
      [this](atom::publish, data_message& msg) {
        last_message = std::move(msg);
      },
    };
  }

  std::string last_prometheus_output() const {
    auto& content = get_data(last_message);
    if (!is<vector>(content))
      return "";
    auto& rows = get<vector>(content);
    if (rows.size() != 3)
      return "";
    if (!is<std::string>(rows[2]))
      return "";
    return get<std::string>(rows[2]);
  }
};

using dummy_core_actor = caf::stateful_actor<dummy_core_state>;

struct fixture : base_fixture {
  caf::actor core;
  caf::actor aut;
  caf::telemetry::int_gauge* foo_bar;
  caf::telemetry::int_histogram* foo_hist;
  caf::telemetry::int_gauge* bar_foo;

  auto& state() {
    return deref<internal::metric_exporter_actor>(aut).state;
  }

  auto& core_state() {
    return deref<dummy_core_actor>(core).state;
  }

  fixture() {
    auto& reg = sys.metrics();
    foo_bar = reg.gauge_singleton("foo", "bar", "FooBar!");
    std::array<int64_t, 3> buckets{{8, 16, 32}};
    auto foo_hist_fam = reg.histogram_family("foo", "hist", {"sys"}, buckets,
                                             "FooHist!", "seconds");
    foo_hist = foo_hist_fam->get_or_add({{"sys", "broker"}});
    bar_foo = reg.gauge_singleton("bar", "foo", "BarFoo!");
    std::vector<std::string> selection{"foo"};
    core = sys.spawn<dummy_core_actor>();
    aut = sys.spawn<internal::metric_exporter_actor>(core);
    sched.run();
    state().impl.interval = 2s;
    state().impl.target = "all/them/metrics";
    state().impl.name = "exporter-1";
  }

  ~fixture() {
    anon_send_exit(aut, caf::exit_reason::user_shutdown);
  }

  std::string last_prometheus_output() {
    return core_state().last_prometheus_output();
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

FIXTURE_SCOPE(telemetry_exporter_tests, fixture)

TEST(the exporter runs once per interval) {
  foo_bar->inc();
  foo_hist->observe(4);
  foo_hist->observe(12);
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  auto is_meta_data = [this](const data& x) {
    using namespace std::literals;
    if (auto row = get_if<vector>(x); row && row->size() == 2)
      return row->at(0) == "exporter-1"s && is<timestamp>(row->at(1));
    else
      return false;
  };
  auto baseline1 = "foo"s;
  CHECK_EQ(baseline1, last_prometheus_output());
  foo_bar->inc();
  foo_hist->observe(64);
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  auto baseline2 = "foo"s;
  CHECK_EQ(baseline2, last_prometheus_output());
}

/*
TEST(the exporter allows changing the interval at runtime) {
  inject((atom::put, timespan), to(aut).with(atom::put_v, timespan{3s}));
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  sched.advance_time(2s);
  disallow((caf::tick_atom), to(aut));
  sched.advance_time(1s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
}

TEST(the exporter allows changing the topic at runtime) {
  auto last_topic = [this] { return get_topic(core_state().last_message); };
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  CHECK_EQUAL(last_topic(), "all/them/metrics"sv);
  inject((atom::put, topic), to(aut).with(atom::put_v, "foo/bar"_t));
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  CHECK_EQUAL(last_topic(), "foo/bar"sv);
}

TEST(the exporter allows changing the ID at runtime) {
  auto has_id = [this](const data& x, const std::string& what) {
    using namespace std::literals;
    if (auto row = get_if<vector>(x); row && row->size() == 2)
      return row->at(0) == what;
    else
      return false;
  };
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(!rows().empty()))
    CHECK(has_id(row(0), "exporter-1"));
  inject((atom::put, std::string), to(aut).with(atom::put_v, "foobar"));
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(!rows().empty()))
    CHECK(has_id(row(0), "foobar"));
}

TEST(the exporter allows changing the prefix selection at runtime) {
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  CHECK_EQUAL(rows().size(), 3u);
  inject((atom::put, filter_type),
         to(aut).with(atom::put_v, filter_type{"foo", "bar"}));
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  CHECK_EQUAL(rows().size(), 4u);
}
*/

FIXTURE_SCOPE_END()
