#define SUITE metric_exporter

#include "broker/internal/metric_exporter.hh"

#include "test.hh"

#include "broker/internal/metric_view.hh"

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
};

using dummy_core_actor = caf::stateful_actor<dummy_core_state>;

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
  if (auto mv = internal::metric_view{rhs})
    return lhs.prefix == mv.prefix() && lhs.name == mv.name()
           && lhs.type == mv.type_str() && lhs.unit == mv.unit()
           && lhs.helptext == mv.helptext() && lhs.is_sum == mv.is_sum()
           && lhs.labels == mv.labels() && lhs.value == mv.value();
  else
    return false;
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
    core = sys.spawn<dummy_core_actor>();
    aut = sys.spawn<internal::metric_exporter_actor>(core, std::move(selection),
                                                     caf::timespan{2s},
                                                     "all/them/metrics",
                                                     "exporter-1");
    sched.run();
  }

  ~fixture() {
    anon_send_exit(aut, caf::exit_reason::user_shutdown);
  }

  auto& state() {
    return deref<internal::metric_exporter_actor>(aut).state;
  }

  auto& core_state() {
    return deref<dummy_core_actor>(core).state;
  }

  const auto& rows() {
    return state().impl.rows();
  }

  const auto& row(size_t index) {
    return rows().at(index);
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
  if (CHECK(rows().size() == 3)) {
    CHECK(is_meta_data(row(0)));
    CHECK_EQUAL(row(1), (metric_row{"foo", "bar", "gauge", "1", "FooBar!",
                                    false, table{}, data{1}}));
    CHECK_EQUAL(row(2), (metric_row{"foo", "hist", "histogram", "seconds",
                                    "FooHist!", false, table{{"sys", "broker"}},
                                    foo_hist_buckets(1, 1, 0, 0, 16)}));
  }
  foo_bar->inc();
  foo_hist->observe(64);
  sched.advance_time(2s);
  expect((caf::tick_atom), to(aut));
  expect((atom::publish, data_message), from(aut).to(core));
  if (CHECK(rows().size() == 3)) {
    CHECK(is_meta_data(row(0)));
    CHECK_EQUAL(row(1), (metric_row{"foo", "bar", "gauge", "1", "FooBar!",
                                    false, table{}, data{2}}));
    CHECK_EQUAL(row(2), (metric_row{"foo", "hist", "histogram", "seconds",
                                    "FooHist!", false, table{{"sys", "broker"}},
                                    foo_hist_buckets(1, 1, 0, 1, 80)}));
  }
}

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

FIXTURE_SCOPE_END()
