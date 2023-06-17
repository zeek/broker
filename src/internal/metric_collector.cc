#include <memory>

#include "broker/internal/metric_collector.hh"

#include "broker/internal/logger.hh"

namespace ct = caf::telemetry;

namespace broker::internal {

namespace {

template <class T>
class remote_counter : public metric_collector::remote_metric {
public:
  using super = metric_collector::remote_metric;

  static constexpr auto type_tag = std::is_same_v<T, integer>
                                     ? ct::metric_type::int_counter
                                     : ct::metric_type::dbl_counter;

  using super::super;

  void update(metric_view mv) override {
    if (mv.type() == type_tag) {
      value_ = get<T>(mv.value());
    } else {
      BROKER_ERROR("conflicting remote metric update received!");
    }
  }

  void append_to(ct::collector::prometheus& f) override {
    f.append_counter(this->parent_, this, value_);
  }

private:
  T value_ = 0;
};

template <class T>
class remote_gauge : public metric_collector::remote_metric {
public:
  using super = metric_collector::remote_metric;

  static constexpr auto type_tag = std::is_same_v<T, integer>
                                     ? ct::metric_type::int_gauge
                                     : ct::metric_type::dbl_gauge;

  using super::super;

  void update(metric_view mv) override {
    if (mv.type() == type_tag) {
      value_ = get<T>(mv.value());
    } else {
      BROKER_ERROR("conflicting remote metric update received!");
    }
  }

  void append_to(ct::collector::prometheus& f) override {
    f.append_gauge(this->parent_, this, value_);
  }

private:
  T value_ = 0;
};

template <class T>
class remote_histogram : public metric_collector::remote_metric {
public:
  using super = metric_collector::remote_metric;

  static constexpr auto type_tag = std::is_same_v<T, integer>
                                     ? ct::metric_type::int_histogram
                                     : ct::metric_type::dbl_histogram;

  using super::super;

  using native_bucket = typename ct::histogram<T>::bucket_type;

  void update(metric_view mv) override {
    if (mv.type() == type_tag) {
      auto& vals = get<vector>(mv.value());
      BROKER_ASSERT(vals.size() >= 2);
      buckets_.clear();
      std::for_each(vals.begin(), vals.end() - 1, [this](const auto& kvp_data) {
        auto& kvp = get<vector>(kvp_data);
        buckets_.emplace_back(get<T>(kvp[0]), get<integer>(kvp[1]));
      });
      sum_ = get<T>(vals.back());
    } else {
      BROKER_ERROR("conflicting remote metric update received!");
    }
  }

  void append_to(ct::collector::prometheus& f) override {
    // The CAF collector expects histogram buckets, which have a `counter`
    // member. Since we can't assign values to counters (only increase them), we
    // work around this limitations by simply re-creating the "native" buckets
    // each time.
    std::unique_ptr<native_bucket[]> buf{new native_bucket[buckets_.size()]};
    for (size_t index = 0; index < buckets_.size(); ++index) {
      auto [upper_bound, count] = buckets_[index];
      buf[index].upper_bound = upper_bound;
      if (count > 0)
        buf[index].count.inc(count);
    }
    auto buf_span = caf::make_span(buf.get(), buckets_.size());
    f.append_histogram(this->parent_, this, buf_span, sum_);
  }

private:
  std::vector<std::pair<T, int64_t>> buckets_;
  T sum_ = 0;
};

} // namespace

// -- member types -------------------------------------------------------------

metric_collector::remote_metric::remote_metric(
  label_list labels, const caf::telemetry::metric_family* parent)
  : super(std::move(labels)), parent_(parent) {
  // nop
}

metric_collector::remote_metric::~remote_metric() {
  // nop
}

// --- constructors and destructors --------------------------------------------

metric_collector::metric_collector() {
  // nop
}

metric_collector::~metric_collector() {
  // nop
}

// -- data management ----------------------------------------------------------

size_t metric_collector::insert_or_update(const data& content) {
  if (auto vec = get_if<vector>(content))
    return insert_or_update(*vec);
  else
    return 0;
}

size_t metric_collector::insert_or_update(const vector& vec) {
  auto has_meta_data = [](const data& x) {
    if (auto meta = get_if<vector>(x); meta && meta->size() == 2)
      return is<std::string>((*meta)[0]) && is<timestamp>((*meta)[1]);
    else
      return false;
  };
  if (vec.size() >= 2 && has_meta_data(vec[0])) {
    auto& meta = get<vector>(vec[0]);
    auto& endpoint_name = get<std::string>(meta[0]);
    auto& ts = get<timestamp>(meta[1]);
    return insert_or_update(endpoint_name, ts,
                            caf::make_span(vec.data() + 1, vec.size() - 1));
  } else {
    return 0;
  }
}

size_t metric_collector::insert_or_update(const std::string& endpoint_name,
                                          timestamp ts,
                                          caf::span<const data> rows) {
  using caf::telemetry::metric_type;
  auto res = size_t{0};
  if (advance_time(endpoint_name, ts))
    for (const auto& row_data : rows)
      if (auto mv = metric_view{row_data})
        if (auto ptr = instance(endpoint_name, mv)) {
          ptr->update(mv);
          ++res;
        }
  return res;
}

std::string_view metric_collector::prometheus_text() {
  if (generator_.begin_scrape()) {
    for (auto& [prefix, names] : prefixes_)
      for (auto& [name, scope] : names)
        for (auto& instance : scope.instances)
          instance->append_to(generator_);
    generator_.end_scrape();
  }
  auto res = generator_.str();
  return {res.data(), res.size()};
}

void metric_collector::clear() {
  label_names_.clear();
  prefixes_.clear();
  last_seen_.clear();
  generator_.reset();
}

// -- time management ----------------------------------------------------------

bool metric_collector::advance_time(const std::string& endpoint_name,
                                    timestamp current_time) {
  auto [i, added] = last_seen_.emplace(endpoint_name, current_time);
  if (added) {
    return true;
  } else if (current_time > i->second) {
    i->second = current_time;
    return true;
  } else {
    return false;
  }
}

// -- lookups ----------------------------------------------------------------

void metric_collector::labels_for(const std::string& endpoint_name,
                                  metric_view row,
                                  metric_collector::label_view_list& result) {
  using namespace std::literals;
  result.clear();
  result.reserve(row.labels().size() + 1);
  // Get insertion point for "endpoint" to keep the vector sorted. Then copy all
  // labels to their final destination.
  auto pos = row.labels().lower_bound(ep_key_);
  for (auto i = row.labels().begin(); i != pos; ++i)
    result.emplace_back(get<std::string>(i->first),
                        get<std::string>(i->second));
  result.emplace_back("endpoint"sv, endpoint_name);
  for (auto i = pos; i != row.labels().end(); ++i)
    result.emplace_back(get<std::string>(i->first),
                        get<std::string>(i->second));
}

metric_collector::string_span
metric_collector::label_names_for(metric_view row) {
  label_names_.clear();
  label_names_.reserve(row.labels().size() + 1);
  label_names_.emplace_back("endpoint");
  for (const auto& kvp : row.labels())
    label_names_.emplace_back(get<std::string>(kvp.first));
  std::sort(label_names_.begin(), label_names_.end());
  return label_names_;
}

namespace {

auto owned(std::string_view x) {
  return std::string{x};
}

auto owned(metric_collector::string_span xs) {
  std::vector<std::string> result;
  if (!xs.empty()) {
    result.reserve(xs.size());
    for (auto& x : xs)
      result.emplace_back(owned(x));
  }
  return result;
}

auto owned(const metric_collector::label_view_list& xs) {
  metric_collector::label_list result;
  if (!xs.empty()) {
    result.reserve(xs.size());
    for (auto& x : xs)
      result.emplace_back(x);
  }
  return result;
}

constexpr metric_collector::labels_equal labels_equal_v{};

} // namespace

metric_collector::remote_metric*
metric_collector::instance(const std::string& endpoint_name, metric_view mv) {
  auto& names = prefixes_[mv.prefix()];
  auto& scope = names[mv.name()];
  if (scope.family == nullptr) {
    auto ptr = new ct::metric_family(mv.type(), mv.prefix(), mv.name(),
                                     owned(label_names_for(mv)), mv.helptext(),
                                     mv.unit(), mv.is_sum());
    scope.family.reset(ptr);
  }
  auto* fptr = scope.family.get();
  labels_for(endpoint_name, mv, labels_);
  auto i = scope.instances.lower_bound(labels_);
  if (i != scope.instances.end() && labels_equal_v(*i, labels_))
    return i->get();
  auto add = [&](auto* ptr) {
    auto j = scope.instances.insert(i, instance_ptr{ptr});
    BROKER_ASSERT(j->get() == ptr);
    return ptr;
  };
  using ct::metric_type;
  switch (mv.type()) {
    case metric_type::int_counter:
      return add(new remote_counter<integer>(owned(labels_), fptr));
    case metric_type::dbl_counter:
      return add(new remote_counter<real>(owned(labels_), fptr));
    case metric_type::int_gauge:
      return add(new remote_gauge<integer>(owned(labels_), fptr));
    case metric_type::dbl_gauge:
      return add(new remote_gauge<real>(owned(labels_), fptr));
    case metric_type::int_histogram:
      return add(new remote_histogram<integer>(owned(labels_), fptr));
    case metric_type::dbl_histogram:
      return add(new remote_histogram<real>(owned(labels_), fptr));
    default:
      return nullptr;
  }
}

} // namespace broker::internal
