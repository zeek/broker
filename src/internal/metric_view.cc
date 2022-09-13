#include "broker/internal/metric_view.hh"

#include <algorithm>

namespace broker::internal {

metric_view::metric_view(const vector* row)
  : type_(static_cast<caf::telemetry::metric_type>(0)) {
  using std::string;
  auto at = [row](field x) -> const auto& {
    return (*row)[index(x)];
  };
  auto row_ok = row != nullptr && row->size() == row_size
                && is<string>(at(field::prefix)) && is<string>(at(field::name))
                && is<string>(at(field::type)) && is<string>(at(field::unit))
                && is<string>(at(field::helptext))
                && is<bool>(at(field::is_sum))
                && has_properly_typed_labels(*row);
  if (row_ok && get_type(*row, type_)) {
    row_ = row;
  } else {
    row_ = nullptr;
  }
}

metric_view::metric_view(const vector& row) : metric_view(&row) {
  // nop
}

metric_view::metric_view(const data& row_data)
  : metric_view(get_if<vector>(row_data)) {
  // nop
}

bool metric_view::has_properly_typed_labels(const vector& row) noexcept {
  if (auto tbl = get_if<table>(row[index(field::labels)])) {
    auto is_string_pair = [](const auto& kvp) {
      return is<std::string>(kvp.first) && is<std::string>(kvp.second);
    };
    return std::all_of(tbl->begin(), tbl->end(), is_string_pair);
  } else {
    return false;
  }
}

namespace {

template <class First, class Second = First>
struct pair_predicate {
  bool operator()(const data& x) const noexcept {
    if (auto vec = get_if<vector>(x); vec && vec->size() == 2) {
      return is<First>((*vec)[0]) && is<Second>((*vec)[1]);
    } else {
      return false;
    }
  }
};

} // namespace

bool metric_view::get_type(const vector& row,
                           caf::telemetry::metric_type& var) noexcept {
  using caf::telemetry::metric_type;
  auto good = [&var](metric_type value) {
    var = value;
    return true;
  };
  static constexpr bool bad = false;
  const auto* t = broker::get_if<std::string>(row[index(field::type)]);
  if (t == nullptr) {
    return bad;
  }
  const auto& v = row[index(field::value)];
  if (*t == "counter") {
    if (is<integer>(v)) {
      return good(metric_type::int_counter);
    } else if (is<real>(v)) {
      return good(metric_type::dbl_counter);
    }
  } else if (*t == "gauge") {
    if (is<integer>(v)) {
      return good(metric_type::int_gauge);
    } else if (is<real>(v)) {
      return good(metric_type::dbl_gauge);
    }
  } else if (*t == "histogram") {
    if (auto vals = get_if<vector>(v); vals && vals->size() >= 2) {
      if (std::all_of(vals->begin(), vals->end() - 1, pair_predicate<integer>{})
          && is<integer>(vals->back())) {
        return good(metric_type::int_histogram);
      } else if (std::all_of(vals->begin(), vals->end() - 1,
                             pair_predicate<real, integer>{})
                 && is<real>(vals->back())) {
        return good(metric_type::dbl_histogram);
      }
    }
  }
  return bad;
}

} // namespace broker::internal
