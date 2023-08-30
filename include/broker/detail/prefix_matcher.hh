#pragma once

#include <string_view>
#include <utility>
#include <vector>

#include "broker/cow_tuple.hh"
#include "broker/topic.hh"

namespace broker::detail {

struct prefix_matcher {
  using filter_type = std::vector<topic>;

  bool operator()(const filter_type& filter, std::string_view t) const noexcept;

  bool operator()(const filter_type& filter, const topic& t) const noexcept {
    return (*this)(filter, std::string_view{t.string()});
  }

  template <class T>
  bool operator()(const filter_type& filter, const T& x) const noexcept {
    return (*this)(filter, get_topic(x));
  }
};

} // namespace broker::detail
