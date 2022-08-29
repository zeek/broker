#include "broker/detail/prefix_matcher.hh"

#include <algorithm>

namespace broker::detail {

bool prefix_matcher::operator()(const filter_type& filter,
                                const topic& what) const noexcept {
  auto pred = [&](const topic& prefix) { return prefix.prefix_of(what); };
  return std::any_of(filter.begin(), filter.end(), pred);
}

} // namespace broker::detail
