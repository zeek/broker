#include "broker/detail/prefix_matcher.hh"

namespace broker::detail {

bool prefix_matcher::operator()(const filter_type& filter,
                                const topic& t) const noexcept {
  for (auto& prefix : filter)
    if (prefix.prefix_of(t))
      return true;
  return false;
}

} // namespace broker::detail
